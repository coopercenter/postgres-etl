if(!("jsonlite" %in% installed.packages()))  install.packages("jsonlite")
if(!("eia" %in% installed.packages()))       install.packages("eia")
if(!("RPostgres" %in% installed.packages())) install.packages("RPostgres")
library(here)
library(tidyverse)
library(stringr)
library(jsonlite)
library(data.table)
library(eia)
library('RPostgreSQL')

source(here("api_data_code","my_eia_api_key.R"))

#Generating datasets for annual state generation ----

get_EIA_series <- function(eiaKey,series_id) {
        require(jsonlite)
        require(data.table)
        # This function retrieves one EIA time-series with metadata
        # The function returns a list of parts of the series:
        #     seriesID,name,units,frequency,data (as data table)
        
        # eiaKey is your EIA API key
        eiaBase = paste0("http://api.eia.gov/series/?api_key=",eiaKey,"&series_id=") 
        
        vv = paste0(eiaBase,series_id)
        temp = readLines(vv, warn = "F")
        rd <- fromJSON(temp)
        print(paste0("Retrieving: ",rd$series$series_id))
        print(paste0(rd$series$name))
        
        # Now take the 'data' element from the list and make a data frame
        rd2 = data.frame(rd$series$data,stringsAsFactors = F)
        rd2 = data.table(rd2)
        
        setnames(rd2,1,"year"); setnames(rd2,2,'value')
        rd2[,value:=as.numeric(value)]
        rd2[,year:=as.numeric(year)]
        returnList = list(
                series_id = rd$series$series_id,
                name = rd$series$name,
                units = rd$series$units,
                frequency = rd$series$f,
                data = rd2
        )
        return(rd2) 
}

states = c("AK","AL","AR","AZ","CA",
           "CO","CT","DE","FL","GA",
           "HI","IA","ID","IL","IN",
           "KS","KY","LA","MA","MD",
           "ME","MI","MN","MO","MS",
           "MT","NC","ND","NE","NH",
           "NJ","NM","NV","NY","OH",
           "OK","OR","PA","RI","SC",
           "SD","TN","TX","UT","VA",
           "VT","WA","WI","WV","WY")

gen_by_state_annual<-NULL

for(state in states){
        #series_list contains the EIA series id for each fuel type that was specificied on Basecamp as well as for total generation
        #each series id is then matched with its fuel type in words
        series_list = data.frame(
                series_id=c(paste0("ELEC.GEN.COW-",state,"-99.A"),
                            paste0("ELEC.GEN.PEL-",state,"-99.A"),
                            paste0("ELEC.GEN.NG-",state,"-99.A"),
                            paste0("ELEC.GEN.NUC-",state,"-99.A"),
                            paste0("ELEC.GEN.WND-",state,"-99.A"),
                            paste0("ELEC.GEN.SUN-",state,"-99.A"),
                            paste0("ELEC.GEN.DPV-",state,"-99.A"),
                            paste0("ELEC.GEN.HYC-",state,"-99.A"),
                            paste0("ELEC.GEN.WWW-",state,"-99.A"),
                            paste0("ELEC.GEN.WAS-",state,"-99.A"),
                            paste0("ELEC.GEN.ALL-",state,"-99.A")),
                fuel=c("coal",
                       "oil",
                       "gas",
                       "nuclear",
                       "wind",
                       "utility_solar", #note utility_solar currently includes all utility-scale solar but this can be changed to just utility-scale PV if needed
                       "distributed_solar",
                       "hydropower",
                       "wood",
                       "other_biomass",
                       "total")
        )
        
        series_list$fuel<-as.character(series_list$fuel)
        
        # Building data table `all_generation` by merging data on generation by several fuel types
        all_generation <- NULL
        for(row in 1:nrow(series_list)){
                table <- series_list[row,"series_id"]
                fuel <- series_list[row,"fuel"]
                
                possibleError <- tryCatch(eia_series(table,key=eiaKey),error=function(e) e) #not every state has data for each fuel type, which would result in an error
                if(inherits(possibleError, "error")) next #so if data for a particular fuel type is missing it will instead move to the next fuel listed in series_id
                
                dt <- get_EIA_series(eiaKey,table)
                setnames(dt,old="value",new=fuel)
                
                if (is.null(all_generation))
                {all_generation <- dt}
                else
                {all_generation <-  merge(all_generation, dt[], by ="year", all=TRUE)}
        }
        
        
        #the resulting "all_generation" table is a data.table showing monthly generation in total and by fuel type for each state
        #this combined table for each state could perhaps be written to the database rather than writing each generation by fuel type for each state to the database?
        all_generation[is.na(all_generation)]=0 #there are a few missing values in the data where it makes sense that they would have a value of 0, so changing these to 0 for graphing purposes later on
        col_count=as.numeric(ncol(all_generation))
        #there are many other fuel type categories listed on EIA than the ones specifically retrieved from the series_list
        #account for these other miscellaneous fuel types by combining them into "other" category
        #other is estimated by subracting the sum of generation for all reported fuel types from total generation
        #the sum of generation for all the fuel types is obtained by summing up the second column through the second to last column
        #the first column is year, the last column is total, but different states will have different amounts of columns depending on which fuel types EIA provides data for
        all_generation[,other:=total-rowSums(.SD),.SDcols=2:(col_count-1)] 
        gen_by_state_annual[[state]]<-all_generation
        
        rm(all_generation, dt)
}


#Generating datasets for annual state generation ----
formatDate <- function(unformattedDate){ #unformattedDate in numeric format yyyymm. ex: 202012. 
        month <- substr(unformattedDate, 5,6)
        year <- substr(unformattedDate, 1,4)
        formattedDate <- paste0(year, "_", month)
}

get_EIA_series_2 <- function(eiaKey,series_id) {
        require(jsonlite)
        require(data.table)
        # This function retrieves one EIA time-series with metadata
        # The function returns a list of parts of the series:
        #     seriesID,name,units,frequency,data (as data table)

        # eiaKey is your EIA API key
        eiaBase = paste0("http://api.eia.gov/series/?api_key=",eiaKey,"&series_id=")

        vv = paste0(eiaBase,series_id)
        temp = readLines(vv, warn = "F")
        rd <- fromJSON(temp)
        print(paste0("Retrieving: ",rd$series$series_id))
        print(paste0(rd$series$name))

        # Now take the 'data' element from the list and make a data frame
        rd2 = data.frame(rd$series$data,stringsAsFactors = F)
        rd2 = data.table(rd2)

        setnames(rd2,1,"Month"); setnames(rd2,2,'value') #Units: Thousand Megawatt Hours
        rd2[,value:=as.numeric(value)]
        rd2[,Month:=as.character(Month)]
        rd2$Month <- sapply(rd2$Month, formatDate) 
        
        returnList = list(
                series_id = rd$series$series_id,
                name = rd$series$name,
                units = rd$series$units,
                frequency = rd$series$f,
                data = rd2
        )
        return(rd2)
}

gen_by_state_monthly<-NULL

for(state in states){
        #series_list contains the EIA series id for each fuel type that was specificied on Basecamp as well as for total generation
        #each series id is then matched with its fuel type in words
        series_list = data.frame(
                series_id=c(paste0("ELEC.GEN.COW-",state,"-99.M"),
                            paste0("ELEC.GEN.PEL-",state,"-99.M"),
                            paste0("ELEC.GEN.NG-",state,"-99.M"),
                            paste0("ELEC.GEN.NUC-",state,"-99.M"),
                            paste0("ELEC.GEN.WND-",state,"-99.M"),
                            paste0("ELEC.GEN.SUN-",state,"-99.M"),
                            paste0("ELEC.GEN.DPV-",state,"-99.M"),
                            paste0("ELEC.GEN.HYC-",state,"-99.M"),
                            paste0("ELEC.GEN.WWW-",state,"-99.M"),
                            paste0("ELEC.GEN.WAS-",state,"-99.M"),
                            paste0("ELEC.GEN.ALL-",state,"-99.M")),
                fuel=c("coal",
                       "oil",
                       "gas",
                       "nuclear",
                       "wind",
                       "utility_solar", #note utility_solar currently includes all utility-scale solar but this can be changed to just utility-scale PV if needed
                       "distributed_solar",
                       "hydropower",
                       "wood",
                       "other_biomass",
                       "total"))
        
        series_list$fuel<-as.character(series_list$fuel)
        
        # Building data table `all_generation` by merging data on generation by several fuel types
        all_generation <- NULL
        for(row in 1:nrow(series_list)){
                table <- series_list[row,"series_id"]
                fuel <- series_list[row,"fuel"]
                
                possibleError <- tryCatch(eia_series(table,key=eiaKey),error=function(e) e) #not every state has data for each fuel type, which would result in an error
                if(inherits(possibleError, "error")) next #so if data for a particular fuel type is missing it will instead move to the next fuel listed in series_id
                
                dt <- get_EIA_series_2(eiaKey,table)
                setnames(dt,old="value",new=fuel)
                
                if (is.null(all_generation))
                {all_generation <- dt}
                else
                {all_generation <-  merge(all_generation, dt[], by ="Month", all=TRUE)}
        }
        
        
        #the resulting "all_generation" table is a data.table showing monthly generation in total and by fuel type for each state
        #this combined table for each state could perhaps be written to the database rather than writing each generation by fuel type for each state to the database?
        all_generation[is.na(all_generation)]=0 #there are a few missing values in the data where it makes sense that they would have a value of 0, so changing these to 0 for graphing purposes later on
        col_count=as.numeric(ncol(all_generation))
        #there are many other fuel type categories listed on EIA than the ones specifically retrieved from the series_list
        #account for these other miscellaneous fuel types by combining them into "other" category
        #other is estimated by subracting the sum of generation for all reported fuel types from total generation
        #the sum of generation for all the fuel types is obtained by summing up the second column through the second to last column
        #the first column is year, the last column is total, but different states will have different amounts of columns depending on which fuel types EIA provides data for
        all_generation[,other:=total-rowSums(.SD),.SDcols=2:(col_count-1)] 
        
        dt_name <- str_to_lower(paste("eia_elec_gen",state,"m",sep="_"))
        assign(dt_name,all_generation)
        gen_by_state_monthly[[state]]<-all_generation
        
        rm(all_generation, dt)
}


#Uploading to database ----
# Connection to the database
# "my_postgres_credentials.R" contains the log-in informations of RAs
source(here("my_postgres_credentials.R"))
db_driver <- dbDriver("PostgreSQL")
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

# check the connection
# if this returns true, it means that you are connected to the database now
dbExistsTable(db, "metadata")

dt_name_annual<-NULL
dt_name_monthly<-NULL

for (i in 1:50){
        dt_name_annual[i]<-str_to_lower(paste("eia_elec_gen", str_to_lower(states[i]),"a",sep="_"))
        dt_name_monthly[i]<-str_to_lower(paste("eia_elec_gen", str_to_lower(states[i]),"m",sep="_"))
}

#Upload individual states annual and monthly generation dataframes to database.
for (i in 1:50){
        dbWriteTable(db, dt_name_annual[i], value = gen_by_state_annual[[states[i]]], append = FALSE, overwrite = TRUE, row.names = FALSE)
        dbWriteTable(db, dt_name_monthly[i], value = gen_by_state_monthly[[states[i]]], append = FALSE, overwrite = TRUE, row.names = FALSE)
}
#Upload total annual and monthly generation dataframes to database.
dbWriteTable(db, "gen_by_state_annually_all_states", value = gen_by_state_annual, append = FALSE, overwrite = TRUE, row.names = FALSE)
dbWriteTable(db, "gen_by_state_monthly_all_states", value = gen_by_state_monthly, append = FALSE, overwrite = TRUE, row.names = FALSE)
