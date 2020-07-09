# Creating the dataframe for metadata
metadata<-data.frame(matrix(ncol = 19, nrow = 0))

# Specify the column names
colnames(metadata) <- c('db_table_name','short_series_name','full_series_name',
                        'column2variable_name_map','units','frequency',
                        'data_source_brief_name','data_source_full_name','url',
                        'api','series_id','json','notes', 'data_type','data_context','corresponding_data','R_script',
                        'latest_data_update','last_db_refresh')

#--------------------------------------------------------------------------------------
# Sample Code used to manually write the metadata

# Connection to the database
# "my_postgres_credentials.R" contains the log-in informations of RAs
library(here)
library('RPostgreSQL')
library(tidyverse)
source(here("my_postgres_credentials.R"))

db_driver <- dbDriver("PostgreSQL")

db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)

rm(ra_pwd)

# check the connection
# if this returns true, it means that you are connected to the database now
dbExistsTable(db, "metadata")

# get the cleaned dataset from the database
## fuel_cleaned should be replaced by the name of your dataset in the database
## (coordinate with Jackson)

# put the column names into a list
fuel_cols<-list(colnames(fuel))

# put the unit of each column into a list
fuel_units <-list(c('Year','dollars_per_million_Btu','Btu_per_pound','percent',
                    'dollars_per_million_Btu','Btu_per_pound','percent',
                    'dollars_per_million_Btu','Btu_per_cubic_foot'))

# Construct a data frame with only one row, if you have more than one dataset,
# construct r2, r3,... if needed
r1<- data.frame(db_table_name = "fuel", short_series_name = "fuel price and quality",
                full_series_name = 'Electric power delivered fuel prices and quality for coal, petroleum, Natural gas, 1990 through 2018',
                column2variable_name_map=I(fuel_cols),units=I(fuel_units),frequency='Y',
                data_source='EIA',data_source_full_name='U.S. Energy Information Administration',
                url='https://www.eia.gov/electricity/state/virginia/state_tables.php',api=NA,
                series_id=NA,json=NA,notes=NA)

library(plyr)
# if you have more than one dataset,rbind the rows first,and then bind it to the metadata
# Example
# r1 <- rbind(r1,r2)
metadata<-rbind(metadata,r1)

# WARNING
# Do not run dbWriteTable before you check with Christina and Chloe
# This will overwrite the existing metadata table in the db
dbWriteTable(con, 'metadata', value = metadata, append = FALSE, overwrite = TRUE, row.names = FALSE)

#------------------------------------------------------------------------------------
#sample code used to get the metadata for EIA datasets
library(eia)
library(here)
library(RPostgreSQL)
library(tidyverse)
library(units)
library(mgsub)


## read in my eia api key
source(here("api_data_code","my_eia_api_key.R"))
eia_set_key(eiaKey)

## create a list of series ids
series_id_vec <- read_file(here("api_data_code","series_ids.txt"))
series_id_list <- unlist(strsplit(series_id_vec,'\r\n'))

## create a empty list to store the metadata tables
all_eia_meta <- vector("list", length(series_id_list))

## loops through the series id list and store the corresponding metadata table
for (i in 1:length(series_id_list)){
  all_eia_meta[[i]] <- eia_series(series_id_list[[i]],n=10)
}


# Fit the metadata tables into the standard structure
## Create an empty list to store the reformatted metadata tables
fit_meta <- vector("list", length(series_id_list))

## create lists to store other info that is not in the metadata extracted using code

get_name <- function(series_id) {
  db_table_name <- str_to_lower(paste("eia", str_replace_all(series_id, "[.-]", "_"), sep="_"))
  return(db_table_name)
}

# apply the function to the list of series id to get the names for the data tables
eia_data_names <- lapply(series_id_list,get_name)


api_link <- vector("list", length(series_id_list))
for (i in 1:length(series_id_list)){
  api_link[[i]] <- paste("http://api.eia.gov/series/?api_key=",eiaKey,"&series_id=",series_id_list[[i]],sep='')
}


eia_short_names<-list('Total co2 emission',
                      'Total energy consumption',
                      'Total energy production',
                      'Annual retail sales',
                      'Monthly retail sales',
                      'Total monthly generation',
                      'Total annual generation',
                      'Total monthly solar generation',
                      'Total monthly conventional hydroelectric generation',
                      'Total monthly average retail price',
                      'Commercial monthly average retail price',
                      'Residential monthly average retail price',
                      'Industrial monthly average retail price',
                      'Transportation monthly average retial price',
                      'Total monthly number of customer accounts',
                      'Total monthly nuclear generation',
                      'Total monthly coal generation',
                      'Total monthly natural gas generation',
                      'Total monthly other renewables generation',
                      'Total monthly petroleum liquids generation',
                      'Total monthly utility scale solar generation',
                      'Total monthly small scale solar generation',
                      'Total monthly utility scale solar generation non cogen',
                      'Total monthly coal electric power generation',
                      'Total monthly utility scale photovoltaic generation',
                      'Total monthly wood and wood derived fuels generation',
                      'Total monthly other biomass generation',
                      'Total monthly hydro electric pumped sotrage',
                      'Total monthly other generation',
                      'Annual coal generation',
                      'Annual petroleum generation',
                      'Annual natural gas generation',
                      'Annual nuclear generation',
                      'Annual utility scale solar generation',
                      'Annual small scale solar generation',
                      'Annual hydroelectric generation',
                      'Annual wood derived fuel generation',
                      'Annual other biomass generation',
                      'Total energy consumed by the residential sector',
                      'Total energy consumed by the commercial sector',
                      'Total energy consumed by the industrial sector',
                      'Total energy consumed by the transportation sector',
                      'Total carbon dioxide emissions, all sectors',
                      'Total coal carbon dioxide emissions, all sectors',
                      'Total natural gas carbon dioxide emissions, all sectors',
                      'Total petroleum carbon dioxide emissions, all sectors',
                      'Annual retail sales for residential sector',
                      'Annual retail sales for commercial sector',
                      'Annual retail sales for industrial sector',
                      'Annual retail sales for transportation sector',
                      'Annual retail sales for other sectors',
                      'Monthly retail sales for residential sector',
                      'Monthly retail sales for commercial sector',
                      'Monthly retail sales for industrial sector',
                      'Monthly retail sales for transportation sector',
                      'Monthly retail sales for other sectors',
                      'Annual hydro-electric pumped storage generation',
                      'Annual other renewables generation',
                      'Annual other generation',
                      'Annual utility-scale photovoltaic generation',
                      )



col2var<-vector("list", length(series_id_list))
cols<-vector("list", length(series_id_list))

for (i in 1:length(series_id_list)){
  col2var[[i]]<-all_eia_meta[[i]][['data']][[1]] %>% 
    dplyr::summarise_all(class) %>% 
    tidyr::gather(variable, class)
  cols[[i]]<-c(col2var[[i]]$variable)
}

data_update <- vector('list', length(series_id_list))
for (i in 1:length(series_id_list)) {
  data_update[[i]] <- all_eia_meta[[i]]$updated
  data_update[[i]] <- gsub("T", " ", data_update[[i]])
  data_update[[i]] <- gsub("-0400", "", data_update[[i]])
  data_update[[i]] <- strptime(data_update[[i]], format="%Y-%m-%d %H:%M:%S")
}


replace_unit <- function(string){
  unit <- mgsub(string,
    c('kilowatthour','kilowatthours','megawatthour','megawatthours','gigawatthour','gigawatthours',
      'kilowatt','kilowatts','megawatt','megawatts','gigawatt','gigawatts', 'million metric tons'),
    c('kWh','kWh','MWh','MWh','GWh','GWh',
      'kW','kW','MW','MW','GW','GW','mmt'))
  return(unit)
}

eia_units <- vector('list', length(series_id_list))
for (i in 1:length(series_id_list)) {
  eia_unit <- all_eia_meta[[i]]$units
  eia_units[[i]] <- replace_unit(eia_unit)
}


# Create metadata dataframes for each dataset and fill in the info
for (i in 1:length(series_id_list)){
  fit_meta[[i]]<-data.frame(db_table_name = eia_data_names[[i]],
                            short_series_name= eia_short_names[[i]],
                            full_series_name = all_eia_meta[[i]]$name,
                            column2variable_name_map=I(list(cols[[i]])),units=eia_units[[i]],frequency=all_eia_meta[[i]]$f,
                            data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                            url=NA, api=api_link[[i]],
                            series_id=all_eia_meta[[i]]$series_id,json=NA,notes=NA, data_type='time-series',
                            data_context='historical', corresponding_data=NA,
                            R_script="fetch_from_eia_api.R", latest_data_update=data_update[[i]],
                            last_db_refresh= lubridate::with_tz(Sys.time(), "UTC"))
  metadata<-rbind(metadata,fit_meta[[i]])
}

db_driver = dbDriver("PostgreSQL")
source(here("my_postgres_credentials.R"))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)


dbWriteTable(db, 'metadata2', value = metadata, append = FALSE, overwrite = TRUE, row.names = FALSE)
#dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)
#dbWriteTable(db, 'test', value = metadata, append = FALSE, overwrite = TRUE, row.names = FALSE)

# setting column constraints
set_pk <- dbSendQuery(db, "ALTER TABLE metadata2 ADD PRIMARY KEY (db_table_name);")
set_freq <- dbSendQuery(db, "ALTER TABLE metadata2 ALTER COLUMN frequency TYPE char(1)")
set_url <- dbSendQuery(db, "ALTER TABLE metadata2 ALTER COLUMN url TYPE VARCHAR(1000)")
set_corr_data <- dbSendQuery(db, 'ALTER TABLE metadata2 ALTER COLUMN corresponding_data TYPE VARCHAR(1000)')
set_notes <- dbSendQuery(db, "ALTER TABLE metadata2 ALTER COLUMN notes TYPE VARCHAR(1000)")
#set_corr_data <- dbSendQuery(db, "ALTER TABLE metadata ALTER COLUMN corresponding_data TYPE varchar(500)")
#set_data_update <- dbSendQuery(db, "ALTER TABLE metadata ALTER COLUMN latest_data_update TYPE TIMESTAMP WITHOUT TIME ZONE")
#set_db_refresh <- dbSendQuery(db, "ALTER TABLE metadata ALTER COLUMN last_db_refresh TYPE TIMESTAMP WITH TIME ZONE")


# Check if column constraints are true
data_types <- dbGetQuery(db, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'metadata2';")

## Close connection
dbDisconnect(db)
