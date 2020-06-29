library(here)
library(eia)
library('RPostgreSQL')
library(tidyverse)
source(here("api_data_code","my_eia_api_key.R"))
source(here("my_postgres_credentials.R"))

eia_set_key(eiaKey)

db_driver <- dbDriver("PostgreSQL")
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

# check the connection
dbExistsTable(db, "metadata")
# TRUE

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

# ----------------------------------------------------------------------------------
all_r1_year <- NULL

for(state in states){
  gen_by_source_cols <- list(c('year','coal','oil','gas','nuclear',
                               'wind','utility_solar','distributed_solar',
                               'hydropower','wood','other_biomass','total'))
  gen_by_source_units <-'GWh'
  
  series_ids=list(c(paste0("ELEC.GEN.COW-",state,"-99.A"),
              paste0("ELEC.GEN.PEL-",state,"-99.A"),
              paste0("ELEC.GEN.NG-",state,"-99.A"),
              paste0("ELEC.GEN.NUC-",state,"-99.A"),
              paste0("ELEC.GEN.WND-",state,"-99.A"),
              paste0("ELEC.GEN.SUN-",state,"-99.A"),
              paste0("ELEC.GEN.DPV-",state,"-99.A"),
              paste0("ELEC.GEN.HYC-",state,"-99.A"),
              paste0("ELEC.GEN.WWW-",state,"-99.A"),
              paste0("ELEC.GEN.WAS-",state,"-99.A"),
              paste0("ELEC.GEN.ALL-",state,"-99.A"))
  )
  
  api_link <- vector("list", length(series_ids))
  for (i in 1:length(series_ids)){
    api_link[[i]] <- paste("http://api.eia.gov/series/?api_key=",eiaKey,"&series_id=",series_ids[[i]],sep='')
  }
  
  data_update <- eia_series_updates(paste0("ELEC.GEN.ALL-",state,"-99.A"))$updated #only timestamping series for total generation per state
  data_update <- gsub("T", " ", data_update)
  data_update <- gsub("-0400", "", data_update)
  data_update <- strptime(data_update, format="%Y-%m-%d %H:%M:%S")

  
  r1_year <- data.frame(db_table_name = paste0("eia_elec_gen_",str_to_lower(state),"_a",sep =""),
                  short_series_name= paste('Total annual generation for ',state,sep=''),
                  full_series_name = paste('Net generation : by fuel : ',state,' : annual',sep=''),
                  column2variable_name_map=I(gen_by_source_cols), units=I(gen_by_source_units),frequency='A',
                  data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                  url=NA, api=I(api_link),
                  series_id=I(series_ids),json=NA,notes=NA, data_type='time-series', data_context='historical', 
                  corresponding_data=NA, 
                  R_script='generation_all_states.R', latest_data_update=data_update,
                  last_db_refresh=lubridate::with_tz(Sys.time(), "UTC"))
  
  if (is.null(all_r1_year))
  {all_r1_year <- r1_year}
  else
  {all_r1_year <-  rbind(all_r1_year, r1_year)}
  
}

all_r1_month <- NULL
for(state in states){
  gen_by_source_cols <- list(c('year','coal','oil','gas','nuclear',
                               'wind','utility_solar','distributed_solar',
                               'hydropower','wood','other_biomass','total'))
  gen_by_source_units <-'GWh'
  
  series_ids=list(c(paste0("ELEC.GEN.COW-",state,"-99.M"),
                    paste0("ELEC.GEN.PEL-",state,"-99.M"),
                    paste0("ELEC.GEN.NG-",state,"-99.M"),
                    paste0("ELEC.GEN.NUC-",state,"-99.M"),
                    paste0("ELEC.GEN.WND-",state,"-99.M"),
                    paste0("ELEC.GEN.SUN-",state,"-99.M"),
                    paste0("ELEC.GEN.DPV-",state,"-99.M"),
                    paste0("ELEC.GEN.HYC-",state,"-99.M"),
                    paste0("ELEC.GEN.WWW-",state,"-99.M"),
                    paste0("ELEC.GEN.WAS-",state,"-99.M"),
                    paste0("ELEC.GEN.ALL-",state,"-99.M"))
  )
  
  api_link <- vector("list", length(series_ids))
  
  for (i in 1:length(series_ids)){
    api_link[[i]] <- paste("http://api.eia.gov/series/?api_key=",eiaKey,"&series_id=",series_ids[[i]],sep='')
  }
  
  data_update <- eia_series_updates(paste0("ELEC.GEN.ALL-",state,"-99.M"))$updated #only timestamping series for total generation per state
  data_update <- gsub("T", " ", data_update)
  data_update <- gsub("-0400", "", data_update)
  data_update <- strptime(data_update, format="%Y-%m-%d %H:%M:%S")
  
  
  r1_month<- data.frame(db_table_name = paste0("eia_elec_gen_",str_to_lower(state),"_m",sep =""),
                  short_series_name= paste('Total monthly generation for ',state, sep=''),
                  full_series_name = paste('Net generation : by fuel : ',state,' : monthly',sep=''),
                  column2variable_name_map=I(gen_by_source_cols), units=I(gen_by_source_units),frequency='M',
                  data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                  url=NA, api=I(api_link),
                  series_id=I(series_ids),json=NA,notes=NA, data_type='time-series', data_context='historical', 
                  corresponding_data=NA, 
                  R_script='generation_all_states.R', latest_data_update=data_update,
                  last_db_refresh=lubridate::with_tz(Sys.time(), "UTC"))
  
  if (is.null(all_r1_month))
  {all_r1_month <- r1_month}
  else
  {all_r1_month <-  rbind(all_r1_month, r1_month)}
  
}

r1 <- rbind(all_r1_year,all_r1_month)
dbWriteTable(db, 'metadata2', value = r1, append = TRUE, overwrite = FALSE, row.names = FALSE)

