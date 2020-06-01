# Connection to the database
# "my_postgres_credentials.R" contains the log-in informations of RAs
library(here)
library('RPostgreSQL')
library(tidyverse)
source(here("api_data_code", "my_postgres_credentials.R"))

db_driver <- dbDriver("PostgreSQL")
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

# check the connection
dbExistsTable(db, "metadata")
metadata <- dbGetQuery(db,'SELECT * from metadata')
# TRUE

# ----------------------------------------------------------------------------------
res_pop<-dbGetQuery(db,'SELECT * FROM residential_population_va')

res_pop <- colnames(res_pop)

get_FRED_meta <- function(api_key,series_id) {
  require(jsonlite)
  require(data.table)
  FREDBase <- paste0("https://api.stlouisfed.org/fred/series?series_id=",series_id,"&api_key=",api_key,"&file_type=json") 
  temp <- readLines(FREDBase, warn = "F")
  rd <- fromJSON(temp)
  
  # Now take the 'data' element from the list and make a data frame
  returnList = list(
    series_id = rd$seriess$id,
    name = rd$seriess$title,
    units = rd$seriess$units,
    frequency = rd$seriess$frequency_short,
  )
  return(returnList) 
}

source(here("api_data_code","my_fred_api_key.R"))
pop_meta <- get_FRED_meta(fredKey,"VAPOP")

offshore_mw_units <-'megawatts'

r1<- data.frame(db_table_name = "total_mw_offshore_wind",
                short_series_name= 'Total megawatt predictions of offshore wind',
                full_series_name = 'Total megawatt predictions of offshore wind energy in phases from 2017 through 2033',
                column2variable_name_map=I(offshore_mw_cols),units=I(offshore_mw_units),frequency='A',
                data_source_brief_name='DEIRP',data_source_full_name='Dominion Energy 2020 Integrated Resource Plan',
                url='https://www.dominionenergy.com/library/domcom/media/about-us/making-energy/2020-va-integrated-resource-plan.pdf?modified=20200501191108',api=NA,
                series_id=NA,json=NA,notes=NA)

# ----------------------------------------------------------------------------------
net_capacity_factor_offshore_wind<-dbGetQuery(db,'SELECT * from net_capacity_factor_offshore_wind')

colnames(net_capacity_factor_offshore_wind)
offshore_cf_cols <- list(c('Year','Pilot','Stage_I','Stage_II',
                           'Stage_III'))
offshore_cf_units <- 'CF'

r2<- data.frame(db_table_name = "net_capacity_factor_offshore_wind",
                short_series_name='Capacity factor predictions of offshore wind',
                full_series_name = 'Capacity factor predictions of offshore wind energy in phases from 2017 through 2035',
                column2variable_name_map=I(offshore_cf_cols),units=I(offshore_cf_units),frequency='A',
                data_source_brief_name='DEIRP',data_source_full_name='Dominion Energy 2020 Integrated Resource Plan',
                url='https://www.dominionenergy.com/library/domcom/media/about-us/making-energy/2020-va-integrated-resource-plan.pdf?modified=20200501191108',api=NA,
                series_id=NA,json=NA,notes=NA)

# ----------------------------------------------------------------------------------
total_production_forecast_offshore_wind<-dbGetQuery(db,'SELECT * from total_production_forecast_offshore_wind')
colnames(total_production_forecast_offshore_wind)
offshore_tp_cols <- list(c('Year','Total_Production_GWh'))
offshore_tp_units <- 'gigawatt'
r3<- data.frame(db_table_name = "total_production_forecast_offshore_wind",
                short_series_name = 'Total production forecast of offshore wind',
                full_series_name = 'Total production forecast of offshore wind energy in phases from 2017 through 2035',
                column2variable_name_map=I(offshore_tp_cols),units=I(offshore_tp_units),frequency='Y',
                data_source_brief_name='DEIRP',data_source_full_name='Dominion Energy 2020 Integrated Resource Plan',
                url='https://www.dominionenergy.com/library/domcom/media/about-us/making-energy/2020-va-integrated-resource-plan.pdf?modified=20200501191108',api=NA,
                series_id=NA,json=NA,notes=NA)

library(plyr)
metadata<-rbind(r1,r2,r3)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)


