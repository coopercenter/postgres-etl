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
dbExistsTable(db, "metadata")
metadata <- dbGetQuery(db,'SELECT * from metadata')
# TRUE

# ----------------------------------------------------------------------------------
total_mw_offshore_wind<-dbGetQuery(db,'SELECT * from total_mw_offshore_wind')
offshore_mw_cols <- list(colnames(total_mw_offshore_wind))
offshore_mw_units <-'MW'

r1<- data.frame(db_table_name = "total_mw_offshore_wind",
                short_series_name= 'Total megawatt predictions of offshore wind',
                full_series_name = 'Total megawatt predictions of offshore wind energy in phases from 2017 through 2033',
                column2variable_name_map=I(offshore_mw_cols),units=I(offshore_mw_units),frequency='A',
                data_source_brief_name='DEIRP',data_source_full_name='Dominion Energy 2020 Integrated Resource Plan',
                url='https://www.dominionenergy.com/library/domcom/media/about-us/making-energy/2020-va-integrated-resource-plan.pdf?modified=20200501191108',
                api=NA, series_id=NA,json=NA,notes=NA, data_type='time-series', data_context='forecast', corresponding_data=NA,
                R_script='cleaning_offshore_wind.R',
                latest_data_update='2020-01-01', last_db_refresh='2020-05-01')

# ----------------------------------------------------------------------------------
net_capacity_factor_offshore_wind<-dbGetQuery(db,'SELECT * from net_capacity_factor_offshore_wind')
offshore_cf_cols <- list(colnames(net_capacity_factor_offshore_wind))
offshore_cf_units <- 'percent'

r2<- data.frame(db_table_name = "net_capacity_factor_offshore_wind",
                short_series_name='Capacity factor predictions of offshore wind',
                full_series_name = 'Capacity factor predictions of offshore wind energy in phases from 2017 through 2035',
                column2variable_name_map=I(offshore_cf_cols),units=I(offshore_cf_units),frequency='A',
                data_source_brief_name='DEIRP',data_source_full_name='Dominion Energy 2020 Integrated Resource Plan',
                url='https://www.dominionenergy.com/library/domcom/media/about-us/making-energy/2020-va-integrated-resource-plan.pdf?modified=20200501191108',api=NA,
                series_id=NA,json=NA,notes=NA, data_type='time-series', data_context='forecast', corresponding_data=NA,
                R_script='cleaning_offshore_wind.R',
                latest_data_update='2020-01-01', last_db_refresh='2020-05-01')

# ----------------------------------------------------------------------------------
total_production_forecast_offshore_wind<-dbGetQuery(db,'SELECT * from total_production_forecast_offshore_wind')
offshore_tp_cols <- list(colnames(total_production_forecast_offshore_wind))
offshore_tp_units <- 'GW'

r3<- data.frame(db_table_name = "total_production_forecast_offshore_wind",
                short_series_name = 'Total production forecast of offshore wind',
                full_series_name = 'Total production forecast of offshore wind energy in phases from 2017 through 2035',
                column2variable_name_map=I(offshore_tp_cols),units=I(offshore_tp_units),frequency='A',
                data_source_brief_name='DEIRP',data_source_full_name='Dominion Energy 2020 Integrated Resource Plan',
                url='https://www.dominionenergy.com/library/domcom/media/about-us/making-energy/2020-va-integrated-resource-plan.pdf?modified=20200501191108',api=NA,
                series_id=NA,json=NA,notes=NA, data_type='time-series', data_context='forecast', corresponding_data=NA,
                R_script='cleaning_offshore_wind.R',
                latest_data_update='2020-01-01', last_db_refresh='2020-05-01')

library(plyr)
metadata<-rbind(r1,r2,r3)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)
