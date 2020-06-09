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
energy_savings_reporting_year_incremental<-dbGetQuery(db,'SELECT * from energy_savings_reporting_year_incremental')

colnames(energy_savings_reporting_year_incremental)
energy_savings_reporting_year_incremental_cols <- list(c('year','total','residential','commercial','industrial'))
energy_savings_reporting_year_incremental_units <-'megawatthour'

r1<- data.frame(db_table_name = "energy_savings_reporting_year_incremental",
                short_series_name= 'Total megawatt hour incremental savings per sector per year',
                full_series_name = 'Total megawatt hour incremental savings per sector from 2013 through 2018',
                column2variable_name_map=I(energy_savings_reporting_year_incremental_cols),units=I(energy_savings_reporting_year_incremental_units),frequency='A',
                data_source_brief_name='EIA State Electricity Profile',data_source_full_name='Energy Information Administration 2018 VA Electricity Profile',
                url='https://www.eia.gov/electricity/state/virginia/',api=NA,
                series_id=NA,json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=0, 
                R_script='cleaning_energy_efficiency.R')

# ----------------------------------------------------------------------------------
energy_savings_incremental_life_cycle<-dbGetQuery(db,'SELECT * from energy_savings_incremental_life_cycle')

colnames(energy_savings_incremental_life_cycle)
energy_savings_incremental_life_cycle_cols <- list(c('Year','Total','residential','commercial','industrial'))
energy_savings_incremental_life_cycle_units <- 'megawatthour'

r2<- data.frame(db_table_name = "energy_savings_incremental_life_cycle",
                short_series_name='Total megawatt hour incremental savings per sector per lifecycle',
                full_series_name = 'Total megawatt hour incremental savings per sector per lifecycle from 2017 through 2035',
                column2variable_name_map=I(energy_savings_incremental_life_cycle_cols),units=I(energy_savings_incremental_life_cycle_units),frequency='A',
                data_source_brief_name='EIA State Electricity Profile',data_source_full_name='Energy Information Administration 2018 VA Electricity Profile',
                url='https://www.eia.gov/electricity/state/virginia/',api=NA,
                series_id=NA,json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=0, 
                R_script='cleaning_energy_efficiency.R')

library(plyr)
metadata<-rbind(r1,r2)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)
