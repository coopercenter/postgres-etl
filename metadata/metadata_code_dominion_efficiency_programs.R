## IMPORTANT:
#  Refer to the data_management_plan.md in postgres-etl repo

# Creating the dataframe for metadata
metadata<-data.frame(matrix(ncol = 19, nrow = 0))

# Specify the column names
colnames(metadata) <- c('db_table_name','short_series_name','full_series_name',
                        'column2variable_name_map','units','frequency',
                        'data_source_brief_name','data_source_full_name','url',
                        'api','series_id','json','notes', 'data_type','data_context','corresponding_data','R_script',
                        'latest_data_update','last_db_refresh')

#--------------------------------------------------------------------------------------
# Connection to the database
# "my_postgres_credentials.R" contains the log-in informations of RAs
library(here)
library('RPostgreSQL')
library(tidyverse)
source(here("my_postgres_credentials.R"))

db_driver <- dbDriver("PostgreSQL")
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)


gross_savings_dominion <- dbGetQuery(db,'SELECT * from gross_savings_dominion')
gross_cols <- list(colnames(gross_savings_dominion))
gross_units <- list(c('Year','kWh','kWh','kWh','kWh','kWh','kWh','kWh','kWh','kWh','kWh','kWh','kWh','kWh','kWh'))

r1<- data.frame(db_table_name = 'gross_savings_dominion',
                short_series_name = 'Dominion Gross Savings',
                full_series_name = 'Dominion Energy Effciency Programs Gross Savings',
                column2variable_name_map = I(gross_cols), units = I(gross_units), frequency = 'A',
                data_source_brief_name = 'Dominion Power', data_source_full_name = 'Dominion Power Company',
                url = NA, api = NA,
                series_id = NA, json = NA, notes = NA, data_type = 'time-series',
                data_context = 'historical', corresponding_data = NA,
                R_script = 'cleaning_dominion_efficiency_programs', latest_data_update = NA, #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))
#-----------
net_savings_dominion <- dbGetQuery(db,'SELECT * from net_savings_dominion')
net_cols <- list(colnames(gross_savings_dominion))
net_units <- list(c('Year','kWh','kWh','kWh','kWh','kWh','kWh','kWh','kWh','kWh','kWh','kWh','kWh','kWh','kWh'))

r2<- data.frame(db_table_name = 'net_savings_dominion',
                short_series_name = 'Dominion Net Savings',
                full_series_name = 'Dominion Energy Effciency Programs Net Savings',
                column2variable_name_map = I(net_cols), units = I(net_units), frequency = 'A',
                data_source_brief_name = 'Dominion Power', data_source_full_name = 'Dominion Power Company',
                url = NA, api = NA,
                series_id = NA, json = NA, notes = NA, data_type = 'time-series',
                data_context = 'historical', corresponding_data = NA,
                R_script = 'cleaning_dominion_efficiency_programs', latest_data_update = NA, #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))

#-----------
program_participants_dominion <- dbGetQuery(db,'SELECT * from program_participants_dominion')
participants_cols <- list(colnames(program_participants_dominion)) #HAVE TO DOUBLE CHECK UNITS FOR PARTICPANTS, CANT FIND THE SOURCE FOR DATASET.
participants_units <- list(c('Year','People','People','People','People','People','People','People','People','People','People','People','People','People','People'))

r3<- data.frame(db_table_name = 'program_participants_dominion',
                short_series_name = 'Dominion EE Participants',
                full_series_name = 'Dominion Energy Effciency Programs Participants',
                column2variable_name_map = I(participants_cols), units = I(participants_units), frequency = 'A',
                data_source_brief_name = 'Dominion Power', data_source_full_name = 'Dominion Power Company',
                url = NA, api = NA,
                series_id = NA, json = NA, notes = NA, data_type = 'time-series',
                data_context = 'historical', corresponding_data = NA,
                R_script = 'cleaning_dominion_efficiency_programs', latest_data_update = NA, #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))


metadata <- rbind(r1,r2,r3)


dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)
