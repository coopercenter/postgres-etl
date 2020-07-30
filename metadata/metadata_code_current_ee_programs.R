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

query <- dbGetQuery(db, 'SELECT * FROM "current_ee_programs"')
cols <- list(colnames(query))
units <- list(c("", "", "", "Years", "Dollars", "", "Years", "", "", "kWh/yr", "kW", "", "" ))

r1<- data.frame(db_table_name = 'current_ee_programs',
                short_series_name = 'Current EE Programs',
                full_series_name = 'Current Energy Energy Effciency Programs for Dominion, APCO, Old Dominion',
                column2variable_name_map = I(cols), units = I(units), frequency = NA,
                data_source_brief_name = 'Dominion Power, APCO', data_source_full_name = 'Dominion Power Company, Appalachian Power Company',
                url = NA, api = NA,
                series_id = NA, json = NA, notes = NA, data_type = 'cross-sectional',
                data_context = 'present-time', corresponding_data = NA,
                R_script = 'cleaning_energy_efficiency_programs.R', latest_data_update = NA, #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))

dbWriteTable(db, 'metadata', value = r1, append = TRUE, overwrite = FALSE, row.names = FALSE)
