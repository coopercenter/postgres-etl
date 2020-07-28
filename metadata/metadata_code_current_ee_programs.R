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


#type in name of data table inside colnames() - this function will extract the table's columns
cols <- list(colnames("current_ee_programs"))

#manually list out units -- make sure to correctly correspond the order of units to the order of columns
#can be NA if cross-sectional data
units <- list(c("", "", "", "Years", "Dollars", "", "Years", "", "", "kWh/yr", "kW", "", "" ))


# construct r2, r3,... if needed
r1<- data.frame(db_table_name = 'current_ee_programs',
                short_series_name = 'Current EE Programs',
                full_series_name = 'Current Energy Energy Effciency Programs for Dominion, APCO, Old Dominion',
                column2variable_name_map = I(cols), units = I(units), frequency = NA,
                data_source_brief_name = 'Dominion Power, APCO', data_source_full_name = 'Dominion Power Company, Appalachian Power Company',
                url = NA, api = NA,
                series_id = NA, json = NA, notes = NA, data_type = NA,
                data_context = NA, corresponding_data = NA,
                R_script = 'cleaning_energy_effciency_programs.R', latest_data_update = NA, #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))

r2<- data.frame(db_table_name = 'dominion_current_EE_data_through_2018',
                short_series_name = 'Dominion EE Data Through 2018',
                full_series_name = 'Dominion Current Energy Effciency Data Through 2018',
                column2variable_name_map = I(cols), units = I(units), frequency = NA,
                data_source_brief_name = 'Dominion Power', data_source_full_name = 'Dominion Power Company',
                url = NA, api = NA,
                series_id = NA, json = NA, notes = NA, data_type = NA,
                data_context = NA, corresponding_data = NA,
                R_script = 'cleaning_energy_effciency_programs.R', latest_data_update = NA, #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))


metadata <- rbind(r1,r2)

db_driver = dbDriver("PostgreSQL")
source(here("my_postgres_credentials.R"))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)

dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)
