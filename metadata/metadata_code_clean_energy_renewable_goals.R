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
clean_energy_renewable_goals <- dbGetQuery(db,'SELECT * from clean_energy_renewable_goals')
cols <- list(colnames(clean_energy_renewable_goals))

#manually list out units -- make sure to correctly correspond the order of units to the order of columns
#can be NA if cross-sectional data
units <- list(c("Year", "Company", "Percent"))

# construct r2, r3,... if needed
r1<- data.frame(db_table_name = 'clean_energy_renewable_goals',
                short_series_name = 'Clean Energy Renewable Goals RPS',
                full_series_name = 'Clean Energy Renewable Goals RPS for APCO and Dominion',
                column2variable_name_map = I(cols), units = I(units), frequency = 'A',
                data_source_brief_name = 'VCEA', data_source_full_name = 'Virginia Clean Economy Act',
                url = NA, api = NA,
                series_id = NA, json = NA, notes = NA, data_type = 'time-series',
                data_context = 'mandate', corresponding_data = NA,
                R_script = 'cleaning_VCEA_goals.R', latest_data_update = NA, #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))


db_driver = dbDriver("PostgreSQL")
source(here("my_postgres_credentials.R"))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)

dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)
