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

#type in name of data table inside colnames() - this function will extract the table's columns. Replace 'datasetName' with the name of dataset in database.
ownership <- dbGetQuery(db,'SELECT * from "ownership"')
cols <- list(colnames(ownership))

#manually list out units -- make sure to correctly correspond the order of units to the order of columns
#can be NA if cross-sectional data
units <- list(c("","", "Persons", "MWh", "percent", "Thousand Dollars", "percent", "cents per kWh"))

# construct r2, r3,... if needed
r1<- data.frame(db_table_name = 'ownership',
                short_series_name = 'Ownership',
                full_series_name = 'Ownership',
                column2variable_name_map = I(cols), units = I(units), frequency = NA,
                data_source_brief_name = NA, data_source_full_name = NA,
                url = NA, api = NA,
                series_id = NA, json = NA, notes = NA, data_type = 'cross-sectional',
                data_context = 'historical', corresponding_data = NA,
                R_script = 'cleaning_ownership', latest_data_update = NA, #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))

#Add r1,r2,r3...ect if they are created.
metadata <- rbind(r1)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)
