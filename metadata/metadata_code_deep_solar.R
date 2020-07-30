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

deep_solar <- dbGetQuery(db,'SELECT * from deep_solar')
metadata_deep_solar <- read.csv(here('raw_data','deepsolar_tract_meta.csv'))

#type in name of data table inside colnames() - this function will extract the table's columns
cols <- list(colnames(deep_solar))

#manually list out units -- make sure to correctly correspond the order of units to the order of columns
#can be NA if cross-sectional data
units <-list(metadata_deep_solar$explanation)

# construct r2, r3,... if needed
r1<- data.frame(db_table_name = 'deep_solar',
                short_series_name = 'Deep Solar',
                full_series_name = 'Deep Solar Project',
                column2variable_name_map = I(cols), units = I(units), frequency = NA,
                data_source_brief_name = 'Deep Solar', data_source_full_name = 'Deep Solar Project Standford, University',
                url = 'http://web.stanford.edu/group/deepsolar/home', api = NA,
                series_id = NA, json = NA, notes = NA, data_type = 'cross-sectional',
                data_context = 'historical', corresponding_data = NA,
                R_script = 'cleaning_deep_solar', latest_data_update = '2019-03-19', #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))


dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)
