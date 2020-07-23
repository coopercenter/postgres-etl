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

# check the connection
dbExistsTable(db, "metadata")

#type in name of data table inside colnames() - this function will extract the table's columns
pjm_gats_generators<-dbGetQuery (db,'SELECT * from pjm_gats_generators')
cols <- list(colnames(pjm_gats_generators))

#manually list out units -- make sure to correctly correspond the order of units to the order of columns
#can be NA if cross-sectional data
#units <- list(c())

#------------------------------------------------------------------------------------------------------------------

r1<- data.frame(db_table_name = 'pjm_gats_generators',
                short_series_name = 'Data on renewable generators by GATS ',
                full_series_name = 'Data on renewable generators registered in Generation Attribute Tracking System ',
                column2variable_name_map = I(cols), units = NA, frequency = NA,
                data_source_brief_name = 'PJM', data_source_full_name = 'PJM Interconnection LLC',
                url = 'https://gats.pjm-eis.com/gats2/PublicReports/RenewableGeneratorsRegisteredinGATS', api = NA,
                series_id = NA, json = NA, notes = NA, data_type = 'cross-sectional',
                data_context = 'historical', corresponding_data = NA,
                R_script = 'cleaning_pjm_gats_generators', latest_data_update = '2020-07-23', #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))

metadata <- rbind(r1)

db_driver = dbDriver("PostgreSQL")
source(here("my_postgres_credentials.R"))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)

dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)