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
#-----------------------------------------------------------------------------------------------------------
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
# TRUE

# -----------------------------------------------------------------------------------------------------------
virginia_annual_savings_2020<-dbGetQuery (db,'SELECT * from virginia_annual_savings_through_2020')
virginia_annual_savings_2020_col<-list(colnames(virginia_annual_savings_2020))

virginia_annual_savings_2022<-dbGetQuery (db,'SELECT * from virginia_annual_savings_through_2022')
virginia_annual_savings_2022_col<-list(colnames(virginia_annual_savings_2022))
#------------------------------------------------------------------------------------------------------------

r1<- data.frame(db_table_name = 'virginia_annual_savings_through_2020',
                short_series_name = 'Virginia annual savings',
                full_series_name = 'Virginia annual savings through 2020',
                column2variable_name_map = I(virginia_annual_savings_2020_col), units = 'MWh', frequency = 'A',
                data_source_brief_name = 'ACEEE', data_source_full_name = 'American Council for an Energy-Efficient Economy',
                url = NA, api = NA,
                series_id = NA, json = NA, notes = NA, data_type = 'cross-sectional',
                data_context = 'Planned', corresponding_data = NA,
                R_script = 'cleaning_virginia_annual_savings.R', latest_data_update = '2020-07-22',
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))

#-------------------------------------------------------------------------------------------------------------

r2<- data.frame(db_table_name = 'virginia_annual_savings_through_2022',
                    short_series_name = 'Virginia annual savings',
                    full_series_name = 'Virginia annual savings through 2022',
                    column2variable_name_map = I(virginia_annual_savings_2022_col), units = 'MWh', frequency = 'A',
                    data_source_brief_name = 'ACEEE', data_source_full_name = 'American Council for an Energy-Efficient Economy',
                    url = NA, api = NA,
                    series_id = NA, json = NA, notes = NA, data_type = 'cross-sectional',
                    data_context = 'Planned', corresponding_data = NA,
                    R_script = 'cleaning_virginia_annual_savings.R', latest_data_update = '2020-07-22',
                    last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))
#------------------------------------------------------------------------------------------------------------

library(plyr)
metadata <- rbind(r1, r2)


dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)
