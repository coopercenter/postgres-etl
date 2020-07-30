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
photovoltaic_net_metering <- dbGetQuery(db,'SELECT * from "photovoltaic_net_metering"')
photo_cols <- list(colnames(photovoltaic_net_metering))

#manually list out units -- make sure to correctly correspond the order of units to the order of columns
#can be NA if cross-sectional data
photo_units <- list(c("year","MW","MW","MW","MW","MW","Persons","Persons","Persons","Persons","Persons"))

# construct r2, r3,... if needed
r1<- data.frame(db_table_name = 'photovoltaic_net_metering',
                short_series_name = 'Photovoltaic Net Metering',
                full_series_name = 'Virginia Photovoltaic Net Metering from 2010 to 2018',
                column2variable_name_map = I(photo_cols), units = I(photo_units), frequency = "A",
                data_source_brief_name = "EIA", data_source_full_name = "U.S Energy Information Administration",
                url = "https://www.eia.gov/electricity/data/eia861/", api = NA,
                series_id = NA, json = NA, notes = "Table 11. Net metering, 2010 through 2018", data_type = 'time-series',
                data_context = 'historical', corresponding_data = NA,
                R_script = 'cleaning_net_metering', latest_data_update = '2020-03-16', #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))

#-----
wind_net_metering <- dbGetQuery(db,'SELECT * from "wind_net_metering"')
wind_cols <- list(colnames(wind_net_metering))
wind_units <- list(c("year","MW","MW","MW","MW","MW","Persons","Persons","Persons","Persons","Persons"))
r2<- data.frame(db_table_name = 'wind_net_metering',
                short_series_name = 'wind Net Metering',
                full_series_name = 'Virginia Wind Net Metering from 2010 to 2018',
                column2variable_name_map = I(wind_cols), units = I(wind_units), frequency = "A",
                data_source_brief_name = "EIA", data_source_full_name = "U.S Energy Information Administration",
                url = "https://www.eia.gov/electricity/data/eia861/", api = NA,
                series_id = NA, json = NA, notes = "Table 11. Net metering, 2010 through 2018", data_type = 'time-series',
                data_context = 'historical', corresponding_data = NA,
                R_script = 'cleaning_net_metering', latest_data_update = '2020-03-16', #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))


#Add r1,r2,r3...ect if they are created.
metadata <- rbind(r1, r2)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)
