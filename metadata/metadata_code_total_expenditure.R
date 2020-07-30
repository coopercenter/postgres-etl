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

db_driver = dbDriver("PostgreSQL")
source(here("my_postgres_credentials.R"))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)

#type in name of data table inside colnames() - this function will extract the table's columns. Replace 'datasetName' with the name of dataset in database.
va_electricity_total_ex_1970_to_2017 <- dbGetQuery(db,'SELECT * from "va_electricity_total_ex_1970_to_2017"')
cols <- list(colnames(va_electricity_total_ex_1970_to_2017))
units <- list(c("Year", "Million Dollars"))

r1 <- data.frame(db_table_name = 'va_electricity_total_ex_1970_to_2017',
                short_series_name = 'Virignia Total Electricity Expenditures from 1970 to 2017',
                full_series_name = 'Virignia Total Electricity Expenditures from 1970 to 2017',
                column2variable_name_map = I(cols), units = I(units), frequency = 'A',
                data_source_brief_name = NA, data_source_full_name = NA,
                url = NA, api = NA,
                series_id = NA, json = NA, notes = NA, data_type = 'time-series',
                data_context = "historical", corresponding_data = NA,
                R_script = 'cleaning_total_expenditure.R', latest_data_update = NA, #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))

#--------------------------------------------------------------------------------------
#type in name of data table inside colnames() - this function will extract the table's columns
query_annual <- dbGetQuery(db, 'SELECT * FROM "elec_sales_through_2019_annual"')
cols_annual <- list(colnames(query_annual))
units_annual <- list(c("Year", "GWh", "GWh", "GWh", "GWh"))


r2 <- data.frame(db_table_name = 'elec_sales_through_2019_annual',
                short_series_name = 'Electricity Sales through 2019',
                full_series_name = 'Electricity Sales through 2019 for Dominion and APCO',
                column2variable_name_map = I(cols_annual), units = I(units_annual), frequency = 'A',
                data_source_brief_name = 'Dominion Power, Bill Shobe', data_source_full_name = 'Dominion Power Company, Bill Shobe',
                url = NA, api = NA,
                series_id = NA, json = NA, notes = NA, data_type = 'time-series',
                data_context = 'historical', corresponding_data = NA,
                R_script = 'cleaning_elec_through_2019.R', latest_data_update = '2019-12-31', #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))

#--------------------------------------------------------------------------------------
query_monthly <- dbGetQuery(db, 'SELECT * FROM "elec_sales_through_2019_monthly"')
cols_monthly <- list(colnames(query_monthly))
units_monthly <- list(c('date','month','year','GWh','GWh'))

r3 <- data.frame(db_table_name = 'elec_sales_through_2019_monthly',
                short_series_name = 'Electricity Sales through 2019',
                full_series_name = 'Electricity Sales through 2019 for Dominion and APCO',
                column2variable_name_map = I(cols_monthly), units = I(units_monthly), frequency = 'M',
                data_source_brief_name = 'Dominion Power, Bill Shobe', data_source_full_name = 'Dominion Power Company, Bill Shobe',
                url = NA, api = NA,
                series_id = NA, json = NA, notes = NA, data_type = 'time-series',
                data_context = 'historical', corresponding_data = NA,
                R_script = 'cleaning_elec_through_2019.R', latest_data_update = '2019-12-31', #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))
#--------------------------------------------------------------------------------------
metadata <- rbind(r1,r2,r3)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)
