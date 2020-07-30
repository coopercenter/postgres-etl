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
plants_by_capacity_va <- dbGetQuery(db,'SELECT * from "plants_by_capacity_va"')
capacity_cols <- list(colnames(plants_by_capacity_va))

plant_generation_data_va <- dbGetQuery(db,'SELECT * from "plant_generation_data_va"')
gen_cols <- list(colnames(plant_generation_data_va))

va_emission_by_plant <- dbGetQuery(db,'SELECT * from "va_emission_by_plant"')
emission_cols <- list(colnames(va_emission_by_plant))

#manually list out units -- make sure to correctly correspond the order of units to the order of columns
#can be NA if cross-sectional data
capacity_units <- list(c("","","","MW"))
gen_units <- list(c("","","","MWh"))
emission_units <- list(c("","Tons","Tons","Tons"))

# construct r2, r3,... if needed
r1<- data.frame(db_table_name = 'plants_by_capacity_va',
                short_series_name = 'Virginia Plant Capacity',
                full_series_name = 'Virginia Plant Capacity 2018',
                column2variable_name_map = I(capacity_cols), units = I(capacity_units), frequency = NA,
                data_source_brief_name = "EIA", data_source_full_name = "U.S Energy Information Administration",
                url = "https://www.eia.gov/electricity/data/eia860/", api = NA,
                series_id = NA, json = NA, notes = "Table 2A. Ten largest plants by capacity, 2018", data_type = 'cross-sectional',
                data_context = 'historical', corresponding_data = NA,
                R_script = 'cleaning_plants.R', latest_data_update = '2020-06-12', #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))

r2<- data.frame(db_table_name = 'plant_generation_data_va',
                short_series_name = 'Virginia Plant Generation',
                full_series_name = 'Virginia Plant Generation 2018',
                column2variable_name_map = I(gen_cols), units = I(gen_units), frequency = NA,
                data_source_brief_name = "EIA", data_source_full_name = "U.S Energy Information Administration",
                url = "https://www.eia.gov/electricity/data/eia923/", api = NA,
                series_id = NA, json = NA, notes = "Table 2B. Ten largest plants by generation, 2018", data_type = 'cross-sectional',
                data_context = 'historical', corresponding_data = NA,
                R_script = 'cleaning_plants.R', latest_data_update = '2020-06-16', #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))

r3<- data.frame(db_table_name = 'va_emission_by_plant',
                short_series_name = 'Virginia Plant Emissions',
                full_series_name = 'Virginia Plant Emissions 2018',
                column2variable_name_map = I(Emission_cols), units = I(emission_units), frequency = NA,
                data_source_brief_name = "EIA", data_source_full_name = "U.S Energy Information Administration",
                url = "https://www.eia.gov/beta/electricity/data/browser/#/topic/1?agg=2,0,1&fuel=vtvv&pt=&pm=&sec=vvo&geo=00000001&wd=&ws=&wsn=&wt=&freq=A&datecode=2018&tab=annual_emissions&pin=&rse=0&maptype=0&ltype=pin&ctype=linechart&end=201710&start=200101", api = NA,
                series_id = NA, json = NA, notes = "Plants for multiple sectors,Virginia,multiple fuel types", data_type = 'cross-sectional',
                data_context = 'historical', corresponding_data = NA,
                R_script = 'cleaning_plants.R', latest_data_update = '2020-03-06', #check with data source last time it was updated
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))


#Add r1,r2,r3...ect if they are created.
metadata <- rbind(r1, r2, r3)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)
