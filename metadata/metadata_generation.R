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

# ----------------------------------------------------------------------------------
elec_ipp_chp_gen <- dbGetQuery(db,'SELECT * from electric_ipp_chp_generation')
elec_ipp_chp_gen_cols <- list(colnames(elec_ipp_chp_gen))

r1 <- data.frame(db_table_name = "electric_ipp_chp_generation",
                 short_series_name= '',
                 full_series_name = '',
                 column2variable_name_map=I(elec_ipp_chp_gen_cols), units='megawatthours', frequency='A',
                 data_source_brief_name='', data_source_full_name='',
                 url=NA, api=NA, series_id=NA,json=NA, 
                 notes= NA, 
                 mandate=0, forecast=0, corresponding_data=NA, R_script='cleaning_generation_by_sector.R')

# ----------------------------------------------------------------------------------
elec_utility_gen <- dbGetQuery(db,'SELECT * from electric_utility_generation')
elec_utility_gen_cols <- list(colnames(elec_utility_gen))

r2 <- data.frame(db_table_name = "electric_utility_generation",
                 short_series_name= '',
                 full_series_name = '',
                 column2variable_name_map=I(elec_utility_gen_cols), units='megawatthours', frequency='A',
                 data_source_brief_name='', data_source_full_name='',
                 url=NA, api=NA, series_id=NA,json=NA, 
                 notes= NA, 
                 mandate=0, forecast=0, corresponding_data=NA, R_script='cleaning_generation_by_sector.R')

# ----------------------------------------------------------------------------------
whole_elec_gen <- dbGetQuery(db,'SELECT * from whole_electric_industry_generation')
whole_elec_gen_cols <- list(colnames(whole_elec_gen))

r3 <- data.frame(db_table_name = "whole_electric_industry_generation",
                 short_series_name= '',
                 full_series_name = '',
                 column2variable_name_map=I(whole_elec_gen_cols), units='megawatthours', frequency='A',
                 data_source_brief_name='', data_source_full_name='',
                 url=NA, api=NA, series_id=NA,json=NA, 
                 notes= NA, 
                 mandate=0, forecast=0, corresponding_data=NA, R_script='cleaning_generation_by_sector.R')

# ----------------------------------------------------------------------------------
library(plyr)
metadata <- rbind(r1,r2,r3)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)


