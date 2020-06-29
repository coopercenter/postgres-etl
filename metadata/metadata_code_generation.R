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
                 short_series_name= 'electric generation from ipp and chp in Virginia',
                 full_series_name = 'electric generation from independent power producers and combined heat and power in Virginia from 1990-2018',
                 column2variable_name_map=I(elec_ipp_chp_gen_cols), units='megawatthour', frequency='A',
                 data_source_brief_name='EIA', data_source_full_name='U.S. Energy Information Administration',
                 url='https://www.eia.gov/electricity/state/virginia/', api=NA, series_id=NA,json=NA, 
                 notes= NA, 
                 data_type='time-series', data_context='historical', 
                 corresponding_data=NA, R_script='cleaning_generation_by_sector.R',
                 latest_data_update='2018-12-31', last_db_refresh='2020-05-01')

# ----------------------------------------------------------------------------------
elec_utility_gen <- dbGetQuery(db,'SELECT * from electric_utility_generation')
elec_utility_gen_cols <- list(colnames(elec_utility_gen))

r2 <- data.frame(db_table_name = "electric_utility_generation",
                 short_series_name= 'utility electric generation Virginia',
                 full_series_name = 'utility electric generation in Virginia from 1990-2018',
                 column2variable_name_map=I(elec_utility_gen_cols), units='megawatthour', frequency='A',
                 data_source_brief_name='EIA', data_source_full_name='U.S. Energy Information Administration',
                 url='https://www.eia.gov/electricity/state/virginia/', api=NA, series_id=NA,json=NA, 
                 notes= NA, 
                 data_type='time-series', data_context='historical', 
                 corresponding_data=NA, R_script='cleaning_generation_by_sector.R',
                 latest_data_update='2018-12-31', last_db_refresh='2020-05-01')

# ----------------------------------------------------------------------------------
whole_elec_gen <- dbGetQuery(db,'SELECT * from whole_electric_industry_generation')
whole_elec_gen_cols <- list(colnames(whole_elec_gen))

r3 <- data.frame(db_table_name = "whole_electric_industry_generation",
                 short_series_name= 'electric generation in Virginia, all sources and sectors',
                 full_series_name = 'electric generation in Virginia, all sources and sectors from 1990-2018',
                 column2variable_name_map=I(whole_elec_gen_cols), units='megawatthour', frequency='A',
                 data_source_brief_name='EIA', data_source_full_name='U.S. Energy Information Administration',
                 url='https://www.eia.gov/electricity/state/virginia/', api=NA, series_id=NA,json=NA, 
                 notes= NA, 
                 data_type='time-series', data_context='historical', 
                 corresponding_data=NA, R_script='cleaning_generation_by_sector.R',
                 latest_data_update='2018-12-31', last_db_refresh='2020-05-01')

# ----------------------------------------------------------------------------------
library(plyr)
metadata <- rbind(r1,r2,r3)
dbWriteTable(db, 'metadata2', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)


