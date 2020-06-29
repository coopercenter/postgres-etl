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
elec_ipp_chp_capacity <- dbGetQuery(db,'SELECT * from electric_ipp_chp_capacity')
elec_ipp_chp_capacity_cols <- list(colnames(elec_ipp_chp_capacity))

r1 <- data.frame(db_table_name = "electric_ipp_chp_capacity",
                 short_series_name= 'electric power industry capacity from ipp and chp in Virginia',
                 full_series_name = 'electric power industry capacity from independent power producers and combined heat and power in Virginia from 1990-2018',
                 column2variable_name_map=I(elec_ipp_chp_capacity_cols), units='MW', frequency='A',
                 data_source_brief_name='EIA', data_source_full_name='U.S. Energy Information Administration',
                 url='https://www.eia.gov/electricity/state/virginia/', api=NA, series_id=NA,json=NA, 
                 notes= NA, data_type='time-series',data_context='historical',
                 corresponding_data=NA, R_script='cleaning_capacity.R',
                 latest_data_update='2018-12-31', last_db_refresh='2020-05-01')

# ----------------------------------------------------------------------------------
elec_utility_capacity <- dbGetQuery(db,'SELECT * from electric_utility_capacity')
elec_utility_capacity_cols <- list(colnames(elec_utility_capacity))

r2 <- data.frame(db_table_name = "electric_utility_capacity",
                 short_series_name= 'electric power industry capacity from utilities',
                 full_series_name = 'electric power industry capacity from utilities in Virginia from 1990-2018',
                 column2variable_name_map=I(elec_utility_capacity_cols), units='MW', frequency='A',
                 data_source_brief_name='EIA', data_source_full_name='U.S. Energy Information Administration',
                 url='https://www.eia.gov/electricity/state/virginia/', api=NA, series_id=NA,json=NA, 
                 notes= NA, data_type='time-series',data_context='historical',
                 corresponding_data=NA, R_script='cleaning_capacity.R',
                 latest_data_update='2018-12-31', last_db_refresh='2020-05-01')

# ----------------------------------------------------------------------------------
whole_elec_capacity <- dbGetQuery(db,'SELECT * from whole_electric_industry_capacity')
whole_elec_capacity_cols <- list(colnames(whole_elec_capacity))

r3 <- data.frame(db_table_name = "whole_electric_industry_capacity",
                 short_series_name= 'electric power industry capacity by energy source',
                 full_series_name = 'electric power industry capacity in Virginia from 1990-2018',
                 column2variable_name_map=I(whole_elec_capacity_cols), units='MW', frequency='A',
                 data_source_brief_name='EIA', data_source_full_name='U.S. Energy Information Administration',
                 url='https://www.eia.gov/electricity/state/virginia/', api=NA, series_id=NA,json=NA, 
                 notes= NA, data_type='time-series',data_context='historical',
                 corresponding_data=NA, R_script='cleaning_capacity.R',
                 latest_data_update='2018-12-31', last_db_refresh='2020-05-01')

# ----------------------------------------------------------------------------------
library(plyr)
metadata <- rbind(r1,r2,r3)
dbWriteTable(db, 'metadata2', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)


