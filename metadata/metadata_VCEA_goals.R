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
VCEA_energy_efficiency <- dbGetQuery(db,'SELECT * from "VCEA_energy_efficiency"')
VCEA_energy_efficiency_cols <- list(colnames(VCEA_energy_efficiency))

r1 <- data.frame(db_table_name = "VCEA_energy_efficiency",
                short_series_name= 'Energy Efficiency Targets as Percentage of 2019 jurisdictional retail sales',
                full_series_name = 'Energy : Efficiency Targets : Share of 2019 Sales',
                column2variable_name_map=I(VCEA_energy_efficiency_cols), units='percent', frequency='A',
                data_source_brief_name='UVA ELC', data_source_full_name='Environmental Law and Regulatory Clinic at the University of Virginia',
                url=NA, api=NA, series_id=NA,json=NA, 
                notes= NA, data_type='time-series', data_contex='mandate',
                corresponding_data=NA, R_script='cleaning_VCEA_goals.R',
                latest_data_update=, last_db_refresh=)

# ----------------------------------------------------------------------------------
VCEA_ws <- dbGetQuery(db,'SELECT * from "VCEA_onshore_wind_solar"')
VCEA_ws_cols <- list(colnames(VCEA_ws))

r2 <- data.frame(db_table_name = "VCEA_onshore_wind_solar",
                short_series_name= 'Procurement targets for renewable installations by wind or solar',
                full_series_name = 'Procurement Targets : Onshore : Wind : Solar : MegaWatts',
                column2variable_name_map=I(VCEA_ws_cols), units='MW', frequency='A',
                data_source_brief_name='UVA ELC', data_source_full_name='Environmental Law and Regulatory Clinic at the University of Virginia',
                url=NA, api=NA, series_id=NA, json=NA,
                notes= NA, 
                data_type='time-series', data_contex='mandate', 
                corresponding_data=NA, R_script='cleaning_VCEA_goals.R',
                latest_data_update=, last_db_refresh=)

# ----------------------------------------------------------------------------------
VCEA_portfolio <- dbGetQuery(db,'SELECT * from "VCEA_renewable_portfolio_standards"')
VCEA_portfolio_cols <- list(colnames(VCEA_portfolio))

r3 <- data.frame(db_table_name = "VCEA_renewable_portfolio_standards",
                 short_series_name= 'Renewable Portfolio Standard',
                 full_series_name = 'Renewable Portfolio Standard',
                 column2variable_name_map=I(VCEA_portfolio_cols), units='MW', frequency='A',
                 data_source_brief_name='UVA ELC', data_source_full_name='Environmental Law and Regulatory Clinic at the University of Virginia',
                 url=NA,api=NA, series_id=NA, json=NA, 
                 notes= NA, 
                 data_type='time-series', data_contex='mandate', 
                 corresponding_data=NA, R_script='cleaning_VCEA_goals.R',
                 latest_data_update=, last_db_refresh=)

# ----------------------------------------------------------------------------------
VCEA_storage <- dbGetQuery(db,'SELECT * from "VCEA_storage"')
VCEA_storage_cols <- list(colnames(VCEA_storage))

r4 <- data.frame(db_table_name = "VCEA_storage",
                 short_series_name= 'Procurement targets for energy storage',
                 full_series_name = 'Storage Goals : MegaWatts',
                 column2variable_name_map=I(VCEA_storage_cols), units='MW', frequency='A',
                 data_source_brief_name='UVA ELC', data_source_full_name='Environmental Law and Regulatory Clinic at the University of Virginia',
                 url=NA, api=NA, series_id=NA, json=NA, 
                 notes= NA, 
                 data_type='time-series', data_contex='mandate', 
                 corresponding_data=NA, R_script='cleaning_VCEA_goals.R',
                 latest_data_update=, last_db_refresh=)

# ----------------------------------------------------------------------------------

library(plyr)
metadata <- rbind(r1,r2,r3,r4)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)

