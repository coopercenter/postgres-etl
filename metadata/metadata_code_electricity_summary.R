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
elec_sum <- dbGetQuery(db,'SELECT * from electricity_summary')
elec_sum_cols <- list(colnames(elec_sum))

r1 <- data.frame(db_table_name = "electricity_summary",
                 short_series_name= 'Summary statistics of the VA Electricity Profile 2018',
                 full_series_name = 'Summary statistics of the VA Electricity Profile 2018',
                 column2variable_name_map=I(elec_sum_cols), units=NA, frequency=NA,
                 data_source_brief_name='EIA', data_source_full_name='U.S. Energy Information Administration',
                 url='https://www.eia.gov/electricity/state/virginia/', api=NA, series_id=NA,json=NA, 
                 notes= "From EIA-860 (Annual Electric Generator Report), 
                 EIA-861 (Annual Electric Power Industry Report), 
                 EIA-923 (Power Plant Operations Report and predecessor forms)", 
                 data_type='cross-sectional', data_context='historical', 
                 corresponding_data=NA, R_script='cleaning_electricity_summary.R',
                 latest_data_update=, last_db_refresh=)

# ----------------------------------------------------------------------------------
library(plyr)
dbWriteTable(db, 'metadata', value = r1, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)


