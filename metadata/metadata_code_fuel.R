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
fuel <- dbGetQuery(db,'SELECT * from fuel')
fuel_cols <- list(colnames(fuel))
fuel_units <- list(c('year','dollars per million Btu', 'Btu per pound','percent',
                     'dollars per million Btu','Btu per gallon','percent','dollars per million Btu',
                     'Btu per cubic foot'))

r1 <- data.frame(db_table_name = "fuel",
                 short_series_name= 'Electric power delivered fuel prices and quality',
                 full_series_name = 'Electric power delivered fuel prices and quality for coal, petroleum, natural gas, 1990 through 2018',
                 column2variable_name_map=I(fuel_cols), units=I(fuel_units), frequency='A',
                 data_source_brief_name='EIA', data_source_full_name='U.S. Energy Information Administration',
                 url='https://www.eia.gov/electricity/state/virginia/', api=NA, series_id=NA,json=NA, 
                 notes= 'From EIA-423 (Monthly Cost and Quality of Fuels for Electric Plants Report),
                 EIA-923 (Power Plant Operations Report), and Federal Energy Regulatory Commission 
                 (FERC) Form 423 (Monthly Cost and Quality of Fuels for Electric Plants)', 
                 data_type='time-series', data_context='historical', corresponding_data=NA, R_script='cleaning_fuel.R',
                 latest_data_update=, last_db_refresh=)

# ----------------------------------------------------------------------------------
library(plyr)
dbWriteTable(db, 'metadata', value = r1, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)


