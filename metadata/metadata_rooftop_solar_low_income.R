# Connection to the database
# "my_postgres_credentials.R" contains the log-in informations of RAs
library(here)
library('RPostgreSQL')
library(tidyverse)
source(here("my_postgres_credentials.R"))

db_driver <- dbDriver("PostgreSQL")
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)
# ----------------------------------------------------------------------------------

rooftop_solar <- dbGetQuery(db,'SELECT * from rooftop_solar_low_income')
rooftop_solar_cols <- list(colnames(rooftop_solar))

r1 <- data.frame(db_table_name = "rooftop_solar_low_income",
                 short_series_name= 'Rooftop Energy Potential of Low Income Communities, VA',
                 full_series_name = 'SEEDS II Rooftop Energy Potential of Low Income Communities in America (REPLICA), VA',
                 column2variable_name_map=I(rooftop_solar_cols), units=NA, frequency=NA,
                 data_source_brief_name='NREL', data_source_full_name='National Renewable Energy Laboratory',
                 url='https://data.nrel.gov/submissions/81', api=NA, series_id='10.7799/1432837',json=NA, 
                 notes= "seeds_ii_replica.csv, filtered for VA", 
                 data_type='cross-sectional', data_context='historical', 
                 corresponding_data=NA, R_script='cleaning_rooftop_solar_low_income.R',
                 latest_data_update='2018-04-03', last_db_refresh='2020-05-01')

# ----------------------------------------------------------------------------------
dbWriteTable(db, 'metadata', value = r1, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)
