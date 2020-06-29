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
retailers <- dbGetQuery(db,'SELECT * from retailers')
retailers_cols <- list(colnames(retailers))

r1 <- data.frame(db_table_name = "retailers",
                 short_series_name= 'The top five retailers of electricity in Virginia, with end-use sectors 2018',
                 full_series_name = 'The top five retailers of electricity in Virginia, with end-use sectors 2018',
                 column2variable_name_map=I(retailers_cols), units='megawatthour', frequency=NA,
                 data_source_brief_name='EIA', data_source_full_name='U.S. Energy Information Administration',
                 url='https://www.eia.gov/electricity/state/virginia/', api=NA, series_id=NA,json=NA, 
                 notes= 'From EIA-861 (Annual Electric Power Industry Report)',
                 data_type='cross-sectional', data_context='historical', corresponding_data=NA, 
                 R_script='cleaning_retailers.R',
                 latest_data_update='2018-12-31', last_db_refresh='2020-05-01')

# ----------------------------------------------------------------------------------
library(plyr)
dbWriteTable(db, 'metadata2', value = r1, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)


