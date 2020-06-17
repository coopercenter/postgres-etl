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
metadata <- dbGetQuery(db,'SELECT * from metadata')
# TRUE
# ----------------------------------------------------------------------------------

annual_cf <- dbGetQuery(db,'SELECT * from capacity_factors_annual')

annual_cf_cols <- list(colnames(annual_cf))
annual_cf_units <-'MW'

r1<- data.frame(db_table_name = "capacity_factors_annual",
                short_series_name= 'capacity factors in Virginia, annual',
                full_series_name = 'capacity factors in Virginia by fuel type, annual',
                column2variable_name_map=I(annual_cf_cols), units=annual_cf_units, frequency='A',
                data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                url='https://www.eia.gov/electricity/state/virginia/',
                api=NA, series_id=NA,json=NA,notes=NA, data_type='time-series',data_context='historical',
                corresponding_data=NA, R_script='cleaning_annual_capacity_factors.R')

# ----------------------------------------------------------------------------------

monthly_cf <- dbGetQuery(db,'SELECT * from capacity_factors_monthly')

monthly_cf_cols <- list(colnames(monthly_cf))
monthly_cf_units <-'MW'

r1<- data.frame(db_table_name = "capacity_factors_monthly",
                short_series_name= 'capacity factors in Virginia, monthly',
                full_series_name = 'capacity factors in Virginia by fuel type, monthly',
                column2variable_name_map=I(monthly_cf_cols), units=monthly_cf_units, frequency='A',
                data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                url='https://www.eia.gov/electricity/state/virginia/',
                api=NA, series_id=NA,json=NA,notes=NA, data_type='time-series',data_context='historical',
                corresponding_data=NA, R_script='cleaning_capacity_monthly.R')

# ----------------------------------------------------------------------------------

library(plyr)
metadata <- rbind(r1,r2)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)

