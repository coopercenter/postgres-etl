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

#-----------------------------------------------------------------------------------------------------
con_per_cap_cols <- list(c('year','consumption_per_capita'))
con_per_cap_units <-'Billion Btu per person'

r1 <- data.frame(db_table_name = "energy_consumption_per_capita_va",
                short_series_name= 'VA energy consumption per capita',
                full_series_name = 'Virginia energy consumption per capita from 1960 through 2018',
                column2variable_name_map=I(con_per_cap_cols),units=I(con_per_cap_units),frequency='A',
                data_source_brief_name=I(list(c('EIA','FRED'))),data_source_full_name=I(list(c('U.S. Energy Information Administration','Federal Reserve Economic Data'))),
                url=NA,api=I(list(c('http://api.eia.gov/series/?api_key=7ee3cdbf1ded6bcfb9de1e50d722ebd4&series_id=SEDS.TETCB.VA.A','https://fred.stlouisfed.org/series/VAPOP'))),
                series_id=I(list(c('SEDS.TETCB.VA.A','VAPOP'))),json=NA,notes=NA, data_type='time-series', 
                data_context='historical', corresponding_data=I(list(c('eia_seds_tetcb_va_a','residential_population_va'))), 
                R_script='deriving_energy_consumption.R',
                latest_data_update='2018-12-31', last_db_refresh='2020-06-17')

#-----------------------------------------------------------------------------------------------------
energy_consumption_per_gdp_cols <- list(c('year','consumption_per_unit_of_gdp'))
energy_consumption_per_gdp_units <-'Btu per dollar of GDP'

r2 <- data.frame(db_table_name = "energy_consumption_per_unit_of_gdp_va",
                short_series_name= 'VA energy consumption per unit of gdp',
                full_series_name = 'Virginia energy consumption per unit of gdp from 1997 through 2019',
                column2variable_name_map=I(energy_consumption_per_gdp_cols),units=I(energy_consumption_per_gdp_units),frequency='A',
                data_source_brief_name=I(list(c('EIA','FRED'))),data_source_full_name=I(list(c('U.S. Energy Information Administration','Federal Reserve Economic Data'))),
                url=NA,api=I(list(c('http://api.eia.gov/series/?api_key=7ee3cdbf1ded6bcfb9de1e50d722ebd4&series_id=SEDS.TETCB.VA.A,https://fred.stlouisfed.org/series/VANGSP'))),
                series_id=I(list(c('SEDS.TETCB.VA.A','VANGSP'))),json=NA,notes=NA, data_type='time-series', 
                data_context='historical', corresponding_data=I(list(c('fred_vangsp','residential_population_va'))), 
                R_script='deriving_energy_consumption.R',
                latest_data_update='2018-12-31', last_db_refresh='2020-06-17')

#-----------------------------------------------------------------------------------------------------

#upload to db
library(plyr)
metadata<-rbind(r1,r2)
dbWriteTable(db, 'metadata2', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)

#close connection
dbDisconnect(db)
