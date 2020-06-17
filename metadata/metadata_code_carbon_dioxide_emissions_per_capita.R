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

con_per_cap<-dbGetQuery(db,'SELECT * from carbon_dioxide_emissions_per_capita_va')
con_per_cap_cols <- list(c('year','emissions_per_capita'))
con_per_cap_units <-'Metric tons CO2 per person'

r1<- data.frame(db_table_name = "co2_emission_per_capita_va",
                short_series_name= 'VA CO2 emission per capita',
                full_series_name = 'Virginia carbon dioxide emissions per capita from 1980 through 2017',
                column2variable_name_map=I(con_per_cap_cols),units=I(con_per_cap_units),frequency='A',
                data_source_brief_name=I(list(c('EIA','FRED'))),data_source_full_name=I(list(c('Energy Information Administration','Federal Reserve Economic Data'))),
                url=NA,api=I(list(c('http://api.eia.gov/series/?api_key=7ee3cdbf1ded6bcfb9de1e50d722ebd4&series_id=EMISS.CO2-TOTV-TT-TO-VA.A','https://fred.stlouisfed.org/series/VAPOP'))),
                series_id=I(list(c('EMISS.CO2-TOTV-TT-TO-VA.A','VAPOP'))),json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=I(list(c('eia_emiss_co2_totv_tt_to_va_a','residential_population_va'))), 
                R_script='deriving_carbon_dioxide_emissions_per_capita')
dbWriteTable(db, 'metadata', value = r1, append = TRUE, overwrite = FALSE, row.names = FALSE)
