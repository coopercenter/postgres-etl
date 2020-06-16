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

con_per_cap<-dbGetQuery(db,'SELECT * from carbon_dioxide_emissions_per_gdp_va')
con_per_cap_cols <- list(c('year','emissions_per_unit_of_GDP'))
con_per_cap_units <-'Billion Btu per ten billion dollars'

r1<- data.frame(db_table_name = "carbon_dioxide_emissions_per_gdp_va",
                short_series_name= 'VA CO2 emission per unit of gdp',
                full_series_name = 'Virginia carbon dioxide emissions per unit of gdp from 1997 through 2017',
                column2variable_name_map=I(con_per_cap_cols),units=I(con_per_cap_units),frequency='A',
                data_source_brief_name='FRED',data_source_full_name='Federal Reserve Economic Data',
                url=NA,api=I(list(c('https://fred.stlouisfed.org/series/VANGSP','https://fred.stlouisfed.org/series/VAPOP'))),
                series_id=I(list(c('VANGSP','VAPOP'))),json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=I(list(c('fred_vangsp','residential_population_va'))), 
                R_script='deriving_carbon_dioxide_emissions_per_GDP')
dbWriteTable(db, 'metadata', value = r1, append = TRUE, overwrite = FALSE, row.names = FALSE)
