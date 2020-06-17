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

energy_consumption_per_gdp<-dbGetQuery(db,'SELECT * from energy_consumption_per_gdp_va')
energy_consumption_per_gdp_cols <- list(c('year','energy_consumption_per_unit_of_GDP'))
energy_consumption_per_gdp_units <-'billion btu per million dollars'

r1<- data.frame(db_table_name = "energy_consumption_per_gdp_va",
                short_series_name= 'VA energy consumption per unit of gdp',
                full_series_name = 'Virginia energy consumption per unit of gdp from 1997 through 2019',
                column2variable_name_map=I(energy_consumption_per_gdp_cols),units=I(energy_consumption_per_gdp_units),frequency='A',
                data_source_brief_name=I(list(c('EIA','FRED'))),data_source_full_name=I(list(c('Energy Information Administration','Federal Reserve Economic Data'))),
                url=NA,api=I(list(c('http://api.eia.gov/series/?api_key=7ee3cdbf1ded6bcfb9de1e50d722ebd4&series_id=SEDS.TETCB.VA.A,https://fred.stlouisfed.org/series/VANGSP'))),
                series_id=I(list(c('SEDS.TETCB.VA.A','VANGSP'))),json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=I(list(c('fred_vangsp','residential_population_va'))), 
                R_script='deriving_energy_consumption_per_unit_GDP')
dbWriteTable(db, 'metadata', value = r1, append = TRUE, overwrite = FALSE, row.names = FALSE)