# Connection to the database
# "my_postgres_credentials.R" contains the log-in informations of RAs
library(here)
library('RPostgreSQL')
library(tidyverse)
source(here("api_data_code", "my_postgres_credentials.R"))
db_driver <- dbDriver("PostgreSQL")

db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)

rm(ra_pwd)

# check the connection
dbExistsTable(db, "metadata")
metadata <- dbGetQuery(db,'SELECT * from metadata')
# TRUE

# ----------------------------------------------------------------------------------
emissions_co2_by_source_va<-dbGetQuery(db,'SELECT * from emissions_co2_by_source_va')

colnames(emissions_co2_by_source_va)
co2_by_source_cols <- list(c('Year','Coal','Natural_gas','other','petroleum',
                             'total'))
co2_by_source_units <-'megawatthour'

r1<- data.frame(db_table_name = "emissions_co2_by_source_va",
                short_series_name= 'Total megawatt hour of co2 emissions',
                full_series_name = 'Total megawatt hour of co2 emissions in phases from 1990 through 2018',
                column2variable_name_map=I(co2_by_source_cols ),units=I(co2_by_source_units),frequency='A',
                data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                url='https://www.eia.gov/electricity/state/virginia/',api=NA,
                series_id=NA,json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=NA, 
                R_script='cleaning_emissions.R')

# ----------------------------------------------------------------------------------
emissions_no_by_source_va<-dbGetQuery(db,'SELECT * from emissions_no_by_source_va')

colnames(emissions_no_by_source_va)
no_by_source_cols <- list(c('Year','Coal','Natural_gas','other','petroleum',
                            'total'))
no_by_source_units <- 'megawatt hour'

r2<- data.frame(db_table_name = "emissions_no_by_source_va",
                short_series_name='Total megawatt hour of no emissions',
                full_series_name = 'Total megawatt hour of no emissions in phases from 1990 through 2018',
                column2variable_name_map=I(no_by_source_cols),units=I(no_by_source_units),frequency='A',
                data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                url='https://www.eia.gov/electricity/state/virginia/',api=NA,
                series_id=NA,json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=NA, 
                R_script='cleaning_emissions.R')

# ----------------------------------------------------------------------------------
emissions_so2_by_source_va<-dbGetQuery(db,'SELECT * from emissions_so2_by_source_va')
colnames(emissions_so2_by_source_va)
so2_by_source_cols<- list(c('Year','Coal','Natural_gas','other','petroleum',
                            'total'))
so2_by_source_units <- 'megawatt hour'
r3<- data.frame(db_table_name = "emissions_so2_by_source_va",
                short_series_name = 'Total megawatt hour of so2 emissions',
                full_series_name = 'Total megawatt hour of no emissions in phases from 1990 through 2018',
                column2variable_name_map=I(so2_by_source_cols),units=I(so2_by_source_units),frequency='A',
                data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                url='https://www.eia.gov/electricity/state/virginia/',api=NA,
                series_id=NA,json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=NA, 
                R_script='cleaning_emissions.R')

library(plyr)
metadata<-rbind(r1,r2,r3)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)
