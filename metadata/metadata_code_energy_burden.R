# Sample Code used to manually write the metadata

# Connection to the database
# "my_postgres_credentials.R" contains the log-in informations of RAs
library(here)
library('RPostgreSQL')
library(tidyverse)
source(here("etl", "my_postgres_credentials.R"))
db_driver <- dbDriver("PostgreSQL")
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

# check the connection
# if this returns true, it means that you are connected to the database now
dbExistsTable(db, "metadata")

# get the cleaned dataset from the database
## fuel_cleaned should be replaced by the name of your dataset in the database
## (coordinate with Jackson)
energy_burden_by_fuel_type<-dbGetQuery (db,'SELECT * from energy_burden_by_fuel_type')
energy_burden_county_expenditures<-dbGetQuery (db,'SELECT * from energy_burden_county_expenditures')
energy_burden_county_percent_income<-dbGetQuery (db,'SELECT * from energy_burden_county_percent_income')

# put the column names into a list
energy_burden_by_fuel_type_cols<-list(colnames(energy_burden_by_fuel_type))
energy_burden_county_expenditures_cols<-list(colnames(energy_burden_county_expenditures))
energy_burden_county_percent_income_cols<-list(colnames(energy_burden_county_percent_income))

# put the unit of each column into a list
energy_burden_by_fuel_type_units <-list(c('Fuel_Type','percent','dollar','dollar','dollar','dollar','percent'))
energy_burden_county_expenditures_units <-list(c('county', 'avg_annual_energy_cost'))
energy_burden_county_percent_income_units <-list(c('county', 'avg_energy_burden_as_percent_income'))

# Construct a data frame with only one row, if you have more than one dataset,
# construct r2, r3,... if needed
r1<- data.frame(db_table_name = "energy_burden_by_fuel_type", 
                short_series_name = "Energy burden by fuel type",
                full_series_name = 'Energy cost annually by fuel type',
                column2variable_name_map=I(energy_burden_by_fuel_type_cols),units=I(energy_burden_by_fuel_type_units),frequency='O',
                data_source_brief_name='LEAD',data_source_full_name='Low-Income Energy Affordability Data Tool',
                url='https://www.energy.gov/eere/slsc/maps/lead-tool',api=NA,
                series_id=NA,json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=NA, 
                R_script='cleaning_eneryg_burden.R')

r2<- data.frame(db_table_name = "energy_burden_county_expenditures",
                short_series_name = "Energy burden by county",
                full_series_name = 'Energy cost annually per county',
                column2variable_name_map=I(energy_burden_county_expenditures_cols),units=I(energy_burden_county_expenditures_units),frequency='O',
                data_source_brief_name='LEAD',data_source_full_name='Low-Income Energy Affordability Data Tool',
                url='https://www.energy.gov/eere/slsc/maps/lead-tool',api=NA,
                series_id=NA,json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=NA, 
                R_script='cleaning_eneryg_burden.R')

r3<- data.frame(db_table_name = "energy_burden_county_percent_income",
                short_series_name = "Energy burden per percent income",
                full_series_name = 'Energy cost annually per percent income',
                column2variable_name_map=I(energy_burden_county_percent_income_cols),units=I(energy_burden_county_percent_income_units),frequency='O',
                data_source_brief_name='LEAD',data_source_full_name='Low-Income Energy Affordability Data Tool',
                url='https://www.energy.gov/eere/slsc/maps/lead-tool',api=NA,
                series_id=NA,json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=NA, 
                R_script='cleaning_eneryg_burden.R')

library(plyr)
metadata <- rbind(r1, r2, r3)


# Append your rows to the metadata table in our database
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)

#close db connection
dbDisconnect(db)
