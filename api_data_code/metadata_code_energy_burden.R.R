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
                short_series_name = "Energy Burden by Fuel Type,",
                full_series_name = 'Energy Cost Annually by Fuel Type',
                column2variable_name_map=I(energy_burden_by_fuel_type_cols),units=I(energy_burden_by_fuel_type_units),frequency='O',
                data_source_brief_name='LEAD',data_source_full_name='Low-Income Energy Affordability Data Tool',
                url='https://www.energy.gov/eere/slsc/maps/lead-tool',api=NA,
                series_id=NA,json=NA,notes=NA)
r2<- data.frame(db_table_name = "energy_burden_county_expenditures",
                short_series_name = "Energy Burden by County,",
                full_series_name = 'Energy Cost Annually per County',
                column2variable_name_map=I(energy_burden_county_expenditures_cols),units=I(energy_burden_county_expenditures_units),frequency='O',
                data_source_brief_name='LEAD',data_source_full_name='Low-Income Energy Affordability Data Tool',
                url='https://www.energy.gov/eere/slsc/maps/lead-tool',api=NA,
                series_id=NA,json=NA,notes=NA)
r3<- data.frame(db_table_name = "energy_burden_county_percent_income",
                short_series_name = "Energy Burden per Percent Income,",
                full_series_name = 'Energy Cost Anually per Percent Income',
                column2variable_name_map=I(energy_burden_county_percent_income_cols),units=I(energy_burden_county_percent_income_units),frequency='O',
                data_source_brief_name='LEAD',data_source_full_name='Low-Income Energy Affordability Data Tool',
                url='https://www.energy.gov/eere/slsc/maps/lead-tool',api=NA,
                series_id=NA,json=NA,notes=NA)

library(plyr)
# if you have more than one dataset,rbind the rows first,and then bind it to the metadata
# Example
# r1 <- rbind(r1,r2)
rows<-rbind(r1, r2, r3)
metadata <- rbind(rows)


# WARNING
# Do not run dbWriteTable before you check with Christina and Chloe
# This will overwrite the existing metadata table in the db
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)
