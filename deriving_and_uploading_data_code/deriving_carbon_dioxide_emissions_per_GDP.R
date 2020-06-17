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

# get the datasets
gdp <- dbGetQuery(db,'SELECT * from fred_vangsp')
# unit of gdp is millions of dollars
gdp_s60<- as.numeric(gdp[1:21,2])*1000000 #Now this information will show total dollars. 1997 to 2017
tot_emission <- dbGetQuery(db,'SELECT * from eia_emiss_co2_totv_tt_to_va_a') 
# original unit of emission is million metric tons CO2
tot_s60<- tot_emission[21:1,1]*1000000 #1997 to 2017

# derive the values
e_per_gdp <- tot_s60/gdp_s60
e_per_gdp_df <- data.frame(1997:2017,e_per_gdp)
colnames(e_per_gdp_df)<- c('year','co2_emission_per_unit_of_GDP')

#upload to db
dbWriteTable(db, 'carbon_dioxide_emissions_per_gdp_va', e_per_gdp_df, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)
