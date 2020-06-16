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
pop <- dbGetQuery(db,'SELECT * from residential_population_va')
# unit of population is thousands of persons
pop_s60<- as.numeric(pop[81:118,4])*1000 #Now this information will show total persons.
tot_consumption <- dbGetQuery(db,'SELECT * from eia_emiss_co2_totv_tt_to_va_a') 
# unit of consumption is billion btu
tot_s60<- tot_consumption[nrow(tot_consumption):1,1] #1980 to 2017

# derive the values
c_per_cap <- (tot_s60/pop_s60)*1000000
c_per_cap_df <- data.frame(1980:2017,c_per_cap)
colnames(c_per_cap_df)<- c('year','co2_emission_per_capita')

#upload to db
dbWriteTable(db, 'carbon_dioxide_emissions_per_capita_va', c_per_cap_df, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)
