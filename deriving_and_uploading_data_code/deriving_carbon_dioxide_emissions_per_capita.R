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
pop_s60<- as.numeric(pop[81:118,4])*1000 
#Now this information will show total persons.
tot_emission <- dbGetQuery(db,'SELECT * from eia_emiss_co2_totv_tt_to_va_a') 
# original unit of emission is million metric tons CO2
tot_s60<- tot_emission[nrow(tot_emission):1,1]*1000000 #1980 to 2017
# now it's metric tons

# derive the values
e_per_cap <- tot_s60/pop_s60
e_per_cap_df <- data.frame(1980:2017,e_per_cap)
colnames(e_per_cap_df)<- c('year','co2_emission_per_capita')

#upload to db
dbWriteTable(db, 'co2_emission_per_capita', e_per_cap_df, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)
