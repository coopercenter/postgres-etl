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
pop_s60<- as.numeric(pop[61:119,4])*1000
tot_consumption <- dbGetQuery(db,'SELECT * from eia_seds_tetcb_va_a')
# unit of population is billion btu
tot_s60<- tot_consumption[nrow(tot_consumption):1,1]*1000000000

# derive the values
c_per_cap <- tot_s60/pop_s60
c_per_cap_df <- data.frame(1960:2018,c_per_cap)
colnames(c_per_cap_df)<- c('year','consumption_per_capita')

#upload to db
dbWriteTable(db, 'consumption_per_capita_va', c_per_cap_df, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)
