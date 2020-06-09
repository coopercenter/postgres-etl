library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library(readxl)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

VCEA <- read_excel(here('raw_data','VCEA_goals.xlsx')) 
VCEA <- as.data.frame(VCEA)

#Creating apco_dominion_rps dataset 
renewable_portfolio_standards <- select(VCEA, c(1:3))

#Creating apco_dominion_onshore_wind_solar
apco_dominion_onshore_wind_solar <- select(VCEA, c(1, 4:5))

#Creating apco_dominion_storage
apco_dominion_storage <- select(VCEA, c(1, 6:7))

#Creating apco_dominion_energy_effciency
apco_dominion_energy_effciency <- select(VCEA, c(1, 8:9))


#upload to db
dbWriteTable(db, 'renewable_portfolio_standards', renewable_portfolio_standards, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'apco_dominion_onshore_wind_solar', apco_dominion_onshore_wind_solar, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'apco_dominion_storage', apco_dominion_storage, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'apco_dominion_energy_effciency', apco_dominion_energy_effciency, row.names=FALSE, overwrite = TRUE)

#close db connection
dbDisconnect(db)