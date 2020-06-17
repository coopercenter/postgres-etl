library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")
library(readxl)

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

#read in dataset
pjm_wind <- read_excel(here('raw_data','pjm_wind.xlsx'))
pjm_solar <- read_excel(here('raw_data','pjm_solar.xlsx'))

dbWriteTable(db, 'pjm_wind', pjm_wind, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'pjm_solar', pjm_solar, row.names=FALSE, overwrite = TRUE)

#close db connection
dbDisconnect(db)
