library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")
library(readxl)

db_driver = dbDriver("PostgreSQL")
source(here('raw_data'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

#read in dataset
dominion_current_EE_programs <- read_excel(here('raw_data','energy_efficiency_programs.xlsx'), col_names = TRUE)
dominion_current_EE_data_through_2018 <- dominion_current_EE_programs[c(2,4:8),]

#upload to db
dbWriteTable(db, 'dominion_current_EE_data_through_2018', dominion_current_EE_data_through_2018, row.names=FALSE, overwrite = TRUE)

#close db connection
dbDisconnect(db)




















