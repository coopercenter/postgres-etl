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

#upload to db
dbWriteTable(db, 'VCEA_goals', VCEA, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)