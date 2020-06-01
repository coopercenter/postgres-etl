library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

#read in dataset
deep_solar <- read.csv(here('raw_data','deep_solar_tract.csv'))
#summary(deep_solar)
#deep_solar <- filter(deep_solar, state=='va')
#write.csv(deep_solar, "Deep_Solar_Tract.csv")
deep_solar <- deep_solar[,3:170]

dbWriteTable(db,'deep_solar',deep_solar, row.names=FALSE)

#close db connection