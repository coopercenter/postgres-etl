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
capacity_factors_annual <- read.csv(here('raw_data','capacity_factors_annual.csv'))
capacity_factors_annual <- as.data.frame(capacity_factors_annual)
capacity_factors_annual <- capacity_factors_annual[,2:20]
capacity_factors_annual <- capacity_factors_annual[-c(2)]
#names(capacity_factors_annual)<-lapply(capacity_factors_annual[1,],as.character)
#capacity_factors_annual <- capacity_factors_annual[-1,]
#capacity_factors_annual <- capacity_factors_annual[-1,]
colnames(capacity_factors_annual)[1] <- 'Year'

dbWriteTable(db, 'capacity_factors_annual', capacity_factors_annual, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)