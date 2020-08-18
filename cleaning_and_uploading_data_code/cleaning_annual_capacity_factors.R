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
capacity_factors_annual <- as.data.frame(t(capacity_factors_annual))
capacity_factors_annual <- capacity_factors_annual[2:20]
capacity_factors_annual <- capacity_factors_annual[-c(2)]
names(capacity_factors_annual)<-lapply(capacity_factors_annual[1,],as.character)
capacity_factors_annual <- capacity_factors_annual[-1,]
colnames(capacity_factors_annual)[1] <- 'Year'

years <- c(2018, 2017,2016,2015,2014,2013,2012,2011,2010, 2009, 2008)
index <- c(2:12)
i <- c(1:11)

for (val in i){
  capacity_factors_annual[index[i],1] = years[i]
}


dbWriteTable(db, 'capacity_factors_annual', capacity_factors_annual, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)