library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

here('raw_data','capacity_factors_monthly.csv')
#read in dataset
capacity.factors.monthly <- read.csv(here('raw_data','capacity_factors_monthly.csv'))
capacity.factors.monthly <- as.data.frame(t(capacity.factors.monthly))
capacity.factors.monthly <- capacity.factors.monthly[,2:19]
capacity.factors.monthly <- select(capacity.factors.monthly, V2, V4, V5,V6, V7,V8,V9,V10,V11,V12,V13,V14,V15,V16,V18,V19)

names(capacity.factors.monthly)<-lapply(capacity.factors.monthly[1,],as.character)
capacity.factors.monthly <- capacity.factors.monthly[-1,]
colnames(capacity.factors.monthly)[1] <- 'Month'

#write.csv(capacity.factors.monthly, "Capacity Factors Monthly .csv") -- remove statement

#upload to db
dbWriteTable(db, 'capacity_factors_monthly', capacity.factors.monthly, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)