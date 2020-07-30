library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

# read in dataset
ex <- read.csv(here("raw_data","ex_all.csv"))
va_ex <- ex[ex['State']=='VA',]
va_ex_total <- va_ex[va_ex['MSN']=='ESTCV',]
va_ex_total <- va_ex_total[,4:ncol(va_ex_total)]
va_ex_total <- as.data.frame(t(va_ex_total))
va_ex_total[,2] <-va_ex_total[,1]
va_ex_total[,1] <- 1970:2017
colnames(va_ex_total)<-c('Year','electricity_total_expenditures_in_million_dollars')

#upload to db
dbWriteTable(db, 'va_electricity_total_ex_1970_to_2017', va_ex_total, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)