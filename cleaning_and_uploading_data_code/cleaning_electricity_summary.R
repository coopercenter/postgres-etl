library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

summary_e <- read.csv(here('raw_data','summary.csv'))
summary_e <- summary_e[2:21,1:3]
#names(summary_e)<-lapply(summary_e[1,],as.character)
names(summary_e)<-c('Data','Value','Rank')
summary_e <- summary_e[-1,]


#upload to db
dbWriteTable(db, 'electricity_summary', summary_e, row.names=FALSE, overwrite = TRUE)

# change to electricity_summary_2018

#close db connection
dbDisconnect(db)