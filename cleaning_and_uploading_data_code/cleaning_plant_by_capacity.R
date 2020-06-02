library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

capacity <- read.csv(here('raw_data','2A_Capacity.csv'))
library(dplyr)
capacity <- capacity[2:12,2:5]
names(capacity)<-lapply(capacity[1,],as.character)
capacity <- capacity[-1,]
colnames(capacity)<-tolower(colnames(capacity))
colnames(capacity)<-str_replace_all(colnames(capacity),' ','_')
capacity[,4]<-as.numeric(gsub(",", "", capacity[,4]))
#upload to db
dbWriteTable(db, 'plant_by_capacity_va', retail, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)