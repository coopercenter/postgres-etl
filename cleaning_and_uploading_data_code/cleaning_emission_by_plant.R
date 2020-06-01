library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

plant_emission <- read.csv(here("raw_data","emission_by_plant_va.csv"),header=F)
e_table<-plant_emission[5:106,-2]
names(e_table)<-lapply(e_table[1,],as.character)
e_table <- e_table[-1,]
colnames(e_table)<-str_replace_all(colnames(e_table),' ','_')

#write.csv(e_table,file = 'va_emission_by_plant.csv') -- remove statement

#upload to db
dbWriteTable(db, 'va_emission_by_plant', e_table, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)