library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

pjm_gats_generators <- read.csv(here("raw_data","pjm_gats_registered_renewable_generators.csv"),header=F)
names(pjm_gats_generators)<-lapply(pjm_gats_generators[1,],as.character)
pjm_gats_generators <- pjm_gats_generators[-1,]
colnames(pjm_gats_generators)<-str_replace_all(colnames(pjm_gats_generators),' ','_')
colnames(pjm_gats_generators)<-tolower(colnames(pjm_gats_generators))

#upload to db
dbWriteTable(db, 'pjm_gats_generators', pjm_gats_generators, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)
