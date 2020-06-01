library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")
library(readxl)

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

#read in dataset
utility_scale_storage_va <- read_excel(here('raw_data','utility_scale_storage.xls'))
colnames(utility_scale_storage_va)<-str_replace_all(colnames(utility_scale_storage_va),' ','_')
utility_scale_storage_va %>% filter(State_Code=='VA') -> utility_scale_storage_va

upload <- function(df){
  df_name <- deparse(substitute(df))
  dbWriteTable(db, df_name, df, row.names=FALSE, overwrite = TRUE)
}

upload(utility_scale_storage_va)

#close db connection