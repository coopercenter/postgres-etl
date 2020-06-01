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
rooftop_solar_low_income <- read.csv(here('raw_data','rooftop_solar_low_income.csv'))
rooftop_solar_low_income %>% filter(state_name =='Virginia') -> rooftop_solar_low_income

upload <- function(df){
  df_name <- deparse(substitute(df))
  dbWriteTable(db, df_name, df, row.names=FALSE, overwrite = TRUE)
}

upload(rooftop_solar_low_income)

#close db connection
dbDisconnect(db)