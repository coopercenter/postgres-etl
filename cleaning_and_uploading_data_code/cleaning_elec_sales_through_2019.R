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
elec_sales_through_2019 <- read_excel(here('raw_data','elec_sales_forecasts_2019-12.xlsx'),sheet = "Annual",col_names = TRUE)
elec_sales_through_2019 <- select(elec_sales_through_2019, c(1,3,5,15,17))

#upload to db
dbWriteTable(db, 'elec_sales_through_2019', elec_sales_through_2019, row.names=FALSE, overwrite = TRUE)

#close db connection
dbDisconnect(db)




















