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
energy_burden_by_fuel_type <- read_excel(here('raw_data','energy_burden_by_fuel_type.xlsx'))
energy_burden_county_expenditures <- read_excel(here('raw_data','energy_burden_county_expenditures.xlsx'))
energy_burden_county_percent_income <- read_excel(here('raw_data','energy_burden_county_percent_income.xlsx'))

#clean energy burden by fuel type
energy_burden_by_fuel_type <- energy_burden_by_fuel_type[9:54,2:8]
names(energy_burden_by_fuel_type)<-lapply(energy_burden_by_fuel_type[1,],as.character)
energy_burden_by_fuel_type <- energy_burden_by_fuel_type[-1,]
colnames(energy_burden_by_fuel_type)<-str_replace_all(colnames(energy_burden_by_fuel_type),' ','_')
energy_burden_by_fuel_type[,3:7] <- sapply(energy_burden_by_fuel_type[,3:7], as.numeric)

#clean county expenditure data
energy_burden_county_expenditures <- energy_burden_county_expenditures[9:142,2:3]
energy_burden_county_expenditures <- energy_burden_county_expenditures[-1,]
colnames(energy_burden_county_expenditures)<-c('county', 'avg_annual_energy_cost')
energy_burden_county_expenditures[,2] <- sapply(energy_burden_county_expenditures[,2], as.numeric)

#clean county percent_income
energy_burden_county_percent_income <- energy_burden_county_percent_income[9:142,2:3]
energy_burden_county_percent_income <- energy_burden_county_percent_income[-1,]
colnames(energy_burden_county_percent_income)<-c('county', 'avg_energy_burden_as_percent_income')
energy_burden_county_percent_income[,2] <- sapply(energy_burden_county_percent_income[,2], as.numeric)

upload <- function(df){
  df_name <- deparse(substitute(df))
  dbWriteTable(db, df_name, df, row.names=FALSE, overwrite = TRUE)
}
upload(energy_burden_by_fuel_type)
upload(energy_burden_county_expenditures)
upload(energy_burden_county_percent_income)

#close db connection
