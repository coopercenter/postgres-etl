library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

here('raw_data','energy_efficiency.csv')
#read in dataset
energy_efficiency <- read.csv(here('raw_data','energy_efficiency.csv'))
reporting_year_incremental_energy_savings <- energy_efficiency[,c(2,4:7)]
for (i in 1:ncol(reporting_year_incremental_energy_savings)){
  reporting_year_incremental_energy_savings[,i]<-as.numeric(gsub(",", "", reporting_year_incremental_energy_savings[,i]))
}
colnames(reporting_year_incremental_energy_savings) <- c('year', 'total','residential','commercial','industrial')

incremental_life_cycle_energy_savings <- energy_efficiency[,c(2,25:28)]
for (i in 1:ncol(incremental_life_cycle_energy_savings)){
  incremental_life_cycle_energy_savings[,i]<-as.numeric(gsub(",", "", incremental_life_cycle_energy_savings[,i]))
}
colnames(incremental_life_cycle_energy_savings) <- c('year', 'total','residential','commercial','industrial')


dbWriteTable(db, 'energy_savings_reporting_year_incremental', reporting_year_incremental_energy_savings, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'energy_savings_incremental_life_cycle', incremental_life_cycle_energy_savings, row.names=FALSE, overwrite = TRUE)
#upload to db
#dbWriteTable(db, 'energy_efficiency', energy.efficiency, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)
