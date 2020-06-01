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
energy.efficiency <- read.csv(here('raw_data','energy_efficiency.csv'))
energy.efficiency <- as.data.frame(t(energy.efficiency))
energy.efficiency <- energy.efficiency[,2:44]
names(energy.efficiency)<-lapply(energy.efficiency[1,],as.character)
energy.efficiency <- energy.efficiency[-1,]
colnames(energy.efficiency)[1] <- 'Year'
energy.efficiency[,1] <- str_replace_all(energy.efficiency[,1], 'Year', '')

#write.csv(energy.efficiency, "Energy_Efficiency_Clean.csv") -- remove statement

#upload to db
dbWriteTable(db, 'energy_efficiency', energy.efficiency, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)
