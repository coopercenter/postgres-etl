library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

#read in dataset
capacity_factors_annual <- read.csv(here('raw_data','capacity_factors_annual.csv'))
capacity_factors_annual <- as.data.frame(t(capacity_factors_annual))
capacity_factors_annual <- capacity_factors_annual[2:20]
capacity_factors_annual <- capacity_factors_annual[-c(2)]
names(capacity_factors_annual)<-lapply(capacity_factors_annual[1,],as.character)
capacity_factors_annual <- capacity_factors_annual[-1,]
colnames(capacity_factors_annual)[1] <- 'Year'


years <- capacity_factors_annual[,1] #gets the first column
years <-(years[2:length(years)]) #Assuming the first slot is not a year, start at index 2
years <- regmatches(years, gregexpr("[[:digit:]]+", years)) #only pulls the numbers out of string values

i <- c(2:(length(years)+1)) #count variable. Start at 2 because we're assuming index 1 isn't a year.

for (val in i){ #For loop to go through and change it through.
  print(val)
  capacity_factors_annual[val,1] = years[[val-1]]
}


dbWriteTable(db, 'capacity_factors_annual', capacity_factors_annual, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)
