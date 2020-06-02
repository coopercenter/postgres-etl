library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

fuel <- read.csv(here('raw_data','fuel_uncleaned.csv')) 
fuel <- as.data.frame(t(fuel))
fuel <- fuel[,2:10]
names(fuel)<-lapply(fuel[1,],as.character)
fuel <- fuel[-1,]

fuel[,1] <-str_replace_all(fuel[,1],'Year','')
colnames(fuel) <- c('year','coal','average_heat_value_coal','average_sulfur_content_coal',
                    'petroleum','average_heat_value_petroleum','average_sulfur_content_petroleum',
                    'natural_gas','average_heat_value_natural_gas')

#upload to db
dbWriteTable(db, 'fuel', fuel, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)