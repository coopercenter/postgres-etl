library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

# read in dataset
prod <- read.csv(here('raw_data', 'total_production.csv'))
va_prod <- prod[prod['StateCode']=='VA',]
va_total_prod <- va_prod[va_prod['MSN']=='TEPRB',]
va_total_prod <- va_total_prod[,4:ncol(va_total_prod)]

va_total_prod <- as.data.frame(t(va_total_prod))

rownames(va_total_prod)<-str_replace_all(rownames(va_total_prod),'X','')
colnames(va_total_prod)<- 'Total_Energy_Production_in_Billion_Btu'

#write.csv(va_total_prod,file = 'va_total_prod_1960_to_2017.csv') -- remove statement

#upload to db
dbWriteTable(db, 'va_total_prod_1960_to_2017', va_total_prod, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)