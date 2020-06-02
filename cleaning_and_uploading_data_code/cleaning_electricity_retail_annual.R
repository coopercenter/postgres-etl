library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

retail <- read.csv(here('raw_data',"retail_sales_of_electricity_va_annual.csv"),header = F)
retail <- retail[5:nrow(retail),]
names(retail)<-lapply(retail[1,],as.character)
retail <-retail[-1,]
retail6<- as.numeric(gsub("--", NA, retail[,6]))
retail7<- as.numeric(gsub("--", NA, retail[,7]))
retail[,6:7]<-c(retail6,retail7)
colnames(retail)<-str_replace_all(colnames(retail),' ','_')
retail_ordered <-retail[nrow(retail):1,]
rownames(retail_ordered) <- NULL
colnames(retail_ordered)<-tolower(colnames(retail_ordered))


#write.csv(retail_ordered,file = 'retail_sales_of_electricity_annual_clean.csv') -- remove statement

#upload to db
dbWriteTable(db, 'retail_sales_of_electricity_annual', retail, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)