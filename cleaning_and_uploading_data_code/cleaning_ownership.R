library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")
library(data.table)

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

#read in dataset
ownership <- read.csv(here("raw_data","ownership_raw.csv"))
ownership<- ownership[3:10,]
names(ownership)<-lapply(ownership[1,],as.character)
ownership<-ownership[-1,]

for (i in 2:ncol(ownership)){
  ownership[,i]<-as.numeric(gsub(",", "", ownership[,i]))
}

colnames(ownership)<-str_replace_all(colnames(ownership),'\n','_')
colnames(ownership)<-str_replace_all(colnames(ownership),'-','_')
colnames(ownership)<-tolower(colnames(ownership))

#Now we are going to make data tidy. Pivoting columns and rows. 
test <- transpose(ownership)
rownames(test) <- colnames(ownership)
colnames(test) <- rownames(ownership)
names(test) <- test[1,]
test <- test[-1,]
test <- setDT(test, keep.rownames = TRUE)[]
colnames(test)[1] <- "Area of Ownership"
ownership <- test

#upload to db
dbWriteTable(db, 'ownership', ownership, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)