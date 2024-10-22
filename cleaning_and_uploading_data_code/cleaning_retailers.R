library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

#read in data
retailers <- read.csv(here('raw_data','retailers.csv'))
year <- colnames(retailers[1])
year <- str_sub(year, start=-4)
retailers <- select(retailers,X,X.1,X.2,X.3,X.4,X.5,X.6)
retailers <- retailers[3:10,]
names(retailers)<-lapply(retailers[1,],as.character)
retailers <- retailers[-1,]
colnames(retailers)<-str_replace_all(colnames(retailers),' ','_')
colnames(retailers)<-tolower(colnames(retailers))

for (i in 3:ncol(retailers)){
  retailers[,i]<-as.numeric(gsub(",", "", retailers[,i]))
}

table_name <- paste('retailers_',year,sep="")

#upload to db
dbWriteTable(db, table_name, retailers, row.names=FALSE, overwrite = TRUE)

#close db connection
dbDisconnect(db)