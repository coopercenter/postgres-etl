library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

capacity_factors_monthly <- read.csv(here('raw_data','capacity_factors_monthly.csv'))
year <- colnames(capacity_factors_monthly[1])
year <- str_sub(year, start=-4)
capacity_factors_monthly <- as.data.frame(t(capacity_factors_monthly))
capacity_factors_monthly <- capacity_factors_monthly[,2:19]
capacity_factors_monthly <- capacity_factors_monthly[-c(2)]
names(capacity_factors_monthly)<-lapply(capacity_factors_monthly[1,],as.character)
capacity_factors_monthly <- capacity_factors_monthly[-1,]
colnames(capacity_factors_monthly)[1] <- 'Month'
table_name <- paste('capacity_factors_monthly_',year,sep="")

#upload to db
dbWriteTable(db, table_name, capacity_factors_monthly, row.names=FALSE, overwrite = TRUE)

#close db connection
dbDisconnect(db)
