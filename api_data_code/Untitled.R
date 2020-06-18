library(tidyverse)
library(stringr)
library(here)
library(httr)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

#load in data from EPA
virginia_ghg <- "https://data.epa.gov/efservice/V_GHG_EMITTER_SECTOR/CSV"
virginia_ghg <- read_csv(virginia_ghg)
virginia_ghg %>% filter(V_GHG_EMITTER_SECTOR.STATE == 'VA') -> virginia_ghg
colnames(virginia_ghg)<-tolower(colnames(virginia_ghg))
virginia_ghg <- virginia_ghg[,c(1:3,5:20)]



#upload to db
dbWriteTable(db, 'virginia_ghg', virginia_ghg, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)


