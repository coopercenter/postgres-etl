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
generation <- here('raw_data','plant_generation_va.csv')
summary(generation)
generation <- select(generation, X, X.1, X.2, X.3)
generation <- generation[2:12,]
names(generation) <- lapply(generation[1, ], as.character)
generation <- generation[-1, ] 
rownames(generation) <- c()

#write.csv(generation, "Plant_Generation_Data_Virginia.csv") -- remove statement

#upload to db
dbWriteTable(db, 'plant_generation_data_va', generation, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)