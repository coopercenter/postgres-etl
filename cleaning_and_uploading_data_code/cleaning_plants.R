library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")
library(readxl)

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

#------------------------------------------------------------------------------
# plants_by_capacity_va

capacity <- read.csv(here('raw_data','plants_by_capacity.csv'))
year <- colnames(capacity[1])
year <- str_sub(year, start=-4)
capacity <- capacity[2:12,2:5]
names(capacity)<-lapply(capacity[1,],as.character)
capacity <- capacity[-1,]
colnames(capacity)<-tolower(colnames(capacity))
colnames(capacity)<-str_replace_all(colnames(capacity),' ','_')
capacity[,4]<-as.numeric(gsub(",", "", capacity[,4]))
table_name_cap <- paste('plants_by_capacity_va_', year, sep="")
dbWriteTable(db, table_name_cap, capacity, row.names=FALSE, overwrite = TRUE)

#------------------------------------------------------------------------------
# plants_by_generation_va

generation <- read.csv(here('raw_data','plant_generation_va.csv'))
year <- colnames(generation[1])
year <- str_sub(year, start=-4)
summary(generation)
generation <- select(generation, X, X.1, X.2, X.3)
generation <- generation[2:12,]
names(generation) <- lapply(generation[1, ], as.character)
generation <- generation[-1, ] 
rownames(generation) <- c()
table_name_gen <- paste('plants_by_generation_va_', year, sep="")
dbWriteTable(db, table_name_gen, generation, row.names=FALSE, overwrite = TRUE)

#------------------------------------------------------------------------------
# va_emission_by_plant

plant_emission <- read.csv(here("raw_data","va_emission_by_plant.csv"),header=F)
e_table<-plant_emission[5:106,-2]
names(e_table)<-lapply(e_table[1,],as.character)
e_table <- e_table[-1,]
colnames(e_table)<-str_replace_all(colnames(e_table),' ','_')
colnames(e_table)<-tolower(colnames(e_table))
dbWriteTable(db, 'va_emission_by_plant', e_table, row.names=FALSE, overwrite = TRUE)

#------------------------------------------------------------------------------
#close db connection
dbDisconnect(db)