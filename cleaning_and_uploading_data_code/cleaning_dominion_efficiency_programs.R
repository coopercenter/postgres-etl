library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")
library(readxl)
library(data.table)

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

#read in dataset
dominion_efficiency <- read_excel(here('raw_data','dominion_energy_efficiency_data.xlsx'))

gross_savings_dominion <- dominion_efficiency[1:15,c(1,3,6,9,12,15)]
net_savings_dominion <- dominion_efficiency[1:15,c(1,4,7,10,13,16)]
program_participants_dominion <- dominion_efficiency[1:15,c(1,5,8,11,14,17)]

#----------------------------------------------------------------------------------
#clean gross savings

gross_savings_dominion <- as.data.frame(t(gross_savings_dominion))
names(gross_savings_dominion)<-lapply(gross_savings_dominion[1,],as.character)
gross_savings_dominion <- gross_savings_dominion[-1,]
colnames(gross_savings_dominion)<-str_replace_all(colnames(gross_savings_dominion),' ','_')
colnames(gross_savings_dominion)<-tolower(colnames(gross_savings_dominion))
gross_savings_dominion <- setDT(gross_savings_dominion, keep.rownames = "year")
gross_savings_dominion <- gross_savings_dominion[,c(1,3:16)]
gross_savings_dominion <- as.data.frame(lapply(gross_savings_dominion, as.character))
for (i in 1:ncol(gross_savings_dominion)){
  gross_savings_dominion[,i]<-as.numeric(gsub(",", "", gross_savings_dominion[,i]))
}

#-----------------------------------------------------------------------------------
#clean net savings

colnames(net_savings_dominion) <- c('...','2017','2016','2015','2014','2013')
net_savings_dominion <- as.data.frame(t(net_savings_dominion))
names(net_savings_dominion)<-lapply(net_savings_dominion[1,],as.character)
net_savings_dominion <- net_savings_dominion[-1,]
colnames(net_savings_dominion)<-str_replace_all(colnames(net_savings_dominion),' ','_')
colnames(net_savings_dominion)<-tolower(colnames(net_savings_dominion))
net_savings_dominion <- setDT(net_savings_dominion, keep.rownames = "year")
net_savings_dominion <- net_savings_dominion[,c(1,3:16)]
net_savings_dominion <- as.data.frame(lapply(net_savings_dominion, as.character))
for (i in 1:ncol(net_savings_dominion)){
  net_savings_dominion[,i]<-as.numeric(gsub(",", "", net_savings_dominion[,i]))
}

#-----------------------------------------------------------------------------------
#clean program participants

colnames(program_participants_dominion) <- c('...','2017','2016','2015','2014','2013')
program_participants_dominion <- as.data.frame(t(program_participants_dominion))
names(program_participants_dominion)<-lapply(program_participants_dominion[1,],as.character)
program_participants_dominion <- program_participants_dominion[-1,]
colnames(program_participants_dominion)<-str_replace_all(colnames(program_participants_dominion),' ','_')
colnames(program_participants_dominion)<-tolower(colnames(program_participants_dominion))
program_participants_dominion <- setDT(program_participants_dominion, keep.rownames = "year")
program_participants_dominion <- program_participants_dominion[,c(1,3:16)]
program_participants_dominion <- as.data.frame(lapply(program_participants_dominion, as.character))
for (i in 1:ncol(program_participants_dominion)){
  program_participants_dominion[,i]<-as.numeric(gsub(",", "", program_participants_dominion[,i]))
}

#-----------------------------------------------------------------------------------
#upload

dbWriteTable(db, 'gross_savings_dominion', gross_savings_dominion, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'net_savings_dominion', net_savings_dominion, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'program_participants_dominion', program_participants_dominion, row.names=FALSE, overwrite = TRUE)



