library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

emission <- read.csv(here('raw_data', 'emission.csv'))

emission <- as.data.frame(t(emission))
emission <- emission[,2:24]
names(emission)<-lapply(emission[1,],as.character)
emission <- emission[-1,]
colnames(emission)[1] <- 'Year'
emission[,1] <-str_replace_all(emission[,1],'Year','')

for (i in 1:ncol(emission)){
  emission[,i]<-as.numeric(gsub(",", "", emission[,i]))
}

colnames(emission)<-str_replace_all(colnames(emission),' ','_')
emission_ordered <-emission[nrow(emission):1,]
co2 <- emission_ordered[,c(1,15:19)]

#write.csv(co2,file = 'CO2.csv') -- remove statement
#write.csv(emission_ordered,file = 'all_emission_by_source.csv') -- remove statement

#upload to db
dbWriteTable(db, 'co2_by_source_va', co2, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'emission', emission, row.names=FALSE, overwrite = TRUE)
#close db connection
dbDisconnect(db)