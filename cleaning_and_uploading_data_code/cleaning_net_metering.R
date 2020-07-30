library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

#read in dataset
here('raw_data','net_metering.csv')
meter <- read.csv(here('raw_data','net_metering.csv'))


names(meter)<-lapply(meter[1,],as.character)
meter <- as.data.frame(t(meter))
meter <- meter[,2:46]
names(meter)<-lapply(meter[1,],as.character)
meter <- meter[-1,]
colnames(meter)[1] <- 'Year'
meter[,1] <-str_replace_all(meter[,1],'Year','')

#set up photovoltaic data subset
photovoltaic_capacity <- meter[,c(1,3:12)]


colnames(photovoltaic_capacity) <- c('year', 'total_capacity', 'residential',
                                  'commercial', 'industrial', 'transportation','customers','residential_customers',
                                  'commercial_customers','industrial_customers','transportation_customers')
rownames(photovoltaic_capacity) <- c()

for (i in 1:ncol(photovoltaic_capacity)){
  photovoltaic_capacity[,i]<-as.numeric(gsub(",", "", photovoltaic_capacity[,i]))
}

#upload to db
dbWriteTable(db, 'photovoltaic_net_metering', photovoltaic_capacity, row.names=FALSE, overwrite = TRUE)
#dbWriteTable(db, 'net_metering', meter, row.names=FALSE, overwrite = TRUE)

#----------------------------------------------------
wind_capacity <- meter[,c(1,14:23)]

colnames(wind_capacity) <- c('year', 'total_capacity', 'residential',
                                     'commercial', 'industrial', 'transportation','customers','residential_customers',
                                     'commercial_customers','industrial_customers','transportation_customers')
rownames(wind_capacity) <- c()

for (i in 1:ncol(wind_capacity)){
  wind_capacity[,i]<-as.numeric(gsub(",", "", wind_capacity[,i]))
}

#upload to db
dbWriteTable(db, 'wind_net_metering', wind_capacity, row.names=FALSE, overwrite = TRUE)


#close db connection
dbDisconnect(db)


