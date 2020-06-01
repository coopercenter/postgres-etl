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
yearmeter <- meter[2,]


#set up wind data subset
wind_capacity <- meter[14:19,]
wind_capacity <- rbind(yearmeter,wind_capacity)
names(meter)<-lapply(meter[1,],as.character)
meter <- as.data.frame(t(meter))
meter <- meter[,2:46]
names(meter)<-lapply(meter[1,],as.character)
meter <- meter[-1,]
colnames(meter)[1] <- 'Year'
meter[,1] <-str_replace_all(meter[,1],'Year','')

#set up photovoltaic data subset
photovoltaic_capacity <- meter[,1:7]

photovoltaic_capacity %>%
  select(Year, `Capacity (MW)`, Residential, Commercial, Industrial, Transportation) -> photovoltaic_capacity

colnames(photovoltaic_capacity) <- c('Year', 'Total_Capacity', 'Residential',
                                  'Commercial', 'Industrial', 'Transpotation')
rownames(photovoltaic_capacity) <- c()

photovoltaic_capacity$Year <- as.numeric(photovoltaic_capacity$Year)

#write.csv(photovoltaic_capacity,file = 'photovoltaic_capacity_clean.csv') -- remove statement

#upload to db
dbWriteTable(db, 'photovoltaic_capacity', photovoltaic_capacity, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'net_metering', meter, row.names=FALSE, overwrite = TRUE)

#close db connection
dbDisconnect(db)

# is the code below used? appears that 
# meter --> net_metering table in postgres
# photovoltaic_capacity --> photovoltaic_capacity table in postgres
# remove all code below this line
#-----------------------------------------------------------------
names(wind_capacity)<-lapply(wind_capacity[1,],as.character)
wind_capacity <- as.data.frame(t(wind_capacity))
wind_capacity <- wind_capacity[,2:7]
names(wind_capacity)<-lapply(wind_capacity[1,],as.character)
wind_capacity <- wind_capacity[-1,]
colnames(wind_capacity)[1] <- 'Year'
wind_capacity[,1] <-str_replace_all(wind_capacity[,1],'Year','')

#write.csv(meter,file = 'net_metering_clean.csv')

#-----------------------------------------------------------------
meter <- read.csv(here('raw_data','net_metering.csv'))
meter <- as.data.frame(t(meter))
meter <- meter[,2:46]
names(meter)<-lapply(meter[1,],as.character)
meter <- meter[-1,]
colnames(meter)[1] <- 'Year'
meter[,1] <-str_replace_all(meter[,1],'Year','')
meter_ordered <-meter[nrow(meter):1,]

for (i in c(8,9,41,42)){
  meter_ordered[,i]<-as.numeric(gsub(",", "", meter_ordered[,i]))
}

colnames(meter_ordered)<-str_replace_all(colnames(meter_ordered),' ','_')

#write.csv(meter_ordered,file = 'net_metering_cleaned.csv') -- remove statement

#-----------------------------------------------------------------
#>>>>>>> c2d1863f60f8b508f43930efd29a37b5e5c92898
