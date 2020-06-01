library(here)
here('raw_data','net_metering.csv')
#read in dataset
meter <- read.csv(here('raw_data','net_metering.csv'))
library(dplyr)
library(stringr) # for replacing strings
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

photovoltaic_capacity <- meter[,1:7]



photovoltaic_capacity %>%
  select(Year, `Capacity (MW)`, Residential, Commercial, Industrial, Transportation) -> photovoltaic_capacity

colnames(photovoltaic_capacity) <- c('Year', 'Total_Capacity', 'Residential',
                                  'Commercial', 'Industrial', 'Transpotation')
rownames(photovoltaic_capacity) <- c()

photovoltaic_capacity$Year <- as.numeric(photovoltaic_capacity$Year)

#write.csv(photovoltaic_capacity,file = 'photovoltaic_capacity_clean.csv') -- remove statement
#upload to db

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
library(dplyr)
library(stringr) # for replacing strings
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
#upload to db

#>>>>>>> c2d1863f60f8b508f43930efd29a37b5e5c92898
