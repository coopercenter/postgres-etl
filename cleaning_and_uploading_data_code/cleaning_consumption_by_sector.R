library(tidyverse)
library(readr)
library(dplyr)
library(here)

total <- read.csv(here('raw_data','consumption_total.csv'))

va_total <-total[total[1]=='VA',2:59]
va_total <- as.data.frame(t(va_total))
va_total<-as.numeric(gsub(",", "", va_total[,1]))

res <- read.csv(here('raw_data','consumption_residential.csv'))
va_res <-res[res[1]=='VA',2:59]
va_res <- as.data.frame(t(va_res))
va_res<-as.numeric(gsub(",", "", va_res[,1]))

comm <- read.csv(here('raw_data','consumption_commercial.csv'))
va_comm <-comm[comm[1]=='VA',2:59]
va_comm <- as.data.frame(t(va_comm))
va_comm<-as.numeric(gsub(",", "", va_comm[,1]))

ind <- read.csv(here('raw_data','consumption_industrial.csv'))
va_ind <-ind[ind[1]=='VA',2:59]
va_ind <- as.data.frame(t(va_ind))
va_ind<-as.numeric(gsub(",", "", va_ind[,1]))

transp <- read.csv(here('raw_data','consumption_transportation.csv'))
va_transp <-transp[transp[1]=='VA',2:59]
va_transp <- as.data.frame(t(va_transp))
va_transp<-as.numeric(gsub(",", "", va_transp[,1]))

va_table <- data.frame(1960:2017,va_total,va_res,va_comm,va_ind,va_transp)
colnames(va_table)<-c('year','total_production','residential_sector','commercial_sector','industrial_sector','transportation_sector')

#write.csv(va_table,file = 'va_consumption_by_sector_1960_to_2017.csv') -- remove statement
#upload to db
