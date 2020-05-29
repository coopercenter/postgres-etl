library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
#here('data','Capacity Factors Annual.csv' )
#read in dataset
#capacity.factors.annual <- read.csv(here('data','Capacity Factors Annual.csv' ))
capacity.factors.annual <- read.csv("~/Downloads/EO43 Data/Capacity Factors Annual.csv")
capacity.factors.annual <- as.data.frame(t(capacity.factors.annual))
capacity.factors.annual <- capacity.factors.annual[,2:20]
names(capacity.factors.annual)<-lapply(capacity.factors.annual[1,],as.character)
capacity.factors.annual <- capacity.factors.annual[-1,]
colnames(capacity.factors.annual)[1] <- 'Year'
capacity.factors.annual[,1] <- str_replace_all(capacity.factors.annual[,1], 'Year', '')
colnames(retail)<-str_replace_all(colnames(retail),' ','_')

write.csv(capacity.factors.annual, "Capacity_Factors_Annual_Clean.csv")

