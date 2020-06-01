library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings

here('raw_data','energy_efficiency.csv')
#read in dataset
energy.efficiency <- read.csv(here('raw_data','energy_efficiency.csv'))
energy.efficiency <- as.data.frame(t(energy.efficiency))
energy.efficiency <- energy.efficiency[,2:44]
names(energy.efficiency)<-lapply(energy.efficiency[1,],as.character)
energy.efficiency <- energy.efficiency[-1,]
colnames(energy.efficiency)[1] <- 'Year'
energy.efficiency[,1] <- str_replace_all(energy.efficiency[,1], 'Year', '')

#write.csv(energy.efficiency, "Energy_Efficiency_Clean.csv") -- remove statement
#upload to db
