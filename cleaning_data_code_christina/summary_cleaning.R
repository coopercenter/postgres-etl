setwd("C:/Users/Christina Chung/Desktop/EO43")
summary_e <- read.csv('summary.csv')
install.packages('tidyverse')
library(dplyr)
library(tidyverse)
summary_e <- summary_e[2:21,1:3]
#names(summary_e)<-lapply(summary_e[1,],as.character)
names(summary_e)<-c('Data','Value','Rank')
summary_e <- summary_e[-1,]
write.csv(summary_e,file = 'electricity_summary.csv')
