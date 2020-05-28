setwd("C:/Users/Christina Chung/Desktop/EO43")
capacity <- read.csv('2A_Capacity.csv')
library(dplyr)
capacity <- capacity[2:12,2:5]
names(capacity)<-lapply(capacity[1,],as.character)
capacity <- capacity[-1,]
colnames(capacity)<-str_replace_all(colnames(capacity),' ','_')

write.csv(capacity,file = 'capacity_2A.csv')
