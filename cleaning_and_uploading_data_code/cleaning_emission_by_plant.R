library('dplyr')
library('here')
library('stringr')
plant_emission <- read.csv(here("data","raw data","va_emission_by_plant.csv"),header=F)
e_table<-plant_emission[5:106,-2]
names(e_table)<-lapply(e_table[1,],as.character)
e_table <- e_table[-1,]
colnames(e_table)<-str_replace_all(colnames(e_table),' ','_')
write.csv(e_table,file = 'va_emission_by_plant.csv')
