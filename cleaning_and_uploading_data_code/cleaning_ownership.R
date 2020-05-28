library('dplyr')
library('here')
library('stringr')
ownership <- read.csv(here("data","raw data","ownership_raw.csv"))
ownership<- ownership[3:10,]
names(ownership)<-lapply(ownership[1,],as.character)
ownership<-ownership[-1,]
for (i in 2:ncol(ownership)){
  ownership[,i]<-as.numeric(gsub(",", "", ownership[,i]))
}
colnames(ownership)<-str_replace_all(colnames(ownership),'\n','_')
colnames(ownership)<-str_replace_all(colnames(ownership),'-','_')
write.csv(ownership,file = 'ownership_clean.csv')

