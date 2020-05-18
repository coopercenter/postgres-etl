library(here)
library(stringr)
retail <- read.csv(here("data","raw_data","Retail_sales_of_electricity_Virginia_annual.csv"),header = F)
retail <- retail[5:nrow(retail),]
names(retail)<-lapply(retail[1,],as.character)
retail <-retail[-1,]
retail6<- as.numeric(gsub("--", NA, retail[,6]))
retail7<- as.numeric(gsub("--", NA, retail[,7]))
retail[,6:7]<-c(retail6,retail7)
colnames(retail)<-str_replace_all(colnames(retail),' ','_')
retail_ordered <-retail[nrow(retail):1,]
rownames(retail_ordered) <- NULL

write.csv(retail_ordered,file = 'retail_sales_of_electricity_annual_clean.csv')
