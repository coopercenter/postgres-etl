setwd("C:/Users/Christina Chung/Desktop/EO43")
fuel <- read.csv('fuel_uncleaned.csv')
library(dplyr)
library(stringr) # for replacing strings
fuel <- as.data.frame(t(fuel))
fuel <- fuel[,2:10]
names(fuel)<-lapply(fuel[1,],as.character)
fuel <- fuel[-1,]
colnames(fuel)[1] <- 'Year'
fuel[,1] <-str_replace_all(fuel[,1],'Year','')
colnames(fuel)<-str_replace_all(colnames(fuel),' ','_')
for (i in c(3,6,9)){
  fuel[,i]<-as.numeric(gsub(",", "", fuel[,i]))
  
}
fuel_ordered <-fuel[nrow(fuel):1,]
write.csv(fuel_ordered,file = 'fuel.csv')
