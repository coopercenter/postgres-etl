setwd("C:/Users/Christina Chung/Desktop/EO43")
retailers <- read.csv('retailers.csv')
library(dplyr)
retailers <- select(retailers,X,X.1,X.2,X.3,X.4,X.5,X.6)
retailers <- retailers[3:10,]
names(retailers)<-lapply(retailers[1,],as.character)
retailers <- retailers[-1,]
colnames(retailers)<-str_replace_all(colnames(retailers),' ','_')
for (i in 3:ncol(retailers)){
  retailers[,i]<-as.numeric(gsub(",", "", retailers[,i]))
  
}
write.csv(retailers,file = 'retailers_clean.csv')
