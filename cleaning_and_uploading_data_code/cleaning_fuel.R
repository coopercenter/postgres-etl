fuel <- read.csv(here('raw_data','fuel.csv'))
library(dplyr)
library(stringr) # for replacing strings
fuel <- as.data.frame(t(fuel))
fuel <- fuel[,2:10]
names(fuel)<-lapply(fuel[1,],as.character)
fuel <- fuel[-1,]
colnames(fuel)[1] <- 'Year'
fuel[,1] <-str_replace_all(fuel[,1],'Year','')

#write.csv(fuel,file = 'fuel.csv') -- remove statement
#upload to db