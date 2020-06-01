summary_e <- read.csv(here('raw_data','summary.csv'))
install.packages('tidyverse')
library(dplyr)
library(tidyverse)
summary_e <- summary_e[2:21,1:3]
#names(summary_e)<-lapply(summary_e[1,],as.character)
names(summary_e)<-c('Data','Value','Rank')
summary_e <- summary_e[-1,]

#write.csv(summary_e,file = 'electricity_summary.csv') -- remove statement
#upload to db