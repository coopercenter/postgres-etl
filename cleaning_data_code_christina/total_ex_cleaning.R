library('dplyr')
library('here')
library('stringr')
ex <- read.csv(here("data","ex_all.csv"))
va_ex <- ex[ex['State']=='VA',]
va_ex_total <- va_ex[va_ex['MSN']=='ESTCV',]
va_ex_total <- va_ex_total[,4:ncol(va_ex_total)]
va_ex_total <- as.data.frame(t(va_ex_total))
va_ex_total[,2] <-va_ex_total[,1]
va_ex_total[,1] <- 1970:2017
colnames(va_ex_total)<-c('Year','electricity_total_expenditures_in_million_dollars')
write.csv(va_ex_total,file = 'va_electricity_total_ex_1970_to_2017.csv')
