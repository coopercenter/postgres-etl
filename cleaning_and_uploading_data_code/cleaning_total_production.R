library(tidyverse)
prod <- read.csv(here('raw_data', 'total_production.csv'))
va_prod <- prod[prod['StateCode']=='VA',]
va_total_prod <- va_prod[va_prod['MSN']=='TEPRB',]
va_total_prod <- va_total_prod[,4:ncol(va_total_prod)]

va_total_prod <- as.data.frame(t(va_total_prod))

rownames(va_total_prod)<-str_replace_all(rownames(va_total_prod),'X','')
colnames(va_total_prod)<- 'Total_Energy_Production_in_Billion_Btu'

#write.csv(va_total_prod,file = 'va_total_prod_1960_to_2017.csv') -- remove statement
#upload to db