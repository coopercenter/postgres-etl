library(dplyr)
library(readr)
library(tidyverse)
library(stringr) # for replacing strings
#read in dataset
here('data','CO2 Electricity Emission Virginia.csv' )
#read in dataset
CO2_Emissions_Virginia <- read.csv(here('data','CO2 Electricity Emission Virginia.csv' ))
#transpose dataset
CO2_Emissions_Virginia <- as.data.frame(t(CO2_Emissions_Virginia))
#replace column names with appropriate titles
CO2_Emissions_Virginia <- CO2_Emissions_Virginia[,2:50]
names(CO2_Emissions_Virginia)<-lapply(CO2_Emissions_Virginia[1,],as.character)
CO2_Emissions_Virginia <- CO2_Emissions_Virginia[-1,]
colnames(CO2_Emissions_Virginia)[1] <- 'Year'
CO2_Emissions_Virginia[,1] <- str_replace_all(CO2_Emissions_Virginia[,1], 'Year', '')
#Select only subset with Virginia data
CO2_Emissions_Virginia %>%
  select(Year, Virginia) -> CO2_Emissions_Virginia
#Get rid of erraneous observations
CO2_Emissions_Virginia <- CO2_Emissions_Virginia[1:38,]
#Change colnames
colnames(CO2_Emissions_Virginia) <- c('Year','CO2_Emissions_mmt')
#convert columns to numeric
CO2_Emissions_Virginia %>%
  mutate_at(c('Year','CO2_Emissions_mmt'),as.numeric) -> CO2_Emissions_Virginia
#read in dataset
write.csv(CO2_Emissions_Virginia, "CO2_Emissions_Electricy_Virginia.csv")
#create a plot
ggplot(CO2_Emissions_Virginia) + geom_line(aes(Year, CO2_Emissions_mmt)) + 
  ggtitle('CO2 Emissions in Virginia Electricity Sector') + ylab("CO2 Emissions in metric million tons")

