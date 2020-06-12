library(dplyr)
library(readr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

#read in dataset
electric_capacity_virginia <- read.csv(here('raw_data','electric_power_capacity_by_sector.csv'))

#---------------------------------------------------------------------------------------
#subset data to include on energy generated for electric utility
electric_capacity_utility <- electric_capacity_virginia[3:21,]
#store year as vector
electric_capacity_year <- electric_capacity_virginia[3,]
#seperate subset for IPP and CHP
electric_capacity_IPP_CHP <- electric_capacity_virginia[22:37,]
#add year to dataframe
electric_capacity_IPP_CHP <- rbind(electric_capacity_year,electric_capacity_IPP_CHP)
#seperate subset for whole industry
whole_capacity_industry <- electric_capacity_virginia[38:56,]
#add year to dataframe
whole_capacity_industry <- rbind(electric_capacity_year,whole_capacity_industry)

#create dataframe for electric utility generation
electric_capacity_utility <- as.data.frame(t(electric_capacity_utility))
#replace column names with appropriate titles
names(electric_capacity_utility)<-lapply(electric_capacity_utility[1,],as.character)
electric_capacity_utility <- electric_capacity_utility[-1,]
colnames(electric_capacity_utility)[1] <- 'Year'
electric_capacity_utility[,1] <- str_replace_all(electric_capacity_utility[,1], 'Year', '')
electric_utility_generation_percentages <- electric_capacity_utility[30:32,]
electric_utility_generation_percentages %>%
  select(Year, `Electric utilities`,Coal, Hydroelectric, 
         `Natural gas`, Nuclear, `Other biomass`, Petroleum, `Pumped storage`, Solar, Wood) -> electric_utility_generation_percentages
#rename columns with appropriate names
colnames(electric_utility_generation_percentages)<-c('Year','Electric_Utilities','Coal','Hydroelectric','Natural_Gas',
                                                     'Nuclear', 'Other_Biomass', 'Petroleum','Pumped_Storage', 'Solar', 'Wood')


#Subset for non-yearly percentage values
electric_capacity_utility <- electric_capacity_utility[1:29,]
#Use pipelines to select the holistic values for each sector
electric_capacity_utility %>%
  select(Year, `Electric utilities`,Coal, Hydroelectric, 
         `Natural gas`, Nuclear, `Other biomass`, Petroleum, `Pumped storage`, Solar, Wood) -> electric_capacity_utility
#rename columns with appropriate names
colnames(electric_capacity_utility)<-c('Year','Electric_Utilities','Coal','Hydroelectric','Natural_Gas',
                                         'Nuclear', 'Other_Biomass', 'Petroleum','Pumped_Storage', 'Solar', 'Wood')
#Convert columns to number values
electric_capacity_utility$Year<-as.numeric(gsub(",", "", electric_capacity_utility$Year))
electric_capacity_utility$Electric_Utilities<-as.numeric(gsub(",", "", electric_capacity_utility$Electric_Utilities))
electric_capacity_utility$Coal<-as.numeric(gsub(",", "", electric_capacity_utility$Coal))
electric_capacity_utility$Hydroelectric<-as.numeric(gsub(",", "", electric_capacity_utility$Hydroelectric))
electric_capacity_utility$Natural_Gas<-as.numeric(gsub(",", "", electric_capacity_utility$Natural_Gas))
electric_capacity_utility$Nuclear<-as.numeric(gsub(",", "", electric_capacity_utility$Nuclear))
electric_capacity_utility$Other_Biomass<-as.numeric(gsub(",", "", electric_capacity_utility$Other_Biomass))
electric_capacity_utility$Petroleum<-as.numeric(gsub(",", "", electric_capacity_utility$Petroleum))
electric_capacity_utility$Pumped_Storage<-as.numeric(gsub(",", "", electric_capacity_utility$Pumped_Storage))
electric_capacity_utility$Solar<-as.numeric(gsub(",", "", electric_capacity_utility$Solar))
electric_capacity_utility$Wood<-as.numeric(gsub(",", "", electric_capacity_utility$Wood))
#Remove introduced row names
rownames(electric_capacity_utility) <- c()

#Write code to CSV file
dbWriteTable(db, 'electric_utility_capacity', electric_capacity_utility, row.names=FALSE, overwrite = TRUE)


#---------------------------------------------------------------------------------------
#create dataframe for electric IPP and CHP generation
electric_capacity_IPP_CHP <- as.data.frame(t(electric_capacity_IPP_CHP))
#replace column names with appropriate titles
names(electric_capacity_IPP_CHP)<-lapply(electric_capacity_IPP_CHP[1,],as.character)
electric_capacity_IPP_CHP <- electric_capacity_IPP_CHP[-1,]
colnames(electric_capacity_IPP_CHP)[1] <- 'Year'
electric_capacity_IPP_CHP[,1] <- str_replace_all(electric_capacity_IPP_CHP[,1], 'Year', '')
electric_generation_IPP_CHP_percentages <- electric_capacity_IPP_CHP[30:32,]
electric_generation_IPP_CHP_percentages %>%
  select(Year, `IPP and CHP`,Coal, Hydroelectric, 
         `Natural gas`, Other, `Other biomass`, Petroleum, Solar, Wood) -> electric_generation_IPP_CHP_percentages
#rename columns with appropriate names
colnames(electric_utility_generation_percentages)<-c('Year','IPP_and_CHP','Coal','Hydroelectric','Natural_Gas','Other',
                                                     'Other_Biomass', 'Petroleum','Solar', 'Wood')
#Get rid of percentage variables
electric_capacity_IPP_CHP <- electric_capacity_IPP_CHP[1:29,]
#Use pipelines to select the holistic values for each sector
electric_capacity_IPP_CHP %>%
  select(Year, `IPP and CHP`,Coal, Hydroelectric, 
         `Natural gas`, Other, `Other biomass`, Petroleum, Solar, Wood) -> electric_capacity_IPP_CHP
#rename columns with appropriate names
colnames(electric_capacity_IPP_CHP)<-c('Year','IPP_and_CHP','Coal','Hydroelectric','Natural_Gas','Other',
                                         'Other_Biomass', 'Petroleum','Solar', 'Wood')
#Convert columns to number values
electric_capacity_IPP_CHP$Year<-as.numeric(gsub(",", "", electric_capacity_IPP_CHP$Year))
electric_capacity_IPP_CHP$IPP_and_CHP<-as.numeric(gsub(",", "", electric_capacity_IPP_CHP$IPP_and_CHP))
electric_capacity_IPP_CHP$Coal<-as.numeric(gsub(",", "", electric_capacity_IPP_CHP$Coal))
electric_capacity_IPP_CHP$Hydroelectric<-as.numeric(gsub(",", "", electric_capacity_IPP_CHP$Hydroelectric))
electric_capacity_IPP_CHP$Natural_Gas<-as.numeric(gsub(",", "", electric_capacity_IPP_CHP$Natural_Gas))
electric_capacity_IPP_CHP$Other<-as.numeric(gsub(",", "", electric_capacity_IPP_CHP$Other))
electric_capacity_IPP_CHP$Other_Biomass<-as.numeric(gsub(",", "", electric_capacity_IPP_CHP$Other_Biomass))
electric_capacity_IPP_CHP$Petroleum<-as.numeric(gsub(",", "", electric_capacity_IPP_CHP$Petroleum))
electric_capacity_IPP_CHP$Solar<-as.numeric(gsub(",", "", electric_capacity_IPP_CHP$Solar))
electric_capacity_IPP_CHP$Wood<-as.numeric(gsub(",", "", electric_capacity_IPP_CHP$Wood))

#Remove introduced row names
rownames(electric_capacity_IPP_CHP) <- c()

#Write code to CSV file
dbWriteTable(db, 'electric_ipp_chp_capacity', electric_capacity_IPP_CHP, row.names=FALSE, overwrite = TRUE)

#---------------------------------------------------------------------------------------
#create dataframe for total electricity generation
whole_capacity_industry <- as.data.frame(t(whole_capacity_industry))
#replace column names with appropriate titles
names(whole_capacity_industry)<-lapply(whole_capacity_industry[1,],as.character)
whole_capacity_industry <- whole_capacity_industry[-1,]
colnames(whole_capacity_industry)[1] <- 'Year'
whole_capacity_industry[,1] <- str_replace_all(whole_capacity_industry[,1], 'Year', '')
whole_electric_industry_percentages <- whole_capacity_industry[30:32,]
whole_electric_industry_percentages %>%
  select(Year, `Total electric industry`,Coal, Hydroelectric, 
         `Natural gas`, Nuclear,Other, `Other biomass`, Petroleum, `Pumped storage`, Solar, Wood) -> whole_electric_industry_percentages
#rename columns with appropriate names
colnames(whole_electric_industry_percentages)<-c('Year','Total_Electric_Industry','Coal','Hydroelectric','Natural_Gas',
                                                 'Nuclear','Other', 'Other_Biomass', 'Petroleum','Pumped_Storage', 'Solar', 'Wood')
#Get rid of percentage variables
whole_capacity_industry <- whole_capacity_industry[1:29,]
#Use pipelines to select the holistic values for each sector
whole_capacity_industry %>%
  select(Year, `Total electric industry`,Coal, Hydroelectric, 
         `Natural gas`, Nuclear,Other, `Other biomass`, Petroleum, `Pumped storage`, Solar, Wood) -> whole_capacity_industry
#rename columns with appropriate names
colnames(whole_capacity_industry)<-c('Year','Total_Electric_Industry','Coal','Hydroelectric','Natural_Gas',
                                     'Nuclear','Other', 'Other_Biomass', 'Petroleum','Pumped_Storage', 'Solar', 'Wood')
#Convert columns to number values
whole_capacity_industry$Year<-as.numeric(gsub(",", "", whole_capacity_industry$Year))
whole_capacity_industry$Total_Electric_Industry<-as.numeric(gsub(",", "", whole_capacity_industry$Total_Electric_Industry))
whole_capacity_industry$Coal<-as.numeric(gsub(",", "", whole_capacity_industry$Coal))
whole_capacity_industry$Hydroelectric<-as.numeric(gsub(",", "", whole_capacity_industry$Hydroelectric))
whole_capacity_industry$Natural_Gas<-as.numeric(gsub(",", "", whole_capacity_industry$Natural_Gas))
whole_capacity_industry$Nuclear<-as.numeric(gsub(",", "", whole_capacity_industry$Nuclear))
whole_capacity_industry$Other<-as.numeric(gsub(",", "", whole_capacity_industry$Other))
whole_capacity_industry$Other_Biomass<-as.numeric(gsub(",", "", whole_capacity_industry$Other_Biomass))
whole_capacity_industry$Petroleum<-as.numeric(gsub(",", "", whole_capacity_industry$Petroleum))
whole_capacity_industry$Pumped_Storage<-as.numeric(gsub(",", "", whole_capacity_industry$Pumped_Storage))
whole_capacity_industry$Solar<-as.numeric(gsub(",", "", whole_capacity_industry$Solar))
whole_capacity_industry$Wood<-as.numeric(gsub(",", "", whole_capacity_industry$Wood))

#Remove introduced row names
rownames(whole_capacity_industry) <- c()

#Write code to CSV file
dbWriteTable(db, 'whole_electric_industry_capacity', whole_capacity_industry, row.names=FALSE, overwrite = TRUE)

#---------------------------------------------------------------------------------------
#close db connection
dbDisconnect(db)
