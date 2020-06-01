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
here('raw_data','generation_by_type_va.csv')
generation.by.sector <- read.csv(here('raw_data','generation_by_type_va.csv'))

#----------------------------------------------------------------------------------------------------------
#subset data to include on energy generated for electric utility
electric_utility_generation <- generation.by.sector[3:23,]
#store year as vector
electric_generation_year <- generation.by.sector[3,]
#seperate subset for IPP and CHP
electric_generation_IPP_CHP <- generation.by.sector[24:42,]
#add year to dataframe
electric_generation_IPP_CHP <- rbind(electric_generation_year,electric_generation_IPP_CHP)
#seperate subset for whole industry
whole_electric_industry <- generation.by.sector[43:63,]
#add year to dataframe
whole_electric_industry <- rbind(electric_generation_year,whole_electric_industry)

#create dataframe for electric utility generation
electric_utility_generation <- as.data.frame(t(electric_utility_generation))
#replace column names with appropriate titles
names(electric_utility_generation)<-lapply(electric_utility_generation[1,],as.character)
electric_utility_generation <- electric_utility_generation[-1,]
colnames(electric_utility_generation)[1] <- 'Year'
electric_utility_generation[,1] <- str_replace_all(electric_utility_generation[,1], 'Year', '')
electric_utility_generation_percentages <- electric_utility_generation[30:32,]
electric_utility_generation_percentages %>%
  select(Year, `Electric utilities`,Coal, Hydroelectric, 
         `Natural gas`, Nuclear, `Other biomass`, Petroleum, `Pumped storage`, Solar, Wood) -> electric_utility_generation_percentages
#rename columns with appropriate names
colnames(electric_utility_generation_percentages)<-c('Year','Electric_Utilities','Coal','Hydroelectric','Natural_Gas',
                                         'Nuclear', 'Other_Biomass', 'Petroleum','Pumped_Storage', 'Solar', 'Wood')


#Subset for non-yearly percentage values
electric_utility_generation <- electric_utility_generation[1:29,]
#Use pipelines to select the holistic values for each sector
electric_utility_generation %>%
  select(Year, `Electric utilities`,Coal, Hydroelectric, 
         `Natural gas`, Nuclear, `Other biomass`, Petroleum, `Pumped storage`, Solar, Wood) -> electric_utility_generation
#rename columns with appropriate names
colnames(electric_utility_generation)<-c('Year','Electric_Utilities','Coal','Hydroelectric','Natural_Gas',
                                         'Nuclear', 'Other_Biomass', 'Petroleum','Pumped_Storage', 'Solar', 'Wood')
#Convert columns to number values
electric_utility_generation$Year<-as.numeric(gsub(",", "", electric_utility_generation$Year))
electric_utility_generation$Electric_Utilities<-as.numeric(gsub(",", "", electric_utility_generation$Electric_Utilities))
electric_utility_generation$Coal<-as.numeric(gsub(",", "", electric_utility_generation$Coal))
electric_utility_generation$Hydroelectric<-as.numeric(gsub(",", "", electric_utility_generation$Hydroelectric))
electric_utility_generation$Natural_Gas<-as.numeric(gsub(",", "", electric_utility_generation$Natural_Gas))
electric_utility_generation$Nuclear<-as.numeric(gsub(",", "", electric_utility_generation$Nuclear))
electric_utility_generation$Other_Biomass<-as.numeric(gsub(",", "", electric_utility_generation$Other_Biomass))
electric_utility_generation$Petroleum<-as.numeric(gsub(",", "", electric_utility_generation$Petroleum))
electric_utility_generation$Pumped_Storage<-as.numeric(gsub(",", "", electric_utility_generation$Pumped_Storage))
electric_utility_generation$Solar<-as.numeric(gsub(",", "", electric_utility_generation$Solar))
electric_utility_generation$Wood<-as.numeric(gsub(",", "", electric_utility_generation$Wood))
#Remove introduced row names
rownames(electric_utility_generation) <- c()

#write.csv(electric_utility_generation, "Electric_Utility_Generation.csv") -- remove statement

#upload to db
dbWriteTable(db, 'electric_utility_generation', electric_utility_generation, row.names=FALSE, overwrite = TRUE)


#----------------------------------------------------------------------------------------------------------
#create dataframe for electric IPP and CHP generation
electric_generation_IPP_CHP <- as.data.frame(t(electric_generation_IPP_CHP))
#replace column names with appropriate titles
names(electric_generation_IPP_CHP)<-lapply(electric_generation_IPP_CHP[1,],as.character)
electric_generation_IPP_CHP <- electric_generation_IPP_CHP[-1,]
colnames(electric_generation_IPP_CHP)[1] <- 'Year'
electric_generation_IPP_CHP[,1] <- str_replace_all(electric_generation_IPP_CHP[,1], 'Year', '')
electric_generation_IPP_CHP_percentages <- electric_generation_IPP_CHP[30:32,]
electric_generation_IPP_CHP_percentages %>%
  select(Year, `IPP and CHP`,Coal, Hydroelectric, 
         `Natural gas`, Other, `Other biomass`, Petroleum, Solar, Wood) -> electric_generation_IPP_CHP_percentages
#rename columns with appropriate names
colnames(electric_utility_generation_percentages)<-c('Year','IPP_and_CHP','Coal','Hydroelectric','Natural_Gas','Other',
                                                     'Other_Biomass', 'Petroleum','Solar', 'Wood')
#Get rid of percentage variables
electric_generation_IPP_CHP <- electric_generation_IPP_CHP[1:29,]
#Use pipelines to select the holistic values for each sector
electric_generation_IPP_CHP %>%
  select(Year, `IPP and CHP`,Coal, Hydroelectric, 
         `Natural gas`, Other, `Other biomass`, Petroleum, Solar, Wood) -> electric_generation_IPP_CHP
#rename columns with appropriate names
colnames(electric_generation_IPP_CHP)<-c('Year','IPP_and_CHP','Coal','Hydroelectric','Natural_Gas','Other',
                                         'Other_Biomass', 'Petroleum','Solar', 'Wood')
#Convert columns to number values
electric_generation_IPP_CHP$Year<-as.numeric(gsub(",", "", electric_generation_IPP_CHP$Year))
electric_generation_IPP_CHP$IPP_and_CHP<-as.numeric(gsub(",", "", electric_generation_IPP_CHP$IPP_and_CHP))
electric_generation_IPP_CHP$Coal<-as.numeric(gsub(",", "", electric_generation_IPP_CHP$Coal))
electric_generation_IPP_CHP$Hydroelectric<-as.numeric(gsub(",", "", electric_generation_IPP_CHP$Hydroelectric))
electric_generation_IPP_CHP$Natural_Gas<-as.numeric(gsub(",", "", electric_generation_IPP_CHP$Natural_Gas))
electric_generation_IPP_CHP$Other<-as.numeric(gsub(",", "", electric_generation_IPP_CHP$Other))
electric_generation_IPP_CHP$Other_Biomass<-as.numeric(gsub(",", "", electric_generation_IPP_CHP$Other_Biomass))
electric_generation_IPP_CHP$Petroleum<-as.numeric(gsub(",", "", electric_generation_IPP_CHP$Petroleum))
electric_generation_IPP_CHP$Solar<-as.numeric(gsub(",", "", electric_generation_IPP_CHP$Solar))
electric_generation_IPP_CHP$Wood<-as.numeric(gsub(",", "", electric_generation_IPP_CHP$Wood))
rownames(electric_generation_IPP_CHP) <- c()

#write.csv(electric_generation_IPP_CHP, "Electric_IPP_CHP_Generation.csv") -- remove statement

#upload to db
dbWriteTable(db, 'electric_ipp_chp_generation', electric_generation_IPP_CHP, row.names=FALSE, overwrite = TRUE)


#----------------------------------------------------------------------------------------------------------
#create dataframe for total electricity generation
whole_electric_industry <- as.data.frame(t(whole_electric_industry))
#replace column names with appropriate titles
names(whole_electric_industry)<-lapply(whole_electric_industry[1,],as.character)
whole_electric_industry <- whole_electric_industry[-1,]
colnames(whole_electric_industry)[1] <- 'Year'
whole_electric_industry[,1] <- str_replace_all(whole_electric_industry[,1], 'Year', '')
whole_electric_industry_percentages <- whole_electric_industry[30:32,]
whole_electric_industry_percentages %>%
  select(Year, `Total electric industry`,Coal, Hydroelectric, 
         `Natural gas`, Nuclear,Other, `Other biomass`, Petroleum, `Pumped storage`, Solar, Wood) -> whole_electric_industry_percentages
#rename columns with appropriate names
colnames(whole_electric_industry_percentages)<-c('Year','Total_Electric_Industry','Coal','Hydroelectric','Natural_Gas',
                                                     'Nuclear','Other', 'Other_Biomass', 'Petroleum','Pumped_Storage', 'Solar', 'Wood')
#Get rid of percentage variables
whole_electric_industry <- whole_electric_industry[1:29,]
#Use pipelines to select the holistic values for each sector
whole_electric_industry %>%
  select(Year, `Total electric industry`,Coal, Hydroelectric, 
         `Natural gas`, Nuclear,Other, `Other biomass`, Petroleum, `Pumped storage`, Solar, Wood) -> whole_electric_industry
#rename columns with appropriate names
colnames(whole_electric_industry)<-c('Year','Total_Electric_Industry','Coal','Hydroelectric','Natural_Gas',
                                         'Nuclear','Other', 'Other_Biomass', 'Petroleum','Pumped_Storage', 'Solar', 'Wood')
#Convert columns to number values
whole_electric_industry$Year<-as.numeric(gsub(",", "", whole_electric_industry$Year))
whole_electric_industry$Total_Electric_Industry<-as.numeric(gsub(",", "", whole_electric_industry$Total_Electric_Industry))
whole_electric_industry$Coal<-as.numeric(gsub(",", "", whole_electric_industry$Coal))
whole_electric_industry$Hydroelectric<-as.numeric(gsub(",", "", whole_electric_industry$Hydroelectric))
whole_electric_industry$Natural_Gas<-as.numeric(gsub(",", "", whole_electric_industry$Natural_Gas))
whole_electric_industry$Nuclear<-as.numeric(gsub(",", "", whole_electric_industry$Nuclear))
whole_electric_industry$Other<-as.numeric(gsub(",", "", whole_electric_industry$Other))
whole_electric_industry$Other_Biomass<-as.numeric(gsub(",", "", whole_electric_industry$Other_Biomass))
whole_electric_industry$Petroleum<-as.numeric(gsub(",", "", whole_electric_industry$Petroleum))
whole_electric_industry$Pumped_Storage<-as.numeric(gsub(",", "", whole_electric_industry$Pumped_Storage))
whole_electric_industry$Solar<-as.numeric(gsub(",", "", whole_electric_industry$Solar))
whole_electric_industry$Wood<-as.numeric(gsub(",", "", whole_electric_industry$Wood))
#remove introduced row names
rownames(whole_electric_industry) <- c()

#write.csv(whole_electric_industry, "Whole_Electric_Industry_Generation.csv") -- remove statement

#upload to db
dbWriteTable(db, 'whole_electric_industry_generation', whole_electric_industry, row.names=FALSE, overwrite = TRUE)

#----------------------------------------------------------------------------------------------------------
#close db connection
dbDisconnect(db)

