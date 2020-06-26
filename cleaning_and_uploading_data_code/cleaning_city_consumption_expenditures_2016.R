library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")
library(readxl)

db_driver = dbDriver("PostgreSQL")
source(here('raw_data'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

#read in dataset
all_states_data_2016_raw <- read_excel(here('raw_data','city_consumption_expenditures_2016.xlsx'), col_names = FALSE)

#break down dataset into subsets
all_states_data_2016 <- all_states_data_2016_raw[6:nrow(all_states_data_2016_raw), 1:ncol(all_states_data_2016_raw)]

#Loop to Find the names of columns by combining rows 
colNames <- vector(length = ncol(all_states_data_2016_raw))

for(c in 1:ncol(all_states_data_2016_raw)){
        word <- ""
        for (r in 1:4) {
                if(is.na(all_states_data_2016_raw[r,c]) == FALSE){
                        word <- paste0(word, all_states_data_2016_raw[r,c], sep = "_")
                }
        }
        #This is to get rid of the last " " at the end of the string
        if(is.na(all_states_data_2016_raw[5,c]) == FALSE){
                        word <- paste0(word, all_states_data_2016_raw[5,c], sep = "")
        }
        colNames[c] <- str_replace_all(word, " ", "_")
}

#replacing row names 
colnames(all_states_data_2016)<- colNames

#Split data set into two, one with NAICS and one without.
not_NAICS_data <- all_states_data_2016[, c(2,5:7,9, 15:56,157:169)]
NAICS_data <- all_states_data_2016[, c(2,5:7,9, 57:156)]

#Filter for only VA states
not_NAICS_data <- filter(not_NAICS_data, state_abbr == "VA")
#NAICS_data <- filter(NAICS_data, state_abbr == "VA") #This line doesnt work, an error about unique column names?

#Now split into residential, commercial, on_road, industry, emissions, and factors
city_residential_data <- not_NAICS_data[, c(1:5, 6:16)]
city_commercial_data <- not_NAICS_data[, c(1:5, 17:32)]
city_on_road_data <- not_NAICS_data[, c(1:5, 33:38)]
city_industry_data <- not_NAICS_data[, c(1:5, 39:47)]
city_residential_emissions_data <- not_NAICS_data[, c(1:5, 48:49)]
city_commercial_emissions_data <- not_NAICS_data[, c(1:5, 50:51)]
city_industry_emissions_data <- not_NAICS_data[, c(1:5, 52:53)]
city_on_road_emissions_data <- not_NAICS_data[, c(1:5, 54:55)]
city_emission_factors_data <- not_NAICS_data[, c(1:5, 56:59)]

#upload to db
dbWriteTable(db, 'city_residential_expenditures_consumption_2016', city_residential_data, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'city_commercial_expenditures_consumption_2016', city_commercial_data, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'city_on_road_expenditures_consumption_2016', city_on_road_data, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'city_industry_expenditures_consumption_2016', city_industry_data, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'city_residential_emissions_2016', city_residential_emissions_data, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'city_commercial_emissions_2016', city_commercial_emissions_data, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'city_industry_emissions_2016', city_industry_emissions_data, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'city_on_road_emissions_2016', city_on_road_emissions_data, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'city_emission_factors_2016', city_emission_factors_data, row.names=FALSE, overwrite = TRUE)

#Uncomment and run this line after the issue of unique column names is fixed above.
#dbWriteTable(db, 'city_NAICS_codes_expenditures_consumption_2016', NAICS_data, row.names=FALSE, overwrite = TRUE)

#close db connection
dbDisconnect(db)




















