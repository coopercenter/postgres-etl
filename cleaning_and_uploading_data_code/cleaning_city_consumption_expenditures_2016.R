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
all_states_data_2016_raw <- read_excel(here('raw_data','2016_city_data.xlsx'), col_names = FALSE)

#break down dataset into subsets
all_states_data_2016 <- all_states_data_2016_raw[6:nrow(all_states_data_2016_raw), 1:ncol(all_states_data_2016_raw)]

#Loop to Find the names of columns by combining rows 
colNames <- vector(length = ncol(all_states_data_2016_raw))

for(c in 1:ncol(all_states_data_2016_raw)){
        word <- ""
        
        for (r in 1:4) {
                if(is.na(all_states_data_2016_raw[r,c]) == FALSE){
                        word <- paste0(word, all_states_data_2016_raw[r,c], sep = " ")
                }
        }
        
        #This is to get rid of the last " " at the end of the string
        if(is.na(all_states_data_2016_raw[5,c]) == FALSE){
                        word <- paste0(word, all_states_data_2016_raw[5,c], sep = "")
        }
        
        colNames[c] <- word
}


#replacing row names 
colnames(all_states_data_2016)<- colNames

#filter to only get VA data
all_states_data_2016 %>% select(state_abbr)





# Separate by commercials
#colnames(all_states_data_2016)<-tolower(colnames(all_states_data_2016))
#all_states_commercial <- all_states_data_2016[,c(1:4,22:35)]
#Separate by industry
#colnames(all_states_data_2016) <- tolower(colnames(all_states_data_2016))
#all_states_industry<- all_states_data_2016[,c(1:4,48:55)]
#Separate by NAICS codes
#colnames(all_states_data_2016) <- tolower(colnames(all_states_data_2016))
#all_states_NAICS_codes<- all_states_data_2016[,c(1:4,57:100)]





