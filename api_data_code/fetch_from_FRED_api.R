library(httr)
library(here)
library(dplyr)
library(tibble)
library(readr)
if(!("RPostgreSQL" %in% installed.packages())) install.packages("eia")
library("RPostgreSQL")

library(stringr)

# read in my FRED api key
source(here("api_data_code","my_fred_api_key.R"))

# function used to fetch the series
get_FRED_series <- function(api_key,series_id) {
  require(jsonlite)
  require(data.table)
  FREDBase <- paste0("https://api.stlouisfed.org/fred/series/observations?series_id=",series_id,"&api_key=",api_key,"&file_type=json") 
  temp <- readLines(FREDBase, warn = "F")
  rd <- fromJSON(temp)

  # Now take the 'data' element from the list and make a data frame
  rd2 <- data.frame(rd$observations,stringsAsFactors = F)
  rd2 <- data.table(rd2)
  return(rd2)
}

# Make a list of series ids
# If you have a large number of datasets, create a txt file storing the series ids
## reads in a txt file containing the series ids of the datasets we need. Makse sure to use series ID for FRED NOT for EIA.
series_id_vec <- read_file(here("api_data_code","series_ids2.txt"))

## transform the content to a list that we can later feed into the fetch function
series_id_list <- unlist(strsplit(series_id_vec,'\r\n'))

## create an empty list to store the data tables
all_tables<- vector("list", length(series_id_list))

# loops through the all data series and store the tables in to a big list
for (i in 1:length(all_tables)){
  all_tables[[i]]<-get_FRED_series(fredKey,series_id_list[[i]])
}

# Connection to the database
# "my_postgres_login.R" contains the log-in informations of RAs
source(here("etl", "my_postgres_credentials.R"))
db_driver = dbDriver("PostgreSQL")
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)

rm(password)

# Function used to change the names of tables to the format used in MySQL
## Key: all lower case and no punctuation other than _
get_name <- function(series_id) {
  db_table_name <- str_to_lower(paste("fred", series_id, sep="_"))
  return(db_table_name)
}

# apply the function to the list of series id to get the names for the data tables
## create an empty list to store thenames for the data tables
db_table_names <- vector("list", length(series_id_list))
db_table_names <- lapply(series_id_list,get_name)

# Loops through the list of tables and write data series to the PostgreSQL database 
# then query the data from postgreSQL (query is SQL language meaning writing commands in SQL)
# "SELECT * FROM datatable_name" means getting the whole table
# Note: This code OVERWRITES EXISTING TABLE!

for (i in 1:length(all_data_series)){
  dbWriteTable(db, db_table_names[[i]], value = all_tables[[i]], append = FALSE, overwrite = TRUE, row.names = FALSE)
  dbGetQuery(db, paste("SELECT * from",db_table_names[[i]])) %>% as_tibble() -> df_postgres
}

# Close connection
dbDisconnect(db)
dbUnloadDriver(db_driver)