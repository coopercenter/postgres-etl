library(httr)
library(here)
library(dplyr)
library(tibble)
library(readr)
if(!("RPostgreSQL" %in% installed.packages())) install.packages("eia")
library("RPostgreSQL")
if(!("eia" %in% installed.packages())) install.packages("eia")
library(eia)
library(stringr)

# creating a list of series ids
series_id_vec <- read_file(here("etl","series_ids.txt"))
series_id_list <- unlist(strsplit(series_id_vec,'\r\n'))
url_root <- "http://api.eia.gov/series/"

# read in my eia api key
source(here("etl","my_eia_api_key.R"))
eia_set_key(my_api_key)

# function used to fetch the eia series
fetch_eia_series <- function(series_id){
  
  data_series <- eia_series(series_id)
  
  return(data_series)
}


# apply the function to each series id in the series id list
all_data_series <-lapply(series_id_list,fetch_eia_series)

# create an empty list to store the data tables
all_tables<- vector("list", length(series_id_list))

# function used to display data
displaydata <- function(series) {
  (series$data[[1]] -> series_data_tbl)
  return(series_data_tbl)
}

# loops through the all data series and store the tables in to a big list
for (i in 1:length(all_data_series)){
  series<-all_data_series[[i]]
  all_tables[[i]]<-displaydata(series)
}


# Connection to the database
# "my_postgres_login.R" contains the log-in informations of RAs
source(here("etl", "my_postgres_login.R"))
drv <- dbDriver("PostgreSQL")

con<- dbConnect(drv, dbname = dbname, host = host, port = port, user = user, password = password)

rm(password)

# check the connection
dbExistsTable(con, "fuel")
# TRUE
dbListTables(con)

# Function used to change the names of tables to the format used in MySQL
get_name <- function(series_id) {
  db_table_name <- str_to_lower(paste("eia", str_replace_all(series_id, "[.-]", "_"), sep="_"))
  return(db_table_name)
}

# apply the function to the list of series id
db_table_names <- lapply(series_id_list,get_name)

# Loops through the list of tables and write data series to the PostgreSQL database 
# "postgres": OVERWRITES EXISTING TABLE!
# then query the data from postgreSQL
for (i in 1:length(all_data_series)){
  dbWriteTable(con, db_table_names[[i]], value = all_tables[[i]], append = FALSE, overwrite = TRUE, row.names = FALSE)
  dbGetQuery(con, paste("SELECT * from",db_table_names[[i]])) %>% as_tibble() -> df_postgres
}

# Close connection
dbDisconnect(con)
dbUnloadDriver(drv)