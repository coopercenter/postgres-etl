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

# Use forward slash or backslash as separator in file paths and URLs, depending on operating system
slash <- ifelse(Sys.info()[['sysname']]=="Windows", "\\", "/")

source(here("etl","my_eia_api_key.R"))
fetch_eia_series <- function(series_id){
  require(eia)
  
  eia_set_key(my_api_key) # set API key if not already set globally
  
  data_series <- eia_series(series_id)
  
  return(data_series)
}
series_id_vec <- read_file(here("etl","almanac_series_ids.txt"))
series_id_list <- strsplit(series_id_vec,'\r\n')
series_id_list <-unlist(series_id_list)
url_root <- paste("http://api.eia.gov/series/",sep=slash)

all_data_series <-lapply(series_id_list,fetch_eia_series)
all_tables<- vector("list", length(series_id_list))

displaydata <- function(series) {
  (series$data[[1]] -> series_data_tbl)
  return(series_data_tbl)
}

for (i in 1:length(all_data_series)){
  series<-all_data_series[[i]]
  all_tables[[i]]<-displaydata(series)
}


# Connection to the database
source(here("etl", "jackson_postgres_login.R"))
drv <- dbDriver("PostgreSQL")

con<- dbConnect(drv, dbname = dbname, host = host, port = port, user = user, password = password)

rm(password)

# check the connection
dbExistsTable(con, "eia_elec_gen_spv_va_99_m")
# TRUE
dbListTables(con)

get_name <- function(series_id) {
  db_table_name <- str_to_lower(paste("eia", str_replace_all(series_id, "[.-]", "_"), sep="_"))
  return(db_table_name)
}

db_table_names <- lapply(series_id_list,get_name)

# write data series to the PostgreSQL database "postgres": OVERWRITES EXISTING TABLE!
dbWriteTable(con, db_table_names[1], value = all_tables[1], append = FALSE, overwrite = TRUE, row.names = FALSE)

for (i in 1:length(all_data_series)){
  dbWriteTable(con, db_table_names[[i]], value = all_tables[[i]], append = FALSE, overwrite = TRUE, row.names = FALSE)
  # query the data from postgreSQL 
  dbGetQuery(con, paste("SELECT * from",db_table_names[[i]])) %>% as_tibble() -> df_postgres
}


# Close connection

dbDisconnect(con)
dbUnloadDriver(drv)
