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

# read in my FRED api key
source(here("etl","my_FRED_api_key.R"))

# function used to fetch the series
get_FRED_series <- function(api_key,series_id) {
  require(jsonlite)
  require(data.table)
  # This function retrieves one EIA time-series with metadata
  # The function returns a list of parts of the series:
  #     seriesID,name,units,frequency,data (as data table)

  
  # Example URL
  # https://api.stlouisfed.org/fred/series?series_id=GNPCA&api_key=abcdefghijklmnopqrstuvwxyz123456&file_type=json
  
  FREDBase = paste0("https://api.stlouisfed.org/fred/series?series_id=",series_id,"&api_key=",api_key,"&file_type=json") 
  temp = readLines(FREDBase, warn = "F")
  rd <- fromJSON(temp)
  print(paste0("Retrieving: ",rd$series$series_id))
  print(paste0(rd$series$name))
  
  # Now take the 'data' element from the list and make a data frame
  rd2 = data.frame(rd$series$data,stringsAsFactors = F)
  rd2 = data.table(rd2)
  
  setnames(rd2,1,"date"); setnames(rd2,2,'value')
  rd2[,value:=as.numeric(value)]
  rd2$date = as.Date(gsub('(\\d{4})(\\d{2})', '\\1-\\2-01', rd2$date))
  returnList = list(
    series_id = rd$series$series_id,
    name = rd$series$name,
    units = rd$series$units,
    frequency = rd$series$f,
    data = rd2
  )
  return(returnList) 
}

data_series = get_FRED_series(my_api_key,"VAPOP")


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

fuel<-dbGetQuery(con,'SELECT * from fuel')
postgresqlDescribeResult(fuel)





# Close connection
dbDisconnect(con)
dbUnloadDriver(drv)



