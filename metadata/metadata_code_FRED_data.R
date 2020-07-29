# Connection to the database
# "my_postgres_credentials.R" contains the log-in informations of RAs
library(here)
library('RPostgreSQL')
library(tidyverse)
source(here("my_postgres_credentials.R"))

db_driver <- dbDriver("PostgreSQL")
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

# check the connection
dbExistsTable(db, "metadata")
metadata <- dbGetQuery(db,'SELECT * from metadata')
# TRUE

# ----------------------------------------------------------------------------------
res_pop<-dbGetQuery(db,'SELECT * FROM fred_vapop')
res_col <- list(c('realtime_start','realtime_end','date','value'))

gdp <-dbGetQuery(db,'SELECT * FROM fred_vangsp')
gdp_col <- list(c('date', 'value'))

get_FRED_meta <- function(api_key,series_id) {
  require(jsonlite)
  require(data.table)
  FREDBase <- paste0("https://api.stlouisfed.org/fred/series?series_id=",series_id,"&api_key=",api_key,"&file_type=json") 
  temp <- readLines(FREDBase, warn = "F")
  rd <- fromJSON(temp)
  
  # Now take the 'data' element from the list and make a data frame
  returnList = list(
    series_id = rd$seriess$id,
    name = rd$seriess$title,
    units = rd$seriess$units,
    frequency = rd$seriess$frequency_short
  )
  return(returnList) 
}

source(here("api_data_code","my_fred_api_key.R"))
pop_meta <- get_FRED_meta(fredKey,"VAPOP")
gdp_meta <- get_FRED_meta(fredKey,"VANGSP")

r1<- data.frame(db_table_name = "fred_vapop",
                short_series_name= 'VA residential population',
                full_series_name = 'Virginia residential population',
                column2variable_name_map=I(res_col),units=I(pop_meta$units),frequency=pop_meta$frequency,
                data_source_brief_name='FRED',data_source_full_name='Federal Reserve Economic Data',
                url=NA,api='https://fred.stlouisfed.org/series/VAPOP',
                series_id='VAPOP',json=NA,notes=NA, data_type='time-series', data_context='historical', 
                corresponding_data=NA,
                R_script='fetch_from_FRED_api.R',
                latest_data_update='2019-01-01', last_db_refresh='2020-05-01')

r2<- data.frame(db_table_name = "fred_vangsp",
                short_series_name= 'VA GDP',
                full_series_name = 'Virginia Gross Domesic Product for Virginia from 1997 to 2019',
                column2variable_name_map=I(gdp_col),units=I(gdp_meta$units),frequency=gdp_meta$frequency,
                data_source_brief_name='FRED',data_source_full_name='Federal Reserve Economic Data',
                url=NA,api='https://fred.stlouisfed.org/series/VANGSP',
                series_id='VANGSP',json=NA,notes=NA, data_type='time-series', data_context='historical', 
                corresponding_data=NA,
                R_script='fetch_from_FRED_api.R',
                latest_data_update='2019-01-01', last_db_refresh='2020-05-01')

dbWriteTable(db, 'metadata', value = r1, append = TRUE, overwrite = FALSE, row.names = FALSE)
dbWriteTable(db, 'metadata', value = r2, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)
