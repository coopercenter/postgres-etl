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
res_pop<-dbGetQuery(db,'SELECT * FROM fred_vangsp')

res_col <- list(c('date', 'value'))

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


source(here("my_fred_api_key.R"))
pop_meta <- get_FRED_meta(fredKey,"VANGSP")

r1<- data.frame(db_table_name = "fred_vangsp",
                short_series_name= 'VA GDP',
                full_series_name = 'Virginia Gross Domesic Product for Virginia from 1997 to 2019',
                column2variable_name_map=I(res_col),units=I(pop_meta$units),frequency=pop_meta$frequency,
                data_source_brief_name='FRED',data_source_full_name='Federal Reserve Economic Data',
                url=NA,api='https://fred.stlouisfed.org/series/VANGSP',
                series_id='VANGSP',json=NA,notes=NA)


dbWriteTable(db, 'metadata', value = r1, append = TRUE, overwrite = FALSE, row.names = FALSE)
