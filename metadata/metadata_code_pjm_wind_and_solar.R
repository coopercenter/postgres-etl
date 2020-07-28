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
# TRUE

# ----------------------------------------------------------------------------------
pjm_solar<-dbGetQuery (db,'SELECT * from pjm_solar')
pjm_solar_cols<-list(colnames(pjm_solar))

r1<- data.frame(db_table_name = "pjm_solar",
                short_series_name= 'Data on active solar plants in Virginia',
                full_series_name = 'Data on active solar plants in Virginia with service dates',
                column2variable_name_map=I(pjm_solar_cols), units='MW', frequency=NA,
                data_source_brief_name='PJM',data_source_full_name='PJM Interconnection LLC',
                url='https://www.pjm.com/planning/services-requests/interconnection-queues.aspx',api=NA,
                series_id=NA,json=NA,notes=NA, data_type='cross-sectional', data_context='historical',
                corresponding_data='pjm_solar.xlsx', R_script='cleaning_pjm_wind_and_solar', 
                latest_data_update=NA, last_db_refresh='2020-06-17')

# ----------------------------------------------------------------------------------
pjm_wind<-dbGetQuery (db,'SELECT * from pjm_wind')
pjm_wind_cols<-list(colnames(pjm_wind))
r2<- data.frame(db_table_name = "pjm_wind",
                short_series_name= 'Data on active wind plants in Virginia',
                full_series_name = 'Data on active wind plants in Virginia with service dates',
                column2variable_name_map=I(pjm_wind_cols), units='MW', frequency=NA,
                data_source_brief_name='PJM',data_source_full_name='PJM Interconnection LLC',
                url='https://www.pjm.com/planning/services-requests/interconnection-queues.aspx',api=NA,
                series_id=NA,json=NA,notes=NA, data_type='cross-sectional', data_context='historical',
                corresponding_data='pjm_wind.xlsx', R_script='cleaning_pjm_wind_and_solar', 
                latest_data_update=NA, last_db_refresh='2020-06-17')

library(plyr)
metadata<-rbind(r1,r2)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)


