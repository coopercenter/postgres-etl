# Connection to the database
# "my_postgres_credentials.R" contains the log-in informations of RAs
library(here)
library('RPostgreSQL')
library(plyr)
library(tidyverse)
source(here("my_postgres_credentials.R"))

db_driver <- dbDriver("PostgreSQL")
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

# check the connection
dbExistsTable(db, "metadata")
# TRUE

# ----------------------------------------------------------------------------------
#PJM Solar
pjm_solar<-dbGetQuery (db,'SELECT * from pjm_solar')
pjm_solar_cols<-list(colnames(pjm_solar))

r1<- data.frame(db_table_name = "pjm_solar",
                short_series_name= 'Data on active solar plants in Virginia',
                full_series_name = 'Data on active solar plants in Virginia with service dates',
                column2variable_name_map=I(pjm_solar_cols), units='MW', frequency=NA,
                data_source_brief_name='PJM',data_source_full_name='PJM Interconnection LLC',
                url='https://www.pjm.com/planning/services-requests/interconnection-queues.aspx',api=NA,
                series_id=NA,json=NA,notes=NA, data_type='cross-sectional', data_context='historical',
                corresponding_data=NA, R_script='cleaning_pjm.R', 
                latest_data_update=NA, last_db_refresh='2020-06-17')

# ----------------------------------------------------------------------------------
#PJM Wind
pjm_wind<-dbGetQuery (db,'SELECT * from pjm_wind')
pjm_wind_cols<-list(colnames(pjm_wind))

r2 <- data.frame(db_table_name = "pjm_wind",
                short_series_name= 'Data on active wind plants in Virginia',
                full_series_name = 'Data on active wind plants in Virginia with service dates',
                column2variable_name_map=I(pjm_wind_cols), units='MW', frequency=NA,
                data_source_brief_name='PJM',data_source_full_name='PJM Interconnection LLC',
                url='https://www.pjm.com/planning/services-requests/interconnection-queues.aspx',api=NA,
                series_id=NA,json=NA,notes=NA, data_type='cross-sectional', data_context='historical',
                corresponding_data=NA, R_script='cleaning_pjm.R', 
                latest_data_update=NA, last_db_refresh='2020-06-17')

# ----------------------------------------------------------------------------------
#PJM Storage
pjm_storage<-dbGetQuery (db,'SELECT * from pjm_storage')
pjm_storage_cols<-list(colnames(pjm_storage))

r3 <- data.frame(db_table_name = "pjm_storage",
                short_series_name= '',
                full_series_name = '',
                column2variable_name_map=I(pjm_storage_cols), units='', frequency=NA,
                data_source_brief_name='PJM',data_source_full_name='PJM Interconnection LLC',
                url='https://www.pjm.com/planning/services-requests/interconnection-queues.aspx',api=NA,
                series_id=NA,json=NA,notes=NA, data_type='cross-sectional', data_context='historical',
                corresponding_data=NA, R_script='cleaning_pjm.R', 
                latest_data_update=NA, last_db_refresh='2020-06-17')

# ----------------------------------------------------------------------------------
#PJM GATS Generators
pjm_gats_generators<-dbGetQuery (db,'SELECT * from pjm_gats_generators')
pjm_gats_cols <- list(colnames(pjm_gats_generators))

r4 <- data.frame(db_table_name = 'pjm_gats_generators',
                short_series_name = 'Data on renewable generators by GATS ',
                full_series_name = 'Data on renewable generators registered in Generation Attribute Tracking System ',
                column2variable_name_map = I(pjm_gats_cols), units = NA, frequency = NA,
                data_source_brief_name = 'PJM', data_source_full_name = 'PJM Interconnection LLC',
                url = 'https://gats.pjm-eis.com/gats2/PublicReports/RenewableGeneratorsRegisteredinGATS', api = NA,
                series_id = NA, json = NA, notes = NA, data_type = 'cross-sectional',
                data_context = 'historical', corresponding_data = NA,
                R_script = 'cleaning_pjm.R', latest_data_update = '2020-07-23',
                last_db_refresh = lubridate::with_tz(Sys.time(), "UTC"))

# ----------------------------------------------------------------------------------


metadata <- rbind(r1,r2,r3,r4)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)


