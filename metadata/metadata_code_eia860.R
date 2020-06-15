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

r1<- data.frame(db_table_name = "eia860_generator_y2018",
                short_series_name= 'Virginia generator data',
                full_series_name = 'In depth Virginia data about existing and planned generators',
                column2variable_name_map=NA,units='megawatt',frequency='A',
                data_source_brief_name='EIA',data_source_full_name='Energy Information Administration 860 Detailed Data, 2018',
                url='https://www.eia.gov/electricity/data/eia860/',api=NA,
                series_id=NA,json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=NA, R_script=NA)

# ----------------------------------------------------------------------------------
r2<- data.frame(db_table_name = "eia860_plant_y2018",
                short_series_name= 'Virginia plant data',
                full_series_name = 'In depth Virginia data about existing and planned plants',
                column2variable_name_map=NA,units='megawatt',frequency='A',
                data_source_brief_name='EIA',data_source_full_name='Energy Information Administration 860 Detailed Data, 2018',
                url='https://www.eia.gov/electricity/data/eia860/',api=NA,
                series_id=NA,json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=NA, R_script=NA)

# ----------------------------------------------------------------------------------

r3<- data.frame(db_table_name = "eia860_utility_y2018",
                short_series_name= 'Virginia utility data',
                full_series_name = 'In depth Virginia data about existing and planned plants and generators',
                column2variable_name_map=NA,units='megawatt',frequency='A',
                data_source_brief_name='EIA',data_source_full_name='Energy Information Administration 860 Detailed Data, 2018',
                url='https://www.eia.gov/electricity/data/eia860/',api=NA,
                series_id=NA,json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=NA, R_script=NA)

library(plyr)
metadata<-rbind(r1,r2,r3)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)
