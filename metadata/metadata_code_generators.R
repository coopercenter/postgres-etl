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

r1<- data.frame(db_table_name = "generator_2014",
                short_series_name= 'Virginia generator data 2014',
                full_series_name = 'In depth Virginia data about existing and planned generators 2014',
                column2variable_name_map=NA,units='MW',frequency='A',
                data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                url='https://www.eia.gov/electricity/data/eia860/',api=NA,
                series_id=NA,json=NA,notes='From EIA 860', data_type='cross-sectional', data_context='historical', 
                corresponding_data=NA, R_script='cleaning_generator_data.R')

# ----------------------------------------------------------------------------------
r2<- data.frame(db_table_name = "generator_2015",
                short_series_name= 'Virginia generator data 2015',
                full_series_name = 'In depth Virginia data about existing and planned generators 2015',
                column2variable_name_map=NA,units='MW',frequency='A',
                data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                url='https://www.eia.gov/electricity/data/eia860/',api=NA,
                series_id=NA,json=NA,notes='From EIA 860', data_type='cross-sectional', data_context='historical', 
                corresponding_data=NA, R_script='cleaning_generator_data.R')

# ----------------------------------------------------------------------------------

r3<- data.frame(db_table_name = "generator_2016",
                short_series_name= 'Virginia generator data 2016',
                full_series_name = 'In depth Virginia data about existing and planned generators 2016',
                column2variable_name_map=NA,units='MW',frequency='A',
                data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                url='https://www.eia.gov/electricity/data/eia860/',api=NA,
                series_id=NA,json=NA,notes='From EIA 860', data_type='cross-sectional', data_context='historical', 
                corresponding_data=NA, R_script='cleaning_generator_data.R')

# ----------------------------------------------------------------------------------

r4<- data.frame(db_table_name = "generator_2017",
                short_series_name= 'Virginia generator data 2017',
                full_series_name = 'In depth Virginia data about existing and planned generators 2017',
                column2variable_name_map=NA,units='MW',frequency='A',
                data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                url='https://www.eia.gov/electricity/data/eia860/',api=NA,
                series_id=NA,json=NA,notes='From EIA 860', data_type='cross-sectional', data_context='historical', 
                corresponding_data=NA, R_script='cleaning_generator_data.R')

# ----------------------------------------------------------------------------------
r5<- data.frame(db_table_name = "generator_2018",
                short_series_name= 'Virginia generator data 2018',
                full_series_name = 'In depth Virginia data about existing and planned generators 2018',
                column2variable_name_map=NA,units='MW',frequency='A',
                data_source_brief_name='EIA 860 Data',data_source_full_name='U.S. Energy Information Administration',
                url='https://www.eia.gov/electricity/data/eia860/',api=NA,
                series_id=NA,json=NA,notes='From EIA 860', data_type='cross-sectional', data_context='historical', 
                corresponding_data=NA, R_script='cleaning_generator_data.R')

# ----------------------------------------------------------------------------------

r6<- data.frame(db_table_name = "generator_2019_early_release",
                short_series_name= 'Virginia generator data 2019 early release',
                full_series_name = 'In depth Virginia data about existing and planned generators 2019, pending data validation',
                column2variable_name_map=NA,units='MW',frequency='A',
                data_source_brief_name='EIA 860 Data',data_source_full_name='U.S. Energy Information Administration',
                url='https://www.eia.gov/electricity/data/eia860/',api=NA,
                series_id=NA,json=NA,notes='From EIA 860', data_type='cross-sectional', data_context='historical', 
                corresponding_data=NA, R_script='cleaning_generator_data.R')

library(plyr)
metadata<-rbind(r1,r2,r3,r4,r5,r6)
dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)