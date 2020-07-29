# Connection to the database
# "my_postgres_credentials.R" contains the log-in informations of RAs
library(here)
library('RPostgreSQL')
source(here("my_postgres_credentials.R"))
library(plyr)
library(tidyverse)

db_driver <- dbDriver("PostgreSQL")
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

# ----------------------------------------------------------------------------------
metadata <- data.frame(matrix(ncol = 19, nrow = 0))
colnames(metadata) <- c('db_table_name','short_series_name','full_series_name',
                        'column2variable_name_map','units','frequency',
                        'data_source_brief_name','data_source_full_name','url',
                        'api','series_id','json','notes', 'data_type','data_context','corresponding_data','R_script',
                        'latest_data_update','last_db_refresh')


eia860_list <- list('eia860_utility_2014','eia860_utility_2015','eia860_utility_2016',
                    'eia860_utility_2017','eia860_utility_2018','eia860_utility_2019',
                    'eia860_plant_2014','eia860_plant_2015','eia860_plant_2016',
                    'eia860_plant_2017','eia860_plant_2018','eia860_plant_2019',
                    'eia860_generator_2014','eia860_generator_2015','eia860_generator_2016',
                    'eia860_generator_2017','eia860_generator_2018','eia860_generator_2019')

eia860_short_names <- list('Utility data, 2014',
                           'Utility data, 2015',
                           'Utility data, 2016',
                           'Utility data, 2017',
                           'Utility data, 2018',
                           'Utility data, 2019',
                           'Plant data, 2014',
                           'Plant data, 2015',
                           'Plant data, 2016',
                           'Plant data, 2017',
                           'Plant data, 2018',
                           'Plant data, 2019',
                           'Generator data, 2014',
                           'Generator data, 2015',
                           'Generator data, 2016',
                           'Generator data, 2017',
                           'Generator data, 2018',
                           'Generator data, 2019')

eia860_full_names <- list('Utility-level data for the plants and generators surveyed in 2014, USA',
                          'Utility-level data for the plants and generators surveyed in 2015, USA',
                          'Utility-level data for the plants and generators surveyed in 2016, USA',
                          'Utility-level data for the plants and generators surveyed in 2017, USA',
                          'Utility-level data for the plants and generators surveyed in 2018, USA',
                          'Utility-level data for the plants and generators surveyed in 2019, USA',
                          'Plant-level data for the generators surveyed in 2014, USA',
                          'Plant-level data for the generators surveyed in 2015, USA',
                          'Plant-level data for the generators surveyed in 2016, USA',
                          'Plant-level data for the generators surveyed in 2017, USA',
                          'Plant-level data for the generators surveyed in 2018, USA',
                          'Plant-level data for the generators surveyed in 2019, USA',
                          'Generator-level data for the surveyed generators in 2014, USA',
                          'Generator-level data for the surveyed generators in 2015, USA',
                          'Generator-level data for the surveyed generators in 2016, USA',
                          'Generator-level data for the surveyed generators in 2017, USA',
                          'Generator-level data for the surveyed generators in 2018, USA',
                          'Generator-level data for the surveyed generators in 2019, USA')

cols<-vector("list", length(eia860_list))
for (i in 1:length(eia860_list)){
  query <- dbGetQuery(db, paste('SELECT * from',eia860_list[[1]], sep=' '))
  cols[[i]] <- list(colnames(query))
}


fit_meta <- vector("list", length(eia860_list))
for (i in 1:length(eia860_list)){
  fit_meta[[i]] <- data.frame(db_table_name = eia860_list[[i]],
                            short_series_name= eia860_short_names[[i]],
                            full_series_name = eia860_full_names[[i]],
                            column2variable_name_map=I(list(cols[[i]])), units=NA, frequency=NA,
                            data_source_brief_name='EIA', data_source_full_name='U.S. Energy Information Administration',
                            url='https://www.eia.gov/electricity/data/eia860/', api=NA,
                            series_id=NA, json=NA, notes='From EIA-860', data_type='cross-sectional',
                            data_context='historical', corresponding_data=NA,
                            R_script="cleaning_eia860.R", latest_data_update=NA,
                            last_db_refresh= lubridate::with_tz(Sys.time(), "UTC"))
    metadata<-rbind(metadata,fit_meta[[i]])
}


dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)

## Close connection
dbDisconnect(db)
