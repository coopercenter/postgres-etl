##updating metadata
install.packages('mgsub')
# Creating the dataframe for metadata
metadata<-data.frame(matrix(ncol = 19, nrow = 0))

# Specify the column names
colnames(metadata) <- c('db_table_name','short_series_name','full_series_name',
                        'column2variable_name_map','units','frequency',
                        'data_source_brief_name','data_source_full_name','url',
                        'api','series_id','json','notes', 'data_type','data_context','corresponding_data','R_script',
                        'latest_data_update','last_db_refresh')

#------------------------------------------------------------------------------------
#sample code used to get the metadata for EIA datasets
library(eia)
library(here)
library(RPostgreSQL)
library(tidyverse)
library(units)
library(mgsub)

## read in my eia api key
source(here("api_data_code","my_eia_api_key.R"))
eia_set_key(eiaKey)

series_id_list <- list('SEDS.ELISP.VA.A')

all_eia_meta <- vector("list", length(series_id_list))

## loops through the series id list and store the corresponding metadata table
for (i in 1:length(series_id_list)){
  all_eia_meta[[i]] <- eia_series(series_id_list[[i]],n=10)
}

# Fit the metadata tables into the standard structure
## Create an empty list to store the reformatted metadata tables
fit_meta <- vector("list", length(series_id_list))

## create lists to store other info that is not in the metadata extracted using code

get_name <- function(series_id) {
  db_table_name <- str_to_lower(paste("eia", str_replace_all(series_id, "[.-]", "_"), sep="_"))
  return(db_table_name)
}

# apply the function to the list of series id to get the names for the data tables
eia_data_names <- lapply(series_id_list,get_name)


api_link <- vector("list", length(series_id_list))
for (i in 1:length(series_id_list)){
  api_link[[i]] <- paste("http://api.eia.gov/series/?api_key=",eiaKey,"&series_id=",series_id_list[[i]],sep='')
}


eia_short_names<-list('Annual net interstate flow of electricity')
                     

col2var<-vector("list", length(series_id_list))
cols<-vector("list", length(series_id_list))

for (i in 1:length(series_id_list)){
  col2var[[i]]<-all_eia_meta[[i]][['data']][[1]] %>% 
    dplyr::summarise_all(class) %>% 
    tidyr::gather(variable, class)
  cols[[i]]<-c(col2var[[i]]$variable)
}

data_update <- vector('list', length(series_id_list))
for (i in 1:length(series_id_list)) {
  data_update[[i]] <- all_eia_meta[[i]]$updated
  data_update[[i]] <- gsub("T", " ", data_update[[i]])
  data_update[[i]] <- gsub("-0400", "", data_update[[i]])
  data_update[[i]] <- strptime(data_update[[i]], format="%Y-%m-%d %H:%M:%S")
}

replace_unit <- function(string){
  unit <- mgsub(string,
                c('kilowatthour'),
                c('kWh'))
  return(unit)
}

eia_units <- vector('list', length(series_id_list))
for (i in 1:length(series_id_list)) {
  eia_unit <- all_eia_meta[[i]]$units
  eia_units[[i]] <- replace_unit(eia_unit)
}

# Create metadata dataframes for each dataset and fill in the info
for (i in 1:length(series_id_list)){
  fit_meta[[i]]<-data.frame(db_table_name = eia_data_names[[i]],
                            short_series_name= eia_short_names[[i]],
                            full_series_name = all_eia_meta[[i]]$name,
                            column2variable_name_map=I(list(cols[[i]])),units=eia_units[[i]],frequency=all_eia_meta[[i]]$f,
                            data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                            url=NA, api=api_link[[i]],
                            series_id=all_eia_meta[[i]]$series_id,json=NA,notes=NA, data_type='time-series',
                            data_context='historical', corresponding_data=NA,
                            R_script="fetch_from_eia_api.R", latest_data_update=data_update[[i]],
                            last_db_refresh= lubridate::with_tz(Sys.time(), "UTC"))
  metadata<-rbind(metadata,fit_meta[[i]])
}

db_driver = dbDriver("PostgreSQL")
source(here("my_postgres_credentials.R"))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)


dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)