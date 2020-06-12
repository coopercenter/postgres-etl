library(here)
library('RPostgreSQL')
library(tidyverse)
#source(here("my_postgres_credentials.R"))
#db_driver <- dbDriver("PostgreSQL")

#db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)

#rm(ra_pwd)

# check the connection
#dbExistsTable(db, "metadata")
# TRUE

states = c("AK","AL","AR","AZ","CA",
           "CO","CT","DE","FL","GA",
           "HI","IA","ID","IL","IN",
           "KS","KY","LA","MA","MD",
           "ME","MI","MN","MO","MS",
           "MT","NC","ND","NE","NH",
           "NJ","NM","NV","NY","OH",
           "OK","OR","PA","RI","SC",
           "SD","TN","TX","UT","VA",
           "VT","WA","WI","WV","WY")

# ----------------------------------------------------------------------------------
all_r1_year <- NULL
for(state in states){
  gen_by_source_cols <- list(c('year','coal','oil','gas','nuclear',
                               'wind','utility_solar','distributed_solar',
                               'hydropower','wood','other_biomass','total'))
  gen_by_source_units <-'gigawatt hours'
  
  r1_year <- data.frame(db_table_name = paste0("eia_elec_gen_",str_to_lower(state),"_a",sep =""),
                  short_series_name= 'Total gigawatt hour of generation by state',
                  full_series_name = 'Total gigawatt hour of generation by state in phases from 2001 through 2020',
                  column2variable_name_map=I(gen_by_source_cols ),units=I(gen_by_source_units),frequency='A',
                  data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                  url='https://www.eia.gov/electricity/data/browser/',api=NA,
                  series_id="ELEC.GEN.ALL-",state,"-99.A",json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=NA, 
                  R_script='generation_all_states.R')
  
  if (is.null(all_r1_year))
  {all_r1_year <- r1_year}
  else
  {all_r1_year <-  rbind(all_r1_year, r1_year)}
  
  
}

all_r1_month <- NULL
for(state in states){
  gen_by_source_cols <- list(c('year','coal','oil','gas','nuclear',
                               'wind','utility_solar','distributed_solar',
                               'hydropower','wood','other_biomass','total'))
  gen_by_source_units <-'gigawatt hours'
  
  r1_month<- data.frame(db_table_name = paste0("eia_elec_gen_",str_to_lower(state),"_m",sep =""),
                  short_series_name= 'Total gigawatt hour of generation by state',
                  full_series_name = 'Total gigawatt hour of generation by state in phases from 2001 through 2020',
                  column2variable_name_map=I(gen_by_source_cols ),units=I(gen_by_source_units),frequency='A',
                  data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                  url='https://www.eia.gov/electricity/data/browser/',api=NA,
                  series_id="ELEC.GEN.ALL-",state,"-99.M",json=NA,notes=NA, mandate=0, forecast=0, corresponding_data=NA, 
                  R_script='generation_all_states.R')
  
  if (is.null(all_r1_month))
  {all_r1_month <- r1_month}
  else
  {all_r1_month <-  rbind(all_r1_month, r1_month)}
  
}


dbWriteTable(db, 'metadata', value = r1, append = TRUE, overwrite = FALSE, row.names = FALSE)


