# Creating the dataframe for metadata
metadata<-data.frame(matrix(ncol = 12, nrow = 0))

# Specify the column names
colnames(metadata) <- c('db_table_name','full_series_name',
                        'column2variable_name_map','units','frequency',
                        'data_source','data_source_full_name','url',
                        'api','series_id','json','notes')

#--------------------------------------------------------------------------------------
# Sample Code used to manually write the metadata

# Connection to the database
# "my_postgres_credentials.R" contains the log-in informations of RAs
library(here)
library('RPostgreSQL')
library(tidyverse)
source(here("etl", "my_postgres_credentials.R"))
db_driver <- dbDriver("PostgreSQL")

db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)

rm(ra_pwd)

# check the connection
# if this returns true, it means that you are connected to the database now
dbExistsTable(db, "metadata")

# get the cleaned dataset from the database
## fuel_cleaned should be replaced by the name of your dataset in the database
## (coordinate with Jackson)
a<-dbGetQuery(db,'SELECT * from metadata')

# put the column names into a list
fuel_cols<-list(colnames(fuel))

# put the unit of each column into a list
fuel_units <-list(c('Year','dollars_per_million_Btu','Btu_per_pound','percent',
                    'dollars_per_million_Btu','Btu_per_pound','percent',
                    'dollars_per_million_Btu','Btu_per_cubic_foot'))

# Construct a data frame with only one row, if you have more than one dataset,
# construct r2, r3,... if needed
r1<- data.frame(db_table_name = "fuel", short_series_name = "fuel price and quality",
                full_series_name = 'Electric power delivered fuel prices and quality for coal, petroleum, Natural gas, 1990 through 2018',
                column2variable_name_map=I(fuel_cols),units=I(fuel_units),frequency='Y',
                data_source='EIA',data_source_full_name='U.S. Energy Information Administration',
                url='https://www.eia.gov/electricity/state/virginia/state_tables.php',api=NA,
                series_id=NA,json=NA,notes=NA)

library(plyr)
# if you have more than one dataset,rbind the rows first,and then bind it to the metadata
# Example
# r1 <- rbind(r1,r2)
metadata<-rbind(metadata,r1)

# WARNING
# Do not run dbWriteTable before you check with Christina and Chloe
# This will overwrite the existing metadata table in the db
dbWriteTable(con, 'metadata', value = metadata, append = FALSE, overwrite = TRUE, row.names = FALSE)

#------------------------------------------------------------------------------------
#sample code used to get the metadata for EIA datasets
library(eia)
library(here)
library(RPostgreSQL)
library(tidyverse)


## read in my eia api key
source(here("etl","my_eia_api_key.R"))
eia_set_key(eiaKey)

## create a list of series ids
series_id_vec <- read_file(here("etl","series_ids.txt"))
series_id_list <- unlist(strsplit(series_id_vec,'\r\n'))

## create a empty list to store the metadata tables
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

# eia_short <- list()
eia_urls <- list('https://www.eia.gov/opendata/qb.php?sdid=EMISS.CO2-TOTV-EC-TO-VA.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=SEDS.TETCB.VA.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=SEDS.TEPRB.VA.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.SALES.VA-ALL.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.SALES.VA-ALL.M',
                 'https://www.eia.gov/opendata/qb.php?category=1&sdid=ELEC.GEN.ALL-VA-99.M',
                 'https://www.eia.gov/opendata/qb.php?category=1736519&sdid=ELEC.GEN.ALL-VA-99.A',
                 'https://www.eia.gov/opendata/qb.php?category=1718408&sdid=ELEC.GEN.TSN-VA-99.M',
                 'https://www.eia.gov/opendata/qb.php?category=12&sdid=ELEC.GEN.HYC-VA-99.M',
                 'https://www.eia.gov/opendata/qb.php?category=40&sdid=ELEC.PRICE.VA-ALL.M',
                 'https://www.eia.gov/opendata/qb.php?category=40&sdid=ELEC.PRICE.VA-COM.M',
                 'https://www.eia.gov/opendata/qb.php?category=40&sdid=ELEC.PRICE.VA-RES.M',
                 'https://www.eia.gov/opendata/qb.php?category=40&sdid=ELEC.PRICE.VA-IND.M',
                 'https://www.eia.gov/opendata/qb.php?category=40&sdid=ELEC.PRICE.VA-TRA.M',
                 'https://www.eia.gov/opendata/qb.php?category=1718389&sdid=ELEC.CUSTOMERS.VA-ALL.M',
                 'https://www.eia.gov/opendata/qb.php?category=11&sdid=ELEC.GEN.NUC-VA-99.M',
                 'https://www.eia.gov/opendata/qb.php?category=11&sdid=ELEC.GEN.COW-VA-99.M',
                 'https://www.eia.gov/opendata/qb.php?category=11&sdid=ELEC.GEN.NG-VA-99.M',
                 'https://www.eia.gov/opendata/qb.php?category=11&sdid=ELEC.GEN.AOR-VA-99.M',
                 'https://www.eia.gov/opendata/qb.php?category=11&sdid=ELEC.GEN.PEL-VA-99.M',
                 'https://www.eia.gov/opendata/qb.php?category=11&sdid=ELEC.GEN.SUN-VA-99.M',
                 'Fhttps://www.eia.gov/opendata/qb.php?category=11&sdid=ELEC.GEN.DPV-VA-99.M',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.SUN-VA-2.M',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.COW-VA-98.M',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.SPV-VA-99.M',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.WWW-VA-99.M',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.WAS-VA-99.M',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.HPS-VA-99.M',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.OTH-VA-99.M'
)

col2var<-vector("list", length(series_id_list))
cols<-vector("list", length(series_id_list))

for (i in 1:length(series_id_list)){
  col2var[[i]]<-all_eia_meta[[i]][['data']][[1]] %>% 
    dplyr::summarise_all(class) %>% 
    tidyr::gather(variable, class)
  cols[[i]]<-c(col2var[[i]]$variable)
}


# Create metadata dataframes for each dataset and fill in the info
for (i in 1:length(series_id_list)){
  fit_meta[[i]]<-data.frame(db_table_name = eia_data_names[[i]],
                            full_series_name = all_eia_meta[[i]]$name,
                            column2variable_name_map=I(list(cols[[i]])),units=all_eia_meta[[i]]$units,frequency=all_eia_meta[[i]]$f,
                            data_source='EIA',data_source_full_name='U.S. Energy Information Administration',
                            url=eia_urls[[i]],api=eiaKey,
                            series_id=all_eia_meta[[i]]$series_id,json=NA,notes=NA)
  metadata<-rbind(metadata,fit_meta[[i]])
}

db_driver = dbDriver("PostgreSQL")
source(here("etl", "my_postgres_credentials.R"))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)

dbWriteTable(db, 'metadata', value = metadata, append = FALSE, overwrite = TRUE, row.names = FALSE)



## Close connection
dbDisconnect(db)
