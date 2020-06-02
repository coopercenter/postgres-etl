library(here)
library('RPostgreSQL')
library(tidyverse)
source(here("api_data_code", "my_postgres_credentials.R"))
db_driver <- dbDriver("PostgreSQL")

db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)

rm(ra_pwd)

#sample code used to get the metadata for EIA datasets
library(eia)
library(here)
library(RPostgreSQL)
library(tidyverse)

metadata <- dbGetQuery(db,'SELECT * from metadata')


## read in my eia api key
source(here("api_data_code","my_eia_api_key.R"))
eia_set_key(eiaKey)

series_id_list <- c("ELEC.GEN.COW-VA-99.M",
                    "ELEC.GEN.COW-VA-99.A",
                    "ELEC.GEN.PEL-VA-99.A",
                    "ELEC.GEN.NG-VA-99.A",
                    "ELEC.GEN.NUC-VA-99.A",
                    "ELEC.GEN.SUN-VA-99.A",
                    "ELEC.GEN.DPV-VA-99.A",
                    "ELEC.GEN.HYC-VA-99.A",
                    "ELEC.GEN.WWW-VA-99.A",
                    "ELEC.GEN.WAS-VA-99.A",
                    "SEDS.TERCB.VA.A",
                    "SEDS.TECCB.VA.A",
                    "SEDS.TEICB.VA.A",
                    "SEDS.TEACB.VA.A","EMISS.CO2-TOTV-TT-TO-VA.A",
                    "EMISS.CO2-TOTV-TT-CO-VA.A","EMISS.CO2-TOTV-TT-NG-VA.A",
                    "EMISS.CO2-TOTV-TT-PE-VA.A")

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

eia_apis <- list('https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.COW-VA-99.M',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.COW-VA-99.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.PEL-VA-99.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.NG-VA-99.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.NUC-VA-99.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.SUN-VA-99.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.DPV-VA-99.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.HYC-VA-99.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.WWW-VA-99.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=ELEC.GEN.WAS-VA-99.A',
                 'https://www.eia.gov/opendata/qb.php?category=711238&sdid=SEDS.TERCB.VA.A',
                 'https://www.eia.gov/opendata/qb.php?category=711238&sdid=SEDS.TECCB.VA.A',
                 'https://www.eia.gov/opendata/qb.php?category=711238&sdid=SEDS.TEICB.VA.A',
                 'https://www.eia.gov/opendata/qb.php?category=711238&sdid=SEDS.TEACB.VA.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=EMISS.CO2-TOTV-TT-TO-VA.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=EMISS.CO2-TOTV-TT-CO-VA.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=EMISS.CO2-TOTV-TT-NG-VA.A',
                 'https://www.eia.gov/opendata/qb.php?sdid=EMISS.CO2-TOTV-TT-PE-VA.A'
)

eia_short_names<-list('Monthly coal generation',
                      'Annual coal generation',
                      'Annual petroleum generation',
                      'Annual natural gas generation',
                      'Annual nuclear generation',
                      'Annual utility scale solar generation',
                      'Annual small scale solar generation',
                      'Annual hydroelectric generation',
                      'Annual wood derived fuel generation',
                      'Annual other biomass generation',
                      'Total energy consumed by the residential sector',
                      'Total energy consumed by the commercial sector',
                      'Total energy consumed by the industrial sector',
                      'Total energy consumed by the transportation sector',
                      'Total carbon dioxide emissions, all sectors',
                      'Total coal carbon dioxide emissions, all sectors',
                      'Total natural gas carbon dioxide emissions, all sectors',
                      'Total petroleum carbon dioxide emissions, all sectors'
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
                            short_series_name= eia_short_names[[i]],
                            full_series_name = all_eia_meta[[i]]$name,
                            column2variable_name_map=I(list(cols[[i]])),units=all_eia_meta[[i]]$units,frequency=all_eia_meta[[i]]$f,
                            data_source_brief_name='EIA',data_source_full_name='U.S. Energy Information Administration',
                            url=NA,api=eia_apis[[i]],
                            series_id=all_eia_meta[[i]]$series_id,json=NA,notes=NA)
  metadata<-rbind(metadata,fit_meta[[i]])
}

dbWriteTable(db, 'metadata', value = metadata, append = TRUE, overwrite = FALSE, row.names = FALSE)



## Close connection
dbDisconnect(db)
