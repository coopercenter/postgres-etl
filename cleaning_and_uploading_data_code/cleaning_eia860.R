library(dplyr)
library(tidyverse)
library(stringr) # for replacing strings
library(here)
library("RPostgreSQL")
library(readxl)

db_driver = dbDriver("PostgreSQL")
source(here('my_postgres_credentials.R'))
db <- dbConnect(db_driver,user=db_user, password=ra_pwd,dbname="postgres", host=db_host)
rm(ra_pwd)

#---------------------------------------------------------------------------------------
# Utility Datasets

#2014
utility_2014 <- read_excel(here('raw_data','eia860_utility_2014.xlsx'))
names(utility_2014)<-lapply(utility_2014[1,],as.character)
utility_2014 <- utility_2014[-1,]
dbWriteTable(db, 'eia860_utility_2014', utility_2014, row.names=FALSE, overwrite = TRUE)

#2015
utility_2015 <- read_excel(here('raw_data','eia860_utility_2015.xlsx'))
names(utility_2015)<-lapply(utility_2015[1,],as.character)
utility_2015 <- utility_2015[-1,]
dbWriteTable(db, 'eia860_utility_2015', utility_2015, row.names=FALSE, overwrite = TRUE)

#2016
utility_2016 <- read_excel(here('raw_data','eia860_utility_2016.xlsx'))
names(utility_2016)<-lapply(utility_2016[1,],as.character)
utility_2016 <- utility_2016[-1,]
dbWriteTable(db, 'eia860_utility_2016', utility_2016, row.names=FALSE, overwrite = TRUE)

#2017
utility_2017 <- read_excel(here('raw_data','eia860_utility_2017.xlsx'))
names(utility_2017)<-lapply(utility_2017[1,],as.character)
utility_2017 <- utility_2017[-1,]
dbWriteTable(db, 'eia860_utility_2017', utility_2017, row.names=FALSE, overwrite = TRUE)

#2018
utility_2018 <- read_excel(here('raw_data','eia860_utility_2018.xlsx'))
names(utility_2018)<-lapply(utility_2018[1,],as.character)
utility_2018 <- utility_2018[-1,]
dbWriteTable(db, 'eia860_utility_2018', utility_2018, row.names=FALSE, overwrite = TRUE)

#2019 Early Release
utility_2019 <- read_excel(here('raw_data','eia860_utility_2019.xlsx'))
utility_2019 <- utility_2019[-1,] #this additional line is needed for early release sets
names(utility_2019)<-lapply(utility_2019[1,],as.character)
utility_2019 <- utility_2019[-1,]
dbWriteTable(db, 'eia860_utility_2019', utility_2019, row.names=FALSE, overwrite = TRUE)




#---------------------------------------------------------------------------------------
# Plant Datasets

#2014
plant_2014 <- read_excel(here('raw_data', 'eia860_plant_2014.xlsx'))
names(plant_2014)<-lapply(plant_2014[1,],as.character)
plant_2014 <- plant_2014[-1,]
dbWriteTable(db, 'eia860_plant_2014', plant_2014, row.names=FALSE, overwrite = TRUE)

#2015
plant_2015 <- read_excel(here('raw_data', 'eia860_plant_2015.xlsx'))
names(plant_2015)<-lapply(plant_2015[1,],as.character)
plant_2015 <- plant_2015[-1,]
dbWriteTable(db, 'eia860_plant_2015', plant_2015, row.names=FALSE, overwrite = TRUE)

#2016
plant_2016 <- read_excel(here('raw_data', 'eia860_plant_2016.xlsx'))
names(plant_2016)<-lapply(plant_2016[1,],as.character)
plant_2016 <- plant_2016[-1,]
dbWriteTable(db, 'eia860_plant_2016', plant_2016, row.names=FALSE, overwrite = TRUE)

#2017
plant_2017 <- read_excel(here('raw_data', 'eia860_plant_2017.xlsx'))
names(plant_2017)<-lapply(plant_2017[1,],as.character)
plant_2017 <- plant_2017[-1,]
dbWriteTable(db, 'eia860_plant_2017', plant_2017, row.names=FALSE, overwrite = TRUE)

#2018
plant_2018 <- read_excel(here('raw_data', 'eia860_plant_2018.xlsx'))
names(plant_2018)<-lapply(plant_2018[1,],as.character)
plant_2018 <- plant_2018[-1,]
dbWriteTable(db, 'eia860_plant_2018', plant_2018, row.names=FALSE, overwrite = TRUE)

#2019 Early Release
plant_2019 <- read_excel(here('raw_data', 'eia860_plant_2019.xlsx'))
plant_2019 <- plant_2019[-1,]
names(plant_2019)<-lapply(plant_2019[1,],as.character)
plant_2019 <- plant_2019[-1,]
dbWriteTable(db, 'eia860_plant_2019', plant_2019, row.names=FALSE, overwrite = TRUE)


#---------------------------------------------------------------------------------------
# Generator Datasets

#2014
generator_2014 <- read_excel(here('raw_data','eia860_generator_2014.xlsx'))
names(generator_2014)<-lapply(generator_2014[1,],as.character)
generator_2014 <- generator_2014[-1,]
dbWriteTable(db, 'eia860_generator_2014', generator_2014, row.names=FALSE, overwrite = TRUE)

#2015
generator_2015 <- read_excel(here('raw_data','eia860_generator_2015.xlsx'))
names(generator_2015)<-lapply(generator_2015[1,],as.character)
generator_2015 <- generator_2015[-1,]
dbWriteTable(db, 'eia860_generator_2015', generator_2015, row.names=FALSE, overwrite = TRUE)

#2016
generator_2016 <- read_excel(here('raw_data','eia860_generator_2016.xlsx'))
names(generator_2016)<-lapply(generator_2016[1,],as.character)
generator_2016 <- generator_2016[-1,]
dbWriteTable(db, 'eia860_generator_2016', generator_2016, row.names=FALSE, overwrite = TRUE)

#2017
generator_2017 <- read_excel(here('raw_data','eia860_generator_2017.xlsx'))
names(generator_2017)<-lapply(generator_2017[1,],as.character)
generator_2017 <- generator_2017[-1,]
dbWriteTable(db, 'eia860_generator_2017', generator_2017, row.names=FALSE, overwrite = TRUE)

#2018
generator_2018 <- read_excel(here('raw_data','eia860_generator_2018.xlsx'))
names(generator_2018)<-lapply(generator_2018[1,],as.character)
generator_2018 <- generator_2018[-1,]
dbWriteTable(db, 'eia860_generator_2018', generator_2018, row.names=FALSE, overwrite = TRUE)

#2019
generator_2019 <- read_excel(here('raw_data','eia860_generator_2019.xlsx'))
generator_2019 <- generator_2019[-1,]
names(generator_2019)<-lapply(generator_2019[1,],as.character)
generator_2019 <- generator_2019[-1,]
dbWriteTable(db, 'eia860_generator_2019', generator_2019, row.names=FALSE, overwrite = TRUE)

#---------------------------------------------------------------------------------------

