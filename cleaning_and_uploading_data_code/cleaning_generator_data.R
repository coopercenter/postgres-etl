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

#read in dataset
generator_2014 <- read_excel(here('raw_data','generator_y2014.xlsx'))
#replace colnames
names(generator_2014)<-lapply(generator_2014[1,],as.character)
generator_2014 <- generator_2014[-1,]

#read in dataset
generator_2015 <- read_excel(here('raw_data','generator_y2015.xlsx'))
#replace colnames
names(generator_2015)<-lapply(generator_2015[1,],as.character)
generator_2015 <- generator_2015[-1,]

#read in dataset
generator_2016 <- read_excel(here('raw_data','generator_y2016.xlsx'))
#replace colnames
names(generator_2016)<-lapply(generator_2016[1,],as.character)
generator_2016 <- generator_2016[-1,]

#read in dataset
generator_2017 <- read_excel(here('raw_data','generator_y2017.xlsx'))
#replace colnames
names(generator_2017)<-lapply(generator_2017[1,],as.character)
generator_2017 <- generator_2017[-1,]


#read in dataset
generator_2018 <- read_excel(here('raw_data','generator_y2018.xlsx'))
#replace colnames
names(generator_2018)<-lapply(generator_2018[1,],as.character)
generator_2018 <- generator_2018[-1,]

#read in dataset
generator_2019 <- read_excel(here('raw_data','generator_y2019_early_release.xlsx'))
#replace colnames
generator_2019 <- generator_2019[-1,]
names(generator_2019)<-lapply(generator_2019[1,],as.character)
generator_2019 <- generator_2019[-1,]

dbWriteTable(db, 'generator_2014', generator_2014, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'generator_2015', generator_2015, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'generator_2016', generator_2016, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'generator_2017', generator_2017, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'generator_2018', generator_2018, row.names=FALSE, overwrite = TRUE)
dbWriteTable(db, 'generator_2019', generator_2019, row.names=FALSE, overwrite = TRUE)

#close db connection
dbDisconnect(db)
