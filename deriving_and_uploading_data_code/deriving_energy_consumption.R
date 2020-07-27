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

#-----------------------------------------------------------------------------------------------
# get the datasets
pop <- dbGetQuery(db,'SELECT * from fred_vapop')
# unit of population is thousands of persons
pop_s60<- as.numeric(pop[61:119,4])*1000
tot_consumption <- dbGetQuery(db,'SELECT * from eia_seds_tetcb_va_a')
# unit of consumption is billion btu
tot_s60<- tot_consumption[nrow(tot_consumption):1,1]

# derive the values
c_per_cap <- tot_s60/pop_s60
c_per_cap_df <- data.frame(1960:2018,c_per_cap)
colnames(c_per_cap_df)<- c('year','consumption_per_capita')

#upload to db
dbWriteTable(db, 'energy_consumption_per_capita_va', c_per_cap_df, row.names=FALSE, overwrite = TRUE)
#-----------------------------------------------------------------------------------------------
# get the datasets
# gdp goes from 1997 to 2019
gdp<- dbGetQuery(db,'SELECT * from fred_vangsp')
# tot_consumption goes from 1960 to 2018
tot_consumption <- dbGetQuery(db,'SELECT * from eia_seds_tetcb_va_a')

# so take the common years: 1997 to 2018

# unit of GDP is in million of dollar
gdp_s97<-as.numeric(gdp[1:22,2])*1000000

# unit of consumption is billion btu
tot_s97 <- tot_consumption[1:22,]
tot_s97<- tot_s97[nrow(tot_s97):1,1]*1000000000
# now it's btu

# derive the values
c_per_gdp <- tot_s97/gdp_s97
c_per_gdp_df <- data.frame(1997:2018,c_per_gdp)
colnames(c_per_gdp_df)<- c('year','consumption_per_unit_gdp')

#upload to db
dbWriteTable(db, 'energy_consumption_per_unit_gdp_va', c_per_gdp_df, row.names=FALSE, overwrite = TRUE)

#-----------------------------------------------------------------------------------------------
#close db connection
dbDisconnect(db)