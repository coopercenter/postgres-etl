#Intent of function is to collect 5 minute real time LMP data from PJM

## Set this up as a function after initial testing. 
#pjm_5min_lmp <- function() { 
  
# Load libraries
library(data.table); library(httr); library(lubridate); library(dplyr); 
library(jsonlite); library(rlist); library(jsonstat); library(ggplot2)
  
# Dominion pnode_id=34964545, AEP pnode_id= .........., pnode_id=34964545
r <- GET("https://api.pjm.com/api/v1/rt_fivemin_hrl_lmps", add_headers("Ocp-Apim-Subscription-Key" = "625845c6fabc4639ab91428486d8d2e2"),
         query = list(rowCount = 3000, startRow = "1", datetime_beginning_ept = 'CurrentWeek', pnode_id=34964545, sort='datetime_beginning_ept'))
  
# Look at the code content when needed
#str(content(r, 'parsed'))
  
# Check status code (200 is good)
http_status(r)
  
# Parse the content using httr
five_min_LMP = httr::content(r,"parsed")

#Check the object type of the parsed response
typeof(five_min_LMP$items) #it is a list

# Create a dataframe from the nested lists 
df_5min_lmp<- rbindlist(five_min_LMP$items, fill=TRUE) # There are some zeroes in non-vital columns

# Check the column names if needed
#colnames(df_5min_lmp)

# Check the data types
#sapply(df_5min_lmp, class)

# Look at the data if needed
#df_5min_lmp # lmp is numeric, datetimes are character, pnode_id is integer


## Dr. Shobe's date parsing method (I was using strptime() and having object issues)
data.table::setkey(df_5min_lmp,"datetime_beginning_utc")
# Eliminate duplicate records
df_5min_lmp = unique(df_5min_lmp)
# Create date variables
df_5min_lmp[,date_time := parse_date_time(datetime_beginning_ept,"Ymd HMS",tz="America/New_York")]   #+hours(5)
df_5min_lmp[,`:=`(date = date(date_time), 
                     hour = hour(date_time), year = year(date_time), 
                     month = month(date_time),
                     day = day(date_time),
                     minute = minute(date_time))]

  
# Cut down dataframe to needed elements
df_5min_lmp_sml <- select(df_5min_lmp, date_time, pnode_id, 
                         total_lmp_rt)

# Check reduced df if needed 
df_5min_lmp_sml


# Basic plot
plot(df_5min_lmp_sml$date_time, df_5min_lmp_sml$total_lmp_rt)


################################################ Visuals under development

# Setup a visual in ggplot2
fivemin_lmp_plot1 <- ggplot(df_5min_lmp_key, aes(x= 'date_time', y='total_lmp_rt'))
fivemin_lmp_plot1

# Add scales
lmpV2 <- lmpV1 + coord_cartesian(ylim=c(0,200))
plot(lmpV2)


################################################# No longer used code 

# Generic conversion of date output to GMT and EPT - this works 
time <- strptime(df_5min_lmp$datetime_beginning_utc, format = '%Y-%m-%dT%H:%M', tz = 'GMT') # 
time # This works, but I can't get it to apply to dataframe

# convert dataframe columns to tz - this isn't working
df_5min_lmp_tz <- mutate(df_5min_lmp,
                          datetime_beginning_ept_tz = strptime(datetime_beginning_ept, format = '%Y-%m-%dT%H:%M', tz = ''),
                          datetime_beginning_utc_tz = strptime(datetime_beginning_utc, format = '%Y-%m-%dT%H:%M', tz = 'GMT')
                        




vignette(package="data.table")
vignette("datatable-intro")



