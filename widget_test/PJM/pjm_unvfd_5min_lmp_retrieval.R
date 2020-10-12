# PJM Unverified 5-Minute Location Marginal Pricing Data Collection 

# This script collects five-minute location marginal pricing (LMP) from PJM's
# Data Miner v2 ap for two zones: Dominion (DOM) and American Electric Power Co., 
# Inc. (AEP). Zones are shown in the file 'pjm-zones.pdf' found in the  '...\api_data_code\pjm_api_info'
#folder 

# The pricing values represent zone-level aggregates specified by pnode ID, with 
# DOM=3496454 and AEP=8445784. These node IDs were found via the online Data Miner query tool.

# API documentation is found in the file 'PJM_API_5min_rt_lmp_unverified', and metadata for this
# feed is in 'PJM_API_5min_rt_lmp_unverified_metadata'. Both files are in the folder
# '...\api_data_code\pjm_api_info'. 

# Description of the API route used : "Searches the Real-Time Unverified Five Minute LMPs feed. 
# This feed contains five minute granular Real-Time unverifiedlocational marginal pricing (LMP) data 
# for all bus locations, including aggregates. These LMP values are produced in Real-Timeand are not 
# settlement quality. These LMPs are subject to change as part of the Verification process. Please refer 
# to Real-Time Five Minute LMPs feed for verified LMP values."

# Verified 5-minute data is also available, but has a lag - values end at 11:55 PM of the prior day.
# It has not been investigated at what time the prior day data becomes available. 
#--------------------------------------------------------------------------------------------

# Load required libraries
library(data.table)
library(httr)
library(lubridate)
library(dplyr) 
library(jsonlite)
library(rlist)
library(jsonstat)
library(ggplot2)


 # Setup a list of nodes to collect: Dominion pnode_id=34964545, AEP pnode_id= 8445784
lmp_nodes <- list(34964545,8445784)

# This function collects and returns the most recent 5-minute unverified LMP data for
pjm_5min_lmp <- function(pnode) { 


 # API request format is:
 # https://api.pjm.com/api/v1/rt_unverified_fivemin_lmps[?download][&rowCount][&sort][&order]
 # [&startRow][&isActiveMetadata][&fields][&datetime_beginning_utc][&datetime_beginning_ept][&pnode_id]

 # Dominion pnode_id=34964545, AEP pnode_id= 8445784
 r <- GET("https://api.pjm.com/api/v1/rt_unverified_fivemin_lmps", add_headers("Ocp-Apim-Subscription-Key" = "625845c6fabc4639ab91428486d8d2e2"),
         query = list(rowCount = 3000, startRow = "1", datetime_beginning_ept = 'CurrentWeek', pnode_id= pnode, sort='datetime_beginning_ept'))

 ## ERROR CHECK OPPORTUNITY -  Check status code (200 is good). Try/except format?
 #http_status(r)

 # Parse the content using httr
 five_min_LMP = httr::content(r,"parsed")

 #Check the object type of the parsed response
 typeof(five_min_LMP$items) #it is a list

 # Create a dataframe from the nested lists 
 df_5min_lmp<- rbindlist(five_min_LMP$items, fill=TRUE) # There are some zeroes in non-vital columns

 #df_5min_lmp is numeric, datetimes are character, pnode_id is integer

 # Set the table key to the UTC timestamp
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
 
 # Select the last LMP value
 current_lmp <- tail(df_5min_lmp_sml$total_lmp_rt, n=1)
 # Select the most recent timestamp
 current_lmp_time <- tail(df_5min_lmp_sml$date_time, n=1)
 # Call the pnode captured
 pnode_called <- tail(df_5min_lmp_sml$pnode_id, n=1)
 
 # Put the desired values in a list
 lmp_results <- list(current_lmp_time, current_lmp, pnode_called)

 return(lmp_results)
}

# Create a list to store results
to_display <- list()

# set initial list position
i <- 1

# Run a for loop that calls results for both nodes and appends to a dataframe
for (pnode in lmp_nodes) {
 # Assign time to a variable
 time_to_display <- pjm_5min_lmp(pnode)[[1]]
 # Assign LMP to a variable
 lmp_to_display <- pjm_5min_lmp(pnode)[[2]]
 #Assign zone name based on node ID
 if(pjm_5min_lmp(pnode)[[3]]==34964545){
   zone_to_display <- 'Dominion'
 } else if(pjm_5min_lmp(pnode)[[3]]==8445784){
   zone_to_display <- 'AEP'
 } else {zone_to_display <- 'Zone Not Recognized'}
 
 # Create output and append. 
 to_display[[i]] <- list(zone_to_display, time_to_display, lmp_to_display)
 
 # Increase list position for next loop
 i <- i + 1
  
}

# OUtput is a list of lists showing Zone, Datetime, and LMP
to_display



