## CO2 Signal PJM Region Electricity Carbon Intensity API Call 
# Arne Newman
# Last updated: 9/23/20

# Intent is to test calling the API and parsing the  response
# API documentation is at https://docs.co2signal.com/#introduction

#install.packages("httr")
#install.packages("jsonlite")

#Require the package so you can use it
require("httr")
require("jsonlite")

auth_token <-  "1a8ea1713e7415b9"    # I'm using my proper API key here
password <- ""

base_url <- "https://api.co2signal.com/v1/latest?"

# Build coordinate input for the call
lon <- '-78.508299'
lat <- '38.033215'
lon_lat <- paste('lon=',lon,'&','lat=',lat, sep='')
# lon_lat # check lon_lat string if needed
zone_call<-paste(base_url,lon_lat,sep='')
# zone_call #check full API string if needed

#Run get request 
get_zone_co2 <- GET(zone_call, add_headers('auth-token' = auth_token))

## If using country code instead of zone, use 2 lines below 
# country_code <- "XX"
# country_call <- paste(url, "countryCode","=", country_code, sep="")
# get_country_co2 <- GET(country_call, add_headers('auth-token' = auth_token))

# Check data type - result is a list
typeof(get_zone_co2)

# Parse the response
co2_zone_parsed <- content(get_zone_co2, "parsed")

#Check the parsed response
co2_zone_parsed

# Assign response to variables
carbonIntensity <- co2_zone_parsed$data$carbonIntensity # Carbon intensity value
countryCode <- co2_zone_parsed$countryCode # Country code, which should be US-MIDA-PJM
fossilFuelPerc <- co2_zone_parsed$data$fossilFuelPercentage # Fossil fuel percentage
intensityUnits <- co2_zone_parsed$units$carbonIntensity # Carbon intensity unit

#check outputs
carbonIntensity
countryCode
fossilFuelPerc
intensityUnits
