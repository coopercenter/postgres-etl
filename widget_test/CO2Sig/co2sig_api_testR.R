# CO2 Signal PJM Region Electricity Carbon Intensity API Call 

# This script collects electricity carbon intensity for the PJM Region. API documentation 
# is available at https://docs.co2signal.com/#introduction

#Require the package so you can use it
require("httr")
require("jsonlite")

auth_token <-  "1a8ea1713e7415b9"    # I'm using my proper API key here
password <- ""

base_url <- "https://api.co2signal.com/v1/latest?"

# Build coordinate input for the call; coordinates for UVA selected. This maps
# to the PJM region
lon <- '-78.508299'
lat <- '38.033215'
# Combine the coordinates for the URL string based on API format
lon_lat <- paste('lon=',lon,'&','lat=',lat, sep='')

# Create the URL string
zone_call<-paste(base_url,lon_lat,sep='')

#Run get request 
get_zone_co2 <- GET(zone_call, add_headers('auth-token' = auth_token))

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
