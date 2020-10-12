## PJM Electricity Pricing from CSV: 5-Minute Real Time Locational Marginal Price
# Arne Newman
# Last updated: 9/23/20

# Data in this file is from a CSV downloaded from PJM Data Miner at 
# https://dataminer2.pjm.com/feed/rt_fivemin_hrl_lmps. All data are associated
# with the Dominion zone-level pnode, from 00:00 09/21/20 to 12:00 09/23/20

# load ggplot2
library(ggplot2)

# Read in file and attach data
data <- read.csv("rt_fivemin_hrl_lmps_DOM_0921-0923.csv", header=TRUE)
attach(data)
# Inspect the data
names(data)

# Assign key data to variables
time_char <- data$datetime_beginning_ept #in eastern time zone


time_char

# Convert time from character to POSIXt date/time format
time <- strptime(time_char, format = '%m/%d/%Y %H:%M', tz = '') # 
rtLMP <- data$total_lmp_rt #real time 5-minute location marginal pricing

# Check the data types 
typeof(time) # List
typeof(rtLMP) # Double

# Setup a plot in basics plt
plot(time, rtLMP)

# Create a new dataframe from the newly defined fields
pjmData <- data.frame(time, rtLMP)


# Setup a visual in ggplot2
lmpV1 <- ggplot(pjmData, aes(x=time, y=rtLMP)) + geom_line()

# Zoom in 
lmpV2 <- lmpV1 + coord_cartesian(ylim=c(0,200))
plot(lmpV2)
