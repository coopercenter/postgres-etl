library(dplyr)
library(tidyverse)

here('raw_data','plant_generation_va.csv')
#read in dataset
summary(generation)
generation <- select(generation, X, X.1, X.2, X.3)
generation <- generation[2:12,]
names(generation) <- lapply(generation[1, ], as.character)
generation <- generation[-1, ] 
rownames(generation) <- c()

#write.csv(generation, "Plant_Generation_Data_Virginia.csv") -- remove statement
#upload to db