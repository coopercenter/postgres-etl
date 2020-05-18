library(dplyr)
library(tidyverse)

here('data','Virginia Plant Generation.csv' )
#read in dataset
generation <- read.csv(here('data','Virginia Plant Generation.csv' ))
summary(generation)
generation <- select(generation, X, X.1, X.2, X.3)
generation <- generation[2:12,]
names(generation) <- lapply(generation[1, ], as.character)
generation <- generation[-1, ] 
rownames(generation) <- c()
write.csv(generation, "Plant_Generation_Data_Virginia.csv")
