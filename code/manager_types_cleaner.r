#' ---
#' title: "Manager Types Cleaner"
#' author: "Pietro Reggiani"
#' date: "September 2020"
#' 
#' output: 
#'        github_document
#' ---
#' 
#' ## Description
#' This file takes the manager types file from Koijen-Yogo, then adds 2018 and 2019 that are missing in the orignal file.
#' It does so by taking all the managers present during 2017 and copying their observations to the future. It spits out a .csv file
#' that should be merged to the S34 holdings.
#'
#' **INPUT**:
#' 
#' * `data/raw/ky_managers.csv` file coming directly from Ralph and Moto
#'  
#' **OUTPUT**:
#' 
#' * `data/processed/ky_managers_cleaned.csv`
#' 


#+ setup , include = FALSE  
remove(list = ls())

source("code/funs_warehouse.r")

library(data.table)
library(zoo)
library(lubridate)
library(magrittr) # to have pipes


#+ Load file ####
ky_managers <- fread("data/raw/ky_managers.csv", data.table= TRUE)

#+ format variables ####

ky_managers[, fdate := dmy(fdate) ]
ky_managers[, yq := format.yearqtr(fdate)]


#' The issue is that this file only arrives to 2017. We try to expand the classification of managers from before 2017,
#' hoping not too many have changed or exited/entered the market.
#' 

#+ create new observations ####

# take out the last 3 years of observations
last_year <- ky_managers[fdate > '2014-12-31'][order( mgrno , fdate )]

# we want one observation for each mgrno, we keep the most recent

# create row index within each mgrno 
last_year<- last_year[    , index:= 1:.N   , by = mgrno][order(mgrno)]
# get number of observations for each mgrno
last_year[ , max_index := max(index), by = mgrno]
# keep only most recent mgrno-type match
managers <- last_year[index == max_index][,c("index", "max_index") := NULL]
# remove date, remove duplicates
managers[, c('fdate', 'yq','rdate', 'mgrid', 'mgrname'):= NULL]
managers <- managers %>% unique()

if (unique_id(managers, 'mgrno')){
    message('There is only one type associated to each manager ID.')
} else{
    stop('mgrno does not uniquely identify observations in the piece you want to append!')
}

# now we have a list of managers and their associated types.
# they need to be used for the years since 2018 that are not in the original file.
# so for each obs, we now need 4 observations per missing year, i.e. 8 new observations, each with increasing dates.
new_piece <- data.table()

# loop over quarters, stick that quarter to managers table and append to the bottom
for (i in seq(from = 2018, to = 2019.75, by= 0.25)){
    managers$yq <- yearqtr(i)
    new_piece <- rbind(new_piece , managers  )
}
 
#+ append these two years to the initial data ####

select_cols = names(new_piece)
ky_managers$yq <- as.yearqtr(ky_managers$yq)

output <- rbindlist( list( new_piece , ky_managers[  , ..select_cols  ])  , use.names = TRUE )
output <- output[order(mgrno,yq)]

#+ export to csv ####
write.table(output , file = 'data/processed/ky_managers_cleaned.csv', sep = ",", row.names = FALSE, quote = FALSE, col.names = TRUE)


#+ clean workspace ####
remove(list = ls())




