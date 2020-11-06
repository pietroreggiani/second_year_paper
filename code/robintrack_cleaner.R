#' ---
#' title: "Robintrack Cleaning"
#' author: "Pietro Reggiani"
#' date: "September 2020"
#' 
#' output: 
#'        github_document
#' ---
#' 
#' This file cleans the Robintrack data and merges all the files together in a panel table.
#'
#' **INPUTS**:
#' 
#' * .csv files from Robintrack all stored in one folder specified at the beginning, named using the relevant tickers
#' 
#' 
#' **OUTPUTS**:
#' 
#' * table with panel of ESG scores for all datastream LAST4ESG firms over time is saved in the .csv file specified at the beginning.
#' 


#+ Setup ###########

# clear environment

rm(list=ls()) 

# Load Packages

library(data.table)
setDTthreads(0)  #tells to use all available cores

library(lubridate)

library(dplyr)

library(tictoc)


#functions folder
source("code/funs_warehouse.r")

# change global option about string import
options(stringsAsFactors = FALSE)

tic('all file')


#' ## Parameters
#' here you can choose parameters that determine what the code does

#+ parameters choice ####

# tell R the folder where you stored the Robintrack data
data.folder <- './data/raw/robintrack/robintrack_popularity_aug14'

# tell R the folder and filename for the output panel .csv
output.file <- './data/processed/robintrack_cleaned.csv'

# tell whether you want to aggregate the data at the daily level
daily <- TRUE


#' ## Data Cleaning
#' 

#+ create empty csv ####

# the variable names will depend on whether you aggregate at the daily level or not
if (daily== FALSE){
    
    collector <- data.table( timestamp = ymd_hms(character()), ticker= character()  , users_holding = double())
    fwrite(collector, file = output.file, sep = ",", row.names = FALSE, quote = FALSE, col.names = TRUE)
    
} else {
    collector <- data.table( date = as.Date(character()) , ticker= character(), users_holding = double() )
    fwrite(collector, file = output.file, sep = ",", row.names = FALSE, quote = FALSE, col.names = TRUE)
    
}


#+ get file paths of data ####

file.paths <- list.files(data.folder, full.names=TRUE, pattern = "*.csv") #list of paths that will be fed to the lapply
num.files <- length(file.paths) # the number of files in the folder



#+ Define function that cleans one file ####

#' Robin Cleaner
#' 
#' @description This function takes path to a .csv file relative to one asset, cleans it and appends to another .csv file. 
#' If you apply it to all asset files it returns a panel of holdings for all assets over time.
#'
#' @param input.path path of .csv file to clean (one asset)
#' @param output.file path of .csv file where you append the data
#' @param daily logical = TRUE if you want to aggregate at daily level
#'
#' @return append cleaned data for input.path file to the output.file
#' 
#' 
robin_cleaner <- function(input.path, output.file, daily = TRUE, verbose = FALSE) {
    
    filename <- tools::file_path_sans_ext(basename(input.path))
    ticker <- gsub( "_","", filename   )
    
    if(verbose ==TRUE){ cat('The asset for this file is \n' , ticker, '\n') }
    
    
    # read csv file for this asset, specifying format for the variables
    asset.obs <- fread(file = input.path , header = TRUE,  data.table = TRUE, colClasses = c("POSIXct","double") )
    
    #add ticker as a new variable
    asset.obs$ticker <- ticker
    
    asset.obs <- asset.obs[, c("timestamp", "ticker", "users_holding")]
    
    if (daily == TRUE){
        # TODO aggregate at daily level
        asset.obs$date = as.Date(asset.obs$timestamp)
        asset.obs <- asset.obs[, .( ticker = .SD[1,ticker], users_holding = mean(users_holding, na.rm = TRUE )) , by = date ]
    }
    
    # append to csv
    fwrite(asset.obs, file = output.file , sep= ",", row.names = FALSE, quote = FALSE, col.names = FALSE , append = TRUE)
    
}

#+ apply function to all files ####

lapply(file.paths,FUN = robin_cleaner, output.file, daily)

toc()

