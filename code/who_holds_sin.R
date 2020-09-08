#' ---
#' title: "Sin Holdings"
#' author: "Pietro Reggiani"
#' date: "September 2020"
#' 
#' output: 
#'        github_document
#' ---
#'
#' **INPUTS**:
#' 
#' * S34 file is taken from the WRDS server directly using SQL
#' * CRSP monthly file needs to be saved in /data sub-folder
#' 
#' **OUTPUTS**:
#' 
#' * table with average sin/fossil holdings by type of investor over time. <br> this table can be used to feed graphs.
#' table
#' 


#+ Setup ###########

# clear environment

#rm(list=ls()) 

# Load Packages

library(data.table)
setDTthreads(threads = 0)  #tells to use all available cores

library(lubridate)

library(RPostgres)  # for WRDS connection

library(ggplot2)
#library(latex2exp)
library(sandwich)
library(stringr)
library(dplyr)
#library(R.utils)
#library(xtable) # to print to Latex
#library(tidyverse)

# Connect to WRDS
# This part below is already in the Rprofile but for some reason it does not work with Markdown
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='preggian')

#functions folder
source("code/funs_warehouse.r")

# change global option about string import
options(stringsAsFactors = FALSE)


##### Load Data ########
#+ Take the Holdings Data from WRDS

res <- dbSendQuery(wrds, "select * from tfn.s34")
s34 <- as.data.table(dbFetch(res, n=-1))  #use data.table package
dbClearResult(res)

s34 <- s34[order(mgrname,cusip, -fdate)] #order by manager, stock and date 

# sample start-end dates
mindate <- min(s34$fdate)
maxdate <- max(s34$fdate)


#' Now we need to attach to the holdings information about the industry codes!
#' We can get this through CRSP I believe.

wanted.vars <- c("PRC","date","COMNAM", "TICKER","PERMNO","PERMCO","CUSIP","NCUSIP","SICCD","NAICS","SHRCD", "SHROUT")

crsp.data      <- fread('data/raw/downloaded zip files/crsp_monthly_aug27.gz', select = wanted.vars)

# some cleaning and formatting
crsp.data$date <- as.Date(crsp.data$date,'%m/%d/%Y')
crsp.data <- crsp.data[ between(date, mindate, maxdate)][order(date,NCUSIP)] #keep only dates in S34
colnames(crsp.data) <- tolower(colnames(crsp.data))
setnames(crsp.data, "date", "fdate")

#remove observations that for some reason are duplicated
crsp.data <- unique(crsp.data)



#' ## Merge CRSP into holdings data.

#create surrogate keys
s34[, identifier := rownames(s34)]
crsp.data[,crsp.id := rownames(crsp.data)]


#' ### Clean Duplicates
#' There are several ncusip-month observations, and this is a problem if I want to merge each one holding to a single stock from CRSP.
#' This part tries to eliminate duplicates.

 # remove duplicates ####
one <- unique_id(crsp.data, c("ncusip","fdate"))

if(!one){
    cat('Ncusip-date do not uniquely identify observations from CRSP!')
}


# extract the part that has no NCUSIP
crsp.noncusip <- crsp.data[ncusip =='']
crsp.data <- crsp.data[ncusip != '']

if(unique_id(crsp.data, c("ncusip","fdate"))){
    cat('Ncusip and date are unique identifiers for the data in CRSP.\n We can proceed to merge.')
    
} else {
    stop('Ncusip is not a unique identifier for observations in CRSP, I cannot proceed.')
    #count number of observations for each ncusip-month value
    crsp.data <- crsp.data[, repetitions := .N, by = c('ncusip','fdate')][order(repetitions,fdate,ncusip)]
}


#' ### Merge

##### Merge CRSP to S34 ###########

setkeyv(s34, c("cusip","fdate"))
setkeyv(crsp.data, c("ncusip","fdate"))

#here you merge crsp in the s34 file, leaving out the unmatched rows
merged <- merge(s34, crsp.data, by.x = c("cusip","fdate"), by.y= c("ncusip","fdate"), all.x = FALSE, all.y =FALSE )

#check whether some of the S34 obs have been duplicated.
test <- unique_id(merged, "identifier")  

if (test == FALSE ){
    #if that is the case, stop the code
    stop(error = 'Some of the observations in S34 have been duplicated in the merge!')
} else {
    # otherwise, it means there were no duplications in S34 which is good.
    # go on with trying to merge any remaining ones from S34
    cat(paste('There are ', dim(s34)[1]-dim(merged)[1], ' observations from S34 that are not matched.'))
    if(  dim(s34)[1]-dim(merged)[1] >0 ) {  #if there are some unmatched obs
        
        nonmerged <- s34[!crsp.data]
        
        # try to merge using cusip
        merged.round2 <- merge(nonmerged, crsp.data, by.x = c("cusip","fdate"), by.y= c("cusip","fdate"), all=FALSE )
        
        #if you match some of them this way
        if (dim(merged.round2)[1]>0){ 
            
            # Add them back to old sample.
            merged <- rbindlist(list( merged, merged.round2  ), use.names = TRUE, fill = TRUE)
            
            #take out the possible matches from the nonmerged table
            nonmerged <- nonmerged[ !( identifier %in% merged.round2$identifier)   ]
        }
        # if you have any unmatched left, now try last attempt using the observations that did not have a ncusip in CRSP
        if(dim(nonmerged)[1]>0){
            merged.round3 <-  merge(nonmerged, crsp.noncusip, by.x = c("cusip","fdate"), by.y= c("cusip","fdate"), all=FALSE )
            #again if you merge some this way, add them to the bottom of merged file
            if(dim(merged.round2)[1]>0){
                merged <- rbindlist(list( merged, merged.round3  ), use.names = TRUE, fill = TRUE)
            }
        }
        
    }
}
    
#' We are left with the `merged` datatable that contains the merged holdings data.

no.merged <- dim(merged)[1]
cat('We merged ', no.merged, 'out of ', dim(s34)[1],' observations in the holdings data to CRSP.')


#' ## Sin and Fossil Stocks Classification
#'

# classify sin and fossil ##### 
# Pick categories for Sin and Fossil stocks
sin.sic <- c(2100:2199, 2080:2085)
sin.naics <- c(7132,71312,713210,71329,713290,72112,721120)

#' create dummy for fossil fuel industry. Following the Fama-French category 30, we have that:
#' * 1200-1299 bituminous coal and lignite mining
#' * 1300-1399 various oil and gas categories
#' * 2900-2912 and 2990-2999 petroleum refining and miscellaneous
fossil.sic <- c(1200:1299, 1300:1399, 2900:2912, 2990:2999)


# Create the indicator variables for sin and fossil holdings

merged$siccd <- as.integer(merged$siccd)

merged[,sin := ifelse( siccd %in% sin.sic | naics %in% sin.naics ,1,0)]
merged[,fossil := ifelse(siccd %in% fossil.sic ,1,0)]

#' Every quarter, split firms by the proportion of their portfolio invested in sin or fossil

#each, quarter, for every firm, compute the portfolio value
test = sum(merged$sin)
testf = sum(merged$fossil)


#' ## Calculate fraction of portfolio invested in sin/fossil stocks for each manager

# value of every position
merged[, pos.value := prc.x * shares]

# total value of manager portfolio each date
merged[, tot.value := sum(pos.value), by = .(mgrno, fdate)]

# calculate fraction of portfolio represented by each asset
merged$frac <- merged[, .(pos.value/tot.value)]

# for each manager, compute the fraction of sin and fossil stocks
merged[, sin.frac := sum(sin * frac) , by = .(mgrno,fdate) ]
merged[, fossil.frac := sum(fossil * frac) , by = .(mgrno,fdate) ]


#' Look at type of investor that changed the most holdings in sin and fossil over time
#' We need a table that has average sin fraction by date for each manager type.
#' The code below does it with both equal and value weighted options. 


# Sin and Fossil Weights by type ####

#weighted portion of portfolios in sin and fossil for each investor type
sin.frac.by.type <- merged[order(fdate,typecode), 
                           .( sin.frac.wei = sum(sin*pos.value)/sum(pos.value),
                              fossil.frac.wei = sum(fossil*pos.value)/sum(pos.value),
                              sin.frac = mean(sin.frac),
                              fossil.frac = mean(fossil.frac) ) ,
                           by = .(fdate,typecode)   ]

sin.frac.by.type$typecode <- as.integer(sin.frac.by.type$typecode)

# export table ####

# the table needs to be saved so that we will do graphs afterwards

saveRDS(sin.frac.by.type, file = 'output/sin_frac_by_type.rds')






 














