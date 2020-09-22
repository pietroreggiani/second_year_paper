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

rm(list=ls()) 

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
library(pryr)
#library(R.utils)
#library(xtable) # to print to Latex
#library(tidyverse)


#functions folder
source("code/funs_warehouse.r")

# change global option about string import
options(stringsAsFactors = FALSE)


#start clock
ptm <- proc.time()


##### Load Data ########

# set number of rows to import from datasets (-1 is all available)
rows = 10000

#+ Take the Holdings Data from WRDS

#use the user defined function
s34 <- wrds.table("select fdate, mgrno, typecode, cusip, shares, prc,shrout1, shrout2  from tfn.s34", 
                  numrows = rows, data.table = TRUE )
gc()

#number of obs
numobs.s34 <- dim(s34)[1]

# sample start-end dates
mindate <- min(s34$fdate)
maxdate <- max(s34$fdate)

mem_used()

## NEW get from WRDS ####

# get CRSP monthly and its link with CCM link table

query <-  "select distinct  a.permno, a.cusip, b.ncusip, ugvkey as gvkey, uiid as iid, date, prc,
                            ret, comnam, siccd as crspsic, naics as crspnaics, shrcd, shrout, b.ticker
                            from crsp.msf a
                            left join crsp.msenames b  on  a.permno = b.permno and  a.date between b.namedt  and  b.nameendt 
                            left join crsp.ccmxpf_lnkused  c 
                            on  a.permno = c.apermno and a.date between ulinkdt and coalesce(ulinkenddt, '2020-09-30' ) 
                            where (ulinktype = 'LU' or ulinktype =  'LC')
                            and (ulinkprim = 'P' or ulinkprim = 'C') 
                            and usedflag = 1 " 

crsp.data <-  wrds.table(query, numrows = rows, data.table =  TRUE)

crsp.data$year <- year(crsp.data$date) #to link to Compustat


# get Compustat yearly, only NAICS and SIC

compustat <- unique (wrds.table("select naicsh as naics, sich as sic, gvkey, datadate as date from comp.funda
                                where naicsh is not NULL OR sich is not NULL",
                                numrows = rows) )
# sich here is historical, in crsp hsic is header.

compustat$year <- year(compustat$date)

#if there are duplicates of gvkey-year
if ( !unique_id(compustat,c("gvkey","year")) ){
    
    message('gvkey and year do not uniquely identify observations from Compustat')
    
    compustat <-  compustat[, N := .N, by = .(gvkey, year)][order(-N,gvkey,-year,-date)]
    
    #take if it has at least naics that's the variable we want
    compustat<- unique ( compustat[N==1 | N>1  &  !is.na(naics) ][, c("N", "date"):=NULL]  )[naics < 999990 & sic < 9995]
    
    
    # keep most recent industry identifier
    
    if (unique_id(compustat,c("gvkey","year"))){
        message("Compustat now has unique identifiers, we can proceed 1")
        
    } else {
        compustat <- compustat[, N := .N, by = .(gvkey, year)][order(-N,gvkey,-year)][N==1 | N>1 & !is.na(sic) & !is.na(naics)]
        if (unique_id(compustat,c("gvkey","year"))){
            message("Compustat now has unique identifiers, we can proceed 2")
            compustat <- compustat[, N:=NULL]
        } else {
            message("Still cannot create unique identifier in Compustat, brutally keep most recent for the duplicates.")
            compustat <- compustat[, c("N", "id") := .(.N, seq_len(.N)), by = .(gvkey, year)][N==1 | N>1 & id==1]#[,c("N", "id"):= NULL]
            
            if( unique_id(compustat,c("gvkey","year"))){
                message("Compustat now has unique identifiers, we can proceed 3")
                
                compustat[, c("N", "id"):= NULL]
                
            } else {
                stop("Still cannot create unique identifier in Compustat")
            }
        }
    }
}

# merge CRSP and naics from compustat
crsp.data <- left_join(crsp.data, compustat, by = c("gvkey","year") ) %>% setDT()
crsp.data$naics <- as.character(crsp.data$naics)

remove(compustat) #to save memory
gc()

#replace missing industry codes from Compustat with the CRSP ones, and the missing NCUSIP with CUSIP

crsp.data <- crsp.data %>% mutate( naics   = fifelse( is.na(naics), crspnaics, naics),
                      sic     = fifelse( is.na(sic)  , crspsic,   sic  ), 
                      ncusip  = fifelse( is.na(ncusip), cusip, ncusip )
                      )   %>%  unique() %>% subset(select = -c(crspnaics, crspsic)) %>% setDT()


setnames(crsp.data, "date.x", "date", skip_absent = TRUE)

mem_used()

##########old piece #######################################

#' Now we need to attach to the holdings information about the industry codes!
#' We can get this through CRSP I believe.

# wanted.vars <- c("PRC","date","COMNAM", "TICKER","PERMNO","PERMCO","CUSIP","NCUSIP","SICCD","NAICS","SHRCD", "SHROUT")
# 
# crsp.data      <- fread('data/raw/crsp_monthly_aug27.csv', select = wanted.vars)
# 
# colnames(crsp.data) <- tolower(colnames(crsp.data))
# crsp.data <- unique(crsp.data)
# setnames(crsp.data, "date", "fdate")
# 
# 
# # extract the part that has no NCUSIP
# crsp.noncusip <- crsp.data[ncusip =='']
# crsp.data <- crsp.data[ncusip != '']



###

# some cleaning and formatting

#crsp.data$date <- list(as.Date(crsp.data$date, format = '%m/%d/%Y'))

insample <-  between(crsp.data$date, mindate, maxdate)
crsp.data <- crsp.data[ insample ] #keep only dates in S34


#' ## Merge CRSP into holdings data.

#create surrogate keys
s34[, identifier := rownames(s34)]
crsp.data$crsp.id <-  rownames(crsp.data)

cat(class(crsp.data))


#' ### Clean Duplicates
#' There are several ncusip-month observations, and this is a problem if I want to merge each one holding to a single stock from CRSP.
#' This part tries to eliminate duplicates.

 # check duplicates ####


if(!unique_id(crsp.data, c("ncusip","date"))){
    cat('Ncusip-date do not uniquely identify observations from CRSP! I cannot proceed')
    crsp.data <- crsp.data[, repetitions := .N, by = c('ncusip','date')][order(repetitions,date,ncusip)]
} else {
    cat('Ncusip and date are unique identifiers for the data in CRSP.\n We can proceed to merge.')
}


#' ### Merge

##### Merge CRSP to S34 ###########

setkeyv(s34, c("cusip","fdate"))
setkeyv(crsp.data, c("ncusip","date"))

#here you merge crsp in the s34 file, leaving out the unmatched rows
merged <- inner_join(s34, crsp.data, by = c("cusip" = "ncusip", "fdate" = "date") ) %>% setDT()

#take non-merged parts of both datasets
nonmerged <- s34[!crsp.data]
nonmerged.crsp <- crsp.data[!s34]

mem_used()

remove(s34, crsp.data) #to save memory

gc()

#count how many merged
numobs.merged <- dim(merged)[1] 

#check whether some of the S34 obs have been duplicated during the merge
test <- unique_id(merged, "identifier")  

if (test == FALSE ){
    #if that is the case, stop the code
    stop(error = 'Some of the observations in S34 have been duplicated in the merge!')
} else {
    # otherwise, it means there were no duplications in S34 which is good.
    # go on with trying to merge any remaining ones from S34
    cat(paste('There are ', numobs.s34-numobs.merged, ' observations from S34 that are not matched.'))
    if( numobs.s34 -numobs.merged >0 ) {  #if there are some unmatched obs
        
        # try to merge using cusip
        merged.round2 <- inner_join(nonmerged, nonmerged.crsp, by = c("cusip" = "cusip", "fdate" = "date") ) %>% setDT()
        
        remove(nonmerged, nonmerged.crsp)
        gc()
        
        #if you match some of them this way
        if (dim(merged.round2)[1]>0){ 
            
            # Add them back to old sample.
            merged <- rbindlist(list( merged, merged.round2  ), use.names = TRUE, fill = TRUE)
        }
        remove(merged.round2)
    }
}
    
#' We are left with the `merged` data.table that contains the merged holdings data.

numobs.merged <- dim(merged)[1]
cat('We merged ', numobs.merged, 'out of ', numobs.s34,' observations in the holdings data to CRSP.')


#' ## Sin and Fossil Stocks Classification
#'

# classify sin and fossil ##### 
# Pick categories for Sin and Fossil stocks, following Hong and Kacperkzyk

sin.sic <-  as.character(   c(2100:2199, 2080:2085) ) # make it string otherwise may give problems

sin.naics <- as.character(c(7132,71312,713210,71329,713290,72112,721120) )

merged$sic <- as.character(merged$sic)
merged$naics <- as.character(merged$naics)

#' create dummy for fossil fuel industry. Following the Fama-French category 30, we have that:
#' * 1200-1299 bituminous coal and lignite mining
#' * 1300-1399 various oil and gas categories
#' * 2900-2912 and 2990-2999 petroleum refining and miscellaneous
fossil.sic <- as.character( c(1200:1299, 1300:1399, 2900:2912, 2990:2999) )


# Create the indicator variables for sin and fossil holdings
merged[,sin := ifelse( sic %in% sin.sic | naics %in% sin.naics ,1,0)]
merged[,fossil := ifelse(sic %in% fossil.sic ,1,0)]


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

remove(merged)

sin.frac.by.type$typecode <- as.integer(sin.frac.by.type$typecode)

# export table ####

# the table needs to be saved so that we will do graphs afterwards

saveRDS(sin.frac.by.type, file = 'output/sin_frac_by_type.rds')


#get total execution time
proc.time() - ptm




