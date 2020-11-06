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

#library(RPostgres)  # for WRDS connection

library(ggplot2)
#library(latex2exp)
library(sandwich)
library(stringr)
library(dplyr)
library(pryr)
library(zoo)
library(tictoc)  #to time the execution
library(lobstr)
#library(R.utils)
#library(xtable) # to print to Latex
#library(tidyverse)


#functions folder
source("code/funs_warehouse.r")

# change global option about string import
options(stringsAsFactors = FALSE)


#start clock
ptm <- proc.time()

#tell tic to reset log 
tic.clearlog()


#' ## Load Data ########
#' For reasons of memory, we will split all datasets in 4 decades and do all the code separately. This should save memory

datebreaks <- c("1979-12-31","1989-12-31","1999-12-31","2009-12-31","2020-12-31" )

# set number of rows to import from datasets (-1 is all available)
rows = -1

for (i in 2:length(datebreaks)){
    
    tic( paste("Loop round", i-1))
    
    first.date <- datebreaks[i-1]
    last.date <-  datebreaks[i]
    
    
    #' ## Get data directly from WRDS ###
    
    ###load data ####
    
    #+ Take the Holdings Data from WRDS
    
    tic("Query S34")
    
    # use the user defined function
    query <- paste("select fdate, mgrno, typecode, cusip, shares, prc,shrout1, shrout2  
               from tfn.s34
               where fdate between '", first.date ,"' and '", last.date, "'", sep= "")
    
    s34 <- wrds.table(query , numrows = rows, data.table = TRUE )
    
    toc(log = TRUE)
    gc()
    
    obj_size(s34)
    
    s34[, yq := format.yearqtr(fdate)]
    
    
    #rename cusip for ease of merge
    setnames(s34, "cusip", "ncusip")
    
    
    #number of obs
    numobs.s34 <- dim(s34)[1]
    
    # sample start-end dates
    #mindate <- min(s34$fdate)
    #maxdate <- max(s34$fdate)
    
    #check for duplicate assets in the holdings data
    if ( !unique_id(s34, c("yq", "ncusip", "mgrno" ) ) ){
        message("Some cusip-manager-quarter observations are duplicated")
    }
    
    mem_used()
    
    #set key for merges
    setkeyv(s34, c("mgrno","yq"))
    
    
    #' We add the corrected manager types using the classification from Koijen-yogo
    
    ## merge in KY managers corrected types ####
    
    # this file comes from another code that cleans the original manager types and adds the missing years at the end.
    
    ky_managers <- fread("data/processed/ky_managers_cleaned.csv", stringsAsFactors = FALSE, 
                         select = c("mgrno", "yq", "type"), data.table = TRUE)
    
   
    setkeyv(ky_managers, c("mgrno","yq"))
    
    # merge it into S34
    
    s34 <- ky_managers[s34, on = c("yq", "mgrno")]
    
    remove(ky_managers)
    
    
    ###### get CRSP monthly and its link with CCM link table ####
    
    query <- paste( "select distinct  a.permno, a.cusip, b.ncusip, ugvkey as gvkey, uiid as iid, date, prc,
                            ret, comnam, siccd as crspsic, naics as crspnaics, shrcd, shrout, b.ticker
                            from crsp.msf a
                            left join crsp.msenames b  on  a.permno = b.permno and  a.date between namedt  and coalesce(nameendt, '2020-09-30') 
                            left join crsp.ccmxpf_lnkused  c 
                            on  a.permno = c.apermno and a.date between ulinkdt and coalesce(ulinkenddt, '2020-09-30' ) 
                            where (ulinktype = 'LU' or ulinktype =  'LC')
                            and (ulinkprim = 'P' or ulinkprim = 'C') 
                            and usedflag = 1 
                            and (shrcd IN (10,11,12,18))
                            and (exchcd between 1 and 3)
                            and date between '", first.date, "' and '", last.date,"'", sep = "") 
    
    tic("CRSP Query")
    
    crsp.data <-  wrds.table(query, numrows = rows, data.table =  TRUE)
    
    toc(log=TRUE)
    
    mem_used()
    obj_size(crsp.data)
    
    #' ### Clean CRSP
    
    crsp.data$year <- year(crsp.data$date) #to link to Compustat
    
    crsp.data[, yq := format.yearqtr(date)]
    
    crsp.data <- crsp.data[order(permno, yq, date)]
    
    #keep only last obs in quarter
    crsp.data[, q_id := seq_len(.N)  , by = .(yq, permno) ][, last := q_id ==.N , by = .(yq, permno)]
    crsp.data <- crsp.data[last == TRUE][, c("last", "q_id") := NULL]
    
    #check that you have only one obs per quarter per asset
    if ( !unique_id(crsp.data, c("yq","ncusip" ) ) ){
        message("Some ncusip-quarter observations in CRSP are duplicated")
    }
    
    # when closing price does not exist, crsp records negative prices. Change that here
    crsp.data$prc <- abs(crsp.data$prc)
    
    
    # get Compustat yearly, only NAICS and SIC
    
    tic('Compustat Query')
    
    query <- paste("select naicsh as naics, sich as sic, gvkey, datadate as date from comp.funda
                                where (naicsh is not NULL OR sich is not NULL)
                                and datadate between '", first.date, "' and '", last.date,"'", sep = "")
    
    compustat <- unique ( wrds.table(query, numrows = rows) ) %>%
        setDT()
    
    toc(log=TRUE)
    
    # sich here is historical, in crsp hsic is header.
    
    #create year to match with CRSP
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
                compustat <- compustat[, c("N", "id") := .(.N, seq_len(.N)), by = .(gvkey, year)][N==1 | N>1 & id==1]
                
                if( unique_id(compustat,c("gvkey","year"))){
                    message("Compustat now has unique identifiers, we can proceed 3")
                    
                    compustat[, c("N", "id"):= NULL]
                    
                } else {
                    stop("Still cannot create unique identifier in Compustat, execution halted.")
                }
            }
        }
    }
    
    #change naics to string as otherwise it can create problems
    compustat$naics <- as.character(compustat$naics)
    crsp.data$crspnaics <- as.character(crsp.data$crspnaics)
    
    
    # merge CRSP and naics from compustat
    tic("CRSP-Compustat Merge")
    
    crsp.data <- left_join(crsp.data, compustat, by = c("gvkey","year") ) %>%
        setDT()
    
    toc(log=TRUE)
    
    remove(compustat) #to save memory
    gc()
    
    #replace missing industry codes from Compustat with the CRSP ones, and the missing NCUSIP with CUSIP
    
    crsp.data <- crsp.data %>% 
        mutate( naics   = fifelse( is.na(naics), crspnaics, naics),
                sic     = fifelse( is.na(sic)  , crspsic,   sic  ), 
                ncusip  = fifelse( is.na(ncusip), cusip, ncusip )
        )   %>% 
        unique() %>%
        subset(select = -c(crspnaics, crspsic)) %>%
        setDT()
    
    
    setnames(crsp.data, "date.x", "date", skip_absent = TRUE)
    
    mem_used()
    
    
    #' ## Merge CRSP into holdings data.
    
    #create surrogate keys
    s34[, identifier := rownames(s34)]
    crsp.data$crsp.id <-  rownames(crsp.data)
    
    
    #' ### Clean Duplicates
    #' If there are several ncusip-quarter observations, it can be a problem if I want to merge each one holding to a single stock from CRSP.
    #' This part tries to eliminate duplicates.
    
    # check duplicates ####
    
    if(!unique_id(crsp.data, c("ncusip","yq"))){
        cat('Ncusip-quarter do not uniquely identify observations from CRSP! I cannot proceed')
        crsp.data <- crsp.data[, repetitions := .N, by = c('ncusip','yq')][order(repetitions,date,ncusip)]
        stop()
    } else {
        cat('Ncusip and quarter are unique identifiers for the data in CRSP.\n We can proceed to merge.')
    }
    
    #' ### Merge
    
    ##### Merge CRSP to S34 ###########
    
    setkeyv(s34, c("ncusip","yq"))
    setkeyv(crsp.data, c("ncusip","yq"))
    
    #here you join crsp to the s34 file, keeping the unmatched holdings
    tic("S34-CRSP merge")
    
    #matched
    s34 <- crsp.data[s34, nomatch = 0]
    #unmatched
    unmatched <- s34[!crsp.data]
    
    toc(log=TRUE)
    
    gc()
    mem_used()
    obj_size( s34 ); obj_size(crsp.data)
    
    #count how many merged
    numobs.merged <- dim( s34 )[1] 
    
    #check whether some of the S34 obs have been duplicated during the merge
    test <- unique_id(s34, "identifier")  
    
    #get id of unmatched observations in s34
    #unmatched.ids <- unmatched[, identifier]
    
    tic("Second round merge S34-CRSP")
    
    if (test == FALSE ){
        #if that is the case, stop the code
        stop(error = 'Some of the observations in S34 have been duplicated in the merge!')
    } else {
        # otherwise, it means there were no duplications in S34 which is good.
        # go on with trying to merge any remaining holdings from S34
        unmatched.frac <- 100*(numobs.s34-numobs.merged)/numobs.s34
        cat(paste( unmatched.frac , 'percent of observations from S34 are not matched.'))
        
        if( unmatched.frac > 0 ) {  #if there are some unmatched obs
            
            # try to merge using cusip instead of ncusip
            merged.round2 <- crsp.data[ unmatched, on = c(cusip = "ncusip", yq = "yq"), nomatch =0 ]
            
            remove(crsp.data, unmatched)
            
            gc()
            
            #if you match some of them this way, append them to the ones you had already matched before
            if ( dim(merged.round2)[1]>0 ){ 
                
                s34 <-  rbindlist( list( s34 , merged.round2  ), use.names = TRUE, fill = TRUE)
            }
            remove(merged.round2)
        }
    }
    toc(log =TRUE)
    
    #' Now the s34 data contains only the holdings matched with the CRSP information
    
    merged.frac  <-  100*dim( s34 )[1] /numobs.s34
    cat('We matched ', merged.frac, 'percent of observations in the holdings data to CRSP.')
    
    
    #' ## Sin and Fossil Stocks Classification
    #'
    
    # classify sin and fossil ##### 
    # Pick categories for Sin and Fossil stocks, following Hong and Kacperkzyk
    
    sin.sic <-  as.character(   c(2100:2199, 2080:2085) ) # make it string otherwise may give problems
    
    sin.naics <- as.character( c(7132,71312,713210,71329,713290,72112,721120)  )
    
    
    s34[, `:=`(sic = as.character(sic), naics = as.character(naics) ) ] 
    
    
    
    #' create dummy for fossil fuel industry. Following the Fama-French category 30, we have that:
    #' * 1200-1299 bituminous coal and lignite mining
    #' * 1300-1399 various oil and gas categories
    #' * 2900-2912 and 2990-2999 petroleum refining and miscellaneous
    fossil.sic <- as.character( c(1200:1299, 1300:1399, 2900:2912, 2990:2999) )
    
    
    # Create the indicator variables for sin and fossil holdings
    s34[,sin    := ifelse( sic %in% sin.sic | naics %in% sin.naics ,1,0)]
    s34[,fossil := ifelse( sic %in% fossil.sic , 1, 0)]
    #noice that NA's are treated as if they are not sin nor fossil
    
    #check whether this step is creating any missing values
    if( any( is.na(s34$sin) ) ){
        message('Be careful, the sin indicator is creating NAs')
    } 
    if ( any( is.na(s34$fossil) )  ){
        message('Be careful, the fossil indicator is creating NAs')
    }
    
    
    #' ## Calculate fraction of portfolio invested in sin/fossil stocks for each manager
    #' 
    #' 
    
    # value of every position
    s34[, pos.value := i.prc * shares]
    
    # total value of manager portfolio each date
    s34[, tot.value := sum(pos.value, na.rm = TRUE), by = .(mgrno, yq)]
    
    # calculate fraction of portfolio represented by each asset
    s34[, frac := (pos.value/tot.value)]
    
    # for each manager, compute the fraction of sin and fossil stocks in portfolio every quarter
    # mind how you handle missing values
    
    s34[, sin.frac    := sum(sin * frac, na.rm = TRUE)    , by = .(mgrno,yq) ]
    s34[, fossil.frac := sum(fossil * frac, na.rm = TRUE) , by = .(mgrno,yq) ]
    
    
    #' Look at type of investor that changed the most holdings in sin and fossil over time
    #' We need a table that has average sin fraction by date for each manager type.
    #' The code below does it with both equal and value weighted options. 
    
    
    # Sin and Fossil Weights by type ####
    
    # Portion of portfolios in sin and fossil for each investor type, weighted by total assets or not
    sin.frac.by.type <- s34[ ,  .( sin.frac.wei    = sum( sin*pos.value, na.rm = TRUE)     / sum( pos.value, na.rm = TRUE),
                                   fossil.frac.wei = sum( fossil*pos.value , na.rm = TRUE) / sum( pos.value, na.rm = TRUE),
                                   sin.frac        = mean(sin.frac,    na.rm = TRUE),
                                   fossil.frac     = mean(fossil.frac, na.rm = TRUE),
                                   total.hldg      = sum(pos.value, na.rm = TRUE) ,
                                   num.managers    = uniqueN(mgrno),
                                   num.obs         = uniqueN(identifier)    ),
                             by = .(yq,type)   ]
    
    #sin.frac.by.type$typecode <- as.integer(sin.frac.by.type$typecode)
    
    #export table
    if (i ==2){
        #first time include row header with names
        fwrite(sin.frac.by.type, file = 'output/sin_frac_by_type3.csv', sep = ",", row.names = FALSE, quote = FALSE, col.names = TRUE)
    } else{
        fwrite(sin.frac.by.type, file = 'output/sin_frac_by_type3.csv', sep= ",", row.names = FALSE, quote = FALSE, col.names = FALSE , append = TRUE)
    }
    
    
    
    remove( s34,sin.frac.by.type )
    
    toc(log=TRUE)

}  #end of loop over different time sub-samples



#get timings log
writeLines(  unlist( tic.log(format = TRUE) ))

#get total execution time
proc.time() - ptm




