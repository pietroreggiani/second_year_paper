#' ---
#' title: "Robintrack Merger"
#' author: "Pietro Reggiani"
#' date: "October 2020"
#' 
#' output: 
#'        github_document
#' ---
#' 
#' This file takes the cleaned Robintrack panel data file `.../robintrack_cleaned.csv`, that comes from the code *robintrack_cleaner.r*,
#' then tries to merge it with other data sources.
#'
#' **INPUTS**:
#' 
#' * .csv file from Robintrack stored in the data.folder specified in the parameters section.
#' * panel data from Datastream cleaned using the `esg_scores_cleaner.R` code, has to be stored in the same folder as
#'   the Robintrack data.
#' * CRSP data are loaded directly from WRDS trough a query. We use the CCM link file to link gvkeys as well.
#' * if choose Compustat parameter instead, then:
#'     * compustat securities daily observations are queried directly from WRDS
#'     * the compustat fundamentals observations are queried directly through WRDS
#'     * factor data from ken french should be stored in the data/raw folder
#' * see parameters section for other minor inputs.
#'  
#' 
#' 
#' **OUTPUTS**:
#' 
#' * `.csv` file containing the merged and cleaned panel of daily Robinhood users, matched with ESG scores and CRSP information.
#' The file also has gvkeys in case you want to join also information from Compustat. You can specify the location and name of the 
#' output file in the parameters section.
#' 
#' ## Description
#' First, we take the Robinhood data. Then, we link it to the header info of the CRSP daily stock file. We keep only the matched
#' observations, linked using ticker. Then, we link to CRSP also gvkeys using CCM.
#' Consequently, we take the panel of yearly ESG scores from datastream. We drop all firms that do not have CUSIP, 
#' so we are left with mainly American and Canadian firms. We merge them to Robinhood-CRSP in using CUSIP and ticker. We drop the
#' 5% of observations that are worst matches according to a measure of distance between the company names.
#' We assign to observations relative to year t to the score as of end of year t-1. 
#' If firms have 2019 scores missing, we pretend 2018 score is still valid in 2019.


#+ Setup ###########

# clear environment

rm(list=ls()) 

# Load Packages

library(data.table)
setDTthreads(0)  #tells to use all available cores

library(lubridate)

library(dplyr)

library(sqldf)

library(tictoc)

library(stringi)

library(pryr)

library(stringdist)

library(DescTools)

#functions folder
source("code/funs_warehouse.r")

# change global option about string import
options(stringsAsFactors = FALSE)

# create log file
# logfile <- file("./output/logs/robintrack_merger_log.txt") # File name of output log
# sink(logfile, append = TRUE, type = "output") # Writing console output to log file
# sink(logfile, append = TRUE, type = "message")


tic('all file')


#' ## Parameters
#' here you can choose parameters that determine what the code does

#+ parameters choice ####

# tell R the folder where you stored the input data
data.folder <- './data/processed/'

# tell R the folder and filename for the output data file
output.file.crsp <- './data/processed/robintrack_crsp_esg_merged_ready.csv'
output.file.compustat <- './data/processed/robintrack_compustat_esg_merged_ready.csv'


#number of rows to import
rows = -1

# want CRSP or Compustat?
CRSP <- FALSE

if(CRSP){
    output.file <- output.file.crsp
} else {
    output.file <- output.file.compustat
}


# tell date range for ESG scores to import
first.year <- 2017
last.year <- 2019

# tell code which datastream asset4 scores are present in the data (check in ESG file)
scores <- c('TRESGS','TRESGCS','ENSCORE')


#+ prepare compustat secd with betas estimation ####

if(!CRSP){
    
    
    query <- "select datadate, tic, gvkey , ajexdi, conm, cusip,trfd, prccd, divd,divsp, cshoc, cshtrd, exchg, tpci, fic
    from comp.secd where (datadate between '2017-01-01' and '2020-08-14') AND tic IS NOT NULL "

    
    tic('secd query')
    comp.secd <- wrds.table(query, numrows= rows)
    toc()
    
    
    comp.secd[, datadate := as.Date(datadate)]
    
    if (!unique_id(comp.secd, c('tic','datadate'))){
        message('NB: ticker does not uniquely identify information in the Compustat security data !!\n 
                Be mindful that some observations from Robintrack might be duplicated in the merge!')
    } 
    
    
    # create issuer 6-digit CUSIP
    comp.secd[, cusip6 := substr(cusip, 1,6)]
    comp.secd[, cusip8 := substr(cusip, 1,8)]
    
    numobs.comp.secd <- dim(comp.secd)[1]
    
    #+ estimate stock betas ###
    
    #we estimate one beta per stock per year
    # to do so we need the French data on the market factor
    
    ff.filepath <- 'data/raw/ff_factors_2017_2020_daily.CSV'
    
    ff.data <- fread(file = ff.filepath , header = TRUE,  data.table = TRUE )
    
    ff.data[, date := ymd( as.character(V1))]
    ff.data[, V1 := NULL]
    
    setnames(ff.data, 'Mkt-RF', 'mkt')
    
    comp.secd <- merge(comp.secd, ff.data, by.x = 'datadate', by.y = 'date', all.x = TRUE)
    
    remove(ff.data)
    
    #+ compute stock betas ####
    comp.secd <- comp.secd[order(tic, datadate)]
    
    #make sure that for every ticker there are no duplicate dates, otherwise the returns are screwed up.
    comp.secd[, test := .N   , by = c('tic', 'datadate')]
    
    if(any(comp.secd$test >1)){
        stop('Ticker-date do not uniquely identify observations in the compustat securities data \n
             this is a problem for return calculation \n
             The code was stopped.')
        
    } else{
        comp.secd[, test := NULL]
    }
    
    
    # compute daily excess returns for each stock
    comp.secd[, retd := ( fifelse(is.na(trfd),1,trfd) * .SD[, prccd/ajexdi] / shift(.SD[, fifelse(is.na(trfd),1,trfd)*prccd/ajexdi],1) -1)*100 , by = tic]
    
    comp.secd[, exc.ret := retd - RF ]
    
    comp.secd[, beta.year := year(datadate) ] # the year used to compute the beta
    
    # for each firm - year, Winsorize excess returns at the 1% level as the outliers may indicate mistakes
    
    comp.secd[, exc.ret := Winsorize( .SD[, exc.ret],
                                      minval = quantile(.SD[, exc.ret], na.rm = TRUE, probs = 0.01) ,
                                      maxval = quantile(.SD[, exc.ret], na.rm = TRUE, probs = 0.99) ,
                                      na.rm = TRUE ), by= c('beta.year', 'tic') ]
    
    # regress on market excess returns for each firm in each year
    
    betas <- comp.secd[!is.na(exc.ret) , .(beta = lm(exc.ret ~ mkt , data = .SD)$coefficients[2]) , by = c('tic', 'beta.year')  ][order(tic, beta.year)]
    
    # add back betas to compustat data
    # you want that the beta is linked to the observation relative to the following year
    # so create lead of year
    comp.secd[, beta.year := year(datadate)-1] # you want obs to be matched to previous year beta
    
    comp.secd <- merge(comp.secd, betas, by = c('tic','beta.year'), all.x = TRUE)
    
    remove(betas)
    
    #remove 2017 observations to save memory, in any case they do not match any Robinhood obs
    comp.secd <- comp.secd[datadate > '2018-05-01']
    
    # write to .csv so we save memory in the meantime
    fwrite(comp.secd,
           file = './data/processed/comp_secd_with_betas.csv' ,
           sep= ",", row.names = FALSE, quote = TRUE, col.names = TRUE , append = FALSE)
    
    remove(comp.secd)
    
}


#+ import robin data ####

robin.path <- paste(data.folder, 'robintrack_cleaned.csv', sep='')

tic('Read Robintrack')
robin.data <- fread(file = robin.path , header = TRUE,  data.table = TRUE, colClasses = c("Date", "character", "double"), nrows = rows )
toc()

# create surrogate key to identify observations
robin.data$id.robin <- as.integer( rownames(robin.data) )

setkey(robin.data, id.robin)

robin.data[, rob.year := year(date)]

numobs.robin <- dim(robin.data)[1]
numfirms.robin <- length(unique(robin.data$ticker))


first.date.robin <- min(robin.data$date)
last.date.robin <- max(robin.data$date)


if(CRSP){

    #+ load and clean CRSP info ####
    
    varlist <- 'permno, permco, cusip as cusip8, hcusip, htick as crsp_ticker, htsymbol, 
    hcomnam as crsp_name, hexcd, hshrcd, hsiccd as crsp_sic , hnaics as crsp_naics '
    
    query <- paste("select ",
                   varlist, 
                   " from crsp.dsfhdr where enddat >= '", first.date.robin,"' ", sep = "")
    
    tic('CRSP query')
    crsp.info <- wrds.table(query, numrows=rows)
    toc()
    
    
    if (!unique_id(crsp.info, 'permno')){
        message('PROBLEM: Permno does not uniquely identify information in the CRSP header data !!')
    } else {
        setkey(crsp.info, permno)
    }
    
    # when ticker is missing replace with trading symbol
    crsp.info[is.na(crsp_ticker) , crsp_ticker := htsymbol  ]
    
    # create issuer 6-digit CUSIP
    crsp.info[, cusip6 := substr(cusip8, 1,6)]
    
    
    if (!unique_id(crsp.info, 'crsp_ticker')){
        message('Ticker does not uniquely identify information in the CRSP header data !! \n 
            Be mindful that some observations from Robintrack might be duplicated in the merge!')
    }
    
    numobs.crsp <- dim(crsp.info)[1]
    
    #+ add gvkey using CCM ####
    
    # get link table from WRDS
    
    query <- paste("select upermno, ugvkey, ulinkdt, ulinkenddt  from crsp.ccmxpf_lnkused
               where (ulinktype = 'LU' or ulinktype = 'LC')
               and (ulinkprim = 'P' or ulinkprim = 'C')
               and usedflag = 1
               and upermno IS NOT NULL
               and ulinkenddt >= '", first.date.robin , "'",  sep = "")
    
    
    tic('CCM query')
    ccm.link <- wrds.table(query, numrows=rows)
    toc()
    
    # we need that each permno is associated to at most one gvkey, otherwise this might create problems.
    # even if two different permnos belong to the same gvkey that is not a big deal (several securities, same firm).
    
    # for each permno, we keep the most recent gvkey because the Robinhood data are so recent.
    ccm.link <- ccm.link[order(upermno, ulinkenddt)]
    ccm.link <- ccm.link[,  .SD[.N , .(gvkey = ugvkey)]  , by = upermno  ]
    
    
    # now you should have only one row (and one gvkey) for every permno in the link-table
    if(!unique_id(ccm.link, 'upermno')){
        stop('Some permnos are associated to more than one gvkey in the link table, this may cause problems. \n 
         Code Killed.')
    }
    
    # merge link table to CRSP
    crsp.info <- merge(crsp.info, ccm.link, all.x = TRUE , by.x = 'permno' , by.y = 'upermno'  )
    
    remove(ccm.link)
    
    # check how many gvkeys you have
    gvkey.merged <- dim( crsp.info[!is.na(gvkey)] )[1]
    gvkey.merged.frac <- 100* gvkey.merged / numobs.crsp
    
    cat('We have gvkeys for ', format(gvkey.merged.frac, digits = 3) , 'percent of the observations from CRSP'  )
    
    
    #+ merge CRSP to Robintrack using ticker ####
    
    merged <- merge(robin.data, crsp.info, all = FALSE , by.x = 'ticker' , by.y = 'crsp_ticker' )
    
    remove(crsp.info, robin.data)
    
    if (!unique_id(merged, 'id.robin')){
        message('PROBLEM: some robinhood observations were duplicated during the merge with CRSP information.')
        
        # get the duplicated observations and count them
        merged[, duplicates := .N, by = id.robin]
        
        duplicated.percent <- format( 100* merged[  duplicates>1 , sum( 1/duplicates  )   ] / numobs.robin , digits = 2)
        cat(duplicated.percent, 'percent of Robintrack observations are duplicated when joining CRSP.')
        
        # look what share codes are duplicated
        duplicated.codes <- 100*  table( merged[ duplicates >1, hshrcd ] ) / dim(merged[ duplicates >1])[1]  
        
        cat('The distribution of share codes among the duplicated is \n')
        print(duplicated.codes )
        
        #seems that most are ETF's
        
        # keep the ones that are share codes 11, 12 or 31 and scrap the rest
        merged[, keep:= TRUE]
        merged[  duplicates >1 ,
                 keep := fifelse( hshrcd %in% c(11:12,31) ,
                                  TRUE  ,
                                  FALSE )   ]
        
        #check whether there are still duplicates
        merged <- merged[ (keep) ][  , c( 'duplicates', 'keep' ) := NULL ][, duplicates := .N, by = id.robin ]
        
        #if yes, delete them
        merged <- merged[duplicates ==1][, duplicates := NULL]
        
        if(!unique_id(merged, 'id.robin')){
            stop('Code was interrupted because some of the Robintrack assets are still duplicated \n 
             even after removing the duplicates for share codes different from 11 and 12.')
        }
    }
    
    numobs.merged <- dim(merged)[1]
    merged.percent <- 100 * numobs.merged / numobs.robin 
    
    cat('We merged', format(merged.percent, digits = 3), 'percent of the Robintrack observations to CRSP' )
    
} else {
    
    #+ load Compustat security daily 
    comp.secd <- fread(file = './data/processed/comp_secd_with_betas.csv' , header = TRUE,  data.table = TRUE, nrows = rows )
    
    comp.secd[, datadate := as.Date(datadate)]
    
    #+ merge Compustat to Robintrack using ticker ####
    
    robin.data <- merge(robin.data,
                        comp.secd,
                        all.x = TRUE ,
                        by.x = c('ticker', 'date') ,
                        by.y = c('tic','datadate') )
    
    remove(comp.secd)
    
    if (!unique_id(robin.data, 'id.robin')){
        stop('PROBLEM: some robinhood observations were duplicated during the merge with Compustat information.\n Code stopped.')
    }
    
    numobs.merged.secd <- dim(robin.data[!is.na(gvkey)])[1]
    
    # count how many firms merged (Compustat does not have weekends so you mechanically lose observations)
    numfirms.merged.secd <- dim( robin.data[!is.na(gvkey) , .SD[1]  , by = ticker] )[1]
    percentfirms.merged.secd <- 100* numfirms.merged.secd / numfirms.robin
    
    cat('We merged', format(percentfirms.merged.secd, digits = 3), 'percent of the Robintrack firms to Compustat securities daily.' )
    
    # drop unmatched observations ( most are weekends !)
    robin.data <- robin.data[!is.na(gvkey)]
    robin.data[, gvkey := as.character(gvkey)]
    
    
    #+ match to Compustat fundamentals ####
    query <- "select datadate as date_funda, a.conm as name_funda, tic, a.gvkey ,
    cusip, at as assets, ceql, seq, txditc, pstkrv , pstkl, ib, ch, che, csho, dvt, dltt, dlc, sic, naics, gsubind, stko
    from comp.funda a
    left join comp.company b on a.gvkey = b.gvkey
    where datadate between '2017-12-31' and '2019-12-31' 
    AND ceql IS NOT NULL" 
    
    
    tic('fundamentals query')
    compustat.funda <- wrds.table(query, numrows=rows)
    toc()
    
    compustat.funda[, id.funda := as.integer(rownames(compustat.funda))]
    compustat.funda[, cusip8.funda := substr(cusip,1,8)]
    
    #lead the year by one so that you can link obs to subsequent year
    compustat.funda[, year.plus := year(date_funda)+1]
    
    
    # for every year-gvkey, keep most recent observation
    compustat.funda <- compustat.funda[order(-date_funda),
                                       .SD[1] ,
                                       by = c('year.plus','gvkey' ) ]
    
    # get vector of variables that belong only to one dataset, to keep merge more clean
    vars.robin <- colnames(robin.data)
    vars.funda <- setdiff( colnames(compustat.funda) , colnames(robin.data)   )
    
    ## round 1
    tic('merge fundamentals')
    
    vars <- c('gvkey', vars.funda)
    # link to Robin data merging on previous year and gvkey
    robin.data <- merge(robin.data,
                        compustat.funda[, ..vars],
                        by.x = c('gvkey','rob.year'), 
                        by.y = c('gvkey','year.plus'), 
                        all.x = TRUE)
    
    if (!unique_id(robin.data, 'id.robin')){
        stop('PROBLEM: some robinhood observations were duplicated during the merge with Compustat information.\n Code stopped.')
    }
    
    numobs.merged <- dim(robin.data[!is.na(date_funda)])[1]
    
    # count how many firms merged (Compustat does not have weekends so you mechanically lose observations)
    numfirms.merged.funda <- dim( robin.data[!is.na(date_funda) , .SD[1]  , by = ticker] )[1]
    percentfirms.merged.funda <- 100* numfirms.merged.funda / numfirms.robin
    
    cat('We merged', format(percentfirms.merged.funda, digits = 3), 'percent of the Robintrack firms to Compustat fundamentals annual using gvkey.' )
    
    ## Round 2: Cusip 8
    
    compustat.funda <- compustat.funda[!(id.funda %in% robin.data$id.funda )] #take obs that are not yet matched
    
    merged.funda.cusip <- merge(robin.data[is.na(date_funda), ..vars.robin],
                                 compustat.funda[, ..vars.funda]  ,
                                 by.x = c('cusip8','rob.year'), 
                                 by.y = c('cusip8.funda','year.plus'), 
                                 all = FALSE)
    
    # count how many firms merged using cusip
    numfirms.merged.funda.cusip <- dim( merged.funda.cusip[, .SD[1], by = ticker] )[1]
    percentfirms.merged.funda.cusip <- 100* numfirms.merged.funda.cusip / numfirms.robin
    
    cat('We merged', format(percentfirms.merged.funda.cusip, digits = 3), 'percent of the Robintrack firms to Compustat fundamentals annual using CUSIP8.' )
    
    ## Round 3: Ticker
    
    # take only unmatched compustat variables and match with unmatched robin.data observations
    compustat.funda <- compustat.funda[!(id.funda %in% robin.data$id.funda )& !(id.funda %in% merged.funda.cusip$id.funda )]
    
    merged.funda.ticker <- merge(robin.data[is.na(date_funda) & !(id.robin %in% merged.funda.cusip$id.robin), ..vars.robin],
                                 compustat.funda[, ..vars.funda],
                                 by.x = c('ticker','rob.year'), 
                                 by.y = c('tic','year.plus'), 
                                 all = FALSE)
    
    # count how many firms merged using ticker
    numfirms.merged.funda.ticker <- dim( merged.funda.ticker[, .SD[1], by = ticker] )[1]
    percentfirms.merged.funda.ticker <- 100* numfirms.merged.funda.ticker / numfirms.robin
    
    cat('We merged', format(percentfirms.merged.funda.ticker, digits = 3), 'percent of the Robintrack firms to Compustat fundamentals annual using ticker.' )
    
    remove(compustat.funda)
    
    # collect all observations, drop unmerged
    robin.data <- robin.data[!is.na(date_funda)]
    
    robin.data <- rbindlist( list(robin.data, merged.funda.cusip, merged.funda.ticker ), use.names = TRUE, fill = TRUE)
    
    #check that no duplicates
    if (!unique_id(robin.data, 'id.robin')){
        stop('PROBLEM: some robinhood observations were duplicated during the merge with Compustat information.\n Code stopped.')
    }
    
    # count how many firms matched
    numobs.merged <- dim(robin.data)[1]
    
    numfirms.merged <- dim(robin.data[,.SD[1], by = ticker]  )[1]
    numfirms.merged.percent <- 100* numfirms.merged/ numfirms.robin
    cat('We merged', format(numfirms.merged.percent , digits = 3), 'percent of the Robintrack firms to Compustat fundamentals annual and SECD.' )
    
    robin.data[, c('tic','year.plus', 'cusip8.funda') := NULL]
    
    toc()
    
}


#+ import ESG scores ####

esg.path <- paste(data.folder, 'esg_scores_cleaned.csv', sep='')

tic('read esg data')
esg.data <- (fread(file = esg.path , header = TRUE,  data.table = TRUE, nrows = rows ))[ between(year, first.year, last.year)]
toc()

esg.data <- esg.data[!is.na(CUSIP)]

numobs.esg <- dim(esg.data)[1]
numfirms.esg <- length(unique(esg.data$CUSIP))

cat('The ESG sample from Datastream has ', numobs.esg, ' yearly observations for ', numfirms.esg, 'firms with non-missing CUSIP codes. 
    The data cover ',last.year - first.year +1 ,'years from ', first.year, 'to', last.year)

cat('The geographic breakdown of observations is the following:')
table(esg.data$GEOGN)

setnames(esg.data, 'V1', 'id.esg')

# we have a 9-digit cusip, let us cut it, remember 6 is the issuer and 8 is the issue.
setnames(esg.data, 'CUSIP', 'cusip')
esg.data[, cusip6 := substr(cusip, 1, 6)]
esg.data[, cusip8 := substr(cusip, 1, 8)]


# create lead of year variable so that the observations from Robin are linked to previous year ESG score
esg.data[, yearlink := year +1 ]

#create more tickers to merge
esg.data[,  `:=`(ticker.mnem = 
                     fifelse(  substr(MNEM,1,1) == '@',
                               substr(MNEM,2, length(MNEM)) ,
                               fifelse( grepl(":", MNEM) ,
                                        substr(MNEM, regexpr( ":" , as.character(MNEM), fixed = TRUE )+1 , length(MNEM)) ,  
                                        as.character(MNEM) )
                     ),
                 ticker.tr = substr(TRticker , 1 , regexpr( "-" , as.character(TRticker), fixed = TRUE ) -1 )
                 )
         ]

# put all tickers together
esg.data %>% setcolorder( neworder = c('id.esg', 'WCticker', 'ticker.mnem', 'ticker.tr', 'year') )
esg.data[, TRticker := NULL]

esgvars <- colnames(esg.data)

#most observations are only until 2018 or max 19

#+ merge ESG to Robin data ####

if (CRSP) {

    
    
    # now I have cusip information from CRSP so I can  use that!
    # beware that many firms in ASSET4 do not have CUSIP data, maybe because they are foreign firms.
    
    # merged <- esg.data[robin.data, on = .( yearlink = rob.year , WCticker = ticker), nomatch = NULL][order(id.robin)]
    
    # first merge using 8-digit CUSIP (issue level, more stringent)
    
    merged8 <- merge( x = merged, y = esg.data, all = FALSE, by.x =c( 'cusip8', 'rob.year') , by.y = c('cusip8', 'yearlink' ))
    
    # take un-merged observations
    merged <- merged[ !(id.robin %in% merged8$id.robin) ]
    
    
    # count how many merged using 8-digit CUSIP
    numobs.merged.esg8 <- dim(merged8)[1]
    merged.percent.esg8 <- 100 *  numobs.merged.esg8 / numobs.merged
    
    cat('We merged ', format(merged.percent.esg8, digits = 3), 'percent of the Robintrack observations to the ESG file using 8-digit CUSIP')
    
    
    # now merge on 6-digit CUSIP (issuer level)
    merged6 <- merge( x = merged, y = esg.data, all = FALSE, by.x =c( 'cusip6', 'rob.year') , by.y = c( 'cusip6', 'yearlink' ))
    
    
    # take those that still remain unmerged
    merged <- merged[ !(id.robin %in% merged6$id.robin) ]
    
    
    # count how many merged using 6-digit CUSIP
    numobs.merged.esg6 <- dim(merged6)[1]
    merged.percent.esg6 <- 100 *  numobs.merged.esg6 / numobs.merged
    
    cat('We merged ', format(merged.percent.esg6, digits = 2), 'percent of the Robintrack observations to the ESG file using 6-digit CUSIP')
    
    
    # Now try three rounds of merge through different Ticker symbols available in Datastream file
    
    # Round 1: Worldscope Ticker
    merged.ticker <- merge( x = merged, y = esg.data, all = FALSE, by.x =c( 'ticker', 'rob.year') , by.y = c( 'WCticker', 'yearlink' ))
    
    
    #take unmatched
    merged <- merged[ !(id.robin %in% merged.ticker$id.robin) ]
    
    # Round 2: another ticker
    merged.ticker2 <- merge( x = merged, y = esg.data, all = FALSE, by.x =c( 'ticker', 'rob.year') , by.y = c( 'ticker.mnem', 'yearlink' ))
    
    #take unmatched
    merged <- merged[ !(id.robin %in% merged.ticker2$id.robin) ]
    
    # Round 3 another ticker
    merged.ticker3 <- merge( x = merged, y = esg.data, all = FALSE, by.x =c( 'ticker', 'rob.year') , by.y = c( 'ticker.tr', 'yearlink' ))
    
    
    # count how many merged using all the tickers
    numobs.merged.esg.tick <- dim(merged.ticker)[1]+ dim(merged.ticker2)[1]+dim(merged.ticker3)[1]
    merged.percent.esg.tick <- 100 *  numobs.merged.esg.tick / numobs.merged
    
    cat('We merged ', format(merged.percent.esg.tick, digits = 3), 'percent of the Robintrack observations to the ESG file using ticker symbols')
    
    
    # Put all rounds together
    merged <- rbindlist( list( merged8, merged6, merged.ticker, merged.ticker2, merged.ticker3), use.names = TRUE, fill = TRUE)
    
    
    remove(merged8, merged6, merged.ticker, merged.ticker2, merged.ticker3 , esg.data)
    
    
    
    # check whether some observations from robinhood have been duplicated during the merge
    if (!unique_id(merged, 'id.robin')){
        message('PROBLEM: some robinhood observations were duplicated during the merge with Datastream information.')
        
        # get the duplicated observations and count them
        merged[, duplicates := .N, by = id.robin]
        
        duplicated.percent2 <- format( 100* merged[  duplicates>1 , sum( 1/duplicates  )   ] / numobs.merged , digits = 2)
        cat(duplicated.percent2, 'percent of Robintrack observations are duplicated when joining CRSP.')
        
        # look what share codes are duplicated
        duplicated.codes2 <- 100*  table( merged[ duplicates >1, hshrcd ] ) / dim(merged[ duplicates >1])[1]  
        
        cat('The distribution of share codes among the duplicated is \n')
        
        print(duplicated.codes2 )
        
        #seems that most are ordinary common shares, then REITS and closed funds and ETFs
        
        # drop all the non-US duplicates because they look wrong
        merged <- merged[ duplicates ==1 | duplicates >1 & GEOGN == 'UNITED STATES' ]
        
        # re-count duplicates
        merged[, duplicates := .N, by = id.robin]
        
        # remove inactive firms
        merged <- merged[ duplicates ==1 | duplicates >1 & A4STATUS == 'Active' ]
        
        if (!unique_id(merged, 'id.robin')){
            stop('There are still duplicated observations even after removing foreign and inactive assets')
        } else{
            merged[, duplicates := NULL]
        }
        
    }
    
    
    # count how many merged overall
    numobs.merged.overall <- dim(merged)[1]
    merged.percent.overall <- 100 *  numobs.merged.overall / numobs.merged
    
    cat('We merged ', format(merged.percent.overall, digits = 3), 'percent of the Robintrack observations to the ESG file.')
    
    
    
    # count how many firms we have
    num.firms <- length(unique(merged$DSCD))
    cat('We have ', num.firms, 'firms in the merged sample.')
    
    
    
    # remove all the redundant variables that got created during the merge rounds
    
    merged[ is.na(cusip8) , cusip8 := fifelse( is.na(cusip8.x), cusip8.y, cusip8.x ) ]
    merged[ is.na(cusip6) , cusip6 := fifelse( is.na(cusip6.x), cusip6.y, cusip6.x ) ]
    
    merged[, c('cusip6.x', 'cusip6.y','cusip8.x', 'cusip8.y' ) := NULL ]
    
    merged %>% setcolorder(neworder = c( 'date', 'ticker', 'cusip8', 'crsp_name', 'NAME') )
    
} else{
    
    #+ Merge to ESG scores
    
    # separate variables that belong to each dataset to keep merge more clean avoiding suffixes
    robinvars<-colnames(robin.data)
    esgvars.only <- setdiff(esgvars, robinvars)
    
    # now I have cusip information from CRSP so I can  use that!
    # beware that many firms in ASSET4 do not have CUSIP data, maybe because they are foreign firms.
    
    # first merge using 8-digit CUSIP (issue level, more stringent)
    
    vars <- c('cusip8', esgvars.only)
    
    merged8 <- merge( x = robin.data,
                      y = esg.data[,  ..vars ],
                      all = FALSE, by.x =c( 'cusip8', 'rob.year') , by.y = c('cusip8', 'yearlink' ))
    
    # take un-merged observations
    robin.data <- robin.data[ !(id.robin %in% merged8$id.robin) ]
    esg.data <- esg.data[!(id.esg %in% merged8$id.esg)]
    
    # count how many merged using 8-digit CUSIP
    numobs.merged.esg8 <- dim(merged8)[1]
    merged.percent.esg8 <- 100 *  numobs.merged.esg8 / numobs.merged
    
    cat('We merged ', format(merged.percent.esg8, digits = 3), 'percent of the Robintrack observations to the ESG file using 8-digit CUSIP')
    
    
    # now merge on 6-digit CUSIP (issuer level)
    
    vars <- c('cusip6', esgvars.only)
    
    merged6 <- merge( x = robin.data,
                      y = esg.data[, ..vars],
                      all = FALSE, by.x =c( 'cusip6', 'rob.year') , by.y = c( 'cusip6', 'yearlink' ))
    
    
    # take those that still remain unmerged
    robin.data <- robin.data[ !(id.robin %in% merged6$id.robin) ]
    esg.data <- esg.data[!(id.esg %in% merged6$id.esg)]
    
    
    # count how many merged using 6-digit CUSIP
    numobs.merged.esg6 <- dim(merged6)[1]
    merged.percent.esg6 <- 100 *  numobs.merged.esg6 / numobs.merged
    
    cat('We merged ', format(merged.percent.esg6, digits = 2), 'percent of the Robintrack observations to the ESG file using 6-digit CUSIP')
    
    
    # Now try three rounds of merge through different Ticker symbols available in Datastream file
    
    # Round 1: Worldscope Ticker
    
    merged.ticker <- merge( x = robin.data ,
                            y = esg.data[,..esgvars.only],
                            all = FALSE, by.x =c( 'ticker', 'rob.year') , by.y = c( 'WCticker', 'yearlink' ))
    
    
    # take those that still remain unmerged
    robin.data <- robin.data[ !(id.robin %in% merged.ticker$id.robin) ]
    esg.data <- esg.data[!(id.esg %in% merged.ticker$id.esg)]
    
    # Round 2: another ticker
    merged.ticker2 <- merge( x = robin.data,
                             y = esg.data[, ..esgvars.only],
                             all = FALSE, by.x =c( 'ticker', 'rob.year') , by.y = c( 'ticker.mnem', 'yearlink' ))
    
    # take those that still remain unmerged
    robin.data <- robin.data[ !(id.robin %in% merged.ticker2$id.robin) ]
    esg.data <- esg.data[!(id.esg %in% merged.ticker2$id.esg)]
    
    # Round 3 another ticker
    merged.ticker3 <- merge( x = robin.data,
                             y = esg.data[, ..esgvars.only],
                             all = FALSE, by.x =c( 'ticker', 'rob.year') , by.y = c( 'ticker.tr', 'yearlink' ))
    
    # take those that still remain unmerged
    robin.data <- robin.data[ !(id.robin %in% merged.ticker3$id.robin) ]
    
    
    # count how many merged using all the tickers
    numobs.merged.esg.tick <- dim(merged.ticker)[1]+ dim(merged.ticker2)[1]+dim(merged.ticker3)[1]
    merged.percent.esg.tick <- 100 *  numobs.merged.esg.tick / numobs.merged
    
    cat('We merged ', format(merged.percent.esg.tick, digits = 3), 'percent of the Robintrack observations to the ESG file using ticker symbols')
    
    
    # Put all rounds together
    merged <- rbindlist( list( merged8, merged6, merged.ticker, merged.ticker2, merged.ticker3, robin.data), use.names = TRUE, fill = TRUE)
    
    
    remove(merged8, merged6, merged.ticker, merged.ticker2, merged.ticker3 , esg.data, robin.data)
    
    
    
    # check whether some observations from robinhood have been duplicated during the merge
    if (!unique_id(merged, 'id.robin')){
        message('PROBLEM: some robinhood observations were duplicated during the merge with Datastream information.')
        
        # get the duplicated observations and count them
        merged[, duplicates := .N, by = id.robin]
        
        duplicated.percent2 <- format( 100* merged[  duplicates>1 , sum( 1/duplicates  )   ] / numobs.merged , digits = 2)
        cat(duplicated.percent2, 'percent of Robintrack observations are duplicated when joining Compustat.')
        
        # drop all the non-US duplicates because they look wrong
        merged <- merged[ duplicates ==1 | duplicates >1 & GEOGN == 'UNITED STATES' ]
        
        # re-count duplicates
        merged[, duplicates := .N, by = id.robin]
        
        # remove inactive firms
        merged <- merged[ duplicates ==1 | duplicates >1 & A4STATUS == 'Active' ]
        
        if (!unique_id(merged, 'id.robin')){
            stop('There are still duplicated observations even after removing foreign and inactive assets')
        } else{
            merged[, duplicates := NULL]
        }
        
    }
    
    
    # count how many merged overall
    numfirms.merged.overall <- length(unique(merged$DSCD))
    numfirms.percent.overall <- 100 *  numfirms.merged.overall / numfirms.merged
    
    cat('We merged ', format(numfirms.percent.overall, digits = 3), 'percent of firms to the ESG file.')
    
    # count how many firms we have
    cat('We have ', numfirms.merged.overall , 'firms in the merged sample with information on ESG scores.')
    
    
    merged %>% setcolorder(neworder = c( 'date', 'ticker', 'cusip8', 'conm', 'NAME') )
    
}



#+ clean data ####


# check and remove matches that look wrong, comparing the names of the firm as given by CRSP vs Datastream

# extract the 'DEAD' description from datastream NAME, so that we can compare the names better
merged[, death :=
           fifelse( regexpr("DEAD" , as.character(NAME) , fixed = TRUE ) != -1,
                    substr( as.character(NAME) ,
                            regexpr("DEAD" , 
                                    as.character(NAME) ,
                                    fixed = TRUE ) ,
                            length(NAME)   ),
                    '' )]

merged[death !='', NAME := substr( as.character(NAME) ,
                                   1,
                                   regexpr("DEAD" , as.character(NAME) , fixed = TRUE )-2
                                   )    ]

# check that matches are correct using the string distance between the names
merged$namedist <- stringdist( merged$crsp_name, merged$NAME , method = 'jw' )  # you can change the method if you like
#merged<- merged[order(namedist)]

# discard the worst 10% of matches based on the name distance
merged <- merged[ is.na(namedist) | (!is.na(namedist) &  namedist <= quantile( merged[!is.na(namedist),namedist] , 0.90))][, namedist := NULL]

# count how many merged overall
numobs.merged.overall <- dim(merged[!is.na(DSCD)])[1]
merged.percent.overall <- 100 *  numobs.merged.overall / numobs.merged

cat('We merged ', format(merged.percent.overall, digits = 3), 'percent of the Robintrack observations to the ESG file (after name check).')

# count how many firms we have
num.firms <- length(unique(merged$DSCD))
cat('We have ', num.firms, 'firms in the merged sample with information on ESG scores.')




#+ take care of the fact that many scores for eoy 2019 are still missing in Datastream ####

# scores for 2019 are very often missing
# if score is missing for Robin observations of 2020 (that have scores from 2019), use the previous year one


# take data of 2020
data2020 <- merged[rob.year ==2020]

# take scores for previous year
scores2018 <- merged[rob.year == 2019, .SD[.N ] , by = ticker, .SDcols = scores]

# merge them to data
data2020 <- merge(data2020, scores2018, by = 'ticker' , all.x = TRUE)


# if the score is missing replace it with the new value
# loop over different scores
for (i in 1:3){
    score <- scores[i]

    data2020[, (score) :=   fifelse( is.na( get( paste(score, '.x', sep ='')    )  )  ,
                                     get( paste(score, '.y', sep ='') ),
                                     get( paste(score, '.x', sep ='') )  )  ]
              
    data2020[, c( paste(score, '.x', sep =''), paste(score, '.y', sep ='')  ):= NULL   ]
    
}

# append these observations back to rest of sample
merged <- rbindlist( list( merged[rob.year <2020], data2020 ), use.names = TRUE, fill = FALSE)

remove(data2020, scores2018)

#+ create useful variables ####

# Sin and fossil variables
sin.sic <-  c(2100:2199, 2080:2085) 
sin.naics <-  c(7132,71312,713210,71329,713290,72112,721120) 

#' create dummy for fossil fuel industry. Following the Fama-French category 30, we have that:
#' * 1200-1299 bituminous coal and lignite mining
#' * 1300-1399 various oil and gas categories
#' * 2900-2912 and 2990-2999 petroleum refining and miscellaneous
fossil.sic <- as.character( c(1200:1299, 1300:1399, 2900:2912, 2990:2999) )


# Create the indicator variables for sin and fossil holdings

if (CRSP){
    merged[,sin    := ifelse( SIC1 %in% sin.sic| SIC2 %in% sin.sic | SIC3 %in% sin.sic |SIC4 %in% sin.sic | crsp_naics %in% sin.naics ,1,0)]
    merged[,fossil := ifelse( SIC1 %in% fossil.sic|SIC2 %in% fossil.sic|SIC3 %in% fossil.sic , 1, 0)]
    
    
} else{
    merged[,sin    := ifelse( sic %in% sin.sic | naics %in% sin.naics ,1,0)]
    merged[,fossil := ifelse( sic %in% fossil.sic , 1, 0)]
    
}


#+ save output data to .csv file ####
fwrite(merged,
       file = output.file ,
       sep= ",", row.names = FALSE, quote = TRUE, col.names = TRUE , append = FALSE)


toc()


#closeAllConnections()


