Robintrack Merger
================
Pietro Reggiani
October 2020

This file takes the cleaned Robintrack panel data file
`./data/processed/robintrack_cleaned.csv`, that comes from the code
*robintrack\_cleaner.r*, then tries to merge it with other data sources.

**INPUTS**:

  - .csv file from Robintrack stored in the `./data/processed` folder.
  - panel data from Datastream cleaned using the `esg_scores_cleaner.R`
    code, also in `./data/processed`.

**OUTPUTS**:

  - 
<!-- end list -->

``` r
# clear environment

rm(list=ls()) 

# Load Packages

library(data.table)
setDTthreads(0)  #tells to use all available cores

library(lubridate)
```

    ## 
    ## Attaching package: 'lubridate'

    ## The following objects are masked from 'package:data.table':
    ## 
    ##     hour, isoweek, mday, minute, month, quarter, second, wday, week,
    ##     yday, year

    ## The following objects are masked from 'package:base':
    ## 
    ##     date, intersect, setdiff, union

``` r
library(dplyr)
```

    ## 
    ## Attaching package: 'dplyr'

    ## The following objects are masked from 'package:data.table':
    ## 
    ##     between, first, last

    ## The following objects are masked from 'package:stats':
    ## 
    ##     filter, lag

    ## The following objects are masked from 'package:base':
    ## 
    ##     intersect, setdiff, setequal, union

``` r
library(sqldf)
```

    ## Loading required package: gsubfn

    ## Loading required package: proto

    ## Loading required package: RSQLite

``` r
library(tictoc)

library(stringi)

library(pryr)
```

    ## Registered S3 method overwritten by 'pryr':
    ##   method      from
    ##   print.bytes Rcpp

    ## 
    ## Attaching package: 'pryr'

    ## The following object is masked from 'package:data.table':
    ## 
    ##     address

``` r
library(stringdist)

#functions folder
source("code/funs_warehouse.r")

# change global option about string import
options(stringsAsFactors = FALSE)

tic('all file')
```

## Parameters

here you can choose parameters that determine what the code does

``` r
# tell R the folder where you stored the input data
data.folder <- './data/processed/'

# tell R the folder and filename for the output data file
output.file <- './data/processed/robintrack_crsp_esg_merged_ready.csv'

#number of rows to import
rows = -1

# tell date range for ESG scores to import
first.year <- 2017
last.year <- 2019

# tell code which datastream asset4 scores are present in the data (check in ESG file)
scores <- c('TRESGS','TRESGCS','ENSCORE')
```

``` r
robin.path <- paste(data.folder, 'robintrack_cleaned.csv', sep='')

tic('Read Robintrack')
robin.data <- fread(file = robin.path , header = TRUE,  data.table = TRUE, colClasses = c("Date", "character", "double"), nrows = rows )
toc()
```

    ## Read Robintrack: 59.32 sec elapsed

``` r
# create surrogate key to identify observations
robin.data$id.robin <- as.integer( rownames(robin.data) )

setkey(robin.data, id.robin)

robin.data[, rob.year := year(date)]

numobs.robin <- dim(robin.data)[1]

first.date.robin <- min(robin.data$date)
last.date.robin <- max(robin.data$date)
```

``` r
varlist <- 'permno, permco, cusip as cusip8, hcusip, htick as crsp_ticker, htsymbol,
hcomnam as crsp_name, hexcd, hshrcd, hsiccd as crsp_sic , hnaics as crsp_naics '

query <- paste("select ",
               varlist, 
               " from crsp.dsfhdr where enddat >= '", first.date.robin,"' ", sep = "")

tic('CRSP query')
crsp.info <- wrds.table(query, numrows=rows)
toc()
```

    ## CRSP query: 4.19 sec elapsed

``` r
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
```

    ## Ticker does not uniquely identify information in the CRSP header data !! 
    ##  
    ##             Be mindful that some observations from Robintrack might be duplicated in the merge!

``` r
numobs.crsp <- dim(crsp.info)[1]
```

``` r
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
```

    ## CCM query: 2.04 sec elapsed

``` r
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
```

    ## We have gvkeys for  6.25 percent of the observations from CRSP

``` r
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
```

    ## PROBLEM: some robinhood observations were duplicated during the merge with CRSP information.

    ## 0.46 percent of Robintrack observations are duplicated when joining CRSP.The distribution of share codes among the duplicated is 
    ## 
    ##        11        12        31        71        73 
    ##  9.097820  3.628374  2.397286  3.313186 81.563334

``` r
numobs.merged <- dim(merged)[1]
merged.percent <- 100 * numobs.merged / numobs.robin 

cat('We merged', format(merged.percent, digits = 3), 'percent of the Robintrack observations to CRSP' )
```

    ## We merged 93 percent of the Robintrack observations to CRSP

``` r
esg.path <- paste(data.folder, 'esg_scores_cleaned.csv', sep='')

tic('read esg data')
esg.data <- fread(file = esg.path , header = TRUE,  data.table = TRUE, nrows = rows )
toc()
```

    ## read esg data: 0.82 sec elapsed

``` r
# remove all companies that do not have a CUSIP, because this means that they are not traded in the US, 
# which makes it very likely that they are not present in the Robinhood sample, possibly causing mismatches later on.
esg.data <- esg.data[!is.na(CUSIP)]

numobs.esg <- dim(esg.data)[1]
numfirms.esg <- length(unique(esg.data$DSCD))

cat('The ESG sample from Datastream has ', numobs.esg, ' observations for ', numfirms.esg, 'firms with non-missing CUSIP codes.')
```

    ## The ESG sample from Datastream has  75145  observations for  3955 firms with non-missing CUSIP codes.

``` r
cat('The geographic breakdown of observations is the following:')
```

    ## The geographic breakdown of observations is the following:

``` r
table(esg.data$GEOGN)
```

    ## 
    ##         BRAZIL         CANADA          CHINA CZECH REPUBLIC        DENMARK 
    ##             19           8531            532             19             19 
    ##         FRANCE        GERMANY      HONG KONG          INDIA        IRELAND 
    ##             19             57             19             19             19 
    ##         ISRAEL          ITALY    NETHERLANDS         NORWAY      SINGAPORE 
    ##             57             19             38             19             19 
    ## UNITED KINGDOM  UNITED STATES 
    ##             76          65664

``` r
setnames(esg.data, 'V1', 'id.esg')

esg.data <- esg.data[between(year, first.year, last.year)]

# we have a 9-digit cusip, let us cut it, remember 6 is the issuer and 8 is the issue.
setnames(esg.data, 'CUSIP', 'cusip')
esg.data[, cusip6 := substr(cusip, 1, 6)]
esg.data[, cusip8 := substr(cusip, 1, 8)]


# create lagged variable so that the observations from Robin are linked to previous year ESG score
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

#most observations are only until 2018 or max 19
```

``` r
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
```

    ## We merged  38.7 percent of the Robintrack observations to the ESG file using 8-digit CUSIP

``` r
# now merge on 6-digit CUSIP (issuer level)
merged6 <- merge( x = merged, y = esg.data, all = FALSE, by.x =c( 'cusip6', 'rob.year') , by.y = c( 'cusip6', 'yearlink' ))


# take those that still remain unmerged
merged <- merged[ !(id.robin %in% merged6$id.robin) ]


# count how many merged using 6-digit CUSIP
numobs.merged.esg6 <- dim(merged6)[1]
merged.percent.esg6 <- 100 *  numobs.merged.esg6 / numobs.merged

cat('We merged ', format(merged.percent.esg6, digits = 2), 'percent of the Robintrack observations to the ESG file using 6-digit CUSIP')
```

    ## We merged  0.48 percent of the Robintrack observations to the ESG file using 6-digit CUSIP

``` r
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
```

    ## We merged  4.2 percent of the Robintrack observations to the ESG file using ticker symbols

``` r
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
```

    ## PROBLEM: some robinhood observations were duplicated during the merge with Datastream information.

    ## 0.071 percent of Robintrack observations are duplicated when joining CRSP.The distribution of share codes among the duplicated is 
    ## 
    ##        11        18        71 
    ## 73.302350 21.120578  5.577072

``` r
# count how many merged overall
numobs.merged.overall <- dim(merged)[1]
merged.percent.overall <- 100 *  numobs.merged.overall / numobs.merged

cat('We merged ', format(merged.percent.overall, digits = 3), 'percent of the Robintrack observations to the ESG file.')
```

    ## We merged  43.3 percent of the Robintrack observations to the ESG file.

``` r
# count how many firms we have
num.firms <- length(unique(merged$DSCD))
cat('We have ', num.firms, 'firms in the merged sample.')
```

    ## We have  3018 firms in the merged sample.

``` r
# remove all the redundant variables that got created during the merge rounds

merged[ is.na(cusip8) , cusip8 := fifelse( is.na(cusip8.x), cusip8.y, cusip8.x ) ]
merged[ is.na(cusip6) , cusip6 := fifelse( is.na(cusip6.x), cusip6.y, cusip6.x ) ]

merged[, c('cusip6.x', 'cusip6.y','cusip8.x', 'cusip8.y' ) := NULL ]

merged %>% setcolorder(neworder = c( 'date', 'ticker', 'cusip8', 'crsp_name', 'NAME') )

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

# discard the worst 5% of matches based on the name distance
merged <- merged[namedist <= quantile( merged$namedist , 0.95)][, namedist := NULL]

# take care of the fact that many scores for eoy 2019 are still missing in Datastream

# scores for 2019 are very often missing
# if score is missing for Robin observations of 2020 (that have scores from 2019), use the previous year one

####
# take data of 2020
data2020 <- merged[rob.year ==2020]

# take scores for previous year
scores2018 <- merged[rob.year == 2019, .SD[.N ] , by = crsp_name, .SDcols = scores]

# merge them to data
data2020 <- merge(data2020, scores2018, by = 'crsp_name' , all.x = TRUE)


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
```

``` r
fwrite(merged, file = output.file , sep= ",", row.names = FALSE, quote = FALSE, col.names = TRUE , append = FALSE)


toc()
```

    ## all file: 129.49 sec elapsed

``` r
# names, share codes (stocks VS ETF, users holding)
# you can look at most held and list held firms
# you can look at distribution of holders for different stocks pre and post covid.
```
