#' ---
#' title: "CRSP Returns analysis"
#' author: "Pietro Reggiani"
#' date: "August 2020"
#' output: github_document
#' ---
#'
#' 
#+ Setup

library(data.table)
setDTthreads(threads = 0)  #tells to use all available cores
library(lubridate)

# This part below is already in the Rprofile but for some reason it does not work with Markdown
library(RPostgres)
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='preggian')

#' ## Load Data from WRDS
#' Here we download the list of all available libraries from WRDS

res <- dbSendQuery(wrds, "select distinct table_schema
                   from information_schema.tables
                   where table_type ='VIEW'
                   or table_type ='FOREIGN TABLE'
                   order by table_schema")
libraries <- dbFetch(res, n=-1)
dbClearResult(res)



#' ### Compustat
#' This part considers the Compustat file.
#' 

res <- dbSendQuery(wrds, "select distinct table_name
                   from information_schema.columns
                   where table_schema='crsp'
                   order by table_name")
crsp.libraries <- dbFetch(res, n=-1)  
dbClearResult(res)

#' comp.fundq is the Fundamentals Quarterly dataset that we are interested in.
#' Now let's print the variable names available
#' These are all the 647 variables available, 
#' but in principle you may want to extract only a portion.

res <- dbSendQuery(wrds, "select column_name
                   from information_schema.columns
                   where table_schema='crsp'
                   and table_name='stocknames'
                   order by column_name")
crsp.varsm <- dbFetch(res, n=-1)  
dbClearResult(res)


#' We will use stock level data from the monthly stock file `msf`. This code gives you the available vars.
res <- dbSendQuery(wrds, "select column_name
                   from information_schema.columns
                   where table_schema='crsp'
                   and table_name='msf'
                   order by column_name")
crsp.vars <- dbFetch(res, n=-1)  
dbClearResult(res)


#' Now we are ready to query the data. Here we get the price information from the CRSP month file.
#' We exclude the financial services sector following Hong and Kacperkzyk.
res <- dbSendQuery(wrds, "select date,permco,permno,cusip,hsiccd,hexcd,shrout, ret, retx, prc from crsp.msf where (hsiccd < 6000 OR hsiccd >6999) AND (date >= '01-01-1980') ")
crsp.data <- as.data.table(dbFetch(res, n=1000))  #use data.table package
dbClearResult(res)

#' This piece queries the CRSP file containing names and share codes of stocks.  We will have to merge this into the crsp data in order to exclude the share codes
#' we don't want, following Hong and Kacperkzyk
res <- dbSendQuery(wrds, "select comnam, permno, permco, cusip,ncusip, ticker, shrcd, shrcls, namedt, nameenddt  from crsp.stocknames where shrcd = 10 OR shrcd=11 order by permco")
crsp.stocknames <- as.data.table(dbFetch(res, n=1000))  #use data.table package
dbClearResult(res)


#' ### CRSP-Compustat link file 
#' We want to match the different datasets, let's get also the CRSP-COMPUSTAT link.

res <- dbSendQuery(wrds, "select column_name
                   from information_schema.columns
                   where table_schema='crsp'
                   and table_name='ccm_qvards'
                   order by column_name")
ccm.vars <- dbFetch(res, n=-1)  
dbClearResult(res)


res <- dbSendQuery(wrds, "select * from crsp.ccmxpf_lnkhist")
ccm <- as.data.table(dbFetch(res, n=10000))  #use data.table package
dbClearResult(res)

ccm <- ccm[order(gvkey, linkdt)]

#' The next step is to figure out how to merge the data sources and get a ready to use file

#' upload the CRSP file directly so it's easier
crsp.data      <- fread('data/raw/crsp_monthly.csv')[order(PERMNO,date)]
crsp.data$RET  <- as.numeric(crsp.data$RET)
crsp.data$date <- as.Date(crsp.data$date,"%m/%d/%Y")

#' Load Factor and industries data from French
suppressWarnings(ff.data    <- fread('data/raw/FF_5factors.csv',     header = TRUE, skip = 3 ), classes = "warning")
suppressWarnings(momentum   <- fread('data/raw/FF_momentum.csv',     header = TRUE, skip = 3 ), classes = "warning")
suppressWarnings(ff.sectors <- fread('data/raw/FF_48industries.csv', header = TRUE, skip = 11), classes = "warning")
#fix date from French to date format
setnames(ff.data,"V1", "date")
ff.data[, year := substr(ff.data$date, 1,4)]
ff.data[, month := substr(ff.data$date, 5,6)]
date <- dmy(    paste("10", ff.data$month, ff.data$year, sep= "/")  )
days = unname(days_in_month(date))
date <- dmy( paste(days, month(date), year(date), sep="/") )

ff.data$date <- date
remove(days, date)


# add momentum to datatable with the factor returns
ff.data[, Mom := momentum[V1 >= 196307, Mom]]
remove(momentum)
# remove older part of sample from industries
ff.sectors <- ff.sectors[V1>= 196307]
# create comparable firms returns (following Hong and Kacperkzyk)
ff.data[, comparables := rowMeans(ff.sectors[,c(3,4,8,44)])]
# put together returns of Coal and Oil Categories just by a simple mean
ff.data[, fossil := rowMeans(ff.sectors[,c("Coal","Oil")])]
# Now I need to aggregate the returns to all the sin stocks in order to get a monthly sin return


#' ## Create Dummies to categorize the stocks into sin and non-sin
#' create dummy for sin stocks
sin.sic <- c(2100:2199, 2080:2085)
sin.naics <- c(7132,71312,713210,71329,713290,72112,721120)
crsp.data[,sin := ifelse( SICCD %in% sin.sic | NAICS %in% sin.naics ,1,0)]

#' create dummy for fossil fuel industry. Following the Fama-French category 30, we have that:
#' * 1200-1299 bituminous coal and lignite mining
#' * 1300-1399 various oil and gas categories
#' * 2900-2912 and 2990-2999 petroleum refining and miscellaneous
    
fossil.sic <- c(1200:1299, 1300:1399, 2900:2912, 2990:2999)
crsp.data[, fossil := ifelse(SICCD %in% fossil.sic ,1,0)]


#' Extract the sin and fossil stocks. Print the names to get an idea of what firms we have.
sins <- crsp.data[sin==1]
fossils <- crsp.data[fossil ==1]
names.sin    <- sort(unique(sins$COMNAM), decreasing=FALSE)
names.fossil <- sort(unique(fossils$COMNAM), decreasing=FALSE)

#' create equal weighted monthly returns for sin and fossil stocks, and then add them to the factor return table.
sin.returns    <- sins[ , .(sinret = mean(RET, na.rm = TRUE)) ,  by = date][order(date)]
fossil.returns <- fossils[ , .(fossilret=mean(RET, na.rm = TRUE)) ,  by = date][order(date)]

setkey(ff.data, "date")
setkey(sin.returns,"date")

ff.data <- sin.returns[ff.data]


#' ## Compare Returns
#' We have categorized stocks within groups of sin or oil. Now we want to compare the returns across the different groups.



