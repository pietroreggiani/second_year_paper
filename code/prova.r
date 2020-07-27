#' ---
#' title: "Data Download and Merge"
#' author: "Pietro Reggiani"
#' date: "July 2020"
#' output: github_document
#' ---
#'
#' This file connects to WRDS and downloads the following data sets:
#' 
#' * Thomson Reuters S34 holdings data
#' * Compustat quarterly file
#' * CRSP monthly stock file
#' * CRSP-Compustat link table
#' 
#' Then, the code merges these data sources to add stock level information to the holdings.
#' The **output** of the code is 

#'
#' ## Load packages and connect to WRDS.
#' 
#+ Setup

library(data.table)
setDTthreads(threads = 0)  #tells to use all available cores

# This part below is already in the Rprofile but for some reason it does not work with Markdown
library(RPostgres)
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                      port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='preggian')

#' ## Load Data from WRDS
#' Here we dowload the list of all available libraries from WRDS

res <- dbSendQuery(wrds, "select distinct table_schema
                   from information_schema.tables
                   where table_type ='VIEW'
                   or table_type ='FOREIGN TABLE'
                   order by table_schema")
libraries <- dbFetch(res, n=-1)
dbClearResult(res)



#' ### Thomson Reuters S34
#' 
#' Here we first input the holdings data from Thomson Reuters

res <- dbSendQuery(wrds, "select * from tfn.s34 where fdate between '2019-12-31' and '2020-03-31'")
s34 <- as.data.table(dbFetch(res, n=1000))  #use data.table package
dbClearResult(res)

s34 <- s34[order(mgrname,cusip, -fdate)] #order by manager, stock and date  

#' ### Compustat
#' This part considers the Compustat file.
#' 

res <- dbSendQuery(wrds, "select distinct table_name
                   from information_schema.columns
                   where table_schema='comp'
                   order by table_name")
comp.libraries <- dbFetch(res, n=-1)  
dbClearResult(res)

#' comp.fundq is the Fundamentals Quarterly dataset that we are interested in.
#' Now let's print the variable names available
#' These are all the 647 variables available, 
#' but in principle you may want to extract only a portion.

res <- dbSendQuery(wrds, "select column_name
                   from information_schema.columns
                   where table_schema='comp'
                   and table_name='fundq'
                   order by column_name")
comp.vars <- dbFetch(res, n=-1)  
dbClearResult(res)


#' Now we are ready to query the Compustat data. Remember that each firm is identified by the gvkey
#' identifier.
res <- dbSendQuery(wrds, "select * from comp.fundq where fyearq = 2020")
comp.data <- as.data.table(dbFetch(res, n=1000))  #use data.table package
dbClearResult(res)


#' ### CRSP Stock Data
#' 
res <- dbSendQuery(wrds, "select distinct table_name
                   from information_schema.columns
                   where table_schema='crspq'
                   order by table_name")
crsp.libraries <- dbFetch(res, n=-1)  
dbClearResult(res)

#' We will use stock level data from the monthly stock file `msf`.

res <- dbSendQuery(wrds, "select column_name
                   from information_schema.columns
                   where table_schema='crsp'
                   and table_name='msf'
                   order by column_name")
crsp.vars <- dbFetch(res, n=-1)  
dbClearResult(res)


#' Now we are ready to query the data.

res <- dbSendQuery(wrds, "select * from crsp.msf where date >= '2019-12-31'")
crsp.data <- as.data.table(dbFetch(res, n=1000))  #use data.table package
dbClearResult(res)

 
#' ### CRSP-Compustat link file 
#' We want to match the different datasets, let's get also the CRSP-COMPUSTAT link.

res <- dbSendQuery(wrds, "select column_name
                   from information_schema.columns
                   where table_schema='crspq'
                   and table_name='ccmxpf_lnkhist'
                   order by column_name")
ccm.vars <- dbFetch(res, n=-1)  
dbClearResult(res)


res <- dbSendQuery(wrds, "select * from crsp.ccmxpf_lnkhist")
ccm <- as.data.table(dbFetch(res, n=10000))  #use data.table package
dbClearResult(res)

ccm <- ccm[order(gvkey, linkdt)]



