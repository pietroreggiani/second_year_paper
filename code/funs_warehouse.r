#' ---
#' title: "Functions Warehouse"
#' author: "Pietro Reggiani"
#' date: "July 2020"
#' output: github_document
#' ---
#'
#' This file stores some small user defined functions for the project. It should be called at the beginning
#' of all scripts using the `source(funs_warehouse.r)` function.


#' Unique ID determination in data.frame
#' 
#' @description
#' * INPUT: a data frame `x` and a character vector `varnames` of variable names.
#' * OUTPUT: a logical value representing whether the two variables determine a unique identifier for the data
#' * OPTIONS: if `verbose` is selected, the function returns a message about whether the variables are a unique identifier or not.
unique_id <- function(x, varnames, verbose = FALSE) {
    
    test <- x[,..varnames]
    logical <- identical(test , unique(test))
    
    if (verbose == TRUE){
        ifelse(logical == TRUE, print('The variables constitute a unique identifier'), print('The variables do NOT constitute a unique identifier !'))
    }
    return(logical)
}


#' Take variables from table in WRDS
#'
#' @param query string including the SQL query syntax for WRDS (see their webpage)
#' @param numrows number of observations you want to get, default is all the available ones.
#' @param data.table set to FALSE if you want a normal data.frame.
#' 
#' @return a data.table or .frame containing the WRDS data requested
#' 
#' @examples data <- wrds.table( "select mgrname from tfn.s34 where fdate between '2013-01-02' and '2015-03-02' ", numrows=100)
#' 
wrds.table <- function(query, numrows = -1, data.table = TRUE) {
    #check if packages are installed, otherwise install them
    if (!require(RPostgres)){
        install.packages("RPostgres")
    }
    if (!require(data.table)){
        install.packages("data.table")
    }
    require(RPostgres, data.table) #load packages
    
    #open connection to wrds
    wrds <- dbConnect(Postgres(),
                      host='wrds-pgdata.wharton.upenn.edu',
                      port=9737,
                      dbname='wrds',
                      sslmode='require',
                      user='preggian')
    
    # query data
    res <- dbSendQuery(wrds, query)
    
    # save as data.table or as normal data.frame
    if (data.table){
        data <- setDT(dbFetch(res, n= numrows ))
    } else {
        data <- dbFetch(res, n= numrows )
    }
    
    dbClearResult(res)  #not sure I need this
    
    return(data)
    
}









