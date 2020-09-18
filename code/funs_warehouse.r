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
#' @param x string name of WRDS table to query
#' @param dates the dates between which you want to search the data, should be a character vector of 2 elements. Formatting should be yyyy-mm-dd
#' @param datename string identifying the relevant date variable name if you want to search for a specific time sub-sample. In S34 for instance it is fdate.
#' @param numrows
#' @param variables
#' @param ordervars to specify an ordering of the table, should be a string or character vector
#' @param data.table optional argument to specify whether you'd like to have the data imported as data.table, default is yes
#' 
#' @return a data.table containing the WRDS data requested
#' @export
#'
#' @examples
wrds.table <- function(x, numrows = -1, dates = NULL, datename = "date", variables = NULL, ordervars = NULL, data.table = TRUE) {
    #check if package is installed, if not install it
    if (!require(RPostgres)){
        install.packages("RPostgres")
    }
    if (!require(data.table)){
        install.packages("data.table")
    }
    
    require(RPostgres, data.table)
    
    wrds <- dbConnect(Postgres(),
                      host='wrds-pgdata.wharton.upenn.edu',
                      port=9737,
                      dbname='wrds',
                      sslmode='require',
                      user='preggian')
    
    if (is.null(variables)){
        # you want all variables
        input <- paste("select * from ", x )
    } else {
        # if you pick only some variables
        input <- paste("select", paste(variables, collapse = " ")  ,"from", x)
    }
    
    if (!is.null(dates)){
        # if you have a start and end date
        input <- paste(input, ' where ', datename ,' between ' ,"'", dates[1],"'", ' and ',"'" , dates[2],"'", sep='' )
    }
    if (!is.null(ordervars)){
        # if you want to order by some variable
        input <- paste(input, 'order by',paste(ordervars, collapse = " ") )
    }
    # query data
    res <- dbSendQuery(wrds, input)
    
    # save as data.table or as normal data.frame
    if (data.table){
        data <- as.data.table(dbFetch(res, n= numrows ))
    } else {
        data <- dbFetch(res, n= numrows )
    }
    
    
    dbClearResult(res)
    
    return(data)
    
}









