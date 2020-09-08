#' ---
#' title: "Functions Warehouse"
#' author: "Pietro Reggiani"
#' date: "July 2020"
#' output: github_document
#' ---
#'
#' This file stores my user defined functions for the project. It should be called at the beginning
#' of all scripts using the `source(funs_warehouse.r)` function.


#' ## Unique ID determination in data.frame
#' INPUT: a data frame `x` and a character vector `varnames` of variable names.
#' OUTPUT: a logical value representing whether the two variables determine a unique identifier for the data
#' OPTIONS: if `verbose` is selected, the function returns a message about whether the variables are a unique identifier or not.

unique_id <- function(x, varnames, verbose = FALSE) {
    
    test <- x[,..varnames]
    logical <- identical(test , unique(test))
    
    if (verbose == TRUE){
        ifelse(logical == TRUE, print('The variables constitute a unique identifier'), print('The variables do NOT constitute a unique identifier !'))
    }
    return(logical)
}