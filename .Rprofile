# open the connection to the WRDS database
# this happens through the Postgres package
# for more info go to
# https://wrds-www.wharton.upenn.edu/pages/support/programming-wrds/programming-r/r-from-your-computer/

library(RPostgres)
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='preggian')

#link to file that contains user defined functions
source("code/funs_warehouse.R")


