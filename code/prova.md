First Data Analysis Attempt
================
Pietro Reggiani
July 2020

Here’s some prose in a very special comment. Let’s summarize the
built-in dataset `VADeaths`.

``` r
## here is a regular code comment, that will remain as such
```

Here’s some more prose. I can use usual markdown syntax to make things
**bold** or *italics*. Let’s use an example from the `dotchart()` help
to make a Cleveland dot plot from the `VADeaths` data. You can name
chunks of code as in a markdown file, using \#+. This is especially
useful when naming figures.

``` r
# First load all libraries we need
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
################################################################################
```

This code spits out all the available libraries in WRDS.

``` r
res <- dbSendQuery(wrds, "select distinct table_schema
                   from information_schema.tables where table_type ='VIEW'
                   order by table_schema")
libraries <- dbFetch(res, n=-1)
dbClearResult(res)
```

Now you can ask for information about one specific library. In our case
it’s the Thomson Reuters one.

``` r
res <- dbSendQuery(wrds, "select distinct table_name
                   from information_schema.columns
                   where table_schema='tfn'
                   order by table_name")
tables <- dbFetch(res, n=-1)
dbClearResult(res)
View(tables)
```

This code instead spits out all the variables relative to a specific
table. In this case let us print the variables that are in the main S34
table.

``` r
res <- dbSendQuery(wrds, "select column_name
                   from information_schema.columns
                   where table_schema='tfn'
                   and table_name='s34'
                   order by column_name")
vars <- dbFetch(res, n=-1)
dbClearResult(res)
View(vars)
```

Ok so now that we have looked at the variables, we are ready to query
the actual data.

``` r
res <- dbSendQuery(wrds, "select * from tfn.s34 where fdate >='2020-03-31'")
data <- as.data.table(dbFetch(res, n=100))  #use data.table package
dbClearResult(res)
View(data)
```
