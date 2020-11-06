#' ---
#' title: "Robintrack Merged Data Prepare"
#' author: "Pietro Reggiani"
#' date: "October 2020"
#' 
#' output: 
#'        github_document
#' ---
#' 
#' This file takes the merged Robintrack-Compustat-Datastream data file `.../robintrack_compustat_esg_merged_ready.csv`, 
#' that comes from the code *robintrack_merger.r*, then selects a sub-sample of variables, firms and creates new variables that will be used in the analysis.
#'
#' **INPUTS**:
#' 
#' * .csv file specified in parameters section, containing a merged panel of firms with Robinhood users, Compustat info and Datastream info.
#' * .csv file with Fama-French daily factors, downloaded directly from French's website. It is typically 
#' saved in the `data/raw/` folder.
#' 
#' 
#' **OUTPUTS**:
#' 
#' * `.csv` file containing the merged and cleaned panel of daily Robinhood users, matched with ESG scores and CRSP information.
#' The file also has gvkeys in case you want to join also information from Compustat. You can specify the location and name of the 
#' output file in the parameters section.


#+ R Setup ###########

# clear environment

rm(list=ls()) 
gc()

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

library(RcppRoll)

#functions folder
source("code/funs_warehouse.r")

# change global option about string import
options(stringsAsFactors = FALSE)


tic('all file')

#+ pick parameters ####
data.filepath <- './data/processed/robintrack_compustat_esg_merged_ready.csv'

output.file <- './data/processed/final_dataset.csv'

rows <- -1

# winsorizing level
winsor.level <- 0.05

tic('All file')

#+ load data ####

data <- fread(file = data.filepath , header = TRUE,  data.table = TRUE,
              nrows = rows, colClasses = list(Date = c("date", "date_funda")) )

data.variables <- sort(colnames(data))
data[, date := as.Date(date)]






#+ variable Selection ####

# select the variables from the sample that you'd like to keep, from all data sources
# in order to check which variables you can choose from, you can look at the robintrack_merged file

vars.robin <- c('date', 'ticker', 'users_holding', 'rob.year', 'id.robin')

vars.secd <- c( 'gvkey', 'cusip' , 'conm',  'divd', 'divsp','cshoc','cshtrd',
                'prccd','trfd' , 'exchg', 'tpci','fic',   'ajexdi',
                'beta', 'retd', 'exc.ret', 'beta.year')

vars.funda <- c( 'date_funda', 'name_funda', 'assets', 'ceql', 'seq', 'txditc',
                 'pstkrv' , 'pstkl', 'ib', 'ch', 'che', 'csho', 'dvt','dltt', 'dlc', 'gsubind', 'sic', 'naics', 'stko')

vars.ds <- c('id.esg', 'DSCD', 'TRESGS', 'TRESGCS', 'ENSCORE', 'TRindustry', 'A4STATUS' )    

# put them together, adding other variables that are created in the merger code

vars <- sort( c( vars.robin, vars.secd, vars.funda, vars.ds, c('sin', 'fossil') ) )


#+ extract only the selected variables ####
data <- data[order(ticker, date), ..vars] %>% select(sort(names(.)))

# rename them
varstorename <- vars %in%   c('ajexdi'    ,'ceql'               , 'ch'  ,'che'    ,'csho'          ,'cshoc'              ,'cshtrd'     ,'dlc'         ,'dltt'   ,'dvt'    ,'ib'        , 'prccd'        , 'pstkl'        , 'pstkrv'       ,'seq'        , 'stko', 'tpci', 'txditc')                                   
names        <-             c('adj.factor','common.equity.liqv' , 'cash','cash.st','com.shares.out','com.shares.out.curr','trading.vol','debt.current','debt.lt','div.tot','net.income',   'price.close', 'pref.stock.lv', 'pref.stock.rv', 'equity.tot', 'stock.own', 'issue.type', 'def.tax')

newnames <- replace( vars , varstorename ,  names )

data %>% setnames( vars , newnames )


#+ create new variables ####

data[, log.assets := log(assets)]

# start from variables that are in Glossner et al. 
data[,  leverage :=  (1 > (debt.lt + debt.current)/assets ) * (debt.lt + debt.current)/assets  +  (1 <= (debt.lt + debt.current)/assets ) * 1  ]

data[, profitability := 100 * net.income/assets]


data[, cash.assets := 100 * cash.st /assets  ]


# define book value as in Fama-French '93
data[, bv := equity.tot + def.tax - fifelse(is.na(pref.stock.lv), pref.stock.lv, pref.stock.rv)]
# exclude negative book value observations
data[bv < 0 , bv :=  NA_integer_    ]


# define market value
data[, mkt.cap := com.shares.out * price.close  ]

data[, log.mkt.cap := log(mkt.cap)]


# define book to market, if book value is positive
data[, BM := fifelse( bv/mkt.cap > 0 , bv/mkt.cap , NA_integer_)    ]


# get total number of users each date and compute fraction of users
# NB it is impossible to get the total number of users that there are in the sample!
# use max number of users on any single stock at any point in time as a proxy for total number of users.


data[, tot.users := max(users_holding, na.rm = TRUE)  , by = date]
# correct the tot.users if there are too few users (there is an outlier on one day where there are only two firms)
data[ tot.users == min(data$tot.users) , tot.users := data[ date ==  .SD[1,date+1], tot.users ][1]  ]

data[, frac.users := 100 * users_holding / tot.users  ]

data[  , log.users := log( users_holding + 1 ) ]
data[ , log.frac :=  log.users - log(tot.users)  ]

# compute approximate total portfolio value assuming each position is always one share
data[, tot.val := sum(users_holding*price.close, na.rm = TRUE) , by = date  ]
# and approximate portfolio weight
data[, weight := 100*(users_holding+1)*price.close/tot.val ]
data[, log.weight := log(weight) ]

# percentage changes in users 
data[, users.diff := 100*(users_holding - shift(users_holding,1, fill = NA))/shift(users_holding,1, fill = NA) , by = ticker ]
data[, frac.diff := 100*(frac.users - shift(frac.users,1, fill = NA))/shift(frac.users,1, fill = NA) , by = ticker ]


# dummy for has ESG score (to control for selection)

data[, has.tresgs := as.factor( 0 + 1*(!is.na(TRESGS)) ) ]
data[, has.enscore := as.factor(0 + 1*(!is.na(ENSCORE)) ) ]


# fix esg for regressions so you don't drop observations
data[has.tresgs == 0 , TRESGS := 0]
data[has.enscore == 0 , ENSCORE := 0]



# momentum
# previous month average daily excess return
data[order(ticker,date)  ,  ret.mom := (RcppRoll::roll_prod(  1 + dplyr::lag(exc.ret)/100 , n=20 , fill=NA, align="right")^(1/20) -1 )*100,  by = ticker ]
data[order(ticker,date)  ,  ret.mom.6m := (RcppRoll::roll_prod(  1 + dplyr::lag(exc.ret)/100 , n=120 , fill=NA, align="right")^(1/120) -1 )*100,  by = ticker ]


# previous month trading volume
data[trading.vol != 0 , log.tv := log(trading.vol)]
data[order(ticker,date)  ,  vol.mom := log(  RcppRoll::roll_sum( dplyr::lag(trading.vol) , n=20 , fill=NA, align="right")/20  ) ,  by = ticker ]
data[order(ticker,date)  ,  vol.mom.6m := log(  RcppRoll::roll_sum( dplyr::lag(trading.vol) , n=120 , fill=NA, align="right")/120  ) ,  by = ticker ]


# industry codes
data[, gindustry := as.factor( substr(gsubind, 1, 6) )]
data[, gsector := as.factor( substr(as.character(gsubind), 1,2) )]

#+ Winsorization ####

# beta is the one for which it is most important cause there are one or two outliers that fuck everything up
data$beta <- Winsorize( data$beta,
                        minval = quantile(data$beta, na.rm = TRUE, probs = winsor.level/2) ,
                        maxval = quantile(data$beta, na.rm = TRUE, probs = 1-winsor.level/2) ,
                        na.rm = TRUE )

data$mkt.cap <- Winsorize( data$mkt.cap,   minval = quantile(data$mkt.cap, na.rm = TRUE, probs = winsor.level/2) ,
                           maxval = quantile(data$mkt.cap, na.rm = TRUE, probs = 1-winsor.level/2) ,
                           na.rm = TRUE )

data$bv <- Winsorize( data$bv,   minval = quantile(data$bv, na.rm = TRUE, probs = winsor.level/2) ,
                      maxval = quantile(data$bv, na.rm = TRUE, probs = 1-winsor.level/2) ,
                      na.rm = TRUE )

data$cash.assets <- Winsorize( data$cash.assets,   minval = quantile(data$cash.assets, na.rm = TRUE, probs = winsor.level/2) ,
                               maxval = quantile(data$cash.assets, na.rm = TRUE, probs = 1-winsor.level/2) ,
                               na.rm = TRUE )

data$profitability <- Winsorize( data$profitability,
                                 minval = quantile(data$profitability, na.rm = TRUE, probs = winsor.level/2) ,
                                 maxval = quantile(data$profitability, na.rm = TRUE, probs = 1-winsor.level/2) ,
                                 na.rm = TRUE )

#+ sample selection ####

# remove exchanges that are not NYSE AMEX or NASDAQ
data <- data[exchg %in% c(11,12,14)]

# remove financial firms according to GICS sector

data <- data[gsector != '40']

# keep only ordinary common shares and ADRs (they represent the vast majority of the data)
data <- data[issue.type %in% c('0', 'F')]


# sort
data <- data[order(ticker, date)]

#+ save data ready for analysis ####

fwrite(data,   file = output.file ,
       sep= ",", row.names = FALSE, quote = TRUE, col.names = TRUE , append = FALSE)

toc()
