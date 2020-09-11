#' ---
#' title: "ESG Scores Cleaning"
#' author: "Pietro Reggiani"
#' date: "September 2020"
#' 
#' output: 
#'        github_document
#' ---
#' 
#' This file cleans and merges the ESG scores downloaded from Datastream, and saves them together in a bigger panel data table.
#'
#' **INPUTS**:
#' 
#' * Excel file `data/raw/datastream/datastream_esg_scores_request_table.xlsm`
#' * Excel file `datastream_esg_companies_list.xlsm`
#' 
#' **OUTPUTS**:
#' 
#' * table with panel of ESG scores for all datastream LAST4ESG firms over time is saved in the .csv file `data/processed/esg_scores_cleaned.csv`.
#' 


#+ Setup ###########

# clear environment

rm(list=ls()) 

# Load Packages

library(data.table)
setDTthreads(0)  #tells to use all available cores
library(readxl)
library(tictoc)


#functions folder
source("code/funs_warehouse.r")

# change global option about string import
options(stringsAsFactors = FALSE)

#' ## ESG tables transpose, clean and merge
#' Here we take the tables from the score excel file, clean them one by one and merge them in a single panel.


#+ ESG tables ####

importfile <- "data/raw/datastream/datastream_esg_scores_request_table.xlsm"
filesheets <- excel_sheets(importfile)


#you need to run this for every table in the sheet starting from the third

score.tables.list <- list()

# so here you'll have a loop over each data sheet in the Excel file

tic('loop')

for (sheetno in 3:length(filesheets)) {

    # Import sheet
    scores <- data.table(read_excel(importfile, sheet = sheetno, na = "NA"))
    scores <- scores[-1] #remove first line that has useless labels
    setnames(scores, 'Date', 'DSCD')
    
    numrows <- dim(scores)[1]
    numcols <- dim(scores)[2]
    
    #create empty data.table that will be filled with the scores stored in the current sheet you are looping on
    this.score.panel <- data.table()
    # loop over rows, transpose each and copy the DSCD on every new row.
    # you will have now 3 columns
    for (row in 1:numrows){
        code <- scores$DSCD[row]
        temp <- transpose(scores[row], keep.names = 'year')
        setnames(temp,'V1',filesheets[sheetno])
        temp <- temp[-1]
        temp$DSCD <- code
        
        #append it to the storage data table created before
        this.score.panel <- rbind(this.score.panel, temp)
    }
    remove(temp)
    
    score.tables.list[[sheetno-2]] <- this.score.panel

} 

toc()

remove(this.score.panel, scores, code, sheetno)


#' Now we have to merge the different tables together to have a panel of ESG scores. 
#' This can be done by merging the tables using the DSCD and year as merging variables.

#put first score in the table
scores.panel <- score.tables.list[[1]]
# for all the other scores, merge on code and year
for (table in 2:length(score.tables.list)) {
    scores.panel <- merge(scores.panel, score.tables.list[[table]]   , by.x = c("DSCD","year"), by.y= c("DSCD","year"), all=FALSE )
}

remove(score.tables.list)


#' ## Merge in the company identifiers
#' The company identifiers come from the other Excel table mentioned in the inputs.

companies <- "data/raw/datastream/datastream_esg_companies_list.xlsm"

companies <- data.table(read_excel(companies, sheet = 'companies_list', skip = 3, na = "NA"))

#rename some of the variables
setnames(companies , c( 'TR3N', 'WC06004' , 'T1C', 'WC05601','WC06010'  ), c('TRindustry', 'CUSIP' , 'TRticker', 'WCticker','WCindcode')  )
setnames(companies, paste('WC070',21:28, sep = ''), paste('SIC', 1:8, sep = ''))



# Merge in panel of ESG scores
scores.panel <- merge(scores.panel, companies, by.x = "DSCD", by.y= "DSCD", all.x= TRUE , all.y = FALSE)[order(DSCD, year)]

remove(companies)


#' Export file to csv format

write.csv(scores.panel, 'data/processed/esg_scores_cleaned.csv')



