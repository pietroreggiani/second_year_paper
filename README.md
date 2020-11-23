# Second Year Paper Project
This repository stores the code for my second year paper.

All the relevant R code is stored in the `code` folder, together with R notebooks for the outputs of the analysis.
The subfolder `code/wrds_cloud_wrappers` contains wrappers that launch the R codes on the cluster.

## Code files ReadMe

This section explains briefly what each script in the folder *code* does.


1. `who_holds_sin.R` queries from WRDS the S34 plus CRSP data and spits out a table with the fraction of sin and fossil stocks by type of investor over time.
2. `esg_scores_cleaner.R` takes the Excel files where we got the Datastream data on the ASSET4 companies (2 excel files in raw data folder) and merges them together into a panel.
3. `plots.Rmd` makes plots out of the S34 data output.
4. `manager_types_cleaner.R`: takes the original managers file from Ralph and Moto, cleans it and expands it to the end of 2019, then writes it to a csv in the processed data folder. No need to run this file again, you can directly use the cleaned file in `data/processed/ky_managers_cleaned.csv`.
5. `robintrack_cleaner.r` takes all the raw files from Robintrack and merges them in a panel. No need to run this again. It spits out the file `data/processed/robintrack_cleaned.csv`, that is the input to the next file below.
6. `robintrack_merger.R` takes the file mentioned above, plus compustat and datastream data to compile a single huge panel of data. It saves the *.csv* output in the processed data folder. Need a lot of RAM to run this.
7. `robin_compustat_esg_prepare.r` takes the output file from the merger code above and prepares the variables for the actual analysis. You should use this file to thin out the variables you do not need and cut any unnecessary observations.



## Instructions for reproduction (Robinhood)
This section explains the order in which different codes need to be run in order to replicate the project. The raw data should be in the data folder, for details about each single data file look into the scripts, which describe the inputs needed.

1. Start by cleaning the Robintrack data files (initially there is one .csv file for every asset in the sample) by running the code `robintrack_cleaner.r`. That will put all the robintrack data files together into the file `data/processed/robintrack_cleaned.csv`.
2. Then clean the Datastream ESG data running the code `esg_scores_cleaner.R`. The raw data should be stored in an Excel file in the relevant folder.
3. Run `robintrack_merger.R`, that takes the outputs from the two files above and queries data from Compustat to create a single large data panel called `./data/processed/robintrack_compustat_esg_merged_ready.csv`.
4. Run  `robin_compustat_esg_prepare.R` that takes the csv just mentioned plus Ken French data on factors, then cleans it prepares the variables and spits out the final dataset called `./data/processed/final_dataset.csv` . 
5. Now you have the data ready, so you can proceed to the analysis using the notebook `analysis.Rmd`. That notebook produces all tables and plots that are included in the presentation and in the paper.














