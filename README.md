# Second Year Paper Project
This repository stores the code for my second year paper.

## Code files ReadMe

This section explains briefly what each script in the folder *code* does.

1. `who_holds_sin.R`
2. `esg_scores_cleaner.R`
3. `plots.Rmd`
4. `manager_types_cleaner.R`: takes the original managers file from Ralph and Moto, cleans it and expands it to the end of 2019, then writes it to a csv in the processed data folder. No need to run this file again, you can directly use the cleaned file in `data/processed/ky_managers_cleaned.csv`.

The subfolder wrds_cloud_wrappers contains wrappers that launch the R codes on the cluster.