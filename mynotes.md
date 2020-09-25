---
title: "Second Year paper notes"
author: Pietro
date: 2020
output: 
  html_document 
---

## Intro
This file will include notes for me to progress.

## Ideas
* Garel and the other guy in the ESG covid paper use a measure of investor horizon that they compute using the 13F data. 
I could use this measure too and associate it to the ESG holdings. 
Maybe the more long term guys have shifted more away from polluting firms.

## To-Do Plan

1. Find ESG rankings from Bloomberg and Datastream;
2. Figure out how to link CRSP and Compustat information and then link to S34 too (unless can use CCM merged);
3. Decide which info from Compustat you want to keep cause there are too many variables;
4. Merge together all the datasets;
5. Descriptive statistics on composition of portfolios in terms of green-ness and other characteristics;
6. Highlight change over time of green/sin holdings, by score and by sector;
7. Look at return differential between green and non-green investors, is there a transfer? 
I guess this will need to be done relative to a factor model for instance, to adjust for risk?


## WRDS cloud operating instructions

The wrapper codes for the WRDS code are in the folder `code/wrappers`. You should upload the .sh wrapper file in WRDS cloud folder `/second_year_paper` and run using the command `qsub wrapper_to_be_run.sh`. The wrapper looks for the .R code to run according to the path chosen in the .sh file. Remember to store the .r code on the server in the folder `second_year_paper/code`.
The folder where the .Output file is saved can be chosen in the .sh file, and in the .R code you run you can determine the destination of the output. <br>
Remember to keep the file structure on the server and locally identical, so that the code can run equally well on 
Before running the code you need to change the current working directory to `second_year_paper` using the command `cd`. 

## Notes

### Call Rob debrief August 24th
Look at cross-sectional valuation regressions in his paper with Ralph and Moto. 
No problem with getting directly the linked Compustat-CRSP file, but what he does is get them separately and use the link file.
In order to do the rolling window regressions you need to estimate the coefficients first, then the point estimate will be the average of these.
The Newey-West estimator for the standard error will be done on these observations, allowing for 35 lags of autocorrelation (not clear how).
He was not very helpful about how to go forward, no suggestions.



