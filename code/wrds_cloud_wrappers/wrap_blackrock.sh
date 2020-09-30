#!/bin/bash
#$ -cwd 
#$ -pe onenode 2
echo "Starting job at `date`"
R CMD BATCH --no-save --no-restore code/blackrock.R output/log/blackrock.Output
echo "Ending job at `date`" 

