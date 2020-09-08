#!/bin/bash
#$ -cwd 
echo "Starting job at `date`"
R CMD BATCH --no-save --no-restore code/who_holds_sin.R output/log/who_holds_sin.Output
echo "Ending job at `date`" 