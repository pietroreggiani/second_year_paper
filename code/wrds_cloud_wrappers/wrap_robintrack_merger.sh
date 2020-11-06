#!/bin/bash
#$ -cwd 
#$ -pe onenode 2
#$ -l m_mem_free=24G
echo "Starting job at `date`"
R CMD BATCH --no-save --no-restore code/robintrack_merger.R output/log/robintrack_merger.Output
echo "Ending job at `date`" 

