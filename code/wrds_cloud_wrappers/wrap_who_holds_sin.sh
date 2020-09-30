#!/bin/bash
#$ -cwd 
#$ -pe onenode 2
#$ -l m_mem_free=24G
echo "Starting job at `date`"
R CMD BATCH --no-save --no-restore code/who_holds_sin.R output/log/who_holds_sin.Output
echo "Ending job at `date`" 

