# PROVA

print('This is an attempt to run on the WRDS cloud')

library(data.table)

data <- data.table( diag(10)   )

saveRDS(data, file = 'second_year_paper/output/prova.rds')

