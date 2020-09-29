# attempt to get Compustat-CRSP merged


library(RPostgres)
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='preggian')

library(RPostgres) 


res <- dbSendQuery(wrds, "select distinct table_name
                   from information_schema.columns
                   where table_schema='crsp'
                   order by table_name")
tables <- dbFetch(res, n=-1)
dbClearResult(res)


res <- dbSendQuery(wrds, "select column_name
                   from information_schema.columns
                   where table_schema='comp'
                   and table_name='funda'
                   order by column_name")
vars <- dbFetch(res, n=-1)
dbClearResult(res)


crsp.msf <- wrds.table("select * from crsp.msf", numrows=1000)

res <- dbSendQuery(wrds, "select * from crsp.ccmxpf_linktable")
crsp.ccmxpf_linktable <- dbFetch(res, n=1000)
dbClearResult(res)

res <- dbSendQuery(wrds, "select * from crsp.ccmxpf_lnkused")
crsp.ccmxpf_lnkused <- dbFetch(res, n=1000)
dbClearResult(res)


crsp.msenames <- wrds.table( "select * from crsp.msenames" , numrows=1000)


# this is the example from the WRDS R page
res <- dbSendQuery(wrds, "select a.permno, a.permco , a.cusip
                   from crsp.msf a join crsp.ccmxpf_lnkused b
                   on a.permno = b.apermno
                   and a.datadate = b.datadate
                   where a.tic = 'IBM'
                   and a.datafmt = 'STD'
                   and a.consol = 'C'
                   and a.indfmt = 'INDL'")
data <- dbFetch(res, n = 1000)
dbClearResult(res)


# try to copy it
res <- dbSendQuery(wrds, "select a.gvkey, a.datadate, a.tic,
                   a.conm, a.at, a.lt, b.prccm, b.cshoq
                   from comp.funda a
                   join comp.secm b 
                   on a.gvkey = b.gvkey
                   and a.datadate = b.datadate
                   where a.tic = 'IBM'
                   and a.datafmt = 'STD'
                   and a.consol = 'C'
                   and a.indfmt = 'INDL'")
data <- dbFetch(res, n = 1000)
dbClearResult(res)


res <- dbSendQuery(wrds,  "select a.permno, a.cusip, a.permco, a.date, comnam, naics, liid as iid , siccd as sic, prc , vol, ret
                    from crsp.msf a 
                    join crsp.msenames b on a.permno = b.permno
                    join crsp.ccmxpf_linktable c on a.permno = c.apermno
                    where ( c.ulinktype in ('LU' 'LC') and c.ulinkprim in ('P' 'C') and usedflag = 1   )")
 prova <- dbFetch(res, n = 1000)
 dbClearResult(res)
 
 # get CRSP monthly and its link with CCM link table
                     
 query <- "select distinct a.permno, ugvkey as gvkey, uiid as iid, date, prc, vol, ret, comnam, siccd as crsp.sic, naics as crsp.naics
                            from crsp.msf a
                            left join crsp.msenames b   on  a.permno = b.permno and  a.date between b.namedt  and  b.nameendt 
                            left join crsp.ccmxpf_lnkused  c 
                            on  a.permno = c.apermno and a.date between ulinkdt and coalesce(ulinkenddt, '2020-09-30' ) 
                            where (ulinktype = 'LU' or ulinktype =  'LC')
                            and (ulinkprim = 'P' or ulinkprim = 'C') 
                            and usedflag = 1 " 
 
 crsp.data <- wrds.table(query, numrows = 1000  )
 
 crsp.data$year <- year(crsp.data$date)
 
 
 
 # get Compustat yearly, only NAICS
 
 naics <- unique (wrds.table("select naicsh as naics, sich as sic, gvkey, datadate as date from comp.funda", numrows = 1000) )
 #sich here is historical, in crsp hsic is header.
 naics$year <- year(naics$date)
 
 
 # merge CRSP and compustat
 crsp.data <- merge(crsp.data, naics, by = c("gvkey","year"), all.x = TRUE, all.y =FALSE )
 
 
 

 
 
test <- unique_id(prova, c("fdate","mgrno"))
 
 
 
 
# solution for manager file from Koijen-Yogo


prova <- fread("data/raw/ky_managers.csv")
 
 
 
 
 
 
 
 
 
