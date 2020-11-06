# Data Notes

## Thomson Reuters guide from WRDS

### Dates
I copy here a point they make about dates:
>  In order for the late reports to be identified with the right holdings date, we use RDATE to signify the date to which the items apply, after lining up the separate data tables using FDATE. Large gaps between FDATE and RDATE for any record (i.e., a fund or manager’s holding) often corresponds to cases with large gaps in the sequence of RDATEs for a fund (FUNDNO) or manager (MGRNO), and thus missing holdings data.

For the 13F database FDATE and RDATE should be virtually always the same.

### Coverage

>The S34 holdings (identified by CUSIPs) are generally equities, but are not necessarily the entire equity holdings of the manager. For one, small >holdings—under 10,000 shares or $200,000—may be omitted from 13f reports, as well as cases where there may be confidentiality issues

## Readme on Data

### Data files
The data files from WRDS I downloaded are:
* Master file
* Names file

There is also a csv file that includes the CCM database quarterly from 1980 to 2019. That file uncompressed should be over 2GB so probably not usable offline. I got that file from WRDS at the link given by Ralph and Moto. The issue is that it seems the file is easy to query online from WRDS, and hard to get directly using the Postgres query of R. From the postgres query I can only get the link file that does not have the data attached to it.

### Compustat
Take NAICS (*naicsh*) from `comp.funda` which is at the annual frequency but for now it's good enough.

The WRDS overview page for Compustat Capital IQ data can be found [here](https://wrds-www.wharton.upenn.edu/pages/support/manuals-and-overviews/compustat/north-america-global-bank/wrds-overview-compustat-north-america-global-bank/).

The complete Compustat Xpressfeed online manual is [here](https://wrds-web.wharton.upenn.edu/wrds/support/Data/_001Manuals%20and%20Overviews/_001Compustat/_001North%20America%20-%20Global%20-%20Bank/_000dataguide/index.cfm).

The instructions to compute total returns are in the Powerpoint file stored in the data dictionaries folder.

### WRDS Compustat-CRSP link instructions



The WRDS overview page for CCM can be accessed [here](https://wrds-www.wharton.upenn.edu/pages/support/manuals-and-overviews/crsp/crspcompustat-merged-ccm/wrds-overview-crspcompustat-merged-ccm/).


This is the [link](https://wrds-www.wharton.upenn.edu/pages/support/applications/linking-databases/linking-crsp-and-compustat/) to the instructions . They provide an example SAS code that I copy here.

```SAS
proc sql;
	create table CCM_LINK as 
	select distinct a.permno, gvkey, liid as iid, 
			date, prc, vol, ret
	from 
		crsp.msf as a, 						/*CRSP Monthly stock file*/
		crsp.msenames 
		(
			where=(shrcd in (10 11))		/*Common stocks of U.S. Companies*/
		) as b, 
		crsp.Ccmxpf_linktable
		(
			where=(
				linktype in ('LU' 'LC') 	/*KEEP reliable LINKS only*/
				and LINKPRIM in ('P' 'C')   /*KEEP primary Links*/
				and USEDFLAG=1 )			/*Legacy condition, no longer necessary*/
		) as c
	where a.permno=b.permno=c.lpermno		/*Linking by permno*/
	and NAMEDT<=a.date<=NAMEENDT			/*CRSP Date range conditions*/
	and linkdt<=a.date<=coalesce(linkenddt, today());	/*LinkTable Date range conditions*/
quit;
```
[This](https://sites.google.com/site/ruidaiwrds/data/linking-crsp-and-compustat) is another useful page that provides roughly the same information about CCM linking.

The WRDS support team sent me this other SAS script that does something similar, it is a little more complicated but may come in handy in the future.

```SAS
/* ********************************************************************************* */
/* ******************** W R D S   R E S E A R C H   M A C R O S ******************** */
/* ********************************************************************************* */
/* WRDS Macro: CCM                                                                   */
/* Summary   : Use CRSP-Compustat Merged Table to Add Permno to Compustat Data       */
/* Date      : October 20, 2010                                                      */
/* Author    : Luis Palacios and Rabih Moussawi, WRDS                                */
/* Variables : - INSET   : Input dataset: should have a gvkey and a date variable    */
/*             - DATEVAR : Date variable to be used in the linking                   */
/*             - LINKTYPE: List of Linktypes: LU LC LX LD LN LS NP NR NU             */
/*             - REMDUPS : Flag 0/1 to remove multiple secondary permno matches      */
/*             - OVERLAP : Date Condition Overlap, in years                          */
/*             - OUTSET  : Compustat-CRSP link table output dataset                  */
/* ********************************************************************************* */
 
%MACRO CCM (INSET=,DATEVAR=DATADATE,OUTSET=CCM,LINKTYPE=LULC,REMDUPS=1,OVERLAP=0);
 
/* Check Validity of CCM Library Assignment */
%if (%sysfunc(libref(CCM))) %then %do; libname CCM ("/wrds/crsp/sasdata/q_ccm/"); %end;
%if (%sysfunc(libref(CCM))) %then %do; libname CCM ("/wrds/crsp/sasdata/a_ccm/") ; %end;
%put; %put ### START. ;
 
/* Convert the overlap distance into months */
%let overlap=%sysevalf(12*&overlap.);
 
options nonotes;
/* Make sure first that the input dataset has no duplicates by GVKEY-&DATEVAR */
proc sort data=&INSET out=_ccm0 nodupkey; by GVKEY &DATEVAR; run;
 
/* Add Permno to Compustat sample */
proc sql;
create table _ccm1 as
select distinct b.lpermno as PERMNO " ", a.*, b.linkprim, b.linkdt
from _ccm0 as a, ccm.ccmxpf_linktable as b
where a.gvkey=b.gvkey and index("&linktype.",strip(b.linktype))>0
and (a.&datevar>= intnx("month",b.linkdt   ,-&overlap.,"b") or missing(b.linkdt)   )
and (a.&datevar<= intnx("month",b.linkenddt, &overlap.,"e") or missing(b.linkenddt));
quit;
  
/* Cleaning Compustat Data for no relevant duplicates                       */
/* 1. Eliminating overlapping matching : few cases where different gvkeys   */
/*   for same permno-date --- some of them are not 'primary' matches in CCM.*/
/*   Use linkprim='P' for selecting just one gvkey-permno-date combination; */
proc sort data=_ccm1;
  by &datevar permno descending linkprim descending linkdt gvkey;
run;
 
/* it ties in the linkprim, then use most recent link or keep all */
data _ccm2;
set _ccm1;
by &datevar permno descending linkprim descending linkdt gvkey;
if first.permno;
%if &REMDUPS=0 %then %do; drop linkprim linkdt; %end;
run;
  
%if &REMDUPS=1 %then
 %do;
   proc sort data=_ccm2; by &datevar gvkey descending linkprim descending linkdt;
   data _ccm2;
   set _ccm2;
   by &datevar gvkey descending linkprim descending linkdt;
   if first.gvkey;
   drop linkprim linkdt;
   run;
   %put ## Removed Multiple PERMNO Matches per GVKEY ;
 %end;
 
/* Sanity Check -- No Duplicates -- and Save Output Dataset */
proc sort data=_ccm2 out=&OUTSET nodupkey; by gvkey &datevar permno; run;
%put ## &OUTSET Linked Table Created;
 
/* House Cleaning */
proc sql;
 drop table _ccm0, _ccm1, _ccm2;
quit;
 
%put ### DONE . ; %put ;
options notes;
%MEND CCM;
 
 
/* ********************************************************************************* */
/* *************  Material Copyright Wharton Research Data Services  *************** */
/* ****************************** All Rights Reserved ****************************** */
/* ********************************************************************************* */

```

## Ralph and Moto Paper

I paste here part of the data section in their paper where they explain what data choices they make. 

>The data on stock prices, dividends, returns, and shares outstanding are from the Centerfor Research in Security Prices (CRSP) Monthly Stock Database. We restrict our sample to ordinary common shares (i.e., share codes 10, 11, 12, and 18) that trade on the NewYork Stock Exchange (NYSE), the American Stock Exchange, and Nasdaq (i.e., exchangecodes 1, 2, and 3). We further restrict our sample to stocks with non-missing price andshares outstanding. Accounting data are from the Compustat North America Fundamentals Annual and Quarterly Databases. We merge the CRSP data with the most recent Compustatdata as of at least 6 months and no more than 18 months prior to the trading date. The lag of at least 6 months ensures that the accounting data were public on the trading date.

Here they describe their approach to merging the holdings data, and which ones they exclude:

>We merge the institutional holdings data with the CRSP-Compustat data by CUSIP number and drop any holdings that do not match (i.e., 13(f) securities whose share codesare not 10, 11, 12, or 18). We compute the dollar holding for each stock that an institution holds as price times shares held. Assets under management is the sum of dollar holdings foreach institution. We compute the portfolio weights as the ratio of dollar holdings to assets under management.

I need to drop those that are not matched then!

## Robintrack Data

I found [this page](https://robinhood.com/us/en/support/articles/investments-you-can-make-on-robinhood/) that explains the universe of investable assets that are in Robinhood.


### CRSP merging
There is a file that has all the information of the firms in the daily stock file. It is the file called `crsp.dsfhdr`. I merge that to Robin using ticker.
Ticker does not uniquely identify observations in CRSP, so when I merge it some assets from Robinhood are duplicated. In any case the fraction of duplicates seems 
low, below 1 percent. Most of the duplicated obs are of share code 73 that indicates ETF's.

My approach is, among the duplicates, to keep only ordinary common shares when there are available, otherwise just delete all duplicated observations.

### Cleaning Choices

* Keep only observations from ASSET4 that have a CUSIP (most are Canada and U.S.A.)
* Discard the worst 10 percent of matches, judging by the discrepancy of the names of the companies after the merge.






