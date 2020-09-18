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

### WRDS Compustat-CRSP link instructions
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