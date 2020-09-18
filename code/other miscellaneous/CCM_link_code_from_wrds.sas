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