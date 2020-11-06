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

### Call Rob October 6th

**Part on Robinhood**
Main point: you are thinking of ESG because that was your starting point. But why ESG makes sense during the pandemic? You could look at a whole host of variables! And ESG should not be relevant only during the pandemic.

Be careful how you measure the shifts in demand. You probably want to normalize the users holding each asset by the total number of users, so you get a *fraction* of users holding that stock!

There are two effects at play during the pandemic: COVID hits, and many new users enter the platform. Ideally you'd like to separate the effect of the two.

Mind that *selection* might be an issue. The fact that firms have an ESG score may already say something about them. You can have a dummy variable for having an ESG score and control for it.

**Additional data sources**
You need more info on the stocks in the sample. Something like assets and other accounting information. You can look at Compustat to begin, maybe also Datastream. Another thing you may need is recent price data, which we do not have on CRSP. Maybe we can find it on bloomberg or Datastream or some API on R? Like Yahoo or Google finance?

**Forward**
Some things you could do:
1. What investors hold in the first place
2. Changes during covid, what variables explain the cross-section?
3. Regressions: what variables explain holdings? What variables the changes in holdings during covid?

**Part on 13F**
They way you weight Sin and Fossil might make a difference.


## Useful Quotes From News

> “We complained forever that the average person was not getting involved in the market. Now that the average person is getting involved, everyone is complaining about it.” [CNBC Trader Talk Jun 9th 2020](https://www.cnbc.com/2020/06/09/retail-investors-buying-beaten-up-stocks-make-for-some-bizarre-trading-days.html)

> “It is frightening to see the complete absence of any common sense,” said a Germany-based trader who has been trading U.S. equities for close to two decades. “Las Vegas and 
>   Macau are closed, and it shows here.” [Thomson Reuters Jun 10th 2020](https://uk.reuters.com/article/uk-health-coronavirus-retail-trading-ana/a-casino-or-stock-market-retail-buying-frenzy-goes-wild-idUKKBN23H2RS)

> A casino or stock market? Retail buying frenzy goes wild [Thomson Reuters Jun 10th 2020](https://uk.reuters.com/article/uk-health-coronavirus-retail-trading-ana/a-casino-or-stock-market-retail-buying-frenzy-goes-wild-idUKKBN23H2RS)]

>	Why would “bros” with Robinhood accounts trudge through bankruptcy dockets when there are memes to post on TikTok and Twitter? A massive rally in bond and equity markets amid a deep recession has caught market observers off guard. Most weirdly of all, shares of companies in or approaching Chapter 11 bankruptcy have rebounded strongly. Notably, stock in Hertz, Whiting Petroleum, Chesapeake Energy and JC Penney have more than doubled on their depressed bases on Monday for no substantive reason.
The proximate cause is a massive flow into risk assets as Covid-19 lockdowns end. Retail investors — particularly millennial and Gen-Z males — are day-trading commission-free on the Robinhood app. This helpfully plots the number of its users who have bought a stock against the respective price chart. [FT June 9th](https://www.ft.com/content/175c3528-6ed9-4914-b8d6-bef97f5008ad)

[Fintech app Robinhood is driving a retail trading renaissance during the stock market’s wild ride](https://www.cnbc.com/2020/06/17/robinhood-drives-retail-trading-renaissance-during-markets-wild-ride.html)

> Covid-19 calls on us not to abandon the tenets of sustainable investing but to speed them up, in order to strengthen the resilience of our societies and companies.
While policymaker initiatives may be understandably diverted in the short-term, there is no time to waste for investors. Private investors can fill in the gap with a long-term perspective as governments are in crisis management mode. For all stakeholders’ sakes, it is time for investors to step up. (FT)
