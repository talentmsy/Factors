library(tidyverse); library(zoo)
require(data.table); library(lubridate)
library(RPostgres)
library(roll)
library(reshape)

nibes<-read.csv("IBES.csv")
ibes<-read.csv("Unadjusted_IBES.csv")
setDT()

ibes<-read.csv("ibes.csv")
ibesdetail<-read.csv("ibesdetail.csv")
ibescrsp<-read.csv("IBESLinking.csv")
setDT(ibescrsp)
setDT(ibes)
setDT(ibesdetail)

ibescrsp<-ibescrsp[SCORE<=5]

newibes<-ibes[FPI==1&MEASURE=="EPS",.(TICKER,CUSIP,STATPERS,MEANEST)]
newibesdetail<-ibesdetail[MEASURE=="EPS",.(TICKER,CUSIP,STATPERS,PRICE)]

newibes<-unique(newibes)
newibesdetail<-unique(newibesdetail)

merge<-merge(newibes,newibesdetail,all.x = TRUE,by=c("TICKER","CUSIP","STATPERS"))
merge<-merge[is.na(PRICE)==FALSE]
merge[,re:=(MEANEST-lag(MEANEST))/(lag(PRICE)),by=TICKER]
merge[,re:=ifelse(is.na(re)==TRUE,0,re)]
merge[,sumre:=roll_sum(re,6),by=TICKER]
merge[,avere:=roll_mean(sumre,6),by=TICKER]
merge<-merge[is.na(TICKER)==FALSE]

lmerge<-ibescrsp[merge,on=.(TICKER),allow.cartesian=TRUE]
lmerge[,sdate:=as.character(sdate)]
lmerge[,startd:=as_date(sdate,format="%d%b%Y",tz = 'America/New_York')]
lmerge[,edate:=as.character(edate)]
lmerge[,endd:=as_date(as.character(edate),format="%d%b%Y",tz = 'America/New_York')]

lmerge[,newSTATPERS:=as_date(as.character(STATPERS),format="%Y%m%d",tz = 'America/New_York')]

newlmerge<-lmerge[newSTATPERS>=startd&newSTATPERS<=endd]





newlmerge[,date:=LastDayInMonth(newSTATPERS)]
setnames(newlmerge,"PERMNO","permno")

ccm<-crsp2[newlmerge,on=.(permno,date)]

ccm<-ccm[is.na(avere)==FALSE&is.na(lme)==FALSE]


nyse<-ccm[exchcd==1&me>0&(shrcd==10|shrcd==11)]
# size breakdown
nyse_sz<-nyse[,median(lme),by=date]
setnames(nyse_sz,"V1","sizemedn")


nyse_bm1<-nyse[is.na(avere)==FALSE,quantile(avere,.1),by=date]
nyse_bm2<-nyse[is.na(avere)==FALSE,quantile(avere,.9),by=date]
nyse_bm<-merge(nyse_bm1,nyse_bm2,by="date")
setnames(nyse_bm,"V1.x","bm30")
setnames(nyse_bm,"V1.y","bm70")
setkey(nyse_sz,date)
setkey(nyse,date)
nyse_breaks<-nyse_sz[nyse_bm,nomatch=0]

# join back size and beme breakdown
setkey(ccm,date)
setkey(nyse_breaks,date)

crsp_SUE<-nyse_breaks[ccm,on="date"]

# assign size portfolio
crsp_SUE[is.na(avere)==FALSE&me>0,szport:=ifelse(lme<=sizemedn,"S","B")]
crsp_SUE[is.na(szport)==TRUE,szport:=""]

# keeping only records that meet the criteria
crsp_SUE[is.na(avere)==FALSE&me>0,bmport:=ifelse(avere<=bm30,"L","")]
crsp_SUE[is.na(avere)==FALSE&me>0,bmport:=ifelse(avere>bm30&avere<=bm70,"M",bmport)]
crsp_SUE[is.na(avere)==FALSE&me>0,bmport:=ifelse(avere>bm70,"H",bmport)]
crsp_SUE[is.na(bmport)==TRUE,bmport:=""]

vwret<-crsp_SUE[,vwret:=weighted.mean(retadj,lme/sum(lme)),by=.(date,szport,bmport)]
vwret[,sbport:=paste0(szport,bmport)]
vwret1<-vwret[,.(date,sbport,vwret)]
vwret1<-unique(vwret1)
vwret1<-na.omit(vwret1)
vwret2<-cast(vwret1,date~sbport)



SUE<-vwret2 
setDT(SUE)
SUE[,WH:=(BH+SH)/2]
SUE[,WL:=(BL+SL)/2]
SUE[,WHML:=WH-WL]

SUE[,WB:=(BL+BM+BH)/3]
SUE[,WS:=(SL+SM+SH)/3]
SUE[,WSMB:=WS-WB]

SUEfactor<-SUE[,.(date,WSMB,WH,WL,WHML)]
# SUEfactor[,WSMB:=100*WSMB]
# SUEfactor[,WHML:=100*WHML]
SUEfactor<-na.omit(SUEfactor)
write.csv(SUEfactor,file="re.csv")
re<-read.csv("re.csv")
