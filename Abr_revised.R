setwd("C:\\Users\\sma13\\OneDrive - stevens.edu\\factors")
library(tidyverse); library(zoo)
require(data.table); library(lubridate)
library(RPostgres)
library(roll)

FF3daily<-read.csv("FF3daily.csv")
setDT(FF3daily)
FF3daily[,fdate:=ymd(Date)]
FF3daily[,Mkt.RF:=Mkt.RF/100]

FF3daily<-FF3daily[,.(fdate,Mkt.RF,RF)]
load("beqAdjusted_COMPQ.RData")
##
setkeyv(data.comp.fundq,c("gvkey","datadate"))
data.comp.fundq[,rdq:=ymd(rdq)]
comp<-data.comp.fundq[rdq>datadate,.(gvkey,datadate,rdq)]
###################
# CRSP Block      #
###################
load("60_22_CRSP_DSF_F&Dadjusted.RData")
crsp<-crsp[,.(jdate,permno,exchcd,retadj,me,prc,shrout)]
crsp[,fdate:=jdate]
crsp[,lagme:=lag(me),permno]
dcrsp<-FF3daily[crsp,on=.(fdate)]
rm(crsp)
#######################
# CCM Block           #
#######################
load("180619 data.ccmlink.RData")
setDT(data.ccmlink)
# if linkenddt is missing then set to today date
data.ccmlink[,tdate:=Sys.Date()]
data.ccmlink[,linkenddt:=as.Date(ifelse(is.na(linkenddt)==TRUE,Sys.Date(),linkenddt))]
ccm1<-merge(comp,data.ccmlink,by="gvkey",all.x = TRUE,allow.cartesian = TRUE)
ccm2<-ccm1[datadate>=linkdt & datadate<=linkenddt]
ccm2[,fdate:=rdq]
## select anomaly
ccmerged<-ccm2[dcrsp,on=.(permno,fdate)]
ccmerged<-ccmerged[,.(permno,datadate,fdate,rdq,exchcd,retadj,Mkt.RF,RF,me,prc,shrout,lagme)]
ccmerged[is.na(rdq)==FALSE,dd:=0,]
ccmerged[,lagd1:=lag(dd),permno]
setkeyv(ccmerged,c("permno","fdate"))
ccmerged[,rsret:=roll_sum(retadj,width = 4),permno]
ccmerged[,rsret:=ifelse(is.na(lagd1),NA,rsret)]
ccmerged[,abr:=rsret-(Mkt.RF+RF)]
ccmerged[,jdate:=ceiling_date(fdate,"m")-1,permno]
ccmerged[,nagroup:=cumsum(!is.na(datadate)),permno]
ccmerged[,nanum:=length(jdate),by=.(permno,nagroup)]
ccmerged[,nagroup_pos:=1:.N,.(permno,nagroup)]

ccmerged[,datadate_forward_fill:=ceiling_date(datadate[1],"m")-1,.(permno,nagroup)]
ccmerged[,diffinMon:=interval(datadate_forward_fill, jdate) %/% months(1) ]

uccmerged<-unique(ccmerged[is.na(abr)==FALSE&is.na(dd)&lagd1==0,.(permno,datadate,fdate,rdq,dd,lagd1,abr,jdate,datadate_forward_fill,diffinMon)])
##resolving two rdqs in one montth
uccmerged[,monthcount:=length(abr),.(permno,jdate)]
##fill up the gap between two RDQs
datecomp<-unique(uccmerged[,.(sdate=min(jdate),edate=max(jdate)),permno])
filldatecomp<-datecomp[,seq.Date(as.Date(as.yearmon(sdate)),as.Date(as.yearmon(edate)), by="month"),permno]
filldatecomp[,jdate:=ceiling_date(V1,"m") - 1]
filldatecomp<-filldatecomp[,.(permno,jdate)]
setkeyv(filldatecomp,c("permno","jdate"))
mycomp<-uccmerged[filldatecomp,on=.(permno,jdate)]
mycomp[,nagroup:=cumsum(!is.na(datadate_forward_fill)),permno]
mycomp[,nanum:=length(jdate),by=.(permno,nagroup)]
mycomp[,nagroup_pos:=1:.N,.(permno,nagroup)]

mycomp[,datadate_forward_fill:=datadate_forward_fill[1],.(permno,nagroup)]
mycomp[,diffinMon:=interval(datadate_forward_fill, jdate) %/% months(1) ]

setkeyv(mycomp,c("permno","jdate"))
mycomp<-mycomp[nagroup_pos<=5,abr_forward_fill:=abr[1],.(permno,nagroup)]

mycomp[,rdqpos:=1:.N,.(permno,jdate)]
mycomp[, lagAbr := lag(abr_forward_fill[replace(seq(.N) - rdqpos, seq(.N) <= rdqpos, NA)],1), permno]
mycomp<-mycomp[diffinMon<=5]
setkeyv(mycomp,c("permno","jdate"))
mycomp<-mycomp[is.na(lagAbr)==FALSE,.(permno,jdate,lagAbr)]

load("60_22_CRSP_Fex&Dadjusted.RData")
crsp<-crsp[,.(jdate,permno,exchcd,retadj,me,prc,shrout)]
crsp[,lagme:=lag(me),permno]

rfp<-mycomp[crsp,on=.(jdate,permno)]
rfp<-rfp[is.na(lagAbr)==FALSE&is.na(lagme)==FALSE]
nyse<-rfp[exchcd==1]
# size breakdown
nyse_sz<-nyse[,median(lagme),by=jdate]
setnames(nyse_sz,"V1","sizemedn")

nyse_d1<-nyse[,quantile(lagAbr,.1),by=jdate]
nyse_d10<-nyse[,quantile(lagAbr,.9),by=jdate]
nyse_dp<-merge(nyse_d1,nyse_d10,by="jdate")
setnames(nyse_dp,"V1.x","d10")
setnames(nyse_dp,"V1.y","d90")
setkey(nyse_sz,jdate)
setkey(nyse,jdate)
nyse_breaks<-nyse_sz[nyse_dp,nomatch=0]
setkey(rfp,jdate)
setkey(nyse_breaks,jdate)

crsp_factor<-nyse_breaks[rfp,on="jdate"]
# assign size portfolio
crsp_factor[,szport:=ifelse(lagme<=sizemedn,"S","B")]
crsp_factor[is.na(szport)==TRUE,szport:=""]
# keeping only records that meet the criteria
crsp_factor[,dport:=ifelse(lagAbr<=d10,"L","")]
crsp_factor[,dport:=ifelse(lagAbr>d10&lagAbr<=d90,"M",dport)]
crsp_factor[,dport:=ifelse(lagAbr>d90,"H",dport)]
crsp_factor[is.na(dport)==TRUE,dport:=""]

vwret<-crsp_factor[,vwret:=weighted.mean(retadj,lagme/sum(lagme)),by=.(jdate,szport,dport)]
vwret[,sbport:=paste0(szport,dport)]
vwret1<-vwret[,.(jdate,sbport,vwret)]
vwret1<-unique(vwret1)
vwret2<-cast(vwret1,jdate~sbport)

factor<-vwret2
setDT(factor)
factor[,WH:=(BH+SH)/2]
factor[,WL:=(BL+SL)/2]
factor[,WHML:=WH-WL]
factor<-factor[,.(jdate,ABR=WHML,ABRH=WH,ABRL=WL)]

write.csv(factor,file="abr_revised.csv")

