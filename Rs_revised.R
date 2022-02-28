library(tidyverse); library(zoo)
require(data.table); library(lubridate)
library(RPostgres)
library(roll)
library(reshape)
load("beqAdjusted_COMPQ.RData")
##
setkeyv(data.comp.fundq,c("gvkey","datadate"))
data.comp.fundq[,rdq:=ymd(rdq)]
data.comp.fundq<-data.comp.fundq[!is.na(ajexq)&!is.na(cshprq)]
data.comp.fundq[,saqeps:=(saleq/(cshprq*ajexq))]
data.comp.fundq[,delta:=saqeps-lag(saqeps,4),by=gvkey]
data.comp.fundq[,sddelta:=ifelse(is.na(roll_sd(delta,8))==TRUE,
                                 ifelse(is.na(roll_sd(delta,7))==TRUE,roll_sd(delta,6),roll_sd(delta,7))
                                 ,roll_sd(delta,8)),by=gvkey]
data.comp.fundq<-data.comp.fundq[!sddelta==0]
data.comp.fundq[,SUE:=saqeps/sddelta]
data.comp.fundq<-data.comp.fundq[order(gvkey,datadate)]
data.comp.fundq<-data.comp.fundq[is.na(SUE)==FALSE]

comp<-data.comp.fundq[,.(gvkey,datadate,fqtr,rdq,year,SUE)]
comp<-comp[rdq>=datadate]
comp[,jdate:=ceiling_date(datadate,"m")-1]

datecomp<-comp[,.(sdate=min(jdate),edate=max(jdate)),gvkey]

filldatecomp<-datecomp[,seq.Date(as.Date(as.yearmon(sdate)),as.Date(as.yearmon(edate)), by="month"),gvkey]
filldatecomp[,jdate:=ceiling_date(V1,"m") - 1]
filldatecomp<-filldatecomp[,.(gvkey,jdate)]
setkeyv(filldatecomp,c("gvkey","jdate"))

mycomp<-comp[filldatecomp,on=.(gvkey,jdate)]
setkeyv(mycomp,c("gvkey","jdate"))
mycomp[,nagroup:=cumsum(!is.na(SUE)),gvkey]
mycomp[,nanum:=length(jdate),by=.(gvkey,nagroup)]

mycomp[,pos:=1:.N,.(gvkey,nagroup)]
mycomp[pos<=5,sue_forward_fill:=SUE[1],.(gvkey,nagroup)]
mycomp[pos<=5,rdq_forward_fill:=rdq[1],.(gvkey,nagroup)]
mycomp[,datadate_forward_fill:=ceiling_date(datadate[1],"m")-1,.(gvkey,nagroup)]
mycomp[,diffinMon:=interval(datadate_forward_fill, jdate) %/% months(1) ]

#mycomp[,lastRDQ := rdq_forward_fill[replace(seq(.N) - pos, seq(.N) <= pos, NA)], gvkey]
mycomp[,lastSUE:=sue_forward_fill[replace(seq(.N) - pos, seq(.N) <= pos, NA)], gvkey]
mycomp[,lastSUE:=ifelse(is.na(sue_forward_fill),NA,lastSUE)]
mycomp[,newSUE:=ifelse(jdate>rdq_forward_fill,sue_forward_fill,NA)]
mycomp[,newSUE:=ifelse(jdate<=rdq_forward_fill,lastSUE,newSUE)]
mycomp[year(jdate)<1972,newSUE:=lag(sue_forward_fill,3),gvkey]
mycomp[,lagSUE:=lag(newSUE,1),gvkey]
mycomp<-mycomp[diffinMon<=5&is.na(lastSUE)==FALSE]
mycomp<-mycomp[pos<=5,.SD,.(gvkey,nagroup)]
setkeyv(mycomp,c("gvkey","jdate"))
mycomp<-unique(mycomp[is.na(lagSUE)==FALSE,.(gvkey,nagroup,jdate,lagSUE)])
###################
# CRSP Block      #
###################
load("60_22_CRSP_F&Dadjusted.RData")
crsp<-crsp[,.(jdate,permno,exchcd,retadj,me,prc,shrout)]
crsp[,lagme:=lag(me),permno]
#######################
# CCM Block           #
#######################
load("180619 data.ccmlink.RData")
setDT(data.ccmlink)
data.ccmlink[,tdate:=Sys.Date()]
data.ccmlink[,linkenddt:=as.Date(ifelse(is.na(linkenddt)==TRUE,Sys.Date(),linkenddt))]
ccm1<-merge(mycomp,data.ccmlink,by="gvkey",all.x = TRUE,allow.cartesian = TRUE)
ccm2<-ccm1[jdate>=linkdt & jdate<=linkenddt]
ccmerged<-ccm2[crsp,on=.(permno,jdate)]
ccmerged<-ccmerged[is.na(lagSUE)==FALSE&is.na(lagme)==FALSE]
ccmerged<-ccmerged[,.(jdate,exchcd,retadj,lagSUE,me,lagme,permno)]
rfp<-ccmerged

nyse<-rfp[exchcd==1]
# size breakdown
nyse_sz<-nyse[,median(lagme),by=jdate]
setnames(nyse_sz,"V1","sizemedn")

nyse_d1<-nyse[,quantile(lagSUE,.1),by=jdate]
nyse_d10<-nyse[,quantile(lagSUE,.9),by=jdate]
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
crsp_factor[,dport:=ifelse(lagSUE<=d10,"L","")]
crsp_factor[,dport:=ifelse(lagSUE>d10&lagSUE<=d90,"M",dport)]
crsp_factor[,dport:=ifelse(lagSUE>d90,"H",dport)]
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

factor<-factor[,.(jdate,RS=WHML,RSH=WH,RSL=WL)]
write.csv(factor,file="Rs_revised.csv")
