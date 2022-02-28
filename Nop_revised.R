setwd("C:\\Users\\A\\Desktop\\test")
load("fullcomp_finexclude_1967.RData")
library(tidyverse); library(zoo)
require(data.table); library(lubridate)
library(RPostgres)
library(roll)
library(reshape)

##########################################################################################################
load("beqAdjusted_COMPM.RData")
setDT(data.comp.funda)
##########################################################################################################
data.comp.funda[,redinpst:=pstkrv-lag(pstkrv),by=gvkey]
data.comp.funda[,redinpst:=ifelse(redinpst<0,redinpst,0)]
data.comp.funda[,posinpst:=pstkrv-lag(pstkrv),by=gvkey]
data.comp.funda[,posinpst:=ifelse(posinpst>0,posinpst,0)]

data.comp.funda[,repurchase:=prstkc+redinpst]
data.comp.funda[,totalpayout:=dvc+repurchase]

data.comp.funda[,equityiss:=sstk-posinpst]
data.comp.funda[,npd:=totalpayout-equityiss]
comp<-data.comp.funda[npd!=0&is.na(npd)==FALSE]
comp[,year:=year(datadate)]
# number of years in Compustat
##why counting years?
comp<-comp[order(gvkey,datadate)]
comp[,count:=ave(gvkey==gvkey, gvkey, FUN=cumsum)]
comp[,count:=count-1]
comp<-comp[,.(gvkey,datadate,year,npd,count)]

###################
# CRSP Block      #
###################
load("60_22_CRSP_F&Dadjusted.RData")
# keep December market cap
crsp[,year:=year(jdate)]
crsp[,month:=month(jdate)]
decme<-crsp[month==12]
setnames(decme,"me","dec_me")
decme<-decme[,.(permno,date,jdate,dec_me,year)]
### July to June dates
crsp[,ffdate:=floor_date(jdate, "month") - months(6)]
crsp[,ffdate:=ceiling_date(ffdate,"m")-1]
crsp[,ffyear:=year(ffdate)]
crsp[,ffmonth:=month(ffdate)]
## R is different from python in carrying out the command in terms of processing NAs
crsp[,retx:=as.numeric(retx)]
crsp[,retx:=ifelse(is.na(retx),0,retx)]
crsp[,retx1:=(1+retx)]
crsp[,retx2:=ifelse(is.na(retx)==TRUE,1,(1+retx))]
# cumret by stock
crsp[,cumretx:=cumprod(retx2),by=.(permno,ffyear)]
crsp[,cumretx:=ifelse(is.na(retx1)==TRUE,NA,cumretx)]
# lag cumret
crsp[,lcumretx:=shift(cumretx,n=1L),by=permno]
# lag market cap
crsp[,lme:=shift(me,n=1L),by=permno]
# if first permno then use me/(1+retx) to replace the missing value
crsp[,count:=ave(permno==permno, permno, FUN=cumsum)]
crsp[,count:=count-1]
crsp[,lme:=ifelse(count==0,me/retx1,lme)]
# baseline me
mebase<-crsp[ffmonth==1,.(permno,ffyear,lme)]
setnames(mebase,"lme","mebase")
# merge result back together
setkeyv(crsp,c("permno","ffyear"))
setkeyv(mebase,c("permno","ffyear"))
crsp3<-mebase[crsp]
crsp3[,wt:=ifelse(ffmonth==1,lme,mebase*lcumretx)]
decme[,year:=year+1]
decme<-decme[,.(permno,year,dec_me)]
# Info as of June
crsp3_jun<-crsp3[month==6]
crsp_jun<-decme[crsp3_jun,on=.(permno,year),nomatch=0]
crsp_jun<-crsp_jun[,.(permno,date, jdate, shrcd,exchcd,retadj,me,wt,cumretx,mebase,lme,dec_me)]
crsp_jun<-unique(crsp_jun)
crsp_jun<-crsp_jun[order(permno,jdate)]

#######################
# CCM Block           #
#######################
load("180619 data.ccmlink.RData")
setDT(data.ccmlink)
# if linkenddt is missing then set to today date
data.ccmlink[,tdate:=Sys.Date()]
data.ccmlink[,linkenddt:=as.Date(ifelse(is.na(linkenddt)==TRUE,Sys.Date(),linkenddt))]
comp1<-copy(comp)
ccm1<-merge(comp1,data.ccmlink,by="gvkey",all.x = TRUE,allow.cartesian = TRUE)

ccm1[,yearend:=ceiling_date(datadate,"year")-days(1)]
ccm1[,jdate:=ceiling_date(yearend,"month")+months(6)-days(1)]
# set link date bounds
ccm2<-ccm1[jdate>=linkdt & jdate<=linkenddt]
ccm2<-ccm2[,.(gvkey,permno,datadate,yearend,jdate,npd,count)]

# link comp and crsp
ccm_jun<-ccm2[crsp_jun,on=.(permno,jdate),nomatch=0]
# define e/p
ccm_jun<-ccm_jun[,nop:=npd/dec_me]

# select NYSE stocks for bucket breakdown
# exchcd = 1 and positive beme and positive me and shrcd in (10,11) and at least 2 years in comp
nyse<-ccm_jun[exchcd==1&is.na(nop)==FALSE&me>0&count>1&(shrcd==10|shrcd==11)]
# size breakdown
nyse_sz<-nyse[,median(me),by=jdate]
setnames(nyse_sz,"V1","sizemedn")
# beme breakdown

nyse_bm1<-nyse[,quantile(nop,.1),by=jdate]
nyse_bm2<-nyse[,quantile(nop,.9),by=jdate]
nyse_bm<-merge(nyse_bm1,nyse_bm2,by="jdate")
setnames(nyse_bm,"V1.x","bm30")
setnames(nyse_bm,"V1.y","bm70")
setkey(nyse_sz,jdate)
setkey(nyse,jdate)
nyse_breaks<-nyse_sz[nyse_bm,nomatch=0]

# join back size and nop breakdown
setkey(ccm_jun,jdate)
setkey(nyse_breaks,jdate)

ccm1_jun<-nyse_breaks[ccm_jun,on="jdate"]

# assign size portfolio
ccm1_jun[is.na(nop)==FALSE&me>0&count>=1,szport:=ifelse(me<=sizemedn,"S","B")]
ccm1_jun[is.na(szport)==TRUE,szport:=""]
# assign book-to-market portfolio
ccm1_jun[is.na(nop)==FALSE&me>0&count>=1,bmport:=ifelse(nop<=bm30,"L","")]
ccm1_jun[is.na(nop)==FALSE&me>0&count>=1,bmport:=ifelse(nop>bm30&nop<=bm70,"M",bmport)]
ccm1_jun[is.na(nop)==FALSE&me>0&count>=1,bmport:=ifelse(nop>bm70,"H",bmport)]
ccm1_jun[is.na(bmport)==TRUE,bmport:=""]

# create positivebmeme and nonmissport variable
#ccm1_jun[,posbm:=ifelse(beme>0&me>0&count>=1,1,0)]
ccm1_jun[,nonmissport:=ifelse(bmport!="",1,0)]
# store portfolio assignment as of June
june<-ccm1_jun[,.(permno,date,jdate,bmport,szport,nonmissport)]
june[,ffyear:=year(jdate)]
# merge back with monthly records
crsp3<-crsp3[,.(date,permno,shrcd,exchcd,retadj,me,wt,cumretx,ffyear,jdate)]
june1<-june[,.(permno,ffyear,szport,bmport,nonmissport)]
ccm3<-june1[crsp3,on=.(permno,ffyear)]
# keeping only records that meet the criteria
ccm4<-ccm3[wt>0&nonmissport==1&(shrcd==10|shrcd==11)]
############################
# FormFactors #
############################
# value-weigthed return
vwret<-ccm4[,vwret:=weighted.mean(retadj,wt/sum(wt)),by=.(jdate,szport,bmport)]
vwret[,sbport:=paste0(szport,bmport)]
vwret1<-vwret[,.(jdate,sbport,vwret)]
vwret1<-unique(vwret1)
vwret2<-cast(vwret1,jdate~sbport)
vwret2<-na.omit(vwret2)
# firm count
vwret_n<-ccm4[,length(.SD$retadj),by=.(jdate,szport,bmport)]
setnames(vwret_n,"V1","n_firms")
vwret_n[,sbport:=paste0(szport,bmport)]
# tranpose
factors<-vwret2
setDT(factors)
factors[,WH:=(BH+SH)/2]
factors[,WL:=(BL+SL)/2]
factors[,WHML:=WH-WL]

factor<-factors[,.(jdate=jdate,NOPH=WH,NOPL=WL,NOP=WHML)]
write.csv(factor,file="Nop_revised.csv")
