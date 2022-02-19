setwd("C:\\Users\\sma13\\OneDrive - stevens.edu\\newwork")

# install.packages("tidyverse")
# install.packages("zoo")
# install.packages("lubridate")
# #install.packages("RPostgres")
# install.packages("data.table")
# install.packages("reshape")

library(tidyverse); library(zoo)
library(data.table); library(lubridate)
library(roll);library(reshape)

load("60_22_CRSP_F&Dadjusted.RData")
crsp.m<-crsp[,.(permno,date,jdate,shrcd,exchcd,siccd,prc,vol,ret,retadj,me,shrout,retx)]
setDT(crsp.m)
setnames(crsp.m,colnames(crsp.m),tolower(colnames(crsp.m)))
################################################################################
crsp.m[,date:=ymd(date)]
crsp.m[,jdate:=ceiling_date(date,"m") - 1]
setkeyv(crsp.m,c("permno","jdate"))
crsp.m[,me:=abs(prc)*shrout]
crsp.m[,lagme:=lag(me),by=permno]
crsp.m[,ret:=as.numeric(ret)]
crsp.m[,ret:=ifelse(is.na(ret),0,ret)]
crsp.m[,mom:=roll_prod((1+ret),width = 11)-1,by=permno]
crsp.m[,mom:=lag(mom,2),by=permno]
crsp.m<-crsp.m[shrcd%in%(10:11)&exchcd%in%(1:3)]
crsp.m<-crsp.m[is.na(mom)==FALSE&is.na(lagme)==FALSE]
nyse<-crsp.m[exchcd==1]
nyse_sz<-nyse[,median(lagme),by=jdate]
setnames(nyse_sz,"V1","sizemedn")
nyse_mom1<-nyse[,quantile(mom,.3),by=jdate]
nyse_mom2<-nyse[,quantile(mom,.7),by=jdate]
nyse_mom<-merge(nyse_mom1,nyse_mom2,by="jdate")
setnames(nyse_mom,"V1.x","mom30")
setnames(nyse_mom,"V1.y","mom70")
setkey(nyse_sz,jdate)
setkey(nyse,jdate)
nyse_breaks<-nyse_sz[nyse_mom,nomatch=0]

# join back size and breakdown
setkey(crsp.m,jdate)
setkey(nyse_breaks,jdate)

crsp.m1<-nyse_breaks[crsp.m,on="jdate"]
crsp.m1[,szport:=ifelse(lagme<=sizemedn,"S","B")]
crsp.m1[is.na(szport)==TRUE,szport:=""]
# assign book-to-market portfolio
crsp.m1[,momport:=ifelse(mom<=mom30,"L","")]
crsp.m1[,momport:=ifelse(mom>mom30&mom<=mom70,"M",momport)]
crsp.m1[,momport:=ifelse(mom>mom70,"H",momport)]

vwret<-crsp.m1[,vwret:=weighted.mean(ret,lagme/sum(lagme)),by=.(jdate,szport,momport)]

vwret[,sbport:=paste0(szport,momport)]
vwret1<-vwret[,.(jdate,sbport,vwret)]
vwret1<-unique(vwret1)
vwret2<-cast(vwret1,jdate~sbport)

factor<-vwret2
setDT(factor)
factor[,WH:=(BH+SH)/2]
factor[,WL:=(BL+SL)/2]
factor[,WHML:=WH-WL]

factor<-factor[,.(jdate,MOM=WHML,MOMH=WH,MOML=WL)]
write.csv(factor,file = "MOM.csv")
