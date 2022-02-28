
library(tidyverse); library(zoo)
require(data.table); library(lubridate)
library(RPostgres)

load("60_22_CRSP_F&Dadjusted.RData")
crsp<-crsp[,.(jdate,permno,exchcd,retadj,me,prc,shrout)]

ff3<-read.csv("FF3_monthly.csv")
setDT(ff3)
ff3[,jdate:=ym(date)]
ff3[,jdate:=ceiling_date(jdate,"m")-1]
crsp.m<-ff3[crsp,on=.(jdate)]
crsp.m<-crsp.m[is.na(Mkt.RF)==FALSE&is.na(retadj)==FALSE&me>0]
crsp.m[,":="(Mkt.RF=Mkt.RF/100,SMB=SMB/100,HML=HML/100,RF=RF/100)]
time<-unique(crsp.m[,jdate])

basket<-c()
for (i in (37:length(time))){
  j<-time[i]
  partreg<-crsp.m[j-dmonths(36)<=jdate&jdate<=j]
  partreg[,retadj:=retadj-RF]
  partreg<-partreg[,count:=length(jdate),by=permno]
  partreg<-partreg[count>=36]
  regresult<-partreg[,as.list(c(coef(lm(retadj ~ Mkt.RF+SMB+HML,data =.SD)),date=as.character(jdate[.N])))
                     ,by=permno]
  basket<-rbind(basket,regresult)
}

setnames(basket,"(Intercept)","res")
basket[,jdate:=as.Date(date)]
basket[,res:=as.numeric(res)]
setkeyv(basket,c("permno","jdate"))

basket[,mom:=roll_prod((1+res),width = 11)-1,by=permno]
basket[,sd:=roll_sd(res,11),by=permno]
basket[,emom:=mom/sd]
basket<-basket[is.na(emom)==FALSE]
basket<-basket[,.(permno,jdate,emom)]

######################################
preport<-basket[crsp.m[,.(permno,jdate,me,retadj,exchcd)],on=c("permno","jdate")]
preport[,lagemom:=lag(emom,2),permno]
preport[,lagme:=lag(me),by=permno]
preport<-preport[is.na(emom)==FALSE&is.na(lagme)==FALSE]

nyse<-preport[exchcd==1]
##size breakdown
nyse_sz<-nyse[,median(lagme),by=jdate]
setnames(nyse_sz,"V1","sizemedn")
##momentum breakdown
nyse_mom1<-nyse[,quantile(emom,.1),by=jdate]
nyse_mom2<-nyse[,quantile(emom,.9),by=jdate]
nyse_mom<-merge(nyse_mom1,nyse_mom2,by="jdate")
setnames(nyse_mom,"V1.x","mom10")
setnames(nyse_mom,"V1.y","mom90")
setkey(nyse_sz,jdate)
setkey(nyse,jdate)
nyse_breaks<-nyse_sz[nyse_mom,nomatch=0]

setkey(preport,jdate)
setkey(nyse_breaks,jdate)
# assign size portfolio
# join back size and breakdown
crsp.m1<-nyse_breaks[preport,on="jdate"]
crsp.m1[,szport:=ifelse(lagme<=sizemedn,"S","B")]
crsp.m1[is.na(szport)==TRUE,szport:=""]
# assign book-to-market portfolio
crsp.m1[,momport:=ifelse(emom<=mom10,"L","")]
crsp.m1[,momport:=ifelse(emom>mom10&emom<=mom90,"M",momport)]
crsp.m1[,momport:=ifelse(emom>mom90,"H",momport)]

vwret<-crsp.m1[,vwret:=weighted.mean(retadj,lagme/sum(lagme)),by=.(jdate,szport,momport)]

vwret[,sbport:=paste0(szport,momport)]
vwret1<-vwret[,.(jdate,sbport,vwret)]
vwret1<-unique(vwret1)
vwret2<-cast(vwret1,jdate~sbport)
# firm count
# vwret_n<-ccm4[,length(.SD$retadj),by=.(jdate,szport,momport)]
# setnames(vwret_n,"V1","n_firms")
# vwret_n[,sbport:=paste0(szport,momport)]
factor<-vwret2
setDT(factor)
factor[,WH:=(BH+SH)/2]
factor[,WL:=(BL+SL)/2]
factor[,WHML:=WH-WL]

factor<-factor[,.(jdate,RM=WHML,RMH=WH,RML=WL)]
write.csv(factor,file="RM_revised.csv")

