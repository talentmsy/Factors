load("beqAdjusted_COMPQ.RData")
##
setkeyv(data.comp.fundq,c("gvkey","datadate"))
data.comp.fundq[,rdq:=ymd(rdq)]
data.comp.fundq<-data.comp.fundq[!is.na(ibq),.(gvkey,datadate,fqtr,rdq,ibq)]
data.comp.fundq<-data.comp.fundq[order(gvkey,datadate)]

comp<-data.comp.fundq
comp<-comp[rdq>=datadate]
comp[,jdate:=ceiling_date(datadate,"m")-1]
comp[,diffinibq:=ibq-lag(ibq,1),.(gvkey,fqtr)]
comp[,point:=ifelse(diffinibq>0,1,NA)]
comp[,nagroup:=cumsum(is.na(point)),gvkey]
comp[,nei:=roll_sum(point,8,min_obs = 1),.(gvkey,nagroup)]
comp[,nei:=ifelse(is.na(nei),0,nei)]

#
datecomp<-comp[,.(sdate=min(jdate),edate=max(jdate)),gvkey]

filldatecomp<-datecomp[,seq.Date(as.Date(as.yearmon(sdate)),as.Date(as.yearmon(edate)), by="month"),gvkey]
filldatecomp[,jdate:=ceiling_date(V1,"m") - 1]
filldatecomp<-filldatecomp[,.(gvkey,jdate)]
setkeyv(filldatecomp,c("gvkey","jdate"))

mycomp<-comp[filldatecomp,on=.(gvkey,jdate)]
mycomp[,SUE:=nei]
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

nyse_bm1<-unique(nyse[lagSUE==0,.SD$lagSUE,by=jdate])
nyse_bm2<-unique(nyse[lagSUE==8,.SD$lagSUE,by=jdate])
nyse_bm<-merge(nyse_bm1,nyse_bm2,by="jdate")
setnames(nyse_bm,"V1.x","d10")
setnames(nyse_bm,"V1.y","d90")
setkey(nyse_sz,jdate)
setkey(nyse,jdate)
nyse_breaks<-nyse_sz[nyse_bm,nomatch=0]
setkey(rfp,jdate)
setkey(nyse_breaks,jdate)

crsp_factor<-nyse_breaks[rfp,on="jdate"]
# assign size portfolio
crsp_factor[,szport:=ifelse(lagme<=sizemedn,"S","B")]
crsp_factor[is.na(szport)==TRUE,szport:=""]
# keeping only records that meet the criteria
crsp_factor[,dport:=ifelse(lagSUE==d10,"L","")]
#crsp_factor[,dport:=ifelse(lagSUE>d10&lagSUE<=d90,"M",dport)]
crsp_factor[,dport:=ifelse(lagSUE==d90,"H",dport)]
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

factor<-factor[,.(jdate,NEI=WHML,NEIH=WH,NEIL=WL)]
write.csv(factor,file="NEI_revised.csv")
