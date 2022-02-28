setwd("C:\\Users\\A\\Desktop\\test")
library(tidyverse); library(zoo)
require(data.table); library(lubridate)
library(RPostgres)

ind<-read.csv("45ind.csv")
setDT(ind)
setnames(ind,"X0","ind")
mydata<-melt(ind,id.vars=c("ind"),variable.name=c("date"),value.name=c("ret"))
setnames(mydata,"variable","date")
setnames(mydata,"value","ret")
setDT(mydata)
mydata[,date:=paste0(substr(date,2,5),"-",substr(date,6,8),"-","01")]
mydata[,date:=as.Date(date,format=c("%Y-%m-%d"))]
setnames(mydata,"ret","retadj")

crsp.m<-mydata[date>=as.Date("1970-01-01")]
setkeyv(crsp.m,c("ind","date"))
crsp.m[,retadj:=retadj/100]

crsp.m[,mom:= (lag(retadj,1)+1)*(lag(retadj,2)+1)*(lag(retadj,3)+1)*(lag(retadj,4)+1)*
          (lag(retadj,5)+1)*(lag(retadj,6)+1)-1,by=ind]
crsp.m<-crsp.m[is.na(mom)==FALSE]
crsp.m<-crsp.m[,bin:=ntile(mom,9),by=date]
crsp.m[,nret:=mean(retadj),by=.(date,bin)]
bin1<-unique(crsp.m[bin==1,.(date,nret)])
bin9<-unique(crsp.m[bin==9,.(date,nret)])
factor<-merge(bin1,bin9,by="date")
factor[,indom:=nret.y-nret.x]
write.csv(factor,"Im.csv")
