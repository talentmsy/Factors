FFIND<-read.csv("ff49transcode.csv")

setDT(FFIND)
setnames(FFIND,"a","siccd")
load("60_22_CRSP_F&Dadjusted.RData")
crsp[,siccd:=as.integer(siccd)]
crsp<-crsp[is.na(siccd)==FALSE]

crsp<-FFIND[crsp,on="siccd"]

crsp[,mom:=lag(retadj,1),by=permno]
crsp[,lme:=lag(me,1),by=permno]
crsp[,mebin:=ntile(lme,10),by=date]
crsp[,llme:=lag(me,2),by=permno]

crsp<-crsp[is.na(llme)==FALSE]
p1<-crsp[,weighted.mean(retadj,lme/sum(lme)),by=.(ind,date)]

indret<-crsp[mebin>=8,weighted.mean(mom,llme/sum(llme)),by=.(ind,date)]
indm<-merge(indret,p1,all.x = TRUE,by=c("ind","date"))

indm[,retbin:=ntile(V1.x,9),by=date]
newindm<-indm[,mean(V1.y),by=.(date,retbin)]
indm1<-newindm[retbin==1]
indm9<-newindm[retbin==9]
mind<-merge(indm1,indm9,by="date")
setDT(mind)
mind[,ilr:=V1.y-V1.x]
mind<-mind[,.(date,V1.y,V1.x,ilr)]
mind<-mind[,.(jdate=date,ILR=ilr,ILRH=V1.y,ILRL=V1.x)]

write.csv(mind,"Ilr.csv")

