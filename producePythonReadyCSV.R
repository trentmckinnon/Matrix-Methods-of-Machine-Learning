rm(list = ls(all=T))
library(feather)
cat("\014")
myDataFrame = read_feather("fullResult.feather")
myDataFrame = myDataFrame[-1,c(ncol(myDataFrame),1:(ncol(myDataFrame)-1))]
#Now we need to add vix to the fray
vixFrame = read.csv("^VIX.csv")
vixFrame$vix = log(c(1,vixFrame$Adj.Close[-1]/vixFrame$Adj.Close[-length(vixFrame$Adj.Close)]))
year = as.numeric(paste(c(substr(as.character(vixFrame$Date),0,4))))
month = as.numeric(paste(c(substr(as.character(vixFrame$Date),6,7))))
day = as.numeric(paste(c(substr(as.character(vixFrame$Date),9,10))))
myDate = year*10000+month*100+day
vixFrame$date = myDate
vixFrame = vixFrame[,-(1:7)]

dataUsedForAnalysis = merge(myDataFrame,vixFrame,by="date")
dataUsedForAnalysis = dataUsedForAnalysis[-1,]

write.csv(dataUsedForAnalysis,"dataUsedForAnalysis.csv")
library(R.matlab)
myXDataFrame = dataUsedForAnalysis[,-c(1,ncol(dataUsedForAnalysis))]
myX = as.matrix(myXDataFrame)
myYDataFrame = as.data.frame(dataUsedForAnalysis[,ncol(dataUsedForAnalysis)])
colnames(myYDataFrame) = "y"
myY = as.matrix(myYDataFrame)
#writeMat("dataUsedForAnalysis.mat",x=myX,y=myY)
write_feather(myXDataFrame,"xUsedForDataAnalysis.feather")
write_feather(myYDataFrame,"yUsedForDataAnalysis.feather")

tempLm = lm(myY~myX)
summary(tempLm)