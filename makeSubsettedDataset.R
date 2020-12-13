rm(list = ls(all=T))
library(parallel)
library(foreach)
library(doParallel)
library(tidyverse)
library(feather)
library(utils)
library(stats)
small=T
if(small){
  setwd("C:/Users/16083/OneDrive - UW-Madison/Classes/2020_S/970 Investments/Research Project VIX")
}else{
  setwd("C:/Users/TAM/OneDrive - UW-Madison/Classes/2020_S/970 Investments/Research Project VIX")
}
# myFullDataTemp = read_feather("filteredData.feather")
# myFullDataTemp[is.na(myFullDataTemp)] = 0
# write_feather(myFullDataTemp,"filteredData_noNA.feather")


cat("\014")
myBounds = c(-100,-.1,-.05,-.025,-.01,0,.01,.025,.05,.1,100)
myFullData = read_feather("filteredData_noNA.feather")
myDates = myFullData$date
tempData = myFullData[8,]

createSubsetDataPointByDate = function(currentDataPoint,myBounds){
  subsetDataPoint = matrix(currentDataPoint[1,1],nrow = 1,ncol = (length(myBounds)-1)*5+1)
  for (i in 1:(length(myBounds)-1)) {
    lowerBound = myBounds[i]
    upperBound = myBounds[i+1]
    croppedData_r = (currentDataPoint[,(4)*(1:(ncol(currentDataPoint)%/%4))])
    indexOfDesiredReturns = 4*which(croppedData_r>lowerBound & croppedData_r<upperBound)
    indexOfDesiredMarketCap = indexOfDesiredReturns-1
    totalMarketCap = sum(as.numeric(currentDataPoint[1,indexOfDesiredMarketCap]))
    averageMarketCap = mean(as.numeric(currentDataPoint[1,indexOfDesiredMarketCap]))
    weightedAverageReturn = 0
    if(totalMarketCap != 0){
      weightedAverageReturn = sum(as.numeric(currentDataPoint[1,indexOfDesiredMarketCap])*as.numeric(currentDataPoint[1,indexOfDesiredReturns])/totalMarketCap)
    }
    averageReturn = mean(as.numeric(currentDataPoint[1,indexOfDesiredReturns]))
    nCompanies = length(indexOfDesiredReturns)
    currentStartingIndex = 5*(i-1)
    subsetDataPoint[1,currentStartingIndex+1:5] = c(totalMarketCap,averageMarketCap,weightedAverageReturn,averageReturn,nCompanies)
    
  }
  return(subsetDataPoint)
}
#createSubsetDataPointByDate(tempData,myBounds)
#now we need to iterate with a for each, also make sure to run garbage collection sporadically to not overload ram if we have a dataleak.
tempFullData = myFullData[1:6,]
createSubsetDataframe = function(fullData,myBounds){
  registerDoParallel()
  result = foreach (i = 1:nrow(fullData),.export = "createSubsetDataPointByDate") %dopar% {
    createSubsetDataPointByDate(fullData[i,],myBounds)
  }
  stopImplicitCluster()
  #Here we need to merge matrices as shown below and then we need to name the columns and sort by date...although I guess date doesn't really matter
  result = as.data.frame(do.call(rbind,result))
  desiredColNames = rep("",1+(length(myBounds)-1)*5)
  for (i in 1:(length(myBounds)-1)) {
    currentIndex = (i-1)*5
    boundLower = myBounds[i]
    boundUpper = myBounds[i+1]
    currentBoundName = as.character(c(boundLower,"_",boundUpper))
    colnames(result)[currentIndex+1]= paste(c("tmc_",currentBoundName),collapse = "",sep="")
    colnames(result)[currentIndex+2]= paste(c("amc_",currentBoundName),collapse = "",sep="")
    colnames(result)[currentIndex+3]= paste(c("war_",currentBoundName),collapse = "",sep="")
    colnames(result)[currentIndex+4]= paste(c("ar_",currentBoundName),collapse = "",sep="")
    colnames(result)[currentIndex+5]= paste(c("nc_",currentBoundName),collapse = "",sep="")
    
  }
  colnames(result)[ncol(result)]="date"
  return(result)
}
fullResult = createSubsetDataframe(myFullData,myBounds = myBounds)
write_feather(fullResult,"fullResult.feather")
gc()
