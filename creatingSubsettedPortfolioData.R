rm(list = ls(all=T))
cat("\014")
options(scipen=999)

#getIndexFrame not working, check on it
#getSubsetDatapoint not working
#Need to get the libraries if you don't have them
library(parallel)
library(foreach)
library(doParallel)
library(tidyverse)
library(feather)
library(utils)
setwd("C:/Users/16083/OneDrive - UW-Madison/Classes/2020_S/970 Investments/Research Project VIX")



organizeDataJustInCase = function(myData){
  if(!is_empty(myData)){
    myData = myData[order(myData$date),]
    dataColnames = colnames(myData)
    #In order to not make this take super long for each data point, I will use the built in structure of the organizedData. Just in case it's unstrucutred, I'll restrucutre
    #Make sure date is in the first column
    splitUpColnames = strsplit(dataColnames[-which(dataColnames=="date")],"_")
    locationOfDate = which(dataColnames=="date")
    if(locationOfDate !=1){
      elements = 1:length(dataColnames)
      myData = myData[,c(locationOfDate,elements[-locationOfDate])]
    }
    temp = matrix(unlist(splitUpColnames),ncol = length(dataColnames)-1,byrow = F)
    if(nrow(temp)>2){  myData = myData[,c(1,order(temp[3,],temp[2,],temp[1,])+1)]}
    else{  myData = myData[,c(1,order(temp[2,],temp[1,])+1)]}
  }
  result= myData
  return(result)
}
getSubsetDatapoint = function(filteredData,myName){
  #Comes prefiltered so all fields that are important are populated
  filteredDataColnames = colnames(filteredData)
  returns = (filteredData[,which(grepl("r_",filteredDataColnames))])
  mC = (filteredData[,which(grepl("mC_",filteredDataColnames))])
  nStock = length(returns)
  
  result = as.data.frame(matrix(nrow=1,ncol=5))
  result[,1] = filteredData$date
  result[,5] = nStock
  resultColumnNames = c("_vW","_eW","_mC","_nS")

  if(nStock>0){
    total_mC = sum(mC,na.rm = T)
    returnsXmC = returns*mC
    equallyWeightedReturn = sum(returns,na.rm = T)/length(returns)
    valueWeightedReturns = sum(returnsXmC,na.rm = T)/total_mC
    result[,2]=valueWeightedReturns
    result[,3]=equallyWeightedReturn
    result[,4]=total_mC
  }
  
  colnames(result) = c("date",paste0(myName,resultColumnNames))
  return(result)
}
getColumnsBasedOnSimilarGVKEY = function(rawDataColumnNames, GVKEYsToFind){
  if(is.data.frame(GVKEYsToFind)){GVKEYsToFind = colnames(GVKEYsToFind)}
  placesToDeleteGVKEYPrefix = which(grepl("g_",GVKEYsToFind))
  if(!is_empty(placesToDeleteGVKEYPrefix)){
    GVKEYsToFind[placesToDeleteGVKEYPrefix] = substring(GVKEYsToFind[placesToDeleteGVKEYPrefix],3)
  }
  result = which(grepl(paste(c("date",GVKEYsToFind),collapse  = "|"),rawDataColumnNames))
  return(result)
}
getColumnsBasedOnExchange = function(rawData, exchangeUpper, exchangeLower){
  rawDataColumnNames = colnames(rawData)
  dataToInvestigate = rawData[,which(grepl("ex_",rawDataColumnNames))]
  columnsToKeep = which(dataToInvestigate>=exchangeLower & dataToInvestigate<=exchangeUpper)
  GVKEYSToKeep = substring(colnames(dataToInvestigate)[columnsToKeep],4)
  
  result = getColumnsBasedOnSimilarGVKEY(rawDataColumnNames,GVKEYSToKeep)
  return(result)
}
getColumnsBasedOnSIC = function(rawData, SICUpper, SICLower){
  rawDataColumnNames = colnames(rawData)
  dataToInvestigate = rawData[,which(grepl("s_",rawDataColumnNames))]
  columnsToKeep = which(dataToInvestigate>=SICLower & dataToInvestigate<=SICUpper)
  GVKEYSToKeep = substring(colnames(dataToInvestigate)[columnsToKeep],3)
  result = getColumnsBasedOnSimilarGVKEY(rawDataColumnNames,GVKEYSToKeep)
  return(result)
}
getColumnsBasedOnR = function(rawData, RUpper, RLower){
  rawDataColumnNames = colnames(rawData)
  dataToInvestigate = rawData[,which(grepl("r_",rawDataColumnNames))]
  columnsToKeep = which(dataToInvestigate>=RLower & dataToInvestigate<=RUpper)
  GVKEYSToKeep = substring(colnames(dataToInvestigate)[columnsToKeep],3)
  result = getColumnsBasedOnSimilarGVKEY(rawDataColumnNames,GVKEYSToKeep)
  return(result)
}
getFilteredDataPoint = function(criteria,fullRowData,indexFrame){
  rowDataColnames = colnames(fullRowData)
  uniqueNames = substring(rowDataColnames[which(grepl("r_",rowDataColnames))],3)
  currentDate = fullRowData$date[1]
  currentIndexValues = indexFrame[which(indexFrame$date==currentDate),]
  currentIndexValues = currentIndexValues[1,which(!is.na(currentIndexValues[1,]))]
  columnsToKeepFromIndex = getColumnsBasedOnSimilarGVKEY(rowDataColnames,currentIndexValues)
  #That gets rid of nonIndex Stocks, now need to filter exchanges
  columnsToKeepFromExchange = getColumnsBasedOnExchange(fullRowData,criteria$ex_upper,criteria$ex_lower)
  #Now do same with SIC
  columnsToKeepFromSIC = getColumnsBasedOnSIC(fullRowData,criteria$s_upper,criteria$s_lower)
  #Now do same with return
  columnsToKeepFromR = getColumnsBasedOnR(fullRowData,criteria$r_upper,criteria$r_lower)
  overlappingCriteria = intersect(intersect(intersect(columnsToKeepFromSIC,columnsToKeepFromExchange),columnsToKeepFromIndex),columnsToKeepFromR)
  result = fullRowData[,overlappingCriteria]
  return(result)
}
getUniqueGVKEYFromStockFrame = function(stockDataframe){
  stockColnames = colnames(stockDataframe)
  GVKEYS = substring(stockColnames[which(grepl("r_",stockColnames))],3)
  result = GVKEYS
  return(result)
}
getIndexFrame = function(indexFileName,stockDataframe){
  indexColnames = colnames(stockDataframe)
  indexFrame = as.data.frame(matrix(1,nrow = length(stockDataframe$date),ncol = length(indexColnames[which(grepl("r_",indexColnames))])+1))
  colnames(indexFrame) = c("date",paste0("g_",getUniqueGVKEYFromStockFrame(stockDataframe)))
  indexFrame$date = stockDataframe$date
  if(!(toupper(indexFileName)=="ALL")){
    indexFrame = read_feather(paste(c(indexFileName,".feather"),collapse = ""))
  }
  result = indexFrame
  return(result)
}
getStockFrame = function(stockFrameName){
  result = organizeDataJustInCase(read_feather(paste(c(stockFrameName,".feather"),collapse = "")))
  return(result)
}
getSubsetName = function(criteria){
  #criteria is DF that holds r_upper,r_lower,ex_upper,ex_lower,s_upper,s_lower,index
  numericDataframe = criteria[,-which(colnames(criteria) == "indexName")]
  locationOfRevalues = which(numericDataframe[1,]<0)
  numericDataframe[1,locationOfRevalues] = paste0("n",abs(numericDataframe[1,locationOfRevalues]))
  newCriteria = data.frame(indexName = criteria$indexName,numericDataframe)
  myName = paste(newCriteria$indexName,"r",newCriteria$r_lower,newCriteria$r_upper,
                 "ex",newCriteria$ex_lower,newCriteria$ex_upper,
                 "s",newCriteria$s_lower,newCriteria$s_upper,sep = "_")
  myName = gsub("-","n",myName)
  result = myName
  return(result)
}
getSubsetDataframe = function(criteria,stockDataFrame,indexDataFrame){
  subsetName = getSubsetName(criteria)
  myDates = stockDataFrame$date
  registerDoParallel()
  # result = foreach(i = myDates, .packages = lsf.str(),  .export = c("criteria","stockDataFrame","myDates","indexDataFrame"))%dopar%{
  #   getSubsetDatapoint(getFilteredDataPoint(criteria,stockDataFrame[which(myDates == i),],indexDataFrame))
  # }
  result = vector("list", length(myDates)*20)
  result = list()
  counter = 1
  for (i in myDates) {
    result[[counter]] = getSubsetDatapoint(getFilteredDataPoint(criteria,stockDataFrame[which(myDates == i),],indexDataFrame),subsetName)
    counter = counter+1
  }
  stopImplicitCluster()
  result = do.call(rbind.data.frame,result)
  return(result)
}
getSubsetDataframeParallel = function(criteria,stockDataFrame,indexDataFrame){
  subsetName = getSubsetName(criteria)
  myDates = stockDataFrame$date
  registerDoParallel()
  result = foreach(i = myDates, .packages = c("tidyverse","utils"), .export = c( "getSubsetDatapoint","getFilteredDataPoint",
                                             "getColumnsBasedOnSimilarGVKEY","getColumnsBasedOnExchange","getColumnsBasedOnSIC","getColumnsBasedOnR"))%dopar%{
    getSubsetDatapoint(getFilteredDataPoint(criteria,stockDataFrame[which(myDates == i),],indexDataFrame),subsetName)
  }
  stopImplicitCluster()
  result = do.call(rbind.data.frame,result)
  return(result)
}
getRPairings = function(r_bounds){
  r_bounds = sort(unique(c(r_bounds,-r_bounds,0,-1)))
  r_bounds = r_bounds[-which(r_bounds< -1)]
  r_pairings = expand.grid(r_bounds,r_bounds)
  r_pairings = r_pairings[-which((r_pairings[,1]==r_pairings[,2])|(r_pairings[,1]>r_pairings[,2])),]
  r_pairings = r_pairings[order(r_pairings[,1],r_pairings[,2]),]
  result = r_pairings
  return(result)
}
getEXPairings = function(ex_bounds){
  ex_bounds = sort(ex_bounds)
  ex_pairings = expand.grid(ex_bounds,ex_bounds)
  ex_pairings = ex_pairings[-which((ex_pairings[,1]==ex_pairings[,2])|(ex_pairings[,1]>ex_pairings[,2])),]
  ex_pairings = ex_pairings[order(ex_pairings[,1],ex_pairings[,2]),]
  result = ex_pairings
  return(result)
}
getSPairings = function(s_bounds){
  s_bounds = sort(s_bounds)
  s_pairings = expand.grid(s_bounds,s_bounds)
  s_pairings = s_pairings[-which((s_pairings[,1]==s_pairings[,2])|(s_pairings[,1]>s_pairings[,2])),]
  s_pairings = s_pairings[order(s_pairings[,1],s_pairings[,2]),]
  result = s_pairings
  return(result)
}
getPartitionedDataFrame = function(r_bounds,ex_bounds,s_bounds,indexFrame,fullDataframe,myIndexName){
  r_pairings = getRPairings(r_bounds)
  ex_pairings = getEXPairings(ex_bounds)
  s_pairings = getSPairings(s_bounds)
  
  bigDataframe = list()
  counter = 1
  totalIterations = nrow(s_pairings)*nrow(ex_pairings)*nrow(r_pairings)
  for (i in 1:nrow(s_pairings)) {
    for (j in 1:nrow(ex_pairings)) {
      for (k in 1:nrow(r_pairings)) {
        criteria = data.frame(r_lower = r_pairings[k,1] , r_upper = r_pairings[k,2], ex_lower = ex_pairings[j,1],ex_upper=ex_pairings[j,2],
                               indexName = myIndexName , s_lower = s_pairings[i,1],s_upper = s_pairings[i,2]  )
        bigDataframe[[counter]] = getSubsetDataframeParallel(criteria,fullDataframe,indexFrame)
        counter = counter+1
        print(paste(c(counter-1,"out of", totalIterations),sep = " ",collapse = " "))
      }
    }
  }
  
  
  result = bigDataframe %>% reduce(left_join,by="date")
  return(result)
}
runTheProgram = function(stockFileName,indexFileName,bounds){
  myStockDataFrame = getStockFrame(stockFileName)
  myIndexDataFrame = getIndexFrame(indexFileName,myStockDataFrame)
  result = getPartitionedDataFrame(bounds$r_bounds,bounds$ex_bounds,bounds$s_bounds,myIndexDataFrame,myStockDataFrame,indexFileName)
  return(result)
}


theSP500_frame = organizeDataJustInCase(read_feather("S&P500_dataFrame.feather"))
theStockData = organizeDataJustInCase(read_feather("organizedData.feather"))
theStockDates = data.frame(date = theStockData$date)
theSubsetThresholds = c(10000,.5,.01,0)
theSubsetThresholds = sort(unique(c(theSubsetThresholds,-theSubsetThresholds)))

c = data.frame(r_lower = -.001,r_upper = .3,ex_upper = 10000, ex_lower = 1,indexName = "S&P500",s_upper = 10000,s_lower = 0)

#f = getPartitionedDataFrame(c(0,10),c(0,100000),c(0,100000),theSP500_frame,theStockData,"S&P500")
#g = getSubsetDataframeParallel(c, theStockData,theSP500_frame)
theBounds = data.frame(r_bounds = c(0,10),ex_bounds = c(0,1000000),s_bounds = c(0,1000000))
h = runTheProgram("organizedData","S&P500",theBounds)
j = runTheProgram("organizedData","all",theBounds)