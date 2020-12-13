rm(list = ls(all=T))
gc()
#Need to get the libraries if you don't have them
library(parallel)
library(foreach)
library(doParallel)
library(tidyverse)
library(feather)

#Author: Trent McKinnon

#Program features:

#Input: CSV of stock data with the following fields- GVKEY,Sic,cshoc(Common shares outstanding),prcod(price open),div (dividend),iid

#Output: .feather file of organized dataframe with following fields-  date,   ex_GVKEY_iid (exchange @ date), s_GVKEY_iid(sic),

onBigComputer = T

if(onBigComputer){
  setwd("C:/Users/TAM/OneDrive - UW-Madison/Classes/2020_S/970 Investments/Research Project VIX")
}else{setwd("C:/Users/16083/OneDrive - UW-Madison/Classes/2020_S/970 Investments/Research Project VIX")}
cat("\014")

theData = read_feather("stockData.feather")

#Start of Functions
#**********************************************************************
getOrganizedStock = function(unorganizedStockData,fullDates){
  #Input- need every datapoint for a specific stock, and only that stock. This way we don't overload ram
  #       dates that we will be using to later merge to large dataframe
  #Output-dataframe with following columns: date,GVKEY_return,GVKEY_marketCap,GVKEY_exchangeCode
  
  #We will define adjusted return to be related to the market cap change over time plus dividends
  #This aviods problems with stock splits. Explicitly: marketCap = cshoc*prcod, 
  #                                                    return[t] = log((marketCap[t]+div[t]*cshoc[t])/marketCap[t-1])
  
  #Double Check to make sure only one stock has been passed
  stockGVKEY = unique(unorganizedStockData$gvkey)
  if(length(stockGVKEY)>1){
    print("Error, had several GVKEYs being passed")
    print(stockGVKEY)
    stop()
  }
  
  
  organizedStockData = unorganizedStockData[order(unorganizedStockData$datadate),]
  organizedStockData$div[is.na(organizedStockData$div)] = 0
  organizedStockData[,"marketCap"] = organizedStockData$cshoc*organizedStockData$prcod
  organizedStockData[,"laggedMarketCap"] = c(NA, organizedStockData$marketCap[-length(organizedStockData$marketCap)])
  organizedStockData[,"return"] = log((organizedStockData$marketCap + (organizedStockData$div*organizedStockData$cshoc))/organizedStockData$laggedMarketCap)
  organizedStockData[,"sic"] = organizedStockData$sic
  
  #Now we trim the excess and rename things
  keep = c("datadate","marketCap","return","exchg","sic")
  organizedStockData = organizedStockData[,(colnames(organizedStockData) %in% keep)]
  colnames(organizedStockData)[which(colnames(organizedStockData) == "datadate")] = "date"
  colnames(organizedStockData)[which(colnames(organizedStockData) == "exchg")] = paste0("ex_",stockGVKEY)
  colnames(organizedStockData)[which(colnames(organizedStockData) == "marketCap")] = paste0("mC_",stockGVKEY)
  colnames(organizedStockData)[which(colnames(organizedStockData) == "return")] = paste0("r_",stockGVKEY)
  colnames(organizedStockData)[which(colnames(organizedStockData) == "sic")] = paste0("s_",stockGVKEY)
  organizedStockData = merge(fullDates,organizedStockData,by="date",all = T)
  organizedStockData = organizedStockData[,order(colnames(organizedStockData))]
  return(organizedStockData)
}

organizeData = function(unorganizedData){
  #Input - dataframe with the following columns: date, GVKEY, prcod, div, cshoc,exchg
  #Output - dataframe with the following columns: date, GVKEY_return,GVKEY_marketCap,GVKEY_exchangeCode
  #If stock information does not occur for a specific date, make it NA
  numberOfCores = detectCores()
  numberOfThreadsImplemented = floor(numberOfCores/1.5)
  unorganizedData$gvkey = paste0(unorganizedData$gvkey,"_",unorganizedData$iid)
  uniqueGVKEYs = sort(unique(unorganizedData$gvkey))
  uniqueDates = data.frame(date = sort(unique(unorganizedData$datadate)))
  print("Begining to stich together organized data.")
  registerDoParallel()
  result = foreach (i = uniqueGVKEYs, .export = "getOrganizedStock") %dopar% {
    getOrganizedStock(unorganizedData[which(unorganizedData$gvkey == i),],uniqueDates)
  }
  print("Finished creating the list, begining to merge it.")
  mergedDataframe = result[[1]]
  for (i in 2:length(result)) {
    if(i %%100 == 0){
      gc()
      print(i)
    }
    mergedDataframe = cbind(mergedDataframe,result[[i]][,-1])
  }
  result = mergedDataframe
  gc()
  print("Finished merging list")
  # result = data.frame(date = uniqueDates)
  # counter = 1
  # 
  # for (i in uniqueGVKEYs) {
  #   print(i)
  #   temp = getOrganizedStock(unorganizedData[which(unorganizedData$gvkey == i),],uniqueDates)
  #   result = merge(result,temp, by= "date")
  # }
  stopImplicitCluster()
  return(result)
}

isAStockWeird = function(returnData,name){
  name = substring(name,3)
  result = NA
  gapThreshold = 5
  toleranceOfGaps = 8
  toleranceOfObservations = 50
  
  locationOfData = which(!is.na(returnData))
  differenceInLocation = abs(locationOfData-c(locationOfData[1],locationOfData[-length(locationOfData)]))

  numberOfGaps = length(which(differenceInLocation>gapThreshold))
  numberOfObservations = length(locationOfData)
  
  if(numberOfObservations<toleranceOfObservations || numberOfGaps>toleranceOfGaps){result = name}
  return(result)
}

findWeirdStocks = function(sortedData){
  #Defining weird stocks as stocks with spotty data, ie have data for 2 weeks, then a missing month. Then have another random 3 days.
  columnsToInvestigate = colnames(sortedData)[which(grepl("r_",colnames(sortedData)))]
  registerDoParallel()
  result = foreach(i = columnsToInvestigate, .export = "isAStockWeird", .combine = c) %dopar%{
    isAStockWeird(sortedData[,which(colnames(sortedData)==i)],i)
  }
  stopImplicitCluster()
  result = result[which(!is.na(result))]
  return(result)
}

filterWeirdStocks = function(sortedData){
  weirdStocks = findWeirdStocks(sortedData)
  print("Deleting weird stock data")
  registerDoParallel()
  columnsToBeDeleted =   foreach (i = weirdStocks, .combine=c) %dopar% {
    which(grepl(i,colnames(sortedData)))
  }
  stopImplicitCluster()
  sortedData = sortedData[,-columnsToBeDeleted]
  return(sortedData)
}

getOrganizedData = function(unorganizedData){
  return(filterWeirdStocks(organizeData(unorganizedData)))
}

runTheProgram = function(unorganizedData){
  temp = getOrganizedData(unorganizedData)
  write_feather(temp,"organizedStockData.feather")
  print("Done! OganizedStockData is now saved as a .feather filetype")
}
#**********************************************************************
#End of Functions

runTheProgram(theData)
# 
# load("theOrganizedButNotFilteredData.RData")
# mergedDataframe = theOrganizedButNotFilteredData[[1]]
# for (i in 2:length(theOrganizedButNotFilteredData)) {
#   if(i %%100 == 0){
#     gc()
#     print(i)
#   }
#   mergedDataframe = cbind(mergedDataframe,theOrganizedButNotFilteredData[[i]][,-1])
# }
# save(mergedDataframe,file ="mergedButUnfilteredDataFrame.RData")
# 
# filteredData = filterWeirdStocks(mergedDataframe)
# write_feather(filteredData,"filteredData.feather")
# save(filteredData, file = "filteredData.RData")
#theOrganizedButNotFilteredData = organizeData(theData)
#save(theOrganizedButNotFilteredData,file = "theOrganizedButNotFilteredData.RData")
# write_feather(theOrganizedButNotFilteredData, "organizedButNotFilteredData.feather")
# theOrganizedData = getOrganizedData(theData)
# write_feather(theOrganizedData,"organizedData.feather")
# theOrganizedData_dates = data.frame(date = theOrganizedData$date)
# write_feather(theOrganizedData_dates,"organizedData_dates.feather")
# 
# j = theOrganizedButNotFilteredData %>% reduce(left_join, by= "date")
# 
# k = Reduce(function(dtf1,dtf2) merge(dtf1,dtf2,by="date",all.x=TRUE),theOrganizedButNotFilteredData)