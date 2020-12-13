rm(list = ls(all=T))
#Need to get the libraries if you don't have them
library(parallel)
library(foreach)
library(doParallel)
library(tidyverse)
library(feather)
#Program features:

#Input: CSV of stock data with the following fields- GVKEY,Sic,cshoc(Common shares outstanding),prcod(price open),div (dividend),iid

#Output: .feather file of organized dataframe with following fields-  date,   ex_GVKEY_iid (exchange @ date), s_GVKEY_iid(sic),
#                                                                             r_GVKEY_iid (return),           mC_GVKEY_iid (market Cap),


setwd("C:/Users/16083/OneDrive - UW-Madison/Classes/2020_S/970 Investments/Research Project VIX")
cat("\014")

theData = read.csv("stockData_chopped_50000.csv")

#Start of Functions
#**********************************************************************
getOrganizedStock = function(unorganizedStockData,fullDates){
  #Input- need every datapoint for a specific stock, and only that stock. This way we don't overload ram
  #       dates that we will be using to later merge to large dataframe
  #Output-dataframe with following columns: date,sic_return,sic_marketCap,sic_exchangeCode
  
  #We will define adjusted return to be related to the market cap change over time plus dividends
  #This aviods problems with stock splits. Explicitly: marketCap = cshoc*prcod, 
  #                                                    return[t] = log((marketCap[t]+div[t]*cshoc[t])/marketCap[t-1])
  
  #Double Check to make sure only one stock has been passed
  stockSic = unique(unorganizedStockData$sic)
  if(length(stockSic)>1){
    print("Error, had several sics being passed")
    print(stockSic)
    stop()
  }
  organizedStockData = unorganizedStockData[order(unorganizedStockData$datadate),]
  organizedStockData$div[is.na(organizedStockData$div)] = 0
  organizedStockData[,"marketCap"] = organizedStockData$cshoc*organizedStockData$prcod
  organizedStockData[,"laggedMarketCap"] = c(NA, organizedStockData$marketCap[-length(organizedStockData$marketCap)])
  organizedStockData[,"return"] = log((organizedStockData$marketCap + (organizedStockData$div*organizedStockData$cshoc))/organizedStockData$laggedMarketCap)
  
  #Now we trim the excess and rename things
  keep = c("datadate","marketCap","return","exchg")
  organizedStockData = organizedStockData[,(colnames(organizedStockData) %in% keep)]
  colnames(organizedStockData)[which(colnames(organizedStockData) == "datadate")] = "date"
  colnames(organizedStockData)[which(colnames(organizedStockData) == "exchg")] = paste0("ex_",stockSic)
  colnames(organizedStockData)[which(colnames(organizedStockData) == "marketCap")] = paste0("mC_",stockSic)
  colnames(organizedStockData)[which(colnames(organizedStockData) == "return")] = paste0("r_",stockSic)
  
  organizedStockData = merge(fullDates,organizedStockData,by="date",all = T)
  return(organizedStockData)
}

organizeData = function(unorganizedData){
  #Input - dataframe with the following columns: date, sic, prcod, div, cshoc,exchg
  #Output - dataframe with the following columns: date, sic_return,sic_marketCap,sic_exchangeCode
  #If stock information does not occur for a specific date, make it NA
  numberOfCores = detectCores()
  numberOfThreadsImplemented = floor(numberOfCores/1.5)
  unorganizedData$sic = paste0(unorganizedData$sic,"_",unorganizedData$iid)
  uniqueSics = unique(unorganizedData$sic)
  uniqueDates = data.frame(date = sort(unique(unorganizedData$datadate)))
  registerDoParallel()
  result = foreach (i = uniqueSics, .export = "getOrganizedStock") %dopar% {
    getOrganizedStock(unorganizedData[which(unorganizedData$sic == i),],uniqueDates)
  }
  stopImplicitCluster()
  result = result %>% reduce(inner_join,by="date")
  return(result)
}

isAStockWeird = function(returnData,name){
  name = substring(name,3)
  result = NA
  gapThreshold = 5
  toleranceOfGaps = 10
  toleranceOfObservations = 100
  
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
  columnsToBeDeleted = which(grepl(weirdStocks,colnames(sortedData)))
  print(columnsToBeDeleted)
  sortedData = sortedData[,-columnsToBeDeleted]
  return(sortedData)
}

getOrganizedData = function(unorganizedData){
  return(filterWeirdStocks(organizeData(unorganizedData)))
}
#**********************************************************************
#End of Functions
theOrganizedData = getOrganizedData(theData)
write_feather(theOrganizedData,"organizedData.feather")
