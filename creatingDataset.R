#rm(list = ls(all=T))
cat("\014")
#Need to get the libraries if you don't have them
library(parallel)
library(foreach)
library(doParallel)
library(tidyverse)
library(feather)



filterOrganizedData = function(organizedData, dataFrameOfGVKEYsAndDates,ignoreFiltering){
  tempCol = 1:ncol(organizedData)
  if(!ignoreFiltering){
    GVKEYsIncluded = substring(colnames(dataFrameOfGVKEYsAndDates)[which(grepl("g_",colnames(dataFrameOfGVKEYsAndDates)))],3)
    tempCol = grep(paste(c("date",GVKEYsIncluded),collapse = "|"),colnames(organizedData))
  }
  return(organizedData[,tempCol])
}
makeStockIndexDatesNonDuplicated = function(stockInformation,datesToBeIncluded){
  result = data.frame(date = datesToBeIncluded)
  from = stockInformation$from
  thru = stockInformation$thru
  if(is.na(thru)){
    thru = 21001231
  }
  result[,as.character(paste0("g_",stockInformation$gvkey))] = NA
  result[which(datesToBeIncluded >= from & datesToBeIncluded <= thru ),2] = 1
  return(result)
}
makeStockIndexDatesDuplicated = function(stockInformation,datesToBeIncluded){
  result = data.frame(date = datesToBeIncluded)
  result[,as.character(paste0("g_",stockInformation$gvkey[1]))] = NA
  for (j in 1:nrow(stockInformation)) {
    from = stockInformation$from[j]
    thru = stockInformation$thru[j]
    if(is.na(thru)){
      thru = 21001231
    }
    result[which(datesToBeIncluded >= from & datesToBeIncluded <= thru ),2] = 1
  }
  return(result)
}
makeGVKEYDataset = function(indexData,datesToBeIncluded){
  result = 1
  #we have to deal with there being several times that a unique stock has been listed and taken off
  locationOfDuplicates = which(indexData$gvkey %in% indexData$gvkey[which(duplicated(indexData$gvkey))])
  listOfDuplicates = unique(indexData$gvkey[locationOfDuplicates])
  nonDuplicated = indexData$gvkey[-locationOfDuplicates]
  
  gvkeyDataset = data.frame(date = datesToBeIncluded)
  registerDoParallel()
  result_nonDup = foreach(i = nonDuplicated,.export = "makeStockIndexDatesNonDuplicated") %dopar%{
    makeStockIndexDatesNonDuplicated(indexData[which(indexData$gvkey == i),],datesToBeIncluded)
  }
  result_dup = foreach(i = listOfDuplicates,.export = "makeStockIndexDatesDuplicated")%dopar%{
    makeStockIndexDatesDuplicated(indexData[which(indexData$gvkey == i),],datesToBeIncluded)
  }
  stopImplicitCluster()
  result = c(result_nonDup,result_dup)
  result = result %>% reduce(inner_join,by="date")
  

  
  
  
  return(result)
}

theDatesToBeIncluded = (read_feather("organizedData_dates.feather")) #This allows for rapid reading while segregating the types of tasks used
theIndexData = read.csv("S&P500_companies.csv")

sp500_frame = makeGVKEYDataset(theIndexData,theDatesToBeIncluded)
write_feather(sp500_frame,"S&P500_dataFrame.feather")

