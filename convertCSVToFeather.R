rm(list = ls(all=T))

onBigComputer = T

if(onBigComputer){
  setwd("C:/Users/TAM/OneDrive - UW-Madison/Classes/2020_S/970 Investments/Research Project VIX")
}else{setwd("C:/Users/16083/OneDrive - UW-Madison/Classes/2020_S/970 Investments/Research Project VIX")}

cat("\014")
library(feather)
csvFileName = "stockData"
csvToBeConverted =read.csv(paste(c(csvFileName,".csv"),collapse = "",sep=""))
#write_feather(csvToBeConverted,paste(c(csvToBeConverted,".feather"),sep="",collapse=""))

write_feather(data.frame(date = runif(10000)),"yes.feather")
write_feather(csvToBeConverted,"stockData.feather")