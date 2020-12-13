rm(list = ls(all=T))
library(feather)
cat("\014")
set.seed(100)
myXDataFrame = read_feather("xUsedForDataAnalysis.feather")
myX = as.matrix(myXDataFrame)
myYDataFrame = read_feather("yUsedForDataAnalysis.feather")
myY = as.matrix(myYDataFrame)
nOfSubgroups = 8
trainingVariables = sort(sample(1:nrow(myX),round((nOfSubgroups-1)*nrow(myX)/nOfSubgroups,0)))
performanceVariables = sort(setdiff(1:nrow(myX),trainingVariables))

averageMeanSquaredError = function(y,y_predict){
  return(mean((y-y_predict)^2))
}

#Now we choose many values of lambda, then for each value train the model with many initial starting weights
#and glean the best starting weight by seeing which has the lowest error rate
#then we have the best fit models compete with the crossValidation data set and pick the best.
#Then we see how that performs on the performance data set.

myOLS = lm(myY[trainingVariables,]~myX[trainingVariables,])
myCoeff = myOLS$coefficients
optimalWeights = myCoeff[-1]
myPrediction = myCoeff[1]+myX[performanceVariables,]%*%myCoeff[-1]

AMSE = averageMeanSquaredError(myY[performanceVariables,],myPrediction)


tiddyResults = as.data.frame(matrix(0,ncol=(length(optimalWeights)+2),nrow=1))
tiddyResults[1,1:length(optimalWeights)] = optimalWeights
tiddyResults[1,length(optimalWeights)+1] = 0
tiddyResults[1,length(optimalWeights)+2] = AMSE
colnames(tiddyResults) = colnames(read_feather("LASSO_result.feather"))
write_feather(tiddyResults,"OLS_result.feather")

temp = read_feather("Ridge_results_10000.feather")
temp2 = read_feather("LASSO_result_10000.feather")
stichedTogether = rbind(temp2,temp,tiddyResults)
row.names(stichedTogether) = c("LASSO","Ridge","OLS")
write_feather(stichedTogether,"allThreeResults.feather")