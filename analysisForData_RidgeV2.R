rm(list = ls(all=T))
library(feather)
cat("\014")
set.seed(100)
myXDataFrame = read_feather("xUsedForDataAnalysis.feather")
myX = as.matrix(myXDataFrame)
myYDataFrame = read_feather("yUsedForDataAnalysis.feather")
myY = as.matrix(myYDataFrame)
nOfSubgroups = 8
trainingAndCrossVariables = sort(sample(1:nrow(myX),round((nOfSubgroups-1)*nrow(myX)/nOfSubgroups,0)))
performanceVariables = sort(setdiff(1:nrow(myX),trainingAndCrossVariables))

#Now we choose many values of lambda, then for each value train the model with many initial starting weights
#and glean the best starting weight by seeing which has the lowest error rate
#then we have the best fit models compete with the crossValidation data set and pick the best.
#Then we see how that performs on the performance data set.

lambdas = exp(seq(-10,10,.05))

performanceOfWeights = function(w,x,y){
  averageSquaredError = mean((y-x%*%w)^2)
  return(averageSquaredError)
}

averageMeanSquaredError = function(y,y_predict){
  return(mean((y-y_predict)^2))
}
library(glmnet)

#Now we will implement the ridge regression with the same spectrum of lambdas again utilizing cross validation

bestAMSE = exp(600)
optimalLambda = -1
optimalRidge = 1

trials = 10000
for (i in 1:trials) {
  cvOutput =  cvOutput = cv.glmnet(myX[trainingAndCrossVariables,],myY[trainingAndCrossVariables,],nfolds = (nOfSubgroups-1),lambda = lambdas,alpha=0)
  bestLambda = cvOutput$lambda.min
  bestRidge = glmnet(myX[trainingAndCrossVariables,],myY[trainingAndCrossVariables,],lambda = bestLambda,alpha=0)
  myPrediction = predict(bestRidge,s=bestLambda,newx = as.matrix(myX[performanceVariables,]))
  AMSE = averageMeanSquaredError(myY[performanceVariables,],myPrediction)
  if(AMSE<bestAMSE){
    bestAMSE = AMSE 
    optimalLambda = bestLambda
    optimalRidge = bestRidge
  }
  if(i%%100 == 0){print(i)}
}
optimalWeights = optimalRidge$beta@x
optimalPredict = predict(optimalRidge,myX[performanceVariables,])
optimalErrors = averageMeanSquaredError(myY[performanceVariables,],optimalPredict)
optimalErrors
bestAMSE

tiddyResults = as.data.frame(matrix(0,ncol=(length(optimalWeights)+2),nrow=1))
tiddyResults[1,1:length(optimalWeights)] = optimalWeights
tiddyResults[1,length(optimalWeights)+1] = optimalLambda
tiddyResults[1,length(optimalWeights)+2] = optimalErrors
colnames(tiddyResults) = colnames(read_feather("LASSO_result.feather"))
write_feather(tiddyResults,"Ridge_results_10000.feather")