rm(list = ls(all=T))
library(feather)
cat("\014")
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

LASSO_score = function(w,x,y,lambda){
  score = sum((y-x%*%w)^2)+lambda*sum(abs(w))
  return(score)
}
performanceOfWeights = function(w,x,y){
  averageSquaredError = mean((y-x%*%w)^2)
  return(averageSquaredError)
}

averageMeanSquaredError = function(y,y_predict){
  return(mean((y-y_predict)^2))
}
library(glmnet)

cvOutput = cv.glmnet(myX[trainingAndCrossVariables,],myY[trainingAndCrossVariables,],nfolds = (nOfSubgroups-1),lambda = lambdas)

bestLambda = cvOutput$lambda.min
bestLASSO = glmnet(myX[trainingAndCrossVariables,],myY[trainingAndCrossVariables,],lambda = bestLambda)
bestAMSE = 1
#Now we will iterate this process using the same data/method to get the best error
trials = 10000
for (i in 1:trials) {
  cvOutput = cv.glmnet(myX[trainingAndCrossVariables,],myY[trainingAndCrossVariables,],nfolds = (nOfSubgroups-1),lambda = lambdas)
  bestLambda = cvOutput$lambda.min
  bestLASSO = glmnet(myX[trainingAndCrossVariables,],myY[trainingAndCrossVariables,],lambda = bestLambda)
  myPrediction = predict(bestLASSO,s=bestLambda,newx = as.matrix(myX[performanceVariables,]))
  AMSE = averageMeanSquaredError(myY[performanceVariables,],myPrediction)
  if(AMSE<bestAMSE){
    bestAMSE=AMSE
    theBestLambda = cvOutput$lambda.min
    theBestLASSO = glmnet(myX[trainingAndCrossVariables,],myY[trainingAndCrossVariables,],lambda = bestLambda)
    thePrediction = predict(bestLASSO,s=bestLambda,newx = as.matrix(myX[performanceVariables,]))
    test = bestAMSE
  }
  if(i%%10==0){print(i)}
}


myPrediction = predict(theBestLASSO,s=theBestLambda,newx = as.matrix(myX[performanceVariables,]))
myLambda_optimal = theBestLambda
myW_optimal = matrix(rep(0,ncol(myX)),ncol=1)
locationOfNonzeroW = theBestLASSO$beta@i+1
myW_optimal[locationOfNonzeroW,1]= theBestLASSO$beta@x
myError = myY[performanceVariables,]-myPrediction

nicelyFormattedInformation = as.data.frame(matrix(0,nrow=1,ncol=nrow(myW_optimal)+2))
nicelyFormattedInformation[1,1:nrow(myW_optimal)]=myW_optimal
nicelyFormattedInformation[1,nrow(myW_optimal)+1] = myLambda_optimal
nicelyFormattedInformation[1,nrow(myW_optimal)+2] = bestAMSE
colnames(nicelyFormattedInformation) = as.character(c(theBestLASSO$beta@Dimnames[[1]], "lambda_best","averageMeanSquaredError"))


