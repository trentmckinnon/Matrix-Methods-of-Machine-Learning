rm(list = ls(all=T))
library(feather)
cat("\014")
results = read_feather("allThreeResults.feather")
results = rbind(results,results[1,]-results[2,],results[1,]-results[3,],results[2,]-results[3,])
row.names(results) = c("LASSO","Ridge","OLS","Delta_LASSO_Ridge","Delta_LASSO_OLS","Delta_Ridge_OLS")
write.csv(results,"finalResults.csv")

realY = as.matrix(read_feather("yUsedForDataAnalysis.feather"))
realX = as.matrix(read_feather("xUsedForDataAnalysis.feather"))

w_ols = matrix(as.numeric(results[3,-c(51,52)]),ncol=1)
w_Ridge = matrix(as.numeric(results[2,-c(51,52)]),ncol=1)
w_LASSO = matrix(as.numeric(results[1,-c(51,52)]),ncol=1)


OLS_y = realX%*%w_ols
Ridge_y = realX%*%w_Ridge
LASSO_y = realX%*%w_LASSO+.23

fullYs = cbind(realY,OLS_y,Ridge_y,LASSO_y)

nPointsToSkip = 35

myTime = (seq(1,nrow(OLS_y),nPointsToSkip))
plot(myTime,fullYs[myTime,1], pch=19,col=1,ylim = c(-.5,.5),xlab = "Index",ylab="y_values")
points(myTime, fullYs[myTime,2],pch=19, col=2)
points(myTime, fullYs[myTime,3],pch=19, col=3)
points(myTime, fullYs[myTime,4],pch= 21, col=9)

