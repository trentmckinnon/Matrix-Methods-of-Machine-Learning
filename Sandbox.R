rm(list = ls(all=T))
cat("\014")

myClass = setClass("myClass",slots = c(vix="data.frame",stock_div = "data.frame",marketCap_Stocks = "data.frame",dim_subset = "data.frame",
                                       eq_w_ret_all = "data.frame",v_w_ret_all = "data.frame",marketCap_all = "data.frame",
                                       eq_w_ret_subset = "data.frame",v_w_ret_subset = "data.frame",marketCap_subset="data.frame"))
subsetClass = setClass("subsetClass",slot = c(bottom = "numeric",top = "numeric"))

setNormalVariables = function(theClass){
  #Fill
  #set eq_w_ret_all, v_w_ret_all, marketCap_all
  #this can just be done using general getSubsetVariables but only use inf, -inf as bounds
  #Rename the columns
  return(theClass)
}
setSubsetVariables = function(theClass){
  #Fill
  #set 
  #Create 3 general dataframes
  #One for eq_w_ret_subset, v_w_ret_subset, and MarketCap_subset
  eq_w_ret_subset = data.frame()
  v_w_ret_subset = data.frame()
  marketCap_subset = data.frame()
  #loop through twice 54,53,52,51,43,42....and make sure to have top and bottom be related which is greater
  subsetThresholds = theClass@dim_subset
  n_thresholds = nrow(subsetThresholds)
  for (i in 1:(n_thresholds-1)) {
    for (j in (i+1):n_thresholds) {
      currentSubset = c(subsetThresholds[i,1],subsetThresholds[j,1]) #this makes a vector like [.1,.0125]
      #Make sure it is in order from low to high
      currentSubset = sort(currentSubset)
      tempOutput = getSubsetVariables(theClass,bottom = currentSubset[1],top = currentSubset[2])
      eq_w_ret_subset = merge(eq_w_ret_subset,tempOutput[[1]])
      v_w_ret_subset = merge(v_w_ret_subset,tempOutput[[2]])
      marketCap_subset = merge(marketCap_subset,tempOutput[[3]])
    }
  }
  #use a function getSubsetVariables: input:(theClass,bottom, top); output:list of (3 dataframes with propperly named columns-eq,v,marketCap)
  #Merge with general dataframe
  theClass@eq_w_ret_subset = eq_w_ret_subset
  theClass@v_w_ret_subset = v_w_ret_subset
  theClass@marketCap_subset = marketCap_subset
  #After looping set theClass's variable
  return(theClass)
}
getSubsetVariables = function(theClass,bottom,top){
  subsetGeneralName = paste0(bottom,"_",top)
  #need to first get a the dates we will be looking at
  #after get dates will parallelize a function that 
}
setValues = function(theClass){
  #need a function for all these subvariables, all take the form of input:myClass, output:myClass_updated
  theClass = setNormalVariables(theClass)
  theClass = setSubsetVariables(theClass)
}