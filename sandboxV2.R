rm(list = ls(all=T))
cat("\014")
a = c("102","1025","121")
b = c("102","25","1")
d = charmatch(a,b)
d

pmatch("m",   c("mean", "median", "mode")) # returns NA
pmatch("me", c("mean", "median", "mode"),duplicates.ok = T)