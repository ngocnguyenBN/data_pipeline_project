# ==================================================================================================
# CCPR_INTRO.R - 2023-10-04 06:16
# ==================================================================================================

.libPaths()
rm(list = ls())
cat("\014")
gc(reset=T)

options(scipen=999)
options(verbose=F)
options(knitr.kable.NA = '.')
options(warn=-1)


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
requiredPackages = c("stringr","rjson", "foreign","data.table", "RMySQL","RCurl","TTR", "httr",
                     "gdata", "tableHTML", "textclean", "rvest", "XML", "Rcrawler","knitr",
                     "BatchGetSymbols", "Quandl","anytime", "quantmod", "dplyr", "tibble", "base64enc",
                     "lubridate", "readxl", "outliers", "openxlsx", "lettercase", "countrycode",
                     "WDI", "tidyr", "vietnameseConverter", 'yfR', 'kableExtra', 'formattable')
# "RBarchart", 

for (p in requiredPackages) {
  print(p, quote=F); cat("-----------------------------------------------------------------------","\n")
  if (!require(p,character.only = TRUE)) install.packages(p, quiet = T)
  library(p,character.only = TRUE, quietly = T)
}

source("c:/R/CONFIG.r", echo=T)
print(ODLibrary)
print(CCPRData)
print(CCPRDrive)
