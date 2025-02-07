# ==================================================================================================
# TRAINEE_INTRO.R - 2024-08-22 14:26
# ==================================================================================================

# .libPaths()
# rm(list = ls())
cat("\014")
gc(reset=T)

options(scipen=999)
options(verbose=F)
options(knitr.kable.NA = '.')
options(warn=-1)

# "lettercase",, 'formattable'
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
requiredPackages = c("stringr","rjson", "foreign","data.table", "RMySQL","RCurl","TTR", "httr",
                     "gdata", "tableHTML", "textclean", "rvest", "XML", "Rcrawler","knitr",
                     "BatchGetSymbols", "Quandl","anytime", "quantmod", "dplyr", "tibble", "base64enc",
                     "lubridate", "readxl", "outliers", "openxlsx",  "countrycode",
                     "WDI", "tidyr", "vietnameseConverter", 'yfR', 'kableExtra', 'httr', 'stringi', 'stringr', 'DBI', 'RMariaDB', 'DT','jsonlite','xml2',
                     'roll')
# "RBarchart", 

for (p in requiredPackages) {
  print(p, quote=F); cat("-----------------------------------------------------------------------","\n")
  if (!require(p,character.only = TRUE)){
    install.packages(p, quiet = T)
  }
  library(p,character.only = TRUE, quietly = T)
}

# # source("c:/R/TRAINEE_CONFIG.r", echo=T)
# print(ODDrive)
# print(ODLibrary)
# print(ODData)
SData     = 'S:/CCPR/DATA/'
SCCPRData = 'S:/CCPR/DATA/'

LIST_SOURCES_SHARES       = list('VST', 'STB', 'CAF', 'C68', 'VND')
LIST_SOURCES_PRICESBOARD  = list('VND','CAF',  'TVSI', 'VST', 'VPS', 'MBS', 'BSC', 'EXC', 'C68')
LIST_SOURCES_FREEFLOAT    = list( 'EXC','VND', 'DNSE', '24H')
LIST_SOURCES_DIVIDEND     = list( 'CAF','VST')
LIST_INDEX_NAMES          = list('LOGISTICS', 'BROKERAGE', 'PETROLIMEX', 'PETROVIETNAM', 'REALESTATE', 'RETAIL', 'BANK', 'HEALTHCARE', 'ENERGY', 'CLEAN ENERGY', 'FINANCE', 'AI')
LIST_PC_FOR_STKVN_REPORT  = list('BEQ-RD4', 'BEQ-RD2', 'BEQ-RD1', 'IFRC-IREEDS', 'BEQ-RD5', 'LOCAL', 'BEQ-RD3', 'BEQ-RD6', 'IFRC-LINUX', 'BEQ-INDEX', 'IFRC-TV', 'IFRC-MHM')
LIST_PC                   = list('BEQ-RD4', 'BEQ-RD2', 'BEQ-RD1', 'IFRC-IREEDS', 'BEQ-RD5', 'LOCAL', 'BEQ-RD3', 'BEQ-RD6', 'IFRC-LINUX', 'BEQ-INDEX', 'BEQ-RD7', 'IFRC-TV', 'IFRC-3630', 'IFRC-MHM', 'BEQ-RD5')
LIST_SOURCE_FOR_REPORT    = list('VND', 'MBS', 'VPS','TVSI', 'CAF', 'BSC','VST', 'C68', 'EXC', 'STB', 'DCL')
LIST_SOURCE_BOARD         = list('CAF', 'VST', 'VND')
LIST_FIRST_CHARS          = list('ABC', 'DEF', 'GHI', 'JKL', 'MNO', 'PQR', 'STU', 'VWXYZ')
LIST_WCEO                 =  list ('AUS' ,	'BEL' ,	'CHN' ,	'DEU', 	'FRA' ,	'HKG' ,	'IDN' ,	'IRL' ,	'ITA' ,	'JPN' ,	'MYS' ,	'NLD' ,	'NOR' ,	'PRT' ,	'SGP' ,	'THA', 	'TWN', 'USA')

assign("LIST_SOURCES_SHARES",      LIST_SOURCES_SHARES,      envir = .GlobalEnv)
assign("LIST_SOURCES_PRICESBOARD", LIST_SOURCES_PRICESBOARD, envir = .GlobalEnv)
assign("LIST_SOURCES_FREEFLOAT", LIST_SOURCES_FREEFLOAT, envir = .GlobalEnv)
assign("LIST_SOURCES_DIVIDEND", LIST_SOURCES_DIVIDEND, envir = .GlobalEnv)
assign("LIST_INDEX_NAMES", LIST_INDEX_NAMES, envir = .GlobalEnv)
assign("LIST_PC_FOR_STKVN_REPORT", LIST_PC_FOR_STKVN_REPORT, envir = .GlobalEnv)
assign("LIST_SOURCE_FOR_REPORT", LIST_SOURCE_FOR_REPORT, envir = .GlobalEnv)
assign("LIST_SOURCE_BOARD", LIST_SOURCE_BOARD, envir = .GlobalEnv)
assign("LIST_FIRST_CHARS", LIST_FIRST_CHARS, envir = .GlobalEnv)
assign("LIST_WCEO", LIST_WCEO, envir = .GlobalEnv)

ToUploadAll = T
UData = "U:/EFRC/DATA/"
# DBL_Upload = 'DEV'
DBL_Upload = 'PRD'

STK_MAXABSRT = 1
IND_MAXABSRT = 0.5
STRUCTURE_IND_COMPO = c("code_mother","ticker","code","name","market","date","shares","capibnvnd","close","symbol","index_code","index_name","sector",
                        "industry","wgt_pc","updated")

STRUCTURE_DAY_HISTORY = c("provider","iso2","country","continent","type","date","name","code","open","high","low","close",
                        "change","varpc", "rt", "scat", "isin", "timestamp","fcat","updated")

STRUCTURE_WORLD_INDEX = c("code","date","iso2","country","type","name","close","fcat","scat","isin","codesource","source",
                          "open","high", "low", "volume", "rt", "close_adj","change","varpc","continent",
                          "var" ,"yyyy","yyyymm","yyyyqn","mtd","qtd","ytd","M1","M2","M3","M4","M5","M6","Q1","Q2","Q3","Q4","Q5",             
                          "Q6" ,"Y1","Y2","Y3","Y4","Y5","Y6","cur","prtr","size","last","updated","version","wgtg" ,          
                          "short_name","excess", "symbol","provider", 
                          "risk_mtd","risk_qtd","risk_ytd","risk_M1","risk_M2","risk_M3","risk_M4","risk_M5","risk_M6",
                          "risk_Q1","risk_Q2","risk_Q3","risk_Q4","risk_Q5",             
                          "risk_Q6" ,"risk_Y1","risk_Y2","risk_Y3","risk_Y4","risk_Y5","risk_Y6","Y6_annualised")