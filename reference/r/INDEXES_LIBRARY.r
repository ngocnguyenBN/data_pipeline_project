# UPDATE : 2023-07-13 09:37

# options(install.packages.check.source = "no")
# install.packages('yfR')

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
requiredPackages = c("stringr","rjson", "foreign","data.table", "RMySQL","RCurl","TTR", "httr",
                     "gdata", "tableHTML", "textclean", "rvest", "XML", "Rcrawler","knitr",
                     "BatchGetSymbols", "Quandl","anytime", "quantmod", "dplyr", "tibble", "base64enc",
                     "lubridate", "readxl", "outliers", "openxlsx", "lettercase", "googlesheets4", "countrycode",
                     "WDI", "tidyr", "vietnameseConverter", "ChineseNames", "yfR")
# "RBarchart", 
for (p in requiredPackages) {
  print(p, quote=F); cat("-----------------------------------------------------------------------","\n")
  if (!require(p,character.only = TRUE)) install.packages(p, quiet = T)
  library(p,character.only = TRUE, quietly = T)
}

options(warn=-1)

My.HeadTail = function(pData)
{
  print(kable(head(pData)))
  print(kable(tail(pData)))
  cat('')
  print(nrow(pData))
}
#=================================================================================================
#FUNCTION MAIN START
#=================================================================================================

# ==================================================================================================
DATA_INDEX_FORMULA = function(StkData = My_STK_Final, FileData, IndexFormula='EWPR', List_codes = list('STKIBM', 'STKAAPL'), ToForce=F) {
  # ------------------------------------------------------------------------------------------------
  My_STK_selection = data.table()
  # FileData = FileName
  if (nchar(FileData)>0)
  {
    CATln('Loading ...')
    My_STK = try(readRDS(FileData))
  } else { My_STK = try(StkData) }
  
  # IndexFormula='EWPR'
  # IndexFormula='CWPR'
  # IndexFormula='FWPR'
  
  if (all(class(My_STK)!='try-error'))
  {
    CATln_Border('Load Data - DONE')
    
    My.Kable.MaxCols(My_STK)
    CATln('Check Compatibility ...')
    # Check Compatibility
    switch(IndexFormula,
           'EWPR' = { 
             Check_Compatibility = all(c('code', 'date', 'rt') %in% names(My_STK)) 
             if (Check_Compatibility) { My_STK_selection = My_STK[, .(code, date, rt, rt_index=rt)]
             }
           },
           'CWPR' = { 
             Check_Compatibility = all(c('code', 'date', 'rt', 'capi_index') %in% names(My_STK)) 
             if (Check_Compatibility) { My_STK_selection = My_STK[, .(code, date, rt, rt_index=rt, capi_index)]
             }
           },
           'FWPR' = { 
             Check_Compatibility = all(c('code', 'date', 'rt', 'fcapi_index') %in% names(My_STK)) 
             if (Check_Compatibility) { My_STK_selection = My_STK[, .(code, date, rt, rt_index=rt, fcapi_index)]
             }
           },
           'EWTR' = { 
             Check_Compatibility = all(c('code', 'date', 'rtd') %in% names(My_STK)) 
             if (Check_Compatibility) { My_STK_selection = My_STK[, .(code, date, rtd, rt_index=rtd)]
             }
           },
           'CWTR' = { 
             Check_Compatibility = all(c('code', 'date', 'rtd', 'capi_index') %in% names(My_STK)) 
             if (Check_Compatibility) { My_STK_selection = My_STK[, .(code, date, rtd, rt_index=rtd, capi_index)]
             }
           },
           'FWTR' = { 
             Check_Compatibility = all(c('code', 'date', 'rt', 'fcapi_index') %in% names(My_STK)) 
             if (Check_Compatibility) { My_STK_selection = My_STK[, .(code, date, rtd, rt_index=rtd, fcapi_index)]
             }
           }
    )
    
    if (Check_Compatibility)
    {
      # Check Avaibility
      CATln('Check Avaibility ...')
      List_Exist = unique(My_STK_selection$code)
      List_intersect = intersect(List_codes, List_Exist)
      if (ToForce || (length(List_intersect)==length(List_codes)))
      {
        CATln_Border('All Data - EXISTS')
        Check_Avaibility = T
        My_STK_selection = My_STK_selection[code %in% List_intersect]
        My.Kable.TB(My_STK_selection)
      } else {
        Check_Avaibility = F
      }
      
      
    }
    switch(IndexFormula,
           'EWPR' = { Check_Compatibility = all(c('code', 'date', 'rt') %in% names(My_STK)) },
           'CWPR' = { Check_Compatibility = all(c('code', 'date', 'rt', 'capi_index') %in% names(My_STK)) },
           'FWPR' = { Check_Compatibility = all(c('code', 'date', 'rt', 'fcapi_index') %in% names(My_STK)) }
    )
  }
  return(My_STK_selection)
}

# ==================================================================================================
CALCULATE_INDEX_WEIGHTING = function(StkPriceCleaned, IndexFormula='EWPR', StartDate='2008-12-31', BaseValue=1000, MaxRt=0.50) {
  # ------------------------------------------------------------------------------------------------
  # StkPrice = My_Indexes_Data
  Start.Calcul = Sys.time()
  StkPrice = StkPriceCleaned[date>=StartDate]
  My.Kable(StkPrice)
  Weighting = substr(IndexFormula,1,2)
  switch(Weighting,
         'EW' = {
           StkPrice[is.na(rt) , rt:=0]
           StkPrice[!is.na(rt) & abs(rt)>MaxRt , rt:=0]
           
           IndexValue = StkPrice[, .(n=.N, mrt=mean(rt, na.rm=T),sumrt=sum(rt, na.rm=T), sumxrt=mean(rt, na.rm=T)), by=c('date')]
           IndexValue = IndexValue[, index_value:=BaseValue] 
           IndexValue = IndexValue [order(date)]
           CurrentIdx = BaseValue*1.00000
           for (i in 2:nrow(IndexValue))
           {
             CurrentIdx = CurrentIdx*(1+IndexValue[i]$mrt)
             IndexValue[i]$index_value = CurrentIdx
           }
           # IndexValue[, weighting:=Weighting]
           # My.Kable(IndexValue)
         },
         'CW' = {        
           # Weighting='CW'
           StkPrice[is.na(rt) , rt:=0]
           StkPrice[!is.na(rt) & abs(rt)>MaxRt , rt:=0]
           
           IndexValue = StkPrice[, .(n=.N, scapi=sum(capi_index, na.rm=T), mrt=mean(rt, na.rm=T),
                                     sumrt=sum(rt, na.rm=T), sumcapixrt=sum(capi_index*rt, na.rm=T)), by='date']
           IndexValue[, index_value:=BaseValue] #[order(date)]
           IndexValue = IndexValue [order(date)]
           CurrentIdx = BaseValue*1.00000
           for (i in 2:nrow(IndexValue))
           {
             # i = 2
             CurrentIdx = CurrentIdx*(1+(IndexValue[i]$sumcapixrt/IndexValue[i]$scapi))
             IndexValue[i]$index_value = CurrentIdx
           }
           # IndexValue[, weighting:=Weighting]
           # My.Kable(IndexValue)
         },
         'FW' = {        
           # Weighting='CW'
           StkPrice[is.na(rt) , rt:=0]
           StkPrice[!is.na(rt) & abs(rt)>MaxRt , rt:=0]
           
           IndexValue = StkPrice[, .(n=.N, scapi=sum(fcapi_index, na.rm=T), mrt=mean(rt, na.rm=T),
                                     sumrt=sum(rt, na.rm=T), sumcapixrt=sum(fcapi_index*rt, na.rm=T)), by='date']
           IndexValue[, index_value:=BaseValue]
           
           CurrentIdx = BaseValue*1.00000
           for (i in 2:nrow(IndexValue))
           {
             CurrentIdx = CurrentIdx*(1+(IndexValue[i]$sumcapixrt/IndexValue[i]$scapi))
             IndexValue[i]$index_value = CurrentIdx
           }
           
         }
  )
  End.Calcul = Sys.time()
  if (nrow(IndexValue)>0)
  {
    CATln_Border(paste('CALCULATION OF INDEXES :', Weighting, StartDate,BaseValue, MaxRt))
    IndexValue[, weighting:=Weighting]
    IndexValue[, formula:= IndexFormula]
    My.Kable.TB(IndexValue)
    catrep('-', NbCarLine)
    CATln(paste('CALCULATION TIME =', Format.Number(difftime(End.Calcul, Start.Calcul, units='secs'),2)))
  }
  return(IndexValue)
}

CCPR_INDEX_CALCULATION = function (My_STK_Final, List_codes = My_List , IndexFormula='EWPR' , StartDate, IndexName, Sector)
{
  
  # My_STK_Final = readRDS( paste0(CCPRData, 'CCPR_STKVN_4INDEX.rds'))
  
  My_STK_Final$stk_code = My_STK_Final$code
  My_STK_Final$capi_index = as.numeric(My_STK_Final$capi)
  My_STK_Final = My_STK_Final[!is.na(capi_index)]
  My_Indexes_Data = DATA_INDEX_FORMULA(StkData=My_STK_Final, FileData='', IndexFormula, List_codes, ToForce=T) 
  My_Indexes_Data = My_Indexes_Data[order(stk_code, date)]
  My_Indexes      = CALCULATE_INDEX_WEIGHTING(StkPriceCleaned=My_Indexes_Data, IndexFormula, StartDate, BaseValue=1000, MaxRt=0.50)
  My_Indexes[, ind := paste0('INDVN',formula, Sector)]
  My_Indexes[, lnrt := log(index_value/shift(index_value))]
  Data_One = My_Indexes[,.(date, code = ind, close = index_value, rt =My_Indexes[,5], lnrt, name = paste(IndexName, formula )) ]
  My.Kable(Data_One)
  
  
  
  # FileName = 'CCPR_INDEXES_HISTORY.rds'
  # pFolder  = CCPRData
  # ODDrive_Data = paste0(ODDrive, substr(pFolder, 1, 1), word(pFolder, 2, 2, ":"))
  # CATln(ODDrive_Data)
  # 
  # if (!file.exists(ODDrive_Data))
  # {
  #   try(dir.create(ODDrive_Data))
  # }
  # 
  # if (!file.exists(pFolder))
  # {
  #   try(dir.create(pFolder))
  # }
  # # as.Date('2023-08-18 15:48:00')
  # FullPath_Local = paste0(pFolder, FileName)
  # FullPath_ODDrive = paste0(ODDrive_Data, FileName)
  # # To_Update
  # if (!file.exists(FullPath_Local)) {
  #   Data_Old = data.table()
  # } else {
  #   Data_Old = CCPR_READ_RDS_WITH_ALTERNATIVE(FileFrm = FullPath_Local, FileAlt = FullPath_ODDrive)[[1]]
  # }
  # Data_MYPC = rbind(Data_Old, Data_One, fill = T)
  # Data_MYPC = unique(Data_MYPC[order(code,date)], by = c('code', 'date'))
  # My.Kable(Data_MYPC)
  
  # try(SAVERDS_WITH_ONEDRIVE(pData=Data_MYPC, FileFolder= pFolder, FileName=FileName , SaveOneDrive=T))
  
  return (Data_One)
}
