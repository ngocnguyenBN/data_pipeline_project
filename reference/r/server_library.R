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
             Check_Compatibility = all(c('stk_code', 'date', 'rt') %in% names(My_STK)) 
             if (Check_Compatibility) { My_STK_selection = My_STK[, .(stk_code, date, rt, rt_index=rt)]
             }
           },
           'CWPR' = { 
             Check_Compatibility = all(c('stk_code', 'date', 'rt', 'capi_index') %in% names(My_STK)) 
             if (Check_Compatibility) { My_STK_selection = My_STK[, .(stk_code, date, rt, rt_index=rt, capi_index)]
             }
           },
           'FWPR' = { 
             Check_Compatibility = all(c('stk_code', 'date', 'rt', 'fcapi_index') %in% names(My_STK)) 
             if (Check_Compatibility) { My_STK_selection = My_STK[, .(stk_code, date, rt, rt_index=rt, fcapi_index)]
             }
           },
           'EWTR' = { 
             Check_Compatibility = all(c('stk_code', 'date', 'rtd') %in% names(My_STK)) 
             if (Check_Compatibility) { My_STK_selection = My_STK[, .(stk_code, date, rtd, rt_index=rtd)]
             }
           },
           'CWTR' = { 
             Check_Compatibility = all(c('stk_code', 'date', 'rtd', 'capi_index') %in% names(My_STK)) 
             if (Check_Compatibility) { My_STK_selection = My_STK[, .(stk_code, date, rtd, rt_index=rtd, capi_index)]
             }
           },
           'FWTR' = { 
             Check_Compatibility = all(c('stk_code', 'date', 'rt', 'fcapi_index') %in% names(My_STK)) 
             if (Check_Compatibility) { My_STK_selection = My_STK[, .(stk_code, date, rtd, rt_index=rtd, fcapi_index)]
             }
           }
    )
    
    if (Check_Compatibility)
    {
      # Check Avaibility
      CATln('Check Avaibility ...')
      List_Exist = unique(My_STK_selection$stk_code)
      List_intersect = intersect(List_codes, List_Exist)
      if (ToForce || (length(List_intersect)==length(List_codes)))
      {
        CATln_Border('All Data - EXISTS')
        Check_Avaibility = T
        My_STK_selection = My_STK_selection[stk_code %in% List_intersect]
        My.Kable.TB(My_STK_selection)
      } else {
        Check_Avaibility = F
      }
      
      
    }
    switch(IndexFormula,
           'EWPR' = { Check_Compatibility = all(c('stk_code', 'date', 'rt') %in% names(My_STK)) },
           'CWPR' = { Check_Compatibility = all(c('stk_code', 'date', 'rt', 'capi_index') %in% names(My_STK)) },
           'FWPR' = { Check_Compatibility = all(c('stk_code', 'date', 'rt', 'fcapi_index') %in% names(My_STK)) }
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
# ==================================================================================================
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
# ==================================================================================================
UPDATE_CCPR_STKVN_DAY = function()
{
  # UPDATE_CCPR_STKVN_DAY ()
  # ins_ref  = readRDS(paste0(CCPRData, 'efrc_ins_ref.rds'))
  step      = 1
  File_Name = 'CCPR_STKVN_DAY_MARKET.rds'
  File_Data = readRDS(paste0(CCPRData, File_Name))
  My.Kable(File_Data)
  if (step == 1) { Final_Data = File_Data}
  Final_Data = merge(Final_Data[, -c('name')], ins_ref[, .(code, name=short_name)], all.x=T, by='code')
  
  
  step      = 2
  File_Name = 'CCPR_STKVN_DAY_SECTOR_INDUSTRY.rds'
  File_Data = readRDS(paste0(CCPRData, File_Name))
  My.Kable(File_Data)
  My.Kable(File_Data[ticker=='VSI'])
  
  Final_Data = merge(Final_Data[, -c('sector', 'industry')], File_Data[, .(code, sector, industry)], all.x=T, by='code')
  My.Kable(Final_Data)
  
  step      = 3
  File_Name = 'CCPR_STKVN_DAY_SHARES.rds'
  File_Data = readRDS(paste0(CCPRData, File_Name))
  My.Kable(File_Data)
  
  Final_Data = merge(Final_Data[, -c('sharesout')], File_Data[, .(code, sharesout)], all.x=T, by='code')
  My.Kable(Final_Data)
  
  step      = 4
  File_Name = 'DOWNLOAD_SSI_STKVN_REF.rds'
  File_Data = readRDS(paste0(CCPRData, File_Name))
  My.Kable(File_Data[, .(code, date, refprice, last=close)])
  # str(File_Data)
  
  Final_Data = merge(Final_Data[, -c('reference', 'last')], File_Data[, .(code, reference=refprice, last=close)], all.x=T, by='code')
  My.Kable(Final_Data, Nb=20)
  My.Kable(Final_Data[market %in% list('HSX', 'HNX')], Nb=20)
  
  SAVERDS_WITH_ONEDRIVE(Final_Data, CCPRData, paste0('CCPR_STKVN_DAY_', gsub('-','',Sys.Date()),'.rds'), SaveOneDrive = T)
  rm(FN_Name_BATCH_FUNCTION)
  FN_Name_BATCH_FUNCTION    = match.call()[[1]]
  FINAL_BATCH_FUNCTION(FNName=FN_Name_BATCH_FUNCTION)
}
# ==================================================================================================
CCPR_INDEX_CALCULATION_FROM_LIST = function (pCodes , pSector, IndexFormula, IndexName, CodeName, StartDate, BaseValue, MaxRt)
{
  My_STK_Final = readRDS( paste0(CCPRData, 'CCPR_STKVN_4INDEX.rds'))
  # data = My_STK_Final [code %in% pCodes]
  NbCarLine =125
  if (pCodes != ''){
    My_List = pCodes } else {My_List = pSector}
  print( paste('CALCULATE:',paste(  My_List, collapse = ',')))
  My_Indexes = CCPR_INDEX_CALCULATION (My_STK_Final, List_codes = My_List , IndexFormula , StartDate, IndexName, Sector=CodeName)
  My.Kable(My_Indexes)
  FullPath_Local = paste0(CCPRData, 'ccpr_indvn_list.rds')
  FullPath_ODDrive = paste0(ODDrive, 'ccpr_indvn_list.rds')
  if (!file.exists(FullPath_Local)) {
    Data_Old = data.table()
  } else {
    Data_Old = CCPR_READ_RDS_WITH_ALTERNATIVE(FileFrm = FullPath_Local, FileAlt = FullPath_ODDrive)[[1]]
  }
  Data_MYPC = rbind(Data_Old, My_Indexes, fill = T)
  Data_MYPC = unique(Data_MYPC[order(code,date)], by = c('code', 'date'))
  SAVERDS_WITH_ONEDRIVE(Data_MYPC, CCPRData, 'ccpr_indvn_list.rds', SaveOneDrive = F)
  return (Data_MYPC)
}

# ==================================================================================================
SAVERDS_WITH_ONEDRIVE = function(pData=My_Data, FileFolder=CCPRData, FileName='download_ssi_stkvn_ref_history.rds' , SaveOneDrive=T, ToKable=F, ToSummary=F, ToPrompt=T)
{
  if (ToKable) { My.Kable.MaxCols(pData) }
  ODDrive_Data = paste0(ODDrive, substr(FileFolder,1,1), word(FileFolder,2,2,":"))
  if (!file.exists(ODDrive_Data))
  {
    try(dir.create(ODDrive_Data, recursive=T))
  }
  FullPath_Local   = paste0(FileFolder, FileName)
  FullPath_ODDrive = paste0(ODDrive_Data, FileName)
  if (ToPrompt) { CATln_Border(paste('SAVE LOCAL = ', FullPath_Local)) }
  if (ToPrompt) { CATrp('Saving ...') }
  saveRDS(pData, FullPath_Local)
  if (ToSummary) { try(CCPR_WRITE_SUMMARY_CURRENT(pData=pData, pFileName=FullPath_Local, ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)) }
  if (ToPrompt) { CATln('Done.') }
  
  if (SaveOneDrive)
  {
    if (ToPrompt) { CATln_Border(paste('SAVE ONEDRIVE = ', FullPath_ODDrive)) }
    if (ToPrompt) { CATrp('Saving ...') }
    saveRDS(pData, FullPath_ODDrive)
    if (ToSummary) { try(CCPR_WRITE_SUMMARY_CURRENT(pData=pData, pFileName=FullPath_ODDrive, ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)) }
    if (ToPrompt) { CATln('Done.') }
  }
}
# ==================================================================================================
CCPR_READ_RDS_WITH_ALTERNATIVE = function(FileFrm, FileAlt='')
{
  FileOK = F
  NameFileOK = ''
  xData  = data.table()
  if (file.exists(FileFrm))
  {
    xData = try(readRDS(FileFrm))
    if (all(class(xData)=='try-error'))
    {
      if (nchar(FileAlt)>0 && file.exists(FileAlt))
      {
        xData = try(readRDS(FileAlt))
        if (all(class(xData)!='try-error'))
        {
          FileOK = T
          NameFileOK = FileAlt
        }
      }
    } else {
      FileOK = T
      NameFileOK = FileFrm
    }
    
  } else {
    if (file.exists(FileAlt))
    {
      xData = try(readRDS(FileAlt))
      if (all(class(xData)!='try-error'))
      {
        FileOK = T
        NameFileOK = FileFrm
      }
    }
  }
  CATln_Border(paste('CCPR_READ_RDS_WITH_ALTERNATIVE, ok = ', NameFileOK, '-', substr(Sys.time(),1,19)))
  return(list(xData, FileOK))
}

#===================================================================================================
CCPR_WRITE_SUMMARY_CURRENT = function(pData=data.table(), pFileName, ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T) {
  # ------------------------------------------------------------------------------------------------
  # xs = WRITE_SUMMARY(UData, "efrc_indexp_history.rds")
  if (nrow(pData)>0)
  {
    x.rds = pData
  } else {
    x.rds     = try(readRDS(pFileName))
  }
  
  if (all(class(x.rds)!='try-error') && nrow(x.rds)>0)
  {
    if (ToPrompt) {My.Kable.MaxCols(x.rds)}
    if (ToPrompt) {str(x.rds)}
    if (!"codesource" %in% colnames(x.rds) & "ticker" %in% colnames(x.rds)) { 
      x.rds[, codesource:=as.character(ticker)]} 
    if (!"codesource" %in% colnames(x.rds)) { x.rds[, codesource:=NA]} else{ x.rds[, codesource:=as.character(codesource)]}
    if (!"source" %in% colnames(x.rds)) { x.rds[, source:=NA]} else{ x.rds[, source:=as.character(source)]}
    if (!"close" %in% colnames(x.rds)) { x.rds[, close:=as.numeric(NA)]}
    if ("code" %in% colnames(x.rds) & "codesource" %in% colnames(x.rds) & nrow(x.rds[!is.na(code)])==0) { 
      x.rds[, code:=as.character(codesource)]} 
    
    setkey(x.rds, code, date)
    
    # my.data[,":="(nbdays=append(NA,diff(date))),by=list(code)]
    if (ToExclude)
    {  my.summary = x.rds[date<=Sys.Date(),.(start=date[1], date=date[.N], nb=.N, datelast=date[.N], last=close[.N], 
                                             nbdays=as.numeric(NA), source=source[.N], name="", type="", 
                                             codesource=codesource[.N]), by=code]} else {
                                               my.summary = my.data[,.( 
                                                 start=date[1], date=date[.N], nb=.N, datelast=date[.N], 
                                                 last=close[.N], nbdays=as.numeric(NA), source=source[.N],
                                                 name="", type="", codesource=codesource[.N]), by=code]}
    my.summary = my.summary[order(-date, code)]
    if (ToPrompt) {My.Kable.TB(my.summary)}
    
    my.FileTXT  = gsub(".rds", "_summary.txt", pFileName)
    my.FileTXT
    my.summary = my.summary[order(-date)]
    my.summary = merge(my.summary[, -c("name", "type", "fcat")], ins_ref[, .(code, name=short_name, type, fcat)], by='code', all.x=T)
    # my.summary = transform(my.summary, type=ins_ref$type[match(code, ins_ref$code)])
    
    # if (AddName) { 
    #   my.summary = transform(my.summary, name=ins_ref$short_name[match(code, ins_ref$code)])
    #   }
    if (ToPrompt) {My.Kable.TB(my.summary)}
    if (ToRemoveCodeNA) { my.summary= my.summary[!is.na(code)] }
    fwrite(my.summary, my.FileTXT, col.names = T, sep="\t", quote = F)
    
    DATACENTER_LINE_BORDER(paste(my.FileTXT, ":", 
                                 min(my.summary[!is.na(start)]$start), 'to', max(my.summary[!is.na(date)]$date),
                                 ">>>", trimws(FormatNS(nrow(my.summary),0,12,"")), "/", 
                                 trimws(FormatNS(sum(my.summary$nb),0,16,"")), " = at", substr(as.character(Sys.time()),12,16)))
    return(my.summary)
  }
} 


CCPR_INDEX_CALCULATION = function (My_STK_Final, List_codes = My_List , IndexFormula='EWPR' , StartDate, IndexName, Sector)
{
  
  # My_STK_Final = readRDS( paste0(CCPRData, 'CCPR_STKVN_4INDEX.rds'))
  
  My_STK_Final$stk_code = My_STK_Final$code
  My_STK_Final$capi_index = as.numeric(My_STK_Final$capi)
  My_STK_Final = My_STK_Final[!is.na(capi_index)]
  My_Indexes_Data = DATA_INDEX_FORMULA(StkData=My_STK_Final, FileData='', IndexFormula, List_codes, ToForce=T) 
  # My_STK_Final [code == 'STKVNVSM']
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
# ==================================================================================================
CATln = function(l.Str, top_border=F, p_border=F, pNbCar=120) {
  # ------------------------------------------------------------------------------------------------
  if (top_border) {catrep('=',pNbCar)}
  cat("\r", paste(l.Str, strrep(" ",max(0,pNbCar - nchar(l.Str)))), "\n")
  if (p_border) {catrep('-',pNbCar)}
}
# ==================================================================================================
catrep = function(l.char, n.times) {
  # ------------------------------------------------------------------------------------------------
  cat("\r", strrep(l.char, n.times), "\n")
}
# ==================================================================================================
My.Kable.MaxCols = function(pData=dt_to, pMax=15, Nb=3, HeadOnly=F) {
  # ------------------------------------------------------------------------------------------------
  if (nrow(pData)>0)
  {
    if (ncol(pData)>pMax) { My.Kable(pData[, 1:pMax], Nb=Nb, HeadOnly = HeadOnly)} else {
      My.Kable(pData, Nb=Nb, HeadOnly = HeadOnly)
    } 
  }
}
# ==================================================================================================
My.Kable = function(l.Table, Nb=3, l.Text="", HeadOnly=F, ClearScreen=F) {
  # ------------------------------------------------------------------------------------------------
  # l.Table = x.ins
  if (ClearScreen) { cat("\014")}
  cat("\r\r")
  if (nrow(l.Table)>0)
  {
    if (nchar(l.Text)>0) { cat(l.Text) }
    print(kable(format.args = list(decimal.mark = ".", big.mark = ","),
                head(l.Table, min(Nb,nrow(l.Table))), format="markdown"))
    # cat("")
    if (! HeadOnly)
    {
      print(kable(format.args = list(decimal.mark = ".", big.mark = ","),
                  tail(l.Table, min(Nb,nrow(l.Table))), format="markdown"))
    }
    # print("", quote=F)
    print(paste(Format.Number(nrow(l.Table),0), "records"), quote = F)
  } else
  { print("No data ...", quote=F)}
}

# ==================================================================================================
My.Kable.TB = function(l.Table, HeadOnly=F, Nb=3, l.Text="") {
  # ------------------------------------------------------------------------------------------------
  # l.Table =
  # library(knitr)
  # l.Text=""; Nb=3
  # l.Table = readRDS("u:/efrc/data/efrc_indipo_history.rds")
  cat("\r\r")
  if (nrow(l.Table)>0)
  {
    if (!HeadOnly) {l.Table.TB = rbind( head(l.Table, min(Nb,nrow(l.Table))), tail(l.Table, min(Nb,nrow(l.Table))), fill=T)} else {
      l.Table.TB = head(l.Table, min(Nb,nrow(l.Table))) }
    if (nchar(l.Text)>0) { cat(l.Text,"\n") }
    print(kable(format.args = list(decimal.mark = ".", big.mark = ","), l.Table.TB, format="markdown"))
    print(paste(Format.Number(nrow(l.Table),0), "records"), quote = F)
  } else
  { print("No data ...", quote=F)}
}
# ==================================================================================================
CATrp = function(l.Str) {
  # ------------------------------------------------------------------------------------------------
  cat("\r", paste(l.Str, strrep(" ",max(0,120-nchar(l.Str)))), "")
}
# ==================================================================================================
CATln_Border = function(pStr) {
  # ------------------------------------------------------------------------------------------------
  CATln(pStr, top_border = T, p_border = T) 
}