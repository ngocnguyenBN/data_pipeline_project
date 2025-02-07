# UPDATE : 2023-09-12 15:52

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

# ==================================================================================================
# FUNCTION MAIN START 2023-08-18 17:29
# ==================================================================================================

CPPR_FILE_QUALITY_FROM_SUMMARY = function(pFolder=CCPRData, pFileRDS='DOWNLOAD_HNX_STKVN_SHARES_DAY.RDS', ToBrowse=F)
{
  pFileSUM = gsub('.RDS', '_SUMMARY.txt', pFileRDS, ignore.case = T)
  if (file.exists(paste0(pFolder, pFileSUM)))
  {
    My_Summary = setDT(fread(paste0(pFolder, pFileSUM)))
    if (ToBrowse) { My.Kable.TB(My_Summary) }
    MaxDate  = max(My_Summary$datelast, na.rm = T)
    MaxpDate = max(My_Summary[date<MaxDate]$datelast, na.rm = T)
    My_Summary_Stats = My_Summary[, .(folder=pFolder, file=pFileRDS, nb_codes=.N, records=sum(nb, na.rm=T), start=min(start, na.rm=T), end=max(datelast, na.rm = T), 
                                      nb_end=sum(ifelse(date==MaxDate,1,0)), pend=MaxpDate, nb_pend=sum(ifelse(date==MaxpDate,1,0)))]
    My_Summary_Stats[, percent_end:=100*nb_end/nb_codes]
    My_Summary_Stats[, percent_pend:=100*nb_pend/nb_codes]
    My.Kable.All(My_Summary_Stats)
  } else {
    My_Summary_Stats = data.table()
  }
  return(My_Summary_Stats)
}
# ==================================================================================================
SUMMARY_DATE_RDS = function(pFileName, ToBrowse=T, DateOnly=T) {
  # ------------------------------------------------------------------------------------------------
  # pFileName = paste0(UData, "download_EXC_stk_history.rds")
  # pFileName = paste0(UData, "download_HSX_stkvn_ref.rds")
  # pFileName = paste0(UData, "download_HSX_stkvn_intraday_history.rds")
  # pFileName = paste0(UData, "download_HSX_stkvn_intraday_day.rds")
  File_Summary = gsub(".rds", "_summary.txt", pFileName, ignore.case=T)
  datemax      = as.Date("1900-01-01")
  xDate        = datemax
  
  if (file.exists(File_Summary))
  {
    x = try(setDT(fread(File_Summary, sep='\t', fill=T)))
    if (all(class(x)!='try-error') && nrow(x)>0)
    {
      # str(x)
      x = CONVERT_IFEXIST_FIELDS_WITH_SEPARATOR(pData=x, AsType="numeric", List_Fields=list("last", "nbdays", "nb", 'sample', 'home'), ToConvertSeparator=F)
      x = CONVERT_IFEXIST_FIELDS_WITH_SEPARATOR(pData=x, AsType="character", List_Fields=list("type", "code", "source", "codesource"), ToConvertSeparator=F)
      if ("type" %in% names(x)) { x[, type:=as.character(type)]}
      x = CONVERT_IFEXIST_FIELDS_WITH_SEPARATOR(pData=x, AsType="date", List_Fields=list("start", "date", "datelast"), ToConvertSeparator=F)
      if (DateOnly){ x[nchar(as.character(date))>10, date:=as.character(substr(date,1,10))]}
      x = x[nchar(as.character(date))==10][, date:=as.Date(date)]
      xDate = max(x[!is.na(date)]$date)
    }
    
    # str(x)
  }
  # x = SUMMARY_DATE(gsub(".rds", "_summary.txt", pFileName, ignore.case=T))
  if (ToBrowse)
  {
    CATln(paste(FormatSS(pFileName, 85), FormatSS(xDate, 12)))
    catrep('-',100)
  }
  return(xDate)
}

# ==================================================================================================
XLS_UPDATE_BATCH_FILES_MONITOR_CCPR_RDS = function(ToReset=F) {
  # ------------------------------------------------------------------------------------------------
  # XLS_UPDATE_MONITOR_CCPR()
  # ............................................................................
  My_Sheet = 'BATCH'
  # ............................................................................
  
  print(CCPRDrive)
  library(tcltk2)
  GC_SILENT()
  DATACENTER_LINE_BORDER_BEGINEND('XLS_UPDATE_MONITOR_CCPR', 'BEGIN')
  # try(tk2dde.request("Excel", "Sheet1", "R1C1:R3C1")
  # x1 = try(tcltk2::tk2dde.request("Excel", 'MONITOR_FILES', "R1C1"), silent = T)
  
  
  Offset_Row = 20
  Offset_Col = 0
  FullPath_Local   = paste0(CCPRData, 'MONITOR/','CHECK_FILE_QUALITY.rds')
  FullPath_ODDrive = paste0(ODDrive, 'R/CCPR/DATA/MONITOR/','CHECK_FILE_QUALITY.rds')
  Monitor_Funtion  = CCPR_READ_RDS_WITH_ALTERNATIVE(FullPath_Local,FullPath_ODDrive) [[1]]
  
  # NEW ........................................................................
  My_EXCEL = openxlsx::read.xlsx(paste0(CCPRDrive, 'CCPR/MONITOR/','MONITOR_CCPR.xlsx'), sheet='BATCH', startRow=20)
  My_EXCEL = setDT(My_EXCEL)
  My_EXCEL = My_EXCEL[,1:2]
  My.Kable.All(My_EXCEL)
  Files_All = list()
  
  for (k in 1:nrow(My_EXCEL))
  {
    # k   = 1
    qf  = CPPR_FILE_QUALITY_FROM_SUMMARY(pFolder=My_EXCEL[k]$LOCATION, pFileRDS=paste0(My_EXCEL[k]$FILES, '.rds'), ToBrowse=F)
    Files_All[[k]] = qf
  }
  
  All_Files = rbindlist(Files_All, fill=T)
  All_Files[, updated:=substr(Sys.time(),1,19)]
  setnames(All_Files, c('folder', 'file'), c('location', 'files'))
  setnames(All_Files, c('nb_codes', 'nb_end', 'percent_end'), c('codes', 'codes_updated', 'updated_pct'))
  
  All_Files[, ':='(codes=as.numeric(codes), records=as.numeric(records))]
  All_Files[, ':='(start=as.Date(start), end=as.Date(end), pend=as.Date(end))]
  # str(All_Files)
  
  My.Kable.All(All_Files)
  # ToReset = T
  if (!ToReset)
  {
    Old_Files = CCPR_READRDS(paste0(CCPRData, 'MONITOR/'),'CHECK_FILE_QUALITY.rds', ToKable = T, ToRestore = T)
    
  } else { 
    Old_Files = data.table()
  }
  
  if (nrow(Old_Files)>0) { 
    Old_Files = CLEAN_COLNAMES(Old_Files)
    Old_Files[, ':='(codes=as.numeric(codes), records=as.numeric(records))]
  }
  
  # str(All_Files)
  # str(Old_Files)
  
  Final_Files = unique(rbind(Old_Files, All_Files, fill=T), by=c('location', 'files'), fromLast = T)
  
  My.Kable.All(Final_Files)
  
  # NEW ........................................................................
  
  # data = list()
  # for (i in nrow(My_EXCEL))
  # {
  #   # i = 22
  #   paste0("R", i + Offset_Row, "C",1+Offset_Col)
  #   Location  = try(tk2dde.request("Excel", My_Sheet, paste0("R", i + Offset_Row, "C",1+Offset_Col)))
  #   File  = try(tk2dde.request("Excel", My_Sheet, paste0("R", i + Offset_Row, "C",2+Offset_Col)))
  #   data [[i]] = data.table(location = Location, file = File)
  # }
  # my_data = setDT(rbindlist(data, fill = T))
  # my_data = my_data[!is.na(file)]
  # table_all = list()
  # Nb_TODO = 0
  # for (k in 1:nrow(my_data))
  # {
  #   # k = 4
  #   file = my_data[k]$file
  #   Location = my_data[k]$location
  #   # x = CCPR_READRDS(FileFolder=CCPRData, FileName=file, ToKable=F, ToPrompt=F, ToRestore=T)
  #   # x =  CCPR_READ_RDS_WITH_ALTERNATIVE(FileFrm = paste0(Location, file, '.rds'), FileAlt = paste0(ODDrive, 'R/CCPR/DATA/', file, '.rds')) [[1]]
  #   # summary = FILE_SUMMARY_CODESOURCE_DATE(paste0(Location,file,'.rds'))
  #   # summary = CCPR_EXTRACT_LIST(pFolder=Location, pSource='CAF', pPrefix='STKVN', pSuffix='_REF', List_From='SUMMARY', 
  #   #                   From_Custom='', Date_Start=365, Date_End=0, ToKable=F, ToCheckDate=F, OutPut='LIST')
  #   if(!is.na(file)) {
  #     summary = FILE_SUMMARY_TEXT (Folder = Location, FileName = paste0(file, '_summary', ".txt"))
  #     
  #     table_all [[k]] = data.table(
  #       location = Location,files = file, codes = summary$codes, start =   as.Date(summary$startdate), end = as.Date(summary$enddate),
  #       codes_updated = Nb_TODO, updated_pct = round((Nb_TODO/summary$codes)*100,2), records = summary$records
  #     )
  #   }
  # }
  # my_table = setDT(rbindlist(table_all, fill =T))
  # 
  # if( file.exists(FullPath_Local))
  # {
  #   data_old = CCPR_READRDS(FileFolder = paste0(CCPRData, 'MONITOR/'), FileName = 'CHECK_FILE_QUALITY.rds', ToRestore = T)
  # } else { data_old = data.table()}
  # # data_old = data.table()
  # # str(my_table)
  # # str(data_old)
  # data_final = unique(rbind(my_table,data_old, fill = T), by = c('location', 'files') ) [order(files)]
  # SAVERDS_WITH_ONEDRIVE(pData = data_final,FileFolder = paste0(CCPRData, 'MONITOR/'),FileName = 'CHECK_FILE_QUALITY.rds',SaveOneDrive = T)
  
  # Monitor_Funtion[, delay:= as.numeric(round(difftime(Sys.time(),as.POSIXct(updated) ,units = 'hours'),2))]
  My.Kable.All(Monitor_Funtion)
  DATACENTER_LINE_BORDER_BEGINEND('XLS_UPDATE_MONITOR_CCPR_BATCH', 'END')
  GC_SILENT()
}

# ==================================================================================================
FormatSS = function(l.Num, l.Wid, l.Sep = "|") {
  # ------------------------------------------------------------------------------------------------
  l.res = paste(format(l.Num, width = l.Wid), l.Sep)
  return(l.res)
}

# ==================================================================================================
IFRC_DOWNLOAD_SGX_STK_FREQUENCY_BY_CODE = function(pCode='1A0', pFrequency='DAY') {
  My.Kable.INSREF(ins_ref[iso2=='SG' & type=='STK' & symbol==pCode])
  INSREF_SGX = ins_ref[symbol==pCode & iso2=='SG' & type=='STK']
  if (nrow(INSREF_SGX)==1)
  {
    pCodeREF = INSREF_SGX$code
    pCode    = INSREF_SGX$symbol
    switch (pFrequency,
            'INTRADAY' = {   pURL = paste0('https://api.sgx.com/securities/v1.1/charts/intraday/stocks/code/', pCode, '/1d?params=trading_time%2Cvl%2Clt') },
            'DAY'      = {   pURL = paste0('https://api.sgx.com/securities/v1.1/charts/historic/stocks/code/', pCode, '/5y?params=trading_time%2Cvl%2Clt') }
    )
    x = try(jsonlite::fromJSON(pURL))
    if (all(class(x)!='try-error'))
    {
      switch (pFrequency,
              'INTRADAY' = {   xData = data.table(x$data$intraday) },
              'DAY'      = {   xData = data.table(x$data$historic) }
      )
      
      Final_Data = xData[, .(source='SGX', codesource=pCode, code=pCodeREF, date=as.Date(substr(trading_time,1,8), '%Y%m%d'), 
                             time=paste0(substr(trading_time,10,11),':', substr(trading_time,12,13),':', substr(trading_time,14,15)), 
                             close=as.numeric(lt), volume=as.numeric(vl))]
      Final_Data[, datetime:=paste(date, time)]
      Final_Data = MERGE_DATASTD_BYCODE(Final_Data)
      Final_Data$capiusd = NULL
      if (pFrequency=='DAY')
      {
        Final_Data$time = NULL
        Final_Data$datetime = NULL
        Final_Data = unique(Final_Data, by='date')
      }
      My.Kable.TB(Final_Data)
    }    
  } else {
    Final_Data = data.table()
  }
  return(Final_Data)
}

# ==================================================================================================
FILE_SUMMARY_TEXT= function(Folder = CCPRData, Source='CAF', Prefix='STKVN', Suffix='_REF', ListFrom='SUMMARY', FromCustom='', DateStart=365, DateEnd=0, ToKable=F, ToCheckDate=F, OutPut='TABLE') {
  FileReports  = data.table()
  FileData =  CCPR_EXTRACT_LIST(pFolder=Folder, pSource= Source, pPrefix = Prefix, pSuffix=Suffix, List_From = ListFrom, 
                                From_Custom = FromCustom, Date_Start=DateStart, Date_End=DateEnd, ToKable, ToCheckDate, OutPut)
  
  FileSummary = FileData[, .(startdate=date[1], enddate=date[.N], records=.N ), by=c('codesource')]
  # FileSummary[, file:=paste0('download_', Source, '_', Prefix, Suffix, '_summary.txt')]
  
  FileReport = FileSummary[, .(file = paste0('download_', Source, '_', Prefix, Suffix, '_summary.txt'), codes=.N, startdate=min(startdate, na.rm=T),
                               enddate=max(enddate, na.rm=T), records=sum(records, na.rm=T), updated_code = nrow(FileSummary[enddate == max(enddate)]) )]
  My.Kable.All(FileReport)
  return(FileReport)
}

# ==================================================================================================
CCPR_LOAD_TEXTFILE = function(p_folder, p_filename, p_active=T, EchoOn=F) {
  # ------------------------------------------------------------------------------------------------
  # p_folder = "u:/efrc/data/"; p_filename="list_demo_yah.txt"; p_active=T
  l_fullname = paste0(p_folder, p_filename)
  if (EchoOn) { CATrp(paste('Loading : ', l_fullname)) }
  if (file.exists(l_fullname))
  {
    l_data = setDT(fread(l_fullname, sep="\t"))
    if (EchoOn) { CATln(paste(paste(l_fullname), '=', nrow(l_data), "rows")) } else { CATln('')}
    # colnames(l_data) = tolower(colnames(l_data))
    if ("active" %in% colnames(l_data) & p_active) { l_data = l_data[active==1]}
    if (EchoOn) { My.Kable.TB(l_data) }
  } else {
    DATACENTER_LINE_BORDER(paste("DATACENTER_LOAD_TEXTFILE :", l_fullname, " - FILE DOES NOT EXIST"))
    l_data=data.table()
  }
  CATrp('')
  return(l_data)
}

# ==================================================================================================
My.Kable.INSREF = function(dt, pNb=3, pOption="CODES") {
  # ------------------------------------------------------------------------------------------------
  # My.Kable.INSREF(ins_ref[type=="FUT" & !is.na(yah)], pOption="ALL")
  switch (pOption,
          "CAPIUSD" = { My.Kable.TB(dt[, .(iso2, symbol, isin, code, eik, rts, blg, yah, inv, gog, nsd, short_name, capiusd, enddate, last)], Nb=pNb) },
          "CODES"   = { My.Kable.TB(dt[, .(iso2, iso3, isin, symbol, code, eik, rts, rts_alt, blg, yah, inv, gog, bin, nsd, cnm, cnd, short_name, enddate)], Nb=pNb) },
          "TB"      = { My.Kable.TB(dt[, .(ord,type, fcat, scat, symbol, isin, code, eik, rts, blg, yah, inv, bin, qdl, stx, gog, short_name, country, continent, enddate, last)], Nb=pNb) },
          "SHORT"   = { My.Kable.TB(dt[, .(ord, type, fcat, scat, symbol, isin, code, eik, rts, blg, yah, gog, inv,short_name, country, continent, enddate, last)], Nb=pNb) },
          "ALL"     = { My.Kable.All(dt[, .(ord,type, fcat, scat, symbol, code, eik, rts, blg, yah, inv, bin, qdl,  stx, gog, short_name, country, continent, enddate, last, cur)])},
          if (pNb==1 | nrow(dt)==1)
          {
            My.Kable.All(dt[, .(ord,type, fcat, scat, symbol, code, eik, rts, blg, yah, inv, bin, qdl, stx, gog, short_name, country, continent, enddate, last, cur, provider)])
          } else {
            My.Kable(dt[, .(ord,type, fcat, scat, symbol, code, eik, rts, blg, yah, inv, bin, qdl, stx, gog, short_name, country, continent, enddate, last, cur, provider)], Nb=pNb)
          }
  )
}

# ==================================================================================================
CCPR_EXTRACT_LIST = function(pFolder=UData, pSource='RTS', pPrefix="FUT", pSuffix='_HISTORY', List_From='SUMMARY', From_Custom='', 
                             Date_Start=5, Date_End=0, ToKable=F, ToCheckDate=F, OutPut='LIST') {
  # ------------------------------------------------------------------------------------------------
  # pSource='BLG'; pPrefix="IND"; List_From='SUMMARY'; From_Custom=''; Date_Start=10; Date_End=0;, ToKable=T
  # pPrefix="BND"
  
  # pFolder=UData; pSource='RTS'; pPrefix="FUT"; List_From='SUMMARY'; From_Custom=''; Date_Start=5; Date_End=0; ToKable=F; ToCheckDate=F; OutPut='LIST'
  # pFolder=UData; pSource='RTS'; pPrefix="FUT"; List_From='SUMMARY'; From_Custom=''; Date_Start=5; Date_End=0; ToKable=F; ToCheckDate=F; OutPut='TABLE'
  
  # pFolder=CCPRData; pSource='CAF'; pPrefix='STKVN'; pSuffix='_REF'; List_From='SUMMARY'; From_Custom=''; Date_Start=365; Date_End=0; ToKable=F; ToCheckDate=F; OutPut='TABLE'
  
  
  RELOAD_INSREF()
  DATACENTER_LINE_BORDER(paste('CCPR_EXTRACT_LIST :',pSource, pPrefix, pFolder, List_From))
  List_Res = list()
  if (nchar(From_Custom)==0)
  {
    switch(List_From,
           "SUMMARY" = {
             File_XXX = paste0('download_', pSource, '_', pPrefix, pSuffix, '_summary.txt')
             FullPath = paste0(pFolder, File_XXX)
             CATln_Border(paste('FILE TO LOAD = ', FullPath, ':', file.exists(FullPath)))
             if (file.exists(FullPath))
             {
               List_XXX = CCPR_LOAD_TEXTFILE(pFolder, File_XXX)
               
               List_XXX = List_XXX[date>Sys.Date()-Date_Start & date<Sys.Date()-Date_End]
               My.Kable.TB(List_XXX)
               List_Res = as.list(List_XXX$codesource)
             } else {
               List_XXX = data.table()
               List_Res = list()
               CATln(paste("FILE NOT EXISTS :", paste0(pFolder, File_XXX)))
             }
             if (ToKable) { My.Kable.MaxCols(List_XXX) ; unlist(List_Res) }
             
           },
           
           "INSREF_TYPE" = {
             # pPrefix = 'FND'; pSource = 'RTS'
             # ins_ref[!is.na(eik) & is.na(rts), rts:=eik]
             # DATACENTER_SAVEDTRDS_AND_LAST(ins_ref, paste0(UData, 'efrc_ins_ref.rds'), ToSummary=T)
             # 
             # My.Kable.INSREF(ins_ref[rts!=eik | (!is.na(eik) & is.na(rts))])
             
             pParseText = paste0('!is.na(', tolower(pSource), ') & type=="', pPrefix, '"')
             # pParseText = paste0('!is.na(', tolower(pSource), ') & !is.na(enddate) & type=="', pPrefix, '"')
             CATln(paste('PARSE TEXT = ', pParseText))
             
             # nrow(ins_ref[!is.na(rts) & type==pPrefix])
             insref_XXX = ins_ref[eval(parse(text = pParseText))][order(-enddate)]
             My.Kable.INSREF(insref_XXX)
             
             pParseText2 = paste0('codesource:=', tolower(pSource))
             insref_XXX  = insref_XXX[, eval(parse(text = pParseText2))]
             # insref_XXX[!is.na(codesource)]
             # insref_XXX[!is.na(eik)]
             
             CATln(paste('PARSE TEXT = ', pParseText))
             
             if (nrow(insref_XXX)>0)
             {
               List_XXX = insref_XXX
               My.Kable.INSREF(List_XXX[!is.na(codesource)])
               if (ToCheckDate) { List_XXX = List_XXX[enddate>Sys.Date()-Date_Start & enddate<Sys.Date()-Date_End] }
               List_Res = as.list(List_XXX$codesource)
             } else {
               List_XXX = data.table()
               List_Res = list()
               CATln(paste("FILE NOT EXISTS :", paste0(UData, File_XXX)))
             }
             # ToKable = T
             if (ToKable) { My.Kable.MaxCols(List_XXX) ; unlist(List_Res) }
             
           },
           
           "INSREF_FCAT" = {
             # pPrefix = 'FND'; pSource = 'RTS'
             # ins_ref[!is.na(eik) & is.na(rts), rts:=eik]
             # DATACENTER_SAVEDTRDS_AND_LAST(ins_ref, paste0(UData, 'efrc_ins_ref.rds'), ToSummary=T)
             # 
             # My.Kable.INSREF(ins_ref[rts!=eik | (!is.na(eik) & is.na(rts))])
             
             pParseText = paste0('!is.na(', tolower(pSource), ') & !is.na(enddate) & fcat=="', pPrefix, '"')
             CATln(paste('PASRE TEXT = ', pParseText))
             
             nrow(ins_ref[!is.na(rts) & !is.na(enddate) & fcat==pPrefix])
             insref_XXX = ins_ref[eval(parse(text = pParseText))][order(-enddate)]
             My.Kable.INSREF(insref_XXX)
             
             pParseText2 = paste0('codesource:=', tolower(pSource))
             insref_XXX  = insref_XXX[, eval(parse(text = pParseText2))]
             # insref_XXX[!is.na(codesource)]
             # insref_XXX[!is.na(eik)]
             
             CATln(paste('PASRE TEXT = ', pParseText))
             
             if (nrow(insref_XXX)>0)
             {
               List_XXX = insref_XXX
               My.Kable.INSREF(List_XXX[!is.na(codesource)])
               List_XXX = List_XXX[enddate>Sys.Date()-Date_Start & enddate<Sys.Date()-Date_End]
               List_Res = as.list(List_XXX$codesource)
             } else {
               List_XXX = data.table()
               List_Res = list()
               CATln(paste("FILE NOT EXISTS :", paste0(UData, File_XXX)))
             }
             # ToKable = T
             if (ToKable) { My.Kable.MaxCols(List_XXX) ; unlist(List_Res) }
             
           }
    )
  }
  if (OutPut=='LIST') { Out_Put = List_Res } else { Out_Put = copy(List_XXX)}
  return(Out_Put)
}

# ==================================================================================================
IFRC_DOWNLOAD_CND_CAPIUSD_BY_CODE = function(pCode = 'AAVE'){
  RELOAD_INSREF ()
  # x = try(IFRC_DOWNLOAD_CND_CAPIUSD_BY_CODE(pCode = 'AAVE'))
  Final_Data = data.table()
  pURL       = paste0('https://www.coindesk.com/price/', tolower(pCode), '/')
  CATln_Border(pURL)
  content    = try(rvest::read_html(pURL))
  if (all(class(content)!='try-error'))
  {
    content    = as.character(content)
    pBody      = str.extract(content, 'Market Cap', '</div></div>')
    pData      = word(pBody,1,1, '</span>')
    pCAPI      = gsub('[$]', '', word(pData,5,5, '>'))
    vCAPI      = CONVERT_IN_VALUE_KMBT(pCAPI, 'T')
    Final_Data = data.table(source='CND', codesource=pCode, date=substr(Sys.time(),1,10), dataset='CAPIUSD', capi_usd=vCAPI, close=vCAPI)
    Final_Data = merge(Final_Data[, -c('code')], ins_ref[!is.na(cnd)][, .(code, codesource=cnd)], all.x=T, by='codesource')
    Final_Data = MERGE_DATASTD_BYCODE(Final_Data)
    
    #     if (nrow(Final_Data[!is.na(code)])>0) { Final_Data = MERGE_DATASTD_BYCODE(Final_Data); Final_Data$capiusd = NULL }
    #     My.Kable(Final_Data[, -c('datetime', 'close_adj')])
    if (nrow(Final_Data[!is.na(code)])>0) { Final_Data = MERGE_DATASTD_BYCODE(Final_Data); Final_Data$capiusd = vCAPI }
    My.Kable.All(Final_Data)
  }
  return(Final_Data)
}

# ==================================================================================================
IFRC_DOWNLOAD_CND_DAY_BY_CODE = function(pCode = 'EEVA')
{
  RELOAD_INSREF ()
  Final_Data = data.table()
  pURL  = paste0('https://production.api.coindesk.com/v2/tb/price/values/', pCode, '?start_date=2000-12-31T00:00&end_date=', Sys.Date(), 'T23:00&ohlc=true')
  CATln_Border(pURL)
  x = try(jsonlite::fromJSON(pURL))
  if (all(class(x)!='try-error'))
  {
    Source_Name   = x$data$name
    Source_Symbol = x$data$iso
    xData = as.data.table(x$data$entries)
    setnames(xData, c('epoch', 'open', 'high', 'low', 'close'))
    xData[, datetime:=as.POSIXct(as.numeric(epoch)/1000, origin="1970-01-01")]
    xData[, date:=as.Date(substr(datetime,1,10))]
    Final_Data = xData[, .(source='CND', codesource=pCode, date, datetime, open, high, low, close, close_adj=close, 
                           change=(close-shift(close)), varpc = 100*((close/shift(close))-1),  rt=(close/shift(close))-1)]
    Final_Data = merge(Final_Data[, -c('code')], ins_ref[!is.na(cnd)][, .(code, codesource=cnd)], all.x=T, by='codesource')
    if (nrow(Final_Data[!is.na(code)])>0) { Final_Data = MERGE_DATASTD_BYCODE(Final_Data); Final_Data$capiusd = NULL }
    My.Kable(Final_Data[, -c('datetime', 'close_adj')])
    
  }
  return(Final_Data)
}

# ==================================================================================================

NEW_DOWNLOAD_CAF_STKVN_FOREIGN_BY_CODE = function(pCode = 'VIC')
{
  # # TO BE IMPROVED
  # # pCode = 'XYZ'
  # # pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=', pCode, '&StartDate=30/12/2022&EndDate=30/12/2023&PageIndex=3&PageSize=20')
  # # NbPagesBack = 20
  # List_Codes = setDT(fread(paste0(CCPRData, 'LIST/', 'LIST_CODE_SOURCE_SPECIAL.txt')))
  # CODEREF    = List_Codes[codesource==toupper(pCode) & source=='CAF'] 
  # if (nrow(CODEREF)==1) { INDEX_CODE = CODEREF$coderef } else { INDEX_CODE = as.character(NA)}
  # CATln_Border(paste('CODE REF = ', INDEX_CODE))
  # 
  Data_List = list()
  ToContinu = T
  k = 1
  
  while (ToContinu)
  {
    # k = 4
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=VNINDEX&StartDate=&EndDate=&PageIndex=2&PageSize=20
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx?Symbol=VNINDEX&StartDate=&EndDate=&PageIndex=1&PageSize=20
    # pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=', pCode, '&StartDate=&EndDate=&PageIndex=', k, '&PageSize=100')
    
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/GDTuDoanh.ashx?Symbol=VNINDEX&StartDate=&EndDate=&PageIndex=1&PageSize=20
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx?Symbol=VIC&StartDate=&EndDate=&PageIndex=1&PageSize=20
    pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx?Symbol=', pCode, '&StartDate=&EndDate=&PageIndex=', k, '&PageSize=20')
    CATrp(pURLk)
    x = try(jsonlite::fromJSON(pURLk))
    if (length(x$Data$Data)>0)
    {
      XData = try(as.data.table(x$Data$Data))
      if (all(class(XData)!='try-error') && nrow(XData)>1)
      {
        XData = CLEAN_COLNAMES(XData)
        
        Final_Data = XData[, .(source='CAF', codesource=pCode, ticker=pCode, code=paste0('STKVN', pCode),
                               date      = as.Date(ngay, '%d/%m/%Y'), 
                               buy_vol   = as.numeric(klmua),
                               sell_vol  = as.numeric(klban),
                               buy_val   = as.numeric(gtmua),
                               sell_val  = as.numeric(gtban),
                               bs_vol    = as.numeric(klgdrong),
                               bs_val    = as.numeric(gtdgrong),
                               room_shares = as.numeric(roomconlai),
                               ownership_pc = as.numeric(dangsohuu)
                               
        )]
        My.Kable.TB(Final_Data)
        Data_List[[k]] = Final_Data
      } else {
        ToContinu = F
      }
    } else {
      ToContinu = F
    }
    k = k+1
  }
  
  CATln(pURLk); CATln('')
  Data_All = rbindlist(Data_List, fill=T)
  if (nrow(Data_All)>0)
  {
    # Data_All[, code:=INDEX_CODE]
    Data_All = Data_All[order(date)]
    if (nrow(Data_All[!is.na(code)])>0)
    {
      RELOAD_INSREF()
      Data_All = merge(Data_All[, -c('type', 'fcat', 'scat', 'name')], ins_ref[, .(code, type, fcat, scat, name=short_name)], all.x=T, by='code')
    }
    My.Kable.TB(Data_All)
  } else {
    Data_All = data.table()
  }
  return(Data_All)
}
# ==================================================================================================
NEW_DOWNLOAD_CAF_STKVN_TRADE_BY_CODE = function(pCode = 'VIC')
{
  # # TO BE IMPROVED
  # # pCode = 'XYZ'
  # # pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=', pCode, '&StartDate=30/12/2022&EndDate=30/12/2023&PageIndex=3&PageSize=20')
  # # NbPagesBack = 20
  # List_Codes = setDT(fread(paste0(CCPRData, 'LIST/', 'LIST_CODE_SOURCE_SPECIAL.txt')))
  # CODEREF    = List_Codes[codesource==toupper(pCode) & source=='CAF'] 
  # if (nrow(CODEREF)==1) { INDEX_CODE = CODEREF$coderef } else { INDEX_CODE = as.character(NA)}
  # CATln_Border(paste('CODE REF = ', INDEX_CODE))
  # 
  Data_List = list()
  ToContinu = T
  k = 1
  
  while (ToContinu)
  {
    # k = 4
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=VNINDEX&StartDate=&EndDate=&PageIndex=2&PageSize=20
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx?Symbol=VNINDEX&StartDate=&EndDate=&PageIndex=1&PageSize=20
    # pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=', pCode, '&StartDate=&EndDate=&PageIndex=', k, '&PageSize=100')
    
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/GDTuDoanh.ashx?Symbol=VNINDEX&StartDate=&EndDate=&PageIndex=1&PageSize=20
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx?Symbol=VIC&StartDate=&EndDate=&PageIndex=1&PageSize=20
    pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx?Symbol=', pCode, '&StartDate=&EndDate=&PageIndex=', k, '&PageSize=20')
    CATrp(pURLk)
    x = try(jsonlite::fromJSON(pURLk))
    if (length(x$Data$Data)>0)
    {
      XData = try(as.data.table(x$Data$Data))
      if (all(class(XData)!='try-error') && nrow(XData)>1)
      {
        XData = CLEAN_COLNAMES(XData)
        
        Final_Data = XData[, .(source='CAF', codesource=pCode, ticker=pCode, code=paste0('STKVN', pCode),
                               date      = as.Date(ngay, '%d/%m/%Y'), 
                               buy_vol   = as.numeric(klmua),
                               sell_vol  = as.numeric(klban),
                               buy_val   = as.numeric(gtmua),
                               sell_val  = as.numeric(gtban),
                               bs_vol    = as.numeric(klgdrong),
                               bs_val    = as.numeric(gtdgrong),
                               room_shares = as.numeric(roomconlai),
                               ownership_pc = as.numeric(dangsohuu)
                               
        )]
        My.Kable.TB(Final_Data)
        Data_List[[k]] = Final_Data
      } else {
        ToContinu = F
      }
    } else {
      ToContinu = F
    }
    k = k+1
  }
  
  CATln(pURLk); CATln('')
  Data_All = rbindlist(Data_List, fill=T)
  if (nrow(Data_All)>0)
  {
    # Data_All[, code:=INDEX_CODE]
    Data_All = Data_All[order(date)]
    if (nrow(Data_All[!is.na(code)])>0)
    {
      RELOAD_INSREF()
      Data_All = merge(Data_All[, -c('type', 'fcat', 'scat', 'name')], ins_ref[, .(code, type, fcat, scat, name=short_name)], all.x=T, by='code')
    }
    My.Kable.TB(Data_All)
  } else {
    Data_All = data.table()
  }
  return(Data_All)
}
# ==================================================================================================
DATACENTER_LINE_BORDER = function(l.msg="hello", l.justify="centre", clearscreen=F) {
  # ------------------------------------------------------------------------------------------------
  if (clearscreen) {shell("cls")}
  CATln(strrep("=",NbCarLine))
  CATln(format(l.msg, width=NbCarLine, justify=l.justify))
  CATln(strrep("=",NbCarLine))
}

# ==================================================================================================
DATACENTER_LINE_BORDER_BEGINEND = function(pOption="EFRC_INDVN_INTRADAY", pMethod="BEGIN", AddText='') {
  # ------------------------------------------------------------------------------------------------
  switch (pMethod,
          "BEGIN"     = { DATACENTER_LINE_BORDER(paste(pOption, "- Executing ...", AddText, Sys.time()), clearscreen=T) },
          "UPDATED"   = { DATACENTER_LINE_BORDER(paste(pOption, "- Already Updated, ", AddText, Sys.time()), clearscreen=F) },
          "END"       = { DATACENTER_LINE_BORDER(paste(pOption, "- Done, ",AddText, Sys.time()), clearscreen=F) }
          
  )
}

# ==================================================================================================
XLS_UPDATE_MONITOR_CCPR_DEV = function(){
  # ............................................................................
  My_Sheet = 'MONITOR_CCPR_DEV'
  # ............................................................................
  
  print(CCPRDrive)
  library(tcltk2)
  GC_SILENT()
  DATACENTER_LINE_BORDER_BEGINEND('XLS_UPDATE_MONITOR_CCPR', 'BEGIN')
  # try(tk2dde.request("Excel", "Sheet1", "R1C1:R3C1")
  # x1 = try(tcltk2::tk2dde.request("Excel", 'MONITOR_FILES', "R1C1"), silent = T)
  x2 = try(tcltk2::tk2dde.request("Excel", My_Sheet, "R1C1"), silent = T)
  x2.time = try(tcltk2::tk2dde.request("Excel", My_Sheet, "R1C2"), silent = T)
  
  if (all(class(x2)!='try-error'))
  {
    # Monitor_All = as.data.table(openxlsx::read.xlsx(paste0('R:/CCPR/', 'MONITOR/MONITORS_CCPR.xlsx'), sheet='MONITOR_CCPR'))
    # Monitor_All[, nnum_row:=seq.int(1, nrow(Monitor_All))]
    Offset_Col = 2
    Offset_Row = 2
    # Monitor_Matrix = as.matrix(Monitor_All)
    
    x.date = try(tcltk2::tk2dde.poke("Excel", My_Sheet, "R1C1", as.character(Sys.Date()) ), silent = T)
    # request : tk2dde.request("Excel", "{[MONITOR_CCPR.xlsm]Data}", "R1C1:R1C1")
    x.time = try(tcltk2::tk2dde.poke("Excel", My_Sheet, "R1C2", substr(as.character(Sys.time()),11,19 )), silent = T)
    CATln_Border(paste(x.date, x.time))
    
    # ----------------------------------------------------------------------------------------------
    
    Monitor_All = readRDS(paste0(CCPRDrive, 'CCPR/DATA/MONITOR/CHECK_MYPC.rds'))
    if ('x' %in% names(Monitor_All)) { 
      Monitor_All$x = NULL
      try(SAVERDS_WITH_ONEDRIVE(Monitor_All, paste0(CCPRData, 'MONITOR/'), 'CHECK_MYPC.rds', SaveOneDrive = T))
    } 
    Monitor_All[, date:=word(updated,1,1, ' ')]
    Monitor_All[, time:=word(updated,2,2, ' ')]
    Monitor_All[, delay:= as.numeric(round(difftime(Sys.time(),as.POSIXct(updated) ,units = 'hours'),2))]
    
    Monitor_All$updated = NULL
    Monitor_All = Monitor_All %>% select(mypc, date, time, everything(), delay)
    
    My.Kable.All(Monitor_All)
    
    Monitor_Mat = as.matrix(Monitor_All)
    Offset_Row = Position_Moniter_PC[1]
    Offset_Col = Position_Moniter_PC[2]
    
    # NEW : --------------------------------------------------------------------
    for (irow in (1:nrow(Monitor_All)))
    {
      # irow = 1
      # irow = 2
      # irow = 3
      Name_MYPC = try(tk2dde.request("Excel", My_Sheet, paste0("R", irow + Offset_Row, "C",1+Offset_Col)))
      Data_MYPC = Monitor_All[mypc == Name_MYPC]
      if (nrow(Data_MYPC)==1)
      {
        Data_MYPC = as.matrix(Data_MYPC)
        for (jcol in (1:ncol(Data_MYPC)))
        {
          # jcol = 1
          # jcol = 2
          # jcol = 3
          # jcol = 7
          # Cell_IJ = Monitor_All[irow, jcol]
          if (!is.na(Data_MYPC[1, jcol]))
          {
            x = try(tk2dde.poke("Excel", My_Sheet, paste0("R",(irow+Offset_Row), "C",jcol+Offset_Col), Data_MYPC[1, jcol]))
          } else {
            x = try(tk2dde.poke("Excel", My_Sheet, paste0("R",(irow+Offset_Row), "C",jcol+Offset_Col), " - "))
          }
        }
      }
    }
    
    # NEW (End)
    
    # OLD : --------------------------------------------------------------------
    # 
    # for (irow in (1:nrow(Monitor_All)))
    # {
    #   # irow = 1+Offset_Row
    #   # irow = 1
    #   for (jcol in (1:ncol(Monitor_All)))
    #   {
    #     # jcol = 1
    #     # jcol = 4
    #     # Cell_IJ = Monitor_All[irow, jcol]
    #     if (!is.na(Monitor_Mat[irow, jcol]))
    #     {
    #       x = try(tk2dde.poke("Excel", My_Sheet, paste0("R",(irow+Offset_Row), "C",jcol), Monitor_Mat[irow, jcol]))
    #     }
    #   }
    # }
    
    # OLD (End)
    
    # ============================================================================
    # FILES
    # ============================================================================
    
    Monitor_File = readRDS(paste0(CCPRData,'monitor_ccpr_files'))
    My.Kable.All(Monitor_File)
    
    Offset_Row = Position_Moniter_File[1]
    Offset_Col = Position_Moniter_File[2]
    
    Monitor_Mat = as.matrix(Monitor_File)
    
    # NEW:------------------------------------------------------------------------
    
    for (irow in (1:nrow(Monitor_File)))
    {
      # irow = 1
      # irow = 2
      Location = try(tk2dde.request("Excel", My_Sheet, paste0("R", irow + Offset_Row, "C",1+Offset_Col)))
      switch (Location,
              'CCPRData' = { File_Folder = CCPRData }
      )
      Name_File = try(tk2dde.request("Excel", My_Sheet, paste0("R", irow + Offset_Row, "C",2+Offset_Col)))
      Data_File = Monitor_File[(location == Location & file == Name_File)]
      if(nrow(Data_File)==1)
      {
        Data_File = as.matrix(Data_File)
        for (jcol in (1:ncol(Data_File)))
        {
          # jcol = 1
          # jcol = 1+Offset_Col
          if (!is.na(Data_File[1, jcol]))
          {
            x = try(tk2dde.poke("Excel", My_Sheet, paste0("R",(irow+Offset_Row), "C",jcol+Offset_Col), Data_File[1, jcol]))
          } else {
            x = try(tk2dde.poke("Excel", My_Sheet, paste0("R",(irow+Offset_Row), "C",jcol+Offset_Col), " - "))
          }
        }
      }
    }
    
    # NEW (END) -----------------------------------------------------------------------
    
    
    # OLD: -----------------------------------------------------------------------
    
    # for (irow in (1:nrow(Monitor_File)))
    # {
    #   # irow = 1+Offset_Row
    #   # irow = 1
    #   for (jcol in (1:ncol(Monitor_File)))
    #   {
    #     # jcol = 1
    #     # jcol = 1+Offset_Col
    #     # Cell_IJ = Monitor_All[irow, jcol]
    #     if (!is.na(Monitor_Mat[irow, jcol]))
    #     {
    #       x = try(tk2dde.poke("Excel", My_Sheet, paste0("R",(irow+Offset_Row), "C",jcol+Offset_Col), Monitor_Mat[irow, jcol]))
    #     } else {
    #       x = try(tk2dde.poke("Excel", My_Sheet, paste0("R",(irow+Offset_Row), "C",jcol+Offset_Col), " - "))
    #     }
    #   }
    # }
    # OLD (END) -----------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------
    
    Sys.sleep(10)
    x = try(tcltk2::tk2dde.poke("Excel", 'MONITOR_CCPR', "R1C2", substr(as.character(Sys.time()),11,19 )), silent = T)
  } else { CATln_Border(paste('FILE EXCEL DONT HAVE SHEET = ', 'MONITOR_FILES/UPDATED')) }
  DATACENTER_LINE_BORDER_BEGINEND('XLS_UPDATE_MONITOR_CCPR', 'END')
  GC_SILENT()
}

# ==================================================================================================
XLS_UPDATE_MONITOR_CCPR = function()
{
  # XLS_UPDATE_MONITOR_CCPR()
  # ............................................................................
  My_Sheet = 'MONITOR_CCPR'
  # ............................................................................
  
  print(CCPRDrive)
  library(tcltk2)
  GC_SILENT()
  DATACENTER_LINE_BORDER_BEGINEND('XLS_UPDATE_MONITOR_CCPR', 'BEGIN')
  # try(tk2dde.request("Excel", "Sheet1", "R1C1:R3C1")
  # x1 = try(tcltk2::tk2dde.request("Excel", 'MONITOR_FILES', "R1C1"), silent = T)
  x2 = try(tcltk2::tk2dde.request("Excel", My_Sheet, "R1C1"), silent = T)
  x2.time = try(tcltk2::tk2dde.request("Excel", My_Sheet, "R1C2"), silent = T)
  
  if (all(class(x2)!='try-error'))
  {
    # Monitor_All = as.data.table(openxlsx::read.xlsx(paste0('R:/CCPR/', 'MONITOR/MONITORS_CCPR.xlsx'), sheet='MONITOR_CCPR'))
    # Monitor_All[, nnum_row:=seq.int(1, nrow(Monitor_All))]
    Offset_Col = 2
    Offset_Row = 2
    # Monitor_Matrix = as.matrix(Monitor_All)
    
    x.date = try(tcltk2::tk2dde.poke("Excel", My_Sheet, "R1C1", as.character(Sys.Date()) ), silent = T)
    x.time = try(tcltk2::tk2dde.poke("Excel", My_Sheet, "R1C2", substr(as.character(Sys.time()),11,19 )), silent = T)
    CATln_Border(paste(x.date, x.time))
    
    # ----------------------------------------------------------------------------------------------
    
    Monitor_All = readRDS(paste0(CCPRDrive, 'CCPR/DATA/MONITOR/CHECK_MYPC.rds'))
    if(all(c('codesource','source','close') %in% names(Monitor_All))){
      Monitor_All = Monitor_All[,-c('codesource','source','close')]
      saveRDS(Monitor_All,paste0(CCPRDrive, 'CCPR/DATA/MONITOR/CHECK_MYPC.rds'))
    }
    
    Monitor_All[, date:=word(updated,1,1, ' ')]
    Monitor_All[, time:=word(updated,2,2, ' ')]
    Monitor_All[, delay:= as.numeric(round(difftime(Sys.time(),as.POSIXct(updated) ,units = 'hours'),2))]
    
    Monitor_All$updated = NULL
    Monitor_All = Monitor_All %>% select(mypc, date, time, everything(), delay)
    
    My.Kable.All(Monitor_All)
    
    Monitor_Mat = as.matrix(Monitor_All)
    Offset_Row = 3
    
    for (irow in (1:nrow(Monitor_All)))
    {
      # irow = 1
      # irow = 2
      # irow = 13
      Name_MYPC = try(tk2dde.request("Excel", My_Sheet, paste0("R", irow + Offset_Row, "C1")))
      if(length(Name_MYPC)!=0){
        Data_MYPC = Monitor_All[mypc == Name_MYPC]
        if (nrow(Data_MYPC)==1)
        {
          Data_MYPC = as.matrix(Data_MYPC)
          for (jcol in (1:ncol(Data_MYPC)))
          {
            # jcol = 1
            # jcol = 2
            # jcol = 3
            # jcol = 7
            # cell_ij = monitor_all[irow, jcol]
            if (!is.na(Data_MYPC[1, jcol]))
            {
              x = try(tk2dde.poke("excel", My_Sheet, paste0("r",(irow+Offset_Row), "c",jcol), Data_MYPC[1, jcol]))
            } else {
              x = try(tk2dde.poke("excel", My_Sheet, paste0("r",(irow+Offset_Row), "c",jcol), " - "))
            }
          }
        }
      }
    }
    
    Monitor_File = readRDS(paste0(CCPRData,'monitor_ccpr_files.rds'))
    My.Kable.All(Monitor_File)
    
    Offset_Row = 3
    Offset_Col = 10
    
    Monitor_Mat = as.matrix(Monitor_File)
    
    # NEW:------------------------------------------------------------------------
    
    for (irow in (1:nrow(Monitor_File)))
    {
      # irow = 1
      # irow = 2
      Location = try(tk2dde.request("Excel", My_Sheet, paste0("R", irow + Offset_Row, "C11")))
      switch (Location,
              'CCPRData' = { File_Folder = CCPRData }
      )
      Name_File = try(tk2dde.request("Excel", My_Sheet, paste0("R", irow + Offset_Row, "C12")))
      Data_File = Monitor_File[(location == Location & file == Name_File)]
      if(nrow(Data_File)==1)
      {
        Data_File = as.matrix(Data_File)
        for (jcol in (1:ncol(Data_File)))
        {
          # jcol = 1
          # jcol = 1+Offset_Col
          if (!is.na(Data_File[1, jcol]))
          {
            x = try(tk2dde.poke("Excel", My_Sheet, paste0("R",(irow+Offset_Row), "C",jcol+Offset_Col), Data_File[1, jcol]))
          } else {
            x = try(tk2dde.poke("Excel", My_Sheet, paste0("R",(irow+Offset_Row), "C",jcol+Offset_Col), " - "))
          }
        }
      }
    }
    
    Sys.sleep(10)
    x = try(tcltk2::tk2dde.poke("Excel", 'MONITOR_CCPR', "R1C2", substr(as.character(Sys.time()),11,19 )), silent = T)
  } else { CATln_Border(paste('FILE EXCEL DONT HAVE SHEET = ', 'MONITOR_FILES/UPDATED')) }
  DATACENTER_LINE_BORDER_BEGINEND('XLS_UPDATE_MONITOR_CCPR', 'END')
  GC_SILENT()
}

# ==================================================================================================
XLS_UPDATE_BATCH_MONITOR_CCPR = function()
{
  # XLS_UPDATE_MONITOR_CCPR()
  # ............................................................................
  My_Sheet = 'BATCH'
  # ............................................................................
  
  print(CCPRDrive)
  library(tcltk2)
  GC_SILENT()
  DATACENTER_LINE_BORDER_BEGINEND('XLS_UPDATE_MONITOR_CCPR', 'BEGIN')
  # try(tk2dde.request("Excel", "Sheet1", "R1C1:R3C1")
  # x1 = try(tcltk2::tk2dde.request("Excel", 'MONITOR_FILES', "R1C1"), silent = T)
  x2 = try(tcltk2::tk2dde.request("Excel", My_Sheet, "R1C1"), silent = T)
  x2.time = try(tcltk2::tk2dde.request("Excel", My_Sheet, "R1C2"), silent = T)
  
  if (all(class(x2)!='try-error'))
  {
    # Monitor_All = as.data.table(openxlsx::read.xlsx(paste0('R:/CCPR/', 'MONITOR/MONITORS_CCPR.xlsx'), sheet='MONITOR_CCPR'))
    # Monitor_All[, nnum_row:=seq.int(1, nrow(Monitor_All))]
    # Monitor_Matrix = as.matrix(Monitor_All)
    
    x.date = try(tcltk2::tk2dde.poke("Excel", My_Sheet, "R1C1", as.character(Sys.Date()) ), silent = T)
    x.time = try(tcltk2::tk2dde.poke("Excel", My_Sheet, "R1C2", substr(as.character(Sys.time()),11,19 )), silent = T)
    CATln_Border(paste(x.date, x.time))
    
    # ----------------------------------------------------------------------------------------------
    
  }
  
  
  Monitor_Funtion = readRDS(paste0(CCPRData,'MONITOR/BATCH_MYPC.rds'))
  
  Monitor_Funtion[, delay:= as.numeric(round(difftime(Sys.time(),as.POSIXct(updated) ,units = 'hours'),2))]
  # My.Kable.All(Monitor_Funtion)
  Monitor_Funtion = Monitor_Funtion [!is.na(pc) & nchar(pc) >2]
  Monitor_Funtions = spread(Monitor_Funtion[,.(pc, function_name,delay)], key = 'pc', value = 'delay')
  My.Kable.All(Monitor_Funtions)
  
  Monitor_Mat = as.matrix(Monitor_Funtions)
  # Offset_Row = 2
  
  # Monitor_File = readRDS(paste0(CCPRData,'monitor_ccpr_files.rds'))
  # My.Kable.All(Monitor_File)
  
  Offset_Row = 3
  Offset_Col = 1
  
  Monitor_Mat = as.matrix(Monitor_Funtions)
  
  # NEW:------------------------------------------------------------------------
  
  for (irow in (1:nrow(Monitor_Funtions)))
  {
    # irow = 1
    # irow = 2
    for (jcol in (1:ncol(Monitor_Mat)))
    {
      # jcol = 1
      # jcol = 1+Offset_Col
      if (!is.na(Monitor_Mat[irow, jcol]))
      {
        x = try(tk2dde.poke("Excel", My_Sheet, paste0("R",(irow+Offset_Row), "C",jcol+Offset_Col), Monitor_Mat[irow, jcol]))
      } else {
        x = try(tk2dde.poke("Excel", My_Sheet, paste0("R",(irow+Offset_Row), "C",jcol+Offset_Col), " - "))
      }
    }
    Sys.sleep(1)
    x = try(tcltk2::tk2dde.poke("Excel", 'BATCH', "R1C2", substr(as.character(Sys.time()),11,19 )), silent = T)
  } 
  
  DATACENTER_LINE_BORDER_BEGINEND('XLS_UPDATE_MONITOR_CCPR_BATCH', 'END')
  GC_SILENT()
}

# ==================================================================================================
XLS_UPDATE_BATCH_FILES_MONITOR_CCPR = function()
{
  # XLS_UPDATE_MONITOR_CCPR()
  # ............................................................................
  My_Sheet = 'BATCH'
  # ............................................................................
  
  print(CCPRDrive)
  library(tcltk2)
  GC_SILENT()
  DATACENTER_LINE_BORDER_BEGINEND('XLS_UPDATE_MONITOR_CCPR', 'BEGIN')
  # try(tk2dde.request("Excel", "Sheet1", "R1C1:R3C1")
  # x1 = try(tcltk2::tk2dde.request("Excel", 'MONITOR_FILES', "R1C1"), silent = T)
  x2 = try(tcltk2::tk2dde.request("Excel", My_Sheet, "R1C1"), silent = T)
  x2.time = try(tcltk2::tk2dde.request("Excel", My_Sheet, "R1C2"), silent = T)
  
  if (all(class(x2)!='try-error'))
  {
    # Monitor_All = as.data.table(openxlsx::read.xlsx(paste0('R:/CCPR/', 'MONITOR/MONITORS_CCPR.xlsx'), sheet='MONITOR_CCPR'))
    # Monitor_All[, nnum_row:=seq.int(1, nrow(Monitor_All))]
    # Monitor_Matrix = as.matrix(Monitor_All)
    
    x.date = try(tcltk2::tk2dde.poke("Excel", My_Sheet, "R19C1", as.character(Sys.Date()) ), silent = T)
    x.time = try(tcltk2::tk2dde.poke("Excel", My_Sheet, "R19C2", substr(as.character(Sys.time()),11,19 )), silent = T)
    CATln_Border(paste(x.date, x.time))
    
    # ----------------------------------------------------------------------------------------------
    
  }
  
  Offset_Row = 20
  Offset_Col = 0
  FullPath_Local = paste0(CCPRData, 'MONITOR/','CHECK_FILE_QUALITY.rds')
  FullPath_ODDrive = paste0(ODDrive, 'R/CCPR/DATA/MONITOR/','CHECK_FILE_QUALITY.rds')
  Monitor_Funtion = CCPR_READRDS(FileFolder = paste0(CCPRData, 'MONITOR/'), FileName = 'CHECK_FILE_QUALITY.rds', ToRestore = T)
  
  Monitor_Funtion = Monitor_Funtion [!is.na(files) & nchar(files) >2]
  # Monitor_Funtions = spread(Monitor_Funtion[,.(pc, function_name,delay)], key = 'pc', value = 'delay')
  Monitor_Funtions = Monitor_Funtion[,.(location,files, codes, start, end,codes_updated, updated_pct, records)]
  My.Kable.All(Monitor_Funtions)
  
  Monitor_Mat = as.matrix(Monitor_Funtions)
  # Offset_Row = 2
  
  # Monitor_File = readRDS(paste0(CCPRData,'monitor_ccpr_files.rds'))
  # My.Kable.All(Monitor_File)
  
  
  
  Monitor_Mat = as.matrix(Monitor_Funtions)
  
  # NEW:------------------------------------------------------------------------
  
  for (irow in (1:nrow(Monitor_Funtions)))
  {
    # irow = 1
    # irow = 2
    for (jcol in (1:ncol(Monitor_Mat)))
    {
      # jcol = 1
      # jcol = 1+Offset_Col
      if (!is.na(Monitor_Mat[irow, jcol]))
      {
        x = try(tk2dde.poke("Excel", My_Sheet, paste0("R",(irow+Offset_Row), "C",jcol+Offset_Col), Monitor_Mat[irow, jcol]))
      } else {
        x = try(tk2dde.poke("Excel", My_Sheet, paste0("R",(irow+Offset_Row), "C",jcol+Offset_Col), " - "))
      }
    }
    Sys.sleep(1)
    x = try(tcltk2::tk2dde.poke("Excel", 'BATCH', "R1C2", substr(as.character(Sys.time()),11,19 )), silent = T)
  } 
  
  DATACENTER_LINE_BORDER_BEGINEND('XLS_UPDATE_MONITOR_CCPR_BATCH', 'END')
  GC_SILENT()
}

# ==================================================================================================
NEW_DOWNLOAD_CAF_INDVN_FOREIGN = function(pCode = 'VNINDEX')
{
  # pCode = 'XYZ'
  # pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=', pCode, '&StartDate=30/12/2022&EndDate=30/12/2023&PageIndex=3&PageSize=20')
  # NbPagesBack = 20
  List_Codes = setDT(fread(paste0(CCPRData, 'LIST/', 'LIST_CODE_SOURCE_SPECIAL.txt')))
  CODEREF    = List_Codes[codesource==toupper(pCode) & source=='CAF'] 
  if (nrow(CODEREF)==1) { INDEX_CODE = CODEREF$coderef } else { INDEX_CODE = as.character(NA)}
  CATln_Border(paste('CODE REF = ', INDEX_CODE))
  
  Data_List = list()
  ToContinu = T
  k = 1
  
  while (ToContinu)
  {
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=VNINDEX&StartDate=&EndDate=&PageIndex=2&PageSize=20
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx?Symbol=VNINDEX&StartDate=&EndDate=&PageIndex=1&PageSize=20
    # pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=', pCode, '&StartDate=&EndDate=&PageIndex=', k, '&PageSize=100')
    pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx?Symbol=', pCode, '&StartDate=&EndDate=&PageIndex=', k, '&PageSize=100')
    CATrp(pURLk)
    x = try(jsonlite::fromJSON(pURLk))
    if (length(x$Data$Data)>0)
    {
      XData = try(as.data.table(x$Data$Data))
      if (all(class(XData)!='try-error'))
      {
        XData = CLEAN_COLNAMES(XData)
        Final_Data = XData[, .(source='CAF', codesource=pCode, ticker=pCode, dataset='FOREIGN',
                               date      = as.Date(ngay, '%d/%m/%Y'), 
                               buy_vol   = as.numeric(klmua),
                               sell_vol  = as.numeric(klban),
                               buy_val   = as.numeric(gtmua),
                               sell_val  = as.numeric(gtban),
                               bs_vol    = as.numeric(klgdrong),
                               bs_val    = as.numeric(gtdgrong),
                               updated=substr(Sys.time(),1,19)
        )]
        My.Kable.TB(Final_Data)
        Data_List[[k]] = Final_Data
      } else {
        ToContinu = F
      }
    } else {
      ToContinu = F
    }
    k = k+1
  }
  
  CATln(pURLk); CATln('')
  Data_All = rbindlist(Data_List, fill=T)
  if (nrow(Data_All)>0)
  {
    Data_All[, code:=INDEX_CODE]
    Data_All = Data_All[order(date)]
    if (nrow(Data_All[!is.na(code)])>0)
    {
      RELOAD_INSREF()
      Data_All[, ':='(bsimb_vol=(buy_vol-sell_vol)/(buy_vol+sell_vol), bsimb_val=(buy_val-sell_val)/(buy_val+sell_val))]
      Data_All = merge(Data_All[, -c('type', 'fcat', 'scat', 'name')], ins_ref[, .(code, type, fcat, scat, name=short_name)], all.x=T, by='code')
    }
    My.Kable.TB(Data_All)
  } else {
    Data_All = data.table()
  }
  return(Data_All)
}

# ==================================================================================================
NEW_DOWNLOAD_CAF_INDVN_TRADE = function(pCode = 'VNINDEX')
{
  # TO BE IMPROVED
  # pCode = 'XYZ'
  # pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=', pCode, '&StartDate=30/12/2022&EndDate=30/12/2023&PageIndex=3&PageSize=20')
  # NbPagesBack = 20
  List_Codes = setDT(fread(paste0(CCPRData, 'LIST/', 'LIST_CODE_SOURCE_SPECIAL.txt')))
  CODEREF    = List_Codes[codesource==toupper(pCode) & source=='CAF'] 
  if (nrow(CODEREF)==1) { INDEX_CODE = CODEREF$coderef } else { INDEX_CODE = as.character(NA)}
  CATln_Border(paste('CODE REF = ', INDEX_CODE))
  
  Data_List = list()
  ToContinu = T
  k = 1
  
  while (ToContinu)
  {
    # k = 4
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=VNINDEX&StartDate=&EndDate=&PageIndex=2&PageSize=20
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx?Symbol=VNINDEX&StartDate=&EndDate=&PageIndex=1&PageSize=20
    # pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=', pCode, '&StartDate=&EndDate=&PageIndex=', k, '&PageSize=100')
    
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/GDTuDoanh.ashx?Symbol=VNINDEX&StartDate=&EndDate=&PageIndex=1&PageSize=20
    pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/GDTuDoanh.ashx?Symbol=', pCode, '&StartDate=&EndDate=&PageIndex=', k, '&PageSize=100')
    CATrp(pURLk)
    x = try(jsonlite::fromJSON(pURLk))
    if (length(x$Data$Data)>0)
    {
      XData = try(as.data.table(x$Data$Data))
      if (all(class(XData)!='try-error') && nrow(XData)>1)
      {
        XData = CLEAN_COLNAMES(XData)
        
        Final_Data = XData[, .(source='CAF', codesource=pCode, ticker=pCode, dataset='TRADE',
                               date      = as.Date(listdatatudoanh_date, '%d/%m/%Y'), 
                               buy_vol   = as.numeric(tongklmua),
                               sell_vol  = as.numeric(tongklban),
                               buy_val   = as.numeric(tonggtmua),
                               sell_val  = as.numeric(tonggtban),
                               updated=substr(Sys.time(),1,19)
        )]
        My.Kable.TB(Final_Data)
        Data_List[[k]] = Final_Data
      } else {
        ToContinu = F
      }
    } else {
      ToContinu = F
    }
    k = k+1
  }
  
  CATln(pURLk); CATln('')
  Data_All = rbindlist(Data_List, fill=T)
  if (nrow(Data_All)>0)
  {
    Data_All[, code:=INDEX_CODE]
    Data_All = Data_All[order(date)]
    if (nrow(Data_All[!is.na(code)])>0)
    {
      Data_All[, ':='(bsimb_vol=(buy_vol-sell_vol)/(buy_vol+sell_vol), bsimb_val=(buy_val-sell_val)/(buy_val+sell_val))]
      RELOAD_INSREF()
      Data_All = merge(Data_All[, -c('type', 'fcat', 'scat', 'name')], ins_ref[, .(code, type, fcat, scat, name=short_name)], all.x=T, by='code')
    }
    My.Kable.TB(Data_All)
  } else {
    Data_All = data.table()
  }
  return(Data_All)
}

# ==================================================================================================
FINAL_DOWNLOAD_C68_STKVN_INTRADAY_BY_CODE = function(pCode = 'VND')
{
  Final_Data = data.table()
  pURL       = paste0('https://www.cophieu68.vn/quote/summary.php?id=', tolower(pCode))
  CATln_Border(pURL)
  content    = try(rvest::read_html(pURL))
  if (all(class(content)!='try-error'))
  {
    tables     = content %>% html_table(fill = TRUE)
    
    xData      = as.data.table(tables[[4]])
    if (nrow(xData)>0)
    {
      Final_Data = xData[, .(date=SYSDATETIME(10), codesource=pCode, source='C68', time=X1, 
                             last=1000*as.numeric(gsub(',','', as.character(X2))),
                             last_volume=as.numeric(gsub(',','', as.character(X4))),
                             volume=as.numeric(gsub(',','', as.character(X5)))
      )]
      Final_Data = Final_Data[!is.na(last)]
      Final_Data[, code:=paste0('STKVN', codesource)]
      Final_Data[, datetime:=paste0(date,' ', time, ':00')]
      Final_Data[, timestamp:=paste0(date,' ', time, ':00')]
      My.Kable(Final_Data)
    }
  }
  return(Final_Data)
}

# ==================================================================================================
DOWNLOAD_HNX_STKVN_SHARES_BY_CODE = function(pCode='PSI')
{
  pURL       = paste0('https://hnx.vn/cophieu-etfs/chi-tiet-chung-khoan-ny-', pCode, '.html?_des_tab=1')
  CATln_Border(pURL)
  pWeb      = WEB_SAVE_AS(pURL = pURL, SaveFolder = 'c:/python/downloads/')
  pWeb = gsub('\n','',pWeb)
  xROWS = unlist(strsplit(pWeb, '<div class="dktimkiem_row row_inline">'))
  # for (k in 2:(length(xROWS)))
  # {
  #   CATln_Border(k)
  #   print(xROWS[[k]])
  # }
  xData = as.data.table(xROWS[2:(length(xROWS))])
  # View(xData)
  xData[, dataset:=word(word(V1,2,2, '<label>'),1,1,'</label>')]
  xData[, datavalue:=trimws(word(word(V1,2,2, 'content">'),1,1,'</div>'))]
  xData[, dataset_en:=vietnameseConverter::decodeVN(dataset, from='Unicode', to='Unicode', diacritics=F)]
  
  LastTrading = CCPR_LAST_TRADING_DAY_VN(pHH=9, ToPrompt = T)
  
  Final_Data = data.table(
    source = 'HNX', codesource = pCode, date = LastTrading, code = paste0('STKVN', pCode), 
    sharesout=as.numeric(gsub('[.]','', xData[grepl('^KLLH', dataset_en)]$datavalue) ),
    shareslis=as.numeric(gsub('[.]','', xData[grepl('^KLNY', dataset_en)]$datavalue) ),
    name_vn  =(gsub('[.]','', xData[grepl('^Ten TCPH', dataset_en)]$datavalue) ),
    name_sector = (gsub('[.]','', xData[grepl('^Ten nganh', dataset_en)]$datavalue) ),
    first_date  =(as.Date(xData[grepl('^Ngay GD dau tien', dataset_en)]$datavalue, '%d/%m/%Y')) )
  if (nrow(Final_Data[!is.na(sharesout)])==0)
  {
    Final_Data = data.table()
  }
  My.Kable.All(Final_Data)
  return(Final_Data)
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
DAILY_ACTION_REPORT = function ()
{
  library(chron)
  RDT_TEAM = setDT(openxlsx::read.xlsx(paste0(ODDrive, 'BeQ/CCPR/DATA/MONITOR/', 'MANAGEMENT_RD_TEAM.xlsx'), sheet='DAILY_ACTIONS'))
  RDT_TEAM[, date:=as.Date(date, origin = "1899-12-30")]
  RDT_TEAM[, hhmmss:=chron::times(time) ]
  My.Kable.TB(RDT_TEAM)
  RDT_TEAM = RDT_TEAM[hhmmss + 0*15*0 < format(Sys.time(), "%H:%M:%S")]
  # x = as.times( format(Sys.time(), "%H:%M:%S"))  > RDT_TEAM[13]$hhmmss + 0*15*0
  # str(RDT_TEAM$hhmmss + 0*15*0)
  
  data = data.table
  member = c('MINH', 'TAN', 'YEN', 'PHUNG', 'PHONG')
  RDT_TEAM [(presence == 'n'), primary := backup]
  data_final = list()
  for (i in 1: length (member)) 
  {
    # i = 3
    
    pMember = member [[i]]
    pTask = nrow(RDT_TEAM[primary == pMember])
    pDone = nrow(RDT_TEAM[primary == pMember][status == 'Done'])
    x =  RDT_TEAM[primary == pMember][status != 'Done'] $ name
    x = strsplit(x, split = T)
    pNote = paste( x, collapse = '; ') 
    
    if ( pDone !=0 ) {
      data_merge = data.table( name = pMember, date = Sys.Date(), updated = Sys.time(), task = pTask, done = pDone,
                               point = round( (pDone/pTask) *100, 2), note = pNote)
    } else {  data_merge = data.table( name = pMember, date = Sys.Date(), updated = Sys.time(), task = pTask, done = pDone,
                                       point = 0, note = pNote)
    }
    
    data_final [[i]] = data_merge
  }
  data = rbindlist(data_final)
  My.Kable.All(data)
  return (data)
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
UPDATE_PC_FUNCTION = function(FN_Name)
{ #--------------------------------------------------------------------------------------------------
  # FN_Name = 'XXX'
  if (file.exists('c:/r/my_pc.txt'))
  {
    MyPC = readLines('c:/r/my_pc.txt')
  } else { MyPC       =  ''}
  My_Updated = data.table(pc=as.character(MyPC), function_name=FN_Name, updated=substr(Sys.time(),1,19))
  My.Kable.All(My_Updated)
  IFRC_SLEEP(5)
  return(My_Updated)
}

#=================================================================================================
BATCH_FUNCTION_V2 = function()
{
  rm(FN_Name_BATCH_FUNCTION)
  FN_Name_BATCH_FUNCTION    = match.call()[[1]]
  print(FN_Name_BATCH_FUNCTION)
  # assign("FN_Name_BATCH_FUNCTION", FN_Name_BATCH_FUNCTION, envir = .GlobalEnv)
  # Sys.sleep(1)
  # MyPC       = GET_MYPC()
  # My_Updated = data.table(pc=MYPC, function_name=FN_Name, updated=substr(Sys.time(),1,19))
  # 
  To_Update  = UPDATE_PC_FUNCTION(FN_Name=as.character(FN_Name_BATCH_FUNCTION))
  # To_Update = My_Updated
  My.Kable.All(To_Update)
}

#=================================================================================================
NEW_DOWNLOAD_C68_STKVN_PERFORMANCE = function(pCode='VND', MaxPages=1)
{
  # x = NEW_DOWNLOAD_C68_STKVN_PERFORMANCE(pCode='VND', MaxPages=1)
  ToContinu = T
  Data_All  = data.table()
  LastTrading = CCPR_LAST_TRADING_DAY_VN(max(9, hour(Sys.time())), ToPrompt = T)
  
  for (iPage in 1:MaxPages)
  {
    iPage = 1
    if (ToContinu)
    {
      pURL       = paste0('https://www.cophieu68.vn/quote/history.php?currentPage=', iPage, '&id=', tolower(pCode))
      content    = try(rvest::read_html(pURL))
      if (all(class(content)!='try-error'))
      {
        RELOAD_INSREF()
        tables     = content %>% html_table(fill = TRUE)
        # tables[[2]] = gsub('\t',' ', tables[[2]])
        xData      = as.data.table(tables[[1]])
        
        if (nrow(xData)>1)
        {
          xHeader    = xData[1,]
          xData      = xData[-1,]
          xData      = CLEAN_COLNAMES(xData)
          # str(xHeader)
          for (k in 1:nrow(xData))
          {
            xData[k]$x1 = REPLACE_LOOP_ALL(pStr=xData[k]$x1, Str_From='\t\t', Str_By='\t')
            xData[k]$x1 = REPLACE_LOOP_ALL(pStr=xData[k]$x1, Str_From='\r\n', Str_By='')
          }
          # xData[, x2:=gsub('  ',' ', x2)]
          Final_Data = xData[, .(source='C68', codesource=pCode, code=paste0('STKVN', pCode), date = LastTrading,
                                 per_mm = as.numeric(word(x1,1,1)),
                                 per_enddate = as.Date(substr(word(x1,2,2,'\\('),1,10)),
                                 change = 1000*as.numeric(gsub(',','', x2)),
                                 varpc  = as.numeric(gsub(',|%','', x3)),
                                 volume = as.numeric(gsub(',','', x4)),
                                 foreign_bs_volume = as.numeric(gsub(',','', x5))
          )]
          # My.Kable.TB(Final_Data)
          Final_Data = Final_Data[order(date)]
          Final_Data = merge(Final_Data[, -c('name')], ins_ref[, .(code, name=short_name)], all.x=T, by='code')
          My.Kable.TB(Final_Data)
          Data_All = unique(rbind(Data_All, Final_Data, fill=T), by=c('code', 'date','per_mm'))
          CATln_Border(paste('TOTAL RECORDS = ', nrow(Data_All)))
          CATln('')
        } else { ToContinu = F }
      }
    }
  }
  if (nrow(Data_All)>0)
  {
    Data_All = Data_All[order(code, date)]
    CATln_Border('FINALLY, ALL DATA :')
    My.Kable.All(Data_All)
  }
  return(Data_All)
}

#=================================================================================================
DOWNLOAD_STB_STKVN_RATIOS_VALUATION = function(pCode = 'VND')
{
  RELOAD_INSREF()
  Final_Data  = data.table()
  LastTrading = CCPR_LAST_TRADING_DAY_VN(max(9, hour(Sys.time())), ToPrompt = T)
  
  pURL  = paste0('http://en.stockbiz.vn/Stocks/', pCode, '/Overview.aspx')
  CATln_Border(pURL)
  content    = try(rvest::read_html(pURL))
  
  if (all(class(content)!='try-error'))
  {
    tables     = content %>% html_table(fill = TRUE)
    xData      = try(SELECT_TABLE_WITH_FIELD(tables, pField='x1', pContent="P/E", like=F))
    if (all(class(xData)!='try-error'))
    {
      # if (all(class(xData)!='try-error'))
      # {
      #   xData = tables [[50]]
      # }
      Final_Data = xData[, .(source='STB', ticker=pCode, codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading, dataset=x1, value=as.numeric(gsub('x$','',x2)))]
      Final_Data = merge(Final_Data[, -c('name')], ins_ref[type=='STK'][, .(code, name=short_name)], all.x=T, by='code')
      My.Kable.All(Final_Data)
    } else { Final_Data = data.table()}
  }
  return(Final_Data)
}

# ==================================================================================================
CHECK_BATCH = function(To_Update ) {
  FileName = 'CHECK_BATCH.rds'
  pFolder  = paste0(CCPRData, 'BATCH/')
  ODDrive_Data = paste0(ODDrive, substr(pFolder, 1, 1), word(pFolder, 2, 2, ":"))
  CATln(ODDrive_Data)
  
  if (!file.exists(ODDrive_Data))
  {
    try(dir.create(ODDrive_Data))
  }
  
  if (!file.exists(pFolder))
  {
    try(dir.create(pFolder))
  }
  # as.Date('2023-08-18 15:48:00')
  FullPath_Local = paste0(pFolder, FileName)
  FullPath_ODDrive = paste0(ODDrive_Data, FileName)
  # To_Update
  Data_One = data.table(
    mypc        = To_Update $pc,
    date        = as.Date (To_Update $updated),
    updated     = To_Update$updated,
    fn          = To_Update $function_name,
    batch       = current_filename()
  )
  if (!file.exists(FullPath_Local)) {
    Data_MYPC = data.table()
  } else {
    Data_MYPC = CCPR_READ_RDS_WITH_ALTERNATIVE(FileFrm = FullPath_Local, FileAlt = FullPath_ODDrive)[[1]]
  }
  Data_MYPC = rbind(Data_MYPC, Data_One, fill = T)
  Data_MYPC = unique(Data_MYPC[order(mypc,-updated)], by = 'mypc')
  My.Kable.All(Data_MYPC)
  
  try(SAVERDS_WITH_ONEDRIVE(pData=Data_MYPC, FileFolder= pFolder, FileName=FileName , SaveOneDrive=T))
}

# ==================================================================================================
CCPR_CHECK_CONNEXION = function(pSleep = 10) {
  # ------------------------------------------------------------------------------------------------
  # try(CCPR_CHECK_MYPC(pSleep=10))
  FileName = 'CHECK_MYCONNEXION.rds'
  pFolder  = paste0(CCPRData, 'CONNEXION/')
  ODDrive_Data = paste0(ODDrive, substr(pFolder, 1, 1), word(pFolder, 2, 2, ":"))
  CATln(ODDrive_Data)
  
  if (!file.exists(ODDrive_Data))
  {
    try(dir.create(ODDrive_Data))
  }
  FullPath_Local = paste0(pFolder, FileName)
  FullPath_ODDrive = paste0(ODDrive_Data, FileName)
  
  Has_Py_Download = ifelse(file.exists('c:/r/my_pc.txt'), 'Y', 'N')
  Has_CCPR        = if_else(file.exists(FullPath_Local),'Y','N')
  Has_Udata       = if_else(file.exists(FullPath_ODDrive),'Y','N')
  
  Has_connetions  = setDT(read.csv ('c:/r/my_connections.txt', header = F))
  My_Data         = list()
  Nb_Todo         = nrow(Has_connetions)
  for (i in 1:Nb_Todo)
  {
    URL = Has_connetions$V1 [[i]]
    CATln_Border(paste( 'CHECKING CONNECTION:',i,'/',Nb_Todo, URL))
    Has_MyCONNEXION = CHECK_CONNEXION(pURL = URL, ToKable=T)
    
    Data = data.table(
      my_url        = Has_MyCONNEXION [[1]],
      date          = Sys.Date(),
      updated       = substr(Sys.time(), 1, 19),
      my_connexion  = Has_MyCONNEXION [[2]],
      ccprdata      = Has_CCPR,
      oddata        = Has_Udata
    )
    My_Data [[i]] = Data
  }
  Data_One  = rbindlist(My_Data, fill = T)
  
  if (!file.exists(FullPath_Local)) {
    Data_MYPC = data.table()
  } else {
    Data_MYPC = CCPR_READ_RDS_WITH_ALTERNATIVE(FileFrm = FullPath_Local, FileAlt = FullPath_ODDrive)[[1]]
  }
  Data_MYPC = rbind(Data_MYPC, Data_One, fill = T)
  Data_MYPC = unique(Data_MYPC[order(my_url,-updated)], by = 'my_url')
  Data_MYPC$x = NULL
  My.Kable.All(Data_MYPC)
  # saveRDS(Data_MYPC, 'R:/CCPR/DATA/CONNEXION/CHECK_MYCONNEXION.rds')
  try(SAVERDS_WITH_ONEDRIVE(pData=Data_MYPC, FileFolder=paste0(CCPRData, 'CONNEXION/'), FileName=FileName , SaveOneDrive=T))
  IFRC_SLEEP(pSleep)
}  

#=================================================================================================
CCPR_UPDATE_MONITOR_CCPR_FILES = function(ToForce=F)
{
  # try(CCPR_UPDATE_MONITOR_CCPR_FILES())
  File_XLS = paste0(CCPRDrive, 'CCPR/MONITOR/', 'MONITOR_CCPR.xlsx')
  CATln_Border(File_XLS)
  x = setDT(openxlsx::read.xlsx(File_XLS, rows = c(3:200), cols = c(11, 12)))
  x = CLEAN_COLNAMES(x)
  x[, nr:=seq.int(1,nrow(x))]
  x[, ':='(exists=as.character(NA), miss_fields=as.character(NA),err_str=as.character(NA), codes=as.numeric(NA), start=as.Date(NA), 
           end=as.Date(NA), end_codes=as.numeric(NA), records=as.numeric(NA))]
  My.Kable.All(x)
  
  for (k in 1:nrow(x))
  {
    # k = 31
    FilePath    = ''
    FileSummary = ''
    File_Summary = data.table()
    pFolder = x[k]$location
    switch (pFolder,
            'CCPRData' = { File_Folder = CCPRData }
    )
    FilePath    = tolower(paste0(File_Folder, x[k]$file, '.rds'))
    FileSummary = gsub('.rds$', '_summary.txt', FilePath, ignore.case = T)
    CATln_Border(FilePath)
    CATln_Border(FileSummary)
    CATln(paste('FILES EXISTS = ', file.exists(FilePath), file.exists(FileSummary)))
    
    if (file.exists(FilePath)) { x[k]$exists='Y' } else { x[k]$exists='N'}
    
    if (file.exists(FileSummary) & file.exists(FilePath))
    {
      File_Summary = try(setDT(fread(FileSummary)))
      if (!all(class(File_Summary)!='try-error'))
      {
        CATln('Writing SUMMARY ...')
        xs = try(WRITE_SUMMARY_FULLPATH(FilePath))
        if (all(class(xs)!='try-error'))
        {
          File_Summary = try(setDT(fread(FileSummary)))
        } else {
          File_Summary = data.table()
          x[k]$miss_fields='Y' 
        }
      }
    } else {
      if (!file.exists(FileSummary) & file.exists(FilePath))
      {
        xf = try(readRDS(FilePath))
        if (all(class(xf)!='try-error'))
        {
          str(xf)
          CATln('Writing SUMMARY ...')
          xs = try(WRITE_SUMMARY_FULLPATH(FilePath))
          if (all(class(xs)!='try-error'))
          {
            File_Summary = try(setDT(fread(FileSummary)))
          } else {
            File_Summary = data.table()
            x[k]$miss_fields='Y' 
          }
        } else {
          x[k]$err_str='Y' 
        }
      }
    }
    
    if (nrow(File_Summary)>0)
    {
      My.Kable(File_Summary)
      MaxDate = max(File_Summary$date)
      File_Info = File_Summary[, .(codes=.N, start=min(start), end=max(date), end_codes=sum(ifelse(date==MaxDate,1,0)),records=sum(nb, na.rm=T))]
      x[k]$start = File_Info$start
      x[k]$end = File_Info$end
      x[k]$codes = File_Info$codes
      x[k]$end = File_Info$end
      x[k]$end_codes = File_Info$end_codes
      x[k]$records = File_Info$records
    }
  }
  x$nr = NULL
  x$x = NULL
  My.Kable.All(x)
  SAVERDS_WITH_ONEDRIVE(pData=x, FileFolder = CCPRData, FileName='monitor_ccpr_files.rds', ToKable = F, SaveOneDrive=T, ToSummary=F)
}
#====================================================================================================
REPLACE_LOOP_ALL = function(pStr='MAI   Huu  Minh', Str_From='  ', Str_By=' ')
{
  pString = pStr
  while (str_count(pString, Str_From)>0)
  {
    pString = gsub(Str_From, Str_By, pString)
  }
  return(pString)
}

# 
# #====================================================================================================
# DOWNLOAD_STB_STKVN_MAJOR_HOLDERS_PERCENTAGE_BY_CODE = function(pCode='VIC')
# {
#   Final_Data = data.table()
#   pURL = paste0('https://www.stockbiz.vn/Stocks/', pCode, '/MajorHolders.aspx')
#   CATln_Border(pURL)
#   content    = try(rvest::read_html(pURL))
#   if (all(class(content)!='try-error'))
#   {
#     tables     = content %>% html_table(fill = TRUE)
#     xData      = as.data.table(tables[[13]])
#     xData      = CLEAN_COLNAMES(xData)
#     # My.Kable(xData)
#     LastTrading = CCPR_LAST_TRADING_DAY_VN(9, ToPrompt = T)
#     Final_Data = xData[, .(source='STB', codesource=pCode, ticker=pCode, code=paste0("STKVN", pCode), 
#                            date = LastTrading,
#                            people_name=tên, position=vịtrí, 
#                            shares=as.numeric(gsub('[.]','',sốcổphần)),
#                            percentage=as.numeric(gsub(',','.', gsub('%','',tỷlệsởhữu))),
#                            lastupdate=as.Date(substr(trimws(ngàycậpnhật),1,10), '%d/%m/%Y')
#     )]
#     Final_Data[position=='---', position:=as.character(NA)]
#     RELOAD_INSREF()
#     Final_Data = merge(Final_Data[, -c('name')], ins_ref[type=='STK'][, .(code, name=short_name)], all.x=T, by='code')
#     Final_Data = Final_Data %>% select(source, code, codesource, ticker, date, name, everything())
#     My.Kable.TB(Final_Data)
#   }
#   return(Final_Data)
# }

#====================================================================================================
DOWNLOAD_CAF_STKVN_ANNUAL_RATIOS_BY_CODE = function(pCode='XDC')
{
  pURL = paste0('https://e.cafef.vn/fi.ashx?symbol=', pCode)
  x    = setDT(try(jsonlite::fromJSON(pURL)))
  if (all(class(x)!='try-error') & (nrow(x) >0))
  {
    xData = as.data.table(x)
    xData = CLEAN_COLNAMES(xData)
    xData[, epoch:=substr(word(lastupdate,2,2,'\\('),1,13)]
    xData$lastupdate=substr(trimws(as.character(as.POSIXct(as.numeric(xData$epoch)/1000, origin="1970-01-01"))),1,19)
    xData$datetime = NULL
    xData$epoch = NULL
    LastTrading = CCPR_LAST_TRADING_DAY_VN(9, ToPrompt = T)
    xData[, ':='(date=LastTrading, source='CAF', codesource=pCode, ticker=pCode, code=paste0("STKVN", pCode))]
    RELOAD_INSREF()
    xData = merge(xData[, -c('name')], ins_ref[type=='STK'][, .(code, name=short_name)], all.x=T, by='code')
    xData = xData %>% select(source, code, codesource, ticker, date, year, everything())
    
    My.Kable.All(xData)
  } else {xData = data.table()}
  return(xData)
}


QUICK_REPORT_BY_FILES = function (CurMonth = '202308' , File_List = 'LIST_YAH_STK_GBL_WCEO', pFolder = CCPRData) {
  
  # CurMonth          = '202308' 
  # File_List         = 'LIST_YAH_STK_GBL_WCEO'
  File_Prices_Close = paste0('DOWNLOAD_', gsub('^LIST_','',File_List), '_PRICES_CLOSE_',CurMonth, '.rds')
  File_Prices_CloseADJ = paste0('DOWNLOAD_', gsub('^LIST_','',File_List), '_PRICES_CLOSEADJ_',CurMonth , '.rds')
  File_Prices_DIV = paste0('DOWNLOAD_', gsub('^LIST_','',File_List), '_DIV_',CurMonth, '.rds')
  File_Prices_BOARD = paste0('DOWNLOAD_', gsub('^LIST_','',File_List), '_BOARD_',CurMonth, '.rds')
  File_Prices_CAPIUSD = paste0('DOWNLOAD_', gsub('^LIST_','',File_List), '_CAPIUSD_',CurMonth, '.rds')
  # pFolder = "R:/CCPR/DATA/"
  list_file = c(File_Prices_Close, File_Prices_CloseADJ, File_Prices_DIV, File_Prices_BOARD, File_Prices_CAPIUSD )
  files = paste0(pFolder, list_file)
  summary = FILE_SUMMARY_CODESOURCE_DATE( files)
  data = setDT(summary[[1]])
  return(data)
}

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


# xURL = CREATE_JQGRID_API_URL(pFolder=CCPRData, FileName = 'ccpr_report_size_overview.rds')

CREATE_JQGRID_API_URL = function(pFolder, FileName = 'ccpr_report_size_overview.rds')
{
  x        = CCPR_READ_RDS_WITH_ALTERNATIVE(FileFrm=paste0(pFolder, FileName))[[1]]
  My.Kable.All(x)
  List_Fields = paste0(names(x), collapse=',' )
  List_Label  = paste0(names(x), collapse=',' )
  typeof(names(x)[1])
  class(names(x)[2])
  z = as.character(str(x))
  
  z = as.data.table(capture.output(str(x)))
  z[, V1:=trimws(V1)]
  # z[2]$V1
  z = z[substr(V1,1,1)=='$']
  z[, fields:=gsub('[$] ','', word(V1,1,1, ':'))]
  z[, type:=word(word(V1,2,2, ':'),1,1, "  ")]
  z[, type:=trimws(word(word(V1,2,2, ':'),1,1, "  "))]
  z[type=='chr', jq_type:='c']
  z[type=='num', jq_type:='n']
  z[type=='int', jq_type:='n']
  z[type=='Date, format', jq_type:='d']
  z[, jq_field:=fields]
  z[, jq_label:=gsub('_',' ', fields)]
  My.Kable.All(z)
  
  List_Fields = paste0(trimws(z$jq_field), collapse=',' )
  List_Labels = gsub(' ','', toupper(paste0(trimws(z$jq_label), collapse=',' )))
  List_Format = tolower(paste0(trimws(z$jq_type), collapse=',' ))
  List_Width  = paste0(as.list(rep(10, nrow(z))), collapse=',' )
  # List_Format = paste0(as.list(rep(10, nrow(z))), collapse=',' )
  
  pURL = paste0('https://datacenter.ccpr.vn/api/get_file/demo@ccpr.vn:Demo2023?filerds=',FileName, '&fields=', List_Fields,
                '&label=',List_Labels,'&width', List_Width, '&format=', List_Format)
  catrep('-',120)
  CATln(pURL)
  return(pURL)
}
#=================================================================================================
UPDATE_DOWNLOAD_PREFIX_HISTORY = function(pFolder, FileNamePrice='download_hsx_stkvn_prices.rds', FileNameHistory='download_hsx_stkvn_prices_history.rds', CheckField='', ToForce=F)
{
  # CheckField='volume'
  CATln_Border(paste('UPDATE_DOWNLOAD_PREFIX_HISTORY :', FileNameHistory))
  CATln('HISTORY :')
  if (ToForce || (SUMMARY_DATETIME_RDS(FileNameRDS=paste0(pFolder, FileNameHistory),ToKable=T) < SUMMARY_DATETIME_RDS(FileNameRDS=paste0(pFolder, FileNamePrice),ToKable=T)))
  {
    if (nchar(CheckField)==0)
    {
      try(CCPR_INTEGRATION(FileFolder=pFolder, FileFrom=FileNamePrice, FileTo=FileNameHistory, GroupBy='code x date', ToSummary=T))
    } else {
      Data_Price = readRDS(paste0(pFolder, FileNamePrice))
      if (CheckField %in% names(Data_Price))
      {
        try(CCPR_INTEGRATION(FileFolder=pFolder, FileFrom=FileNamePrice, FileTo=FileNameHistory, GroupBy='code x date', ToSummary=T))
      }
    }
  } else { CATln_Border(paste("ALREADY UPDATED :",FileNameHistory)) }
}
#===================================================================================================
SUMMARY_DATETIME = function(FileName, ToKable=F){
  # ------------------------------------------------------------------------------------------------
  # FileName=paste0(UData, "download_hsx_stkvn_intraday_day_summary.txt")
  # SUMMARY_DATE(FileName=paste0(UData, "efrc_indoth_history_summaryx.txt"), ToKable=F)
  datemax = as.Date("1900-01-01")
  if (file.exists(FileName))
  {
    dt_txt = try(DATACENTER_LOADTXTFILE(FileName, ToKable))
    if (class(dt_txt)!='try-error' && nrow(dt_txt)>0)
    {
      datemax = as.character(substr(max(dt_txt[!is.na(date)]$date),1,19))
    }
  } else {CATln(paste("FILE DOES NOT EXIST = ", FileName))}
  return(datemax)
}

#===================================================================================================
SUMMARY_DATETIME_RDS = function(FileNameRDS, ToKable=F){
  # ------------------------------------------------------------------------------------------------
  # FileName=paste0(UData, "download_hsx_stkvn_intraday_day_summary.txt")
  # SUMMARY_DATETIME_RDS(FileNameRDS=paste0(UData, "efrc_indoth_history.rds"), ToKable=F)
  datemax = as.Date("1900-01-01")
  FileName = gsub('.rds$', '_summary.txt', FileNameRDS, ignore.case = T)
  if (file.exists(FileName))
  {
    dt_txt = try(DATACENTER_LOADTXTFILE(FileName, ToKable))
    if (all(class(dt_txt)!='try-error') && nrow(dt_txt)>0)
    {
      datemax = as.character(substr(max(dt_txt[!is.na(date)]$date),1,19))
    }
  } else {CATln(paste("FILE DOES NOT EXIST = ", FileName))}
  return(datemax)
}

#===================================================================================================
CCPR_WRITE_SUMMARY_CURRENT_V1 = function(FileNameRDS, x.rds, ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T) {
  # ------------------------------------------------------------------------------------------------
  # xs = WRITE_SUMMARY(UData, "efrc_indexp_history.rds")
  
  # x.rds     = try(readRDS(pFileName))
  # if (all(class(x.rds)!='try-error') && nrow(x.rds)>0)
  # {
  RELOAD_INSREF()
  # str(ins_ref)
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
  
  my.FileTXT  = gsub(".rds", "_summary.txt", FileNameRDS, ignore.case = T)
  # my.FileTXT
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
  # }
} 

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

#==================================================================================
FINAL_DOWNLOAD_C68_STKVN_SHARES_BY_CODE = function(pCode = 'VND', ToCheckTime=9)
{
  # pCode = 'YRC'
  Final_Data = data.table()
  pURL = paste0('https://www.cophieu68.vn/quote/profile.php?id=', tolower(pCode))
  CATln_Border(pURL)
  content    = try(rvest::read_html(pURL))
  if (all(class(content)!='try-error'))
  {
    tables     = content %>% html_table(fill = TRUE)
    if (length(tables)>0)
    {
      xData      = as.data.table(tables[[1]])
      xData$field = decodeVN(xData$X1, from='Unicode', to='Unicode', diacritics=F)
      SharesOut  = as.numeric(gsub(',','', xData[field=='KL luu hanh']$X2))
      SharesLis  = as.numeric(gsub(',','', xData[field=='KL niem yet']$X2))
      LastTrading = CCPR_LAST_TRADING_DAY_VN(max(9, hour(Sys.time())), ToPrompt = T)
      Final_Data = data.table(source='C68', codesource=pCode, ticker=pCode, date=LastTrading, code=paste0('STKVN', pCode), 
                              sharesout=SharesOut, shareslis=SharesLis, updated=substr(as.character(Sys.time()),1,19)) 
      My.Kable.All(Final_Data)
    }
  }
  return(Final_Data)
}

#==================================================================================
FINAL_DOWNLOAD_VST_STKVN_SHARES_BY_CODE = function(pCode = 'VND')
{
  # pCode = 'YRC'
  Final_Data = data.table()
  pURL = paste0('https://finance.vietstock.vn/', toupper(pCode), '/profile.htm')
  CATln_Border(pURL)
  content    = try(rvest::read_html(pURL))
  
  if (all(class(content)!='try-error'))
  {
    tables     = content %>% html_table(fill = TRUE)
    if (length(tables)>0)
    {
      xData      = try(as.data.table(tables[[5]]))
      if (any(class(xData)=='try-error'))
      {
        xData      = try(as.data.table(tables[[2]]))
      }
      if (all(class(xData)!='try-error') & any(grepl('KL Cổ phiếu đang lưu hành',xData$X1)))
      {
        xData$field   = decodeVN(xData$X1, from='Unicode', to='Unicode', diacritics=F)
        SharesOut  = as.numeric(gsub(',','', xData[field=='KL Co phieu dang luu hanh']$X2))
        SharesLis  = as.numeric(gsub(',','', xData[field=='KL Niem yet hien tai']$X2))
        SharesFirst  = as.numeric(gsub(',','', xData[field=='KL Niem yet lan dau']$X2))
        DateFirst    = as.Date(xData[field=='Ngay giao dich dau tien']$X2,'%d/%m/%Y')
        
        Final_Data = data.table(source='VST', codesource=pCode, ticker=pCode, date=SYSDATETIME(9), code=paste0('STKVN', pCode), 
                                sharesout=SharesOut, shareslis=SharesLis, shares1st=SharesFirst, date1st=DateFirst, updated=substr(as.character(Sys.time()),1,19)) 
        My.Kable.All(Final_Data)
      }
    }
  } else {
    Final_Data = data.table(source='VST', codesource=pCode, ticker=pCode, date=SYSDATETIME(9), code=paste0('STKVN', pCode))
  }
  
  # if  (nrow(Final_Data)==)
  # {
  #   Final_Data = data.table(source='VST', codesource=pCode, ticker=pCode, date=SYSDATETIME(9), code=paste0('STKVN', pCode))
  # }
  return(Final_Data)
}

# ==================================================================================================
DOWNLOAD_YAH_STK_INSIDER = function(pCode = 'IBM') {
  # ------------------------------------------------------------------------------------------------
  # x = try(DOWNLOAD_YAH_STK_INSIDER(pCode = 'IBM'))
  pURL  = paste0('https://finance.yahoo.com/quote/', pCode, '/holders?p=', pCode)
  CATln_Border(pURL)
  content  = rvest::read_html(pURL)
  tables   = content %>% html_table(fill = TRUE)
  
  if (length(tables)>=2)
  {
    x.Insider = as.data.table(tables[[2]])
    x.Insider = CLEAN_COLNAMES(x.Insider)
    # str(x.Insider)
    x.Insider[, ':='(source='YAH', codesource=pCode, updated=Sys.Date(), dataset='INSIDER')]
    if ('shares' %in% names(x.Insider)) { x.Insider[, shares := as.numeric(gsub(',','', shares))] }
    if ('value' %in% names(x.Insider)) { x.Insider[, value := as.numeric(gsub(',','', shares))] }
    if ('datereported' %in% names(x.Insider)) { x.Insider[, date:=as.Date(datereported , '%B %d, %Y')] }
    if ('percent_out' %in% names(x.Insider)) { x.Insider[, percent_out :=as.numeric(gsub('%','', percent_out))] }
    x.Insider       = x.Insider %>% select(source, codesource, date, dataset, close=percent_out, everything())
    x.Insider1      = x.Insider 
    My.Kable.All(x.Insider)
  }
  
  if (length(tables)>=3)
  {
    x.Insider = as.data.table(tables[[3]])
    x.Insider = CLEAN_COLNAMES(x.Insider)
    # str(x.Insider)
    x.Insider[, ':='(source='YAH', codesource=pCode, updated=Sys.Date(), dataset='INSIDER')]
    if ('shares' %in% names(x.Insider)) { x.Insider[, shares := as.numeric(gsub(',','', shares))] }
    if ('value' %in% names(x.Insider)) { x.Insider[, value := as.numeric(gsub(',','', shares))] }
    if ('datereported' %in% names(x.Insider)) { x.Insider[, date:=as.Date(datereported , '%B %d, %Y')] }
    if ('percent_out' %in% names(x.Insider)) { x.Insider[, percent_out :=as.numeric(gsub('%','', percent_out))] }
    x.Insider       = x.Insider %>% select(source, codesource, date, dataset, close=percent_out, everything())
    x.Insider2      = x.Insider 
    My.Kable.All(x.Insider)
  }
  return(x.Insider)
}
# ==================================================================================================
DOWNLOAD_YAH_STK_HOLDERS = function(pCode = 'IBM') {
  # ------------------------------------------------------------------------------------------------
  # x = try(DOWNLOAD_YAH_STK_HOLDERS(pCode = 'IBM'))
  
  pURL  = paste0('https://finance.yahoo.com/quote/', pCode, '/holders?p=', pCode)
  CATln_Border(pURL)
  content  = rvest::read_html(pURL)
  tables   = content %>% html_table(fill = TRUE)
  
  if (length(tables)>=1)
  {
    x.Ownership = as.data.table(tables[[1]])
    x.Ownership[, ':='(source='YAH', codesource=pCode, updated=Sys.Date(), dataset='OWNERSHIP', dataname=X2)]
    x.Ownership[grepl('%',X1), datavalue:=as.numeric(gsub('%','', X1))]
    x.Ownership[!grepl('%',X1), datavalue:=as.numeric(gsub(',','',X1))]
    x.Ownership$X1 = NULL
    x.Ownership$X2 = NULL
    x.Ownership       = x.Ownership %>% select(source, codesource, date=updated, dataset, close=datavalue, everything())
    My.Kable.All(x.Ownership)
  }
  return(x.Ownership)
}
#==================================================================================================
DOWNLOAD_YAH_STK_SPLIT = function(pCode = 'AAPL') {
  # ------------------------------------------------------------------------------------------------
  # x = try(DOWNLOAD_YAH_STK_SPLIT(pCode = 'AAPL', NbYears=20))
  # pCode = 'X12345'
  # pCode = 'XRXR'
  # pCode = 'IBM'
  Epoch_Now     = floor(EPOCH_NOW())
  # Epoch_Start   = floor(EPOCH_NOW()-1000*NbYears*365*24*60*60)
  # pURL = 'https://query1.finance.yahoo.com/v8/finance/chart/AAPL?region=US&lang=en-US&includePrePost=false&interval=1mo&useYfid=true&range=1d&corsDomain=finance.yahoo.com&.tsrc=finance'
  pURL  = paste0('https://finance.yahoo.com/quote/', pCode, '/history?period1=', 0, '&period2=', Epoch_Now, '&interval=capitalGain%7Cdiv%7Csplit&filter=split&frequency=1d&includeAdjustedClose=true')
  # pURL  = paste0('https://finance.yahoo.com/quote/', pCode, '/history?period1=962409600&period2=1688169600&interval=capitalGain%7Cdiv%7Csplit&filter=div&frequency=1d&includeAdjustedClose=true')
  # pURL = 'https://finance.yahoo.com/quote/IBM/history?period1=962409600&period2=1688169600&interval=capitalGain%7Cdiv%7Csplit&filter=div&frequency=1d'
  CATln_Border(pURL)
  content  = rvest::read_html(pURL)
  tables   = content %>% html_table(fill = TRUE)
  rm(d.dates)
  d.dates  = try(as.data.table(tables[[1]]), silent = T)
  if (! ( is.null(nrow(d.dates)) || nrow(d.dates)==0) )
  {
    
    MyData       = d.dates[, .(Date, Close=as.character(word(Open,1,1)))]
    MyData       = CLEAN_COLNAMES(MyData)
    # str(MyData)
    MyData[, date := as.Date(date, '%B %d, %Y')][!is.na(date)]
    MyData[, ':='(source='YAH', codesource=pCode, dataset='SPLIT', updated=Sys.Date())]
    MyData       = MyData %>% select(source, codesource, dataset, date, close, everything())
    MyData       = MyData[order(date)][!is.na(date)]
    My.Kable.TB(MyData)
  } else {
    CATln('NO DATA')
    MyData = data.table() 
  }
  return(MyData)
}

#=================================================================================================
WRITE_SUMMARY_DATE = function(pFileName, ToCalculate=F, ToPrint=F)
{
  Result = as.Date('1900-01-01')
  if (ToCalculate & file.exists(pFileName))
  {
    # ToCalculate = T
    xs = WRITE_SUMMARY_FULLPATH(pFileName=pFileName, ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)
  }
  
  File_Summary = gsub('.rds', '_summary.txt', pFileName)
  if (file.exists(File_Summary))
  {
    xt = setDT(fread(File_Summary))
    Result = max(xt[!is.na(datelast)]$datelast)
  }
  if (ToPrint) { print(Result)}
  return(Result)
}

#=================================================================================================
CCPR_INTEGRATION = function(FileFolder, FileFrom, FileTo, GroupBy='codesource x date', ToSummary=T)
{
  try(RELOAD_INSREF())
  FileFrom_Full = paste0(FileFolder, FileFrom)
  if (file.exists(FileFrom_Full))
  {
    CATln(paste('Loading', FileFrom_Full))
    FileFrom_Data = try(readRDS(FileFrom_Full))
    if (all(class(FileFrom_Data)!='try-error'))
    {
      FileTo_Full   = paste0(FileFolder, FileTo)
      if (file.exists(FileTo_Full))
      {
        CATln(paste('Loading', FileTo_Full))
        FileTo_Data = try(readRDS(FileTo_Full))
      } else {
        FileTo_Data = try(data.table())
      }
      
      if (all(class(FileTo_Data)=='try-error'))
      {
        FileTo_Data = data.table()
      }
      FileAll_Data = rbind(FileFrom_Data, FileTo_Data, fill=T)
      switch(GroupBy,
             'codesource x date' = {
               FileAll_Data = unique(FileAll_Data, by=c('codesource', 'date'))
             },
             'code x date' = {
               FileAll_Data = unique(FileAll_Data, by=c('code', 'date'))
             },
             'code x date x datetime' = {
               FileAll_Data = unique(FileAll_Data, by=c('code', 'date', 'datetime'))
             }
      )
      My.Kable.TB(FileAll_Data)
      CATln(paste('Saving ', FileTo_Full))
      saveRDS(FileAll_Data, FileTo_Full)
      CATln('SAVED.')
      if (ToSummary)
      {
        try(CCPR_WRITE_SUMMARY_CURRENT(FileAll_Data, paste0(FileFolder, FileTo), ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T))
      }
    }
  } else {
    CATln_Border(paste('FILE NOT EXIST :', FileFrom_Full))
  }
}

DOWNLOAD_SSI_STKVN_INTRADAY = function()
{
  # try(DOWNLOAD_SSI_STKVN_INTRADAY()) # and REF
  UData  ='U:/EFRC/DATA/'
  try(RELOAD_INSREF())
  My_SSI = GET_SSI_DATA_TODAY(ToSave = '')
  str(My_SSI)
  My_SSI_INTRADAY = My_SSI[, .(market, code, codesource, source, date, timestamp=substr(Sys.time(),1,19),
                               ref=as.numeric(refprice), high=as.numeric(highest), low=as.numeric(lowest),
                               volume=as.numeric(matchedvolume),
                               change=as.numeric(pricechange), varpc=as.numeric(pricechangepercent))]
  My_SSI_INTRADAY = merge(My_SSI_INTRADAY, ins_ref[, .(code, name=short_name, iso2, country, continent)], all.x=T, by='code')
  My.Kable.TB(My_SSI_INTRADAY)
  saveRDS(My_SSI_INTRADAY, paste0(UData, 'download_ssi_stkvn_intraday_day.rds'))
  xs = WRITE_SUMMARY_FULLPATH(pFileName=paste0(UData, 'download_ssi_stkvn_intraday_day.rds'), ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)
  
  Data_Old = readRDS(paste0(UData, 'download_ssi_stkvn_intraday_history.rds'))
  if ('timestamp' %in% names(Data_Old) ) { Data_Old[nchar(timestamp)>19, timestamp:=substr(timestamp,1,19)] }
  if ('updated' %in% names(Data_Old) ) { Data_Old[nchar(updated)>19, updated :=substr(updated ,1,19)] }
  My.Kable.TB(Data_Old)
  
  Data_Old = unique(rbind(Data_Old, My_SSI_INTRADAY, fill=T), by=c('code', 'timestamp'))[!is.na(timestamp)]
  My.Kable.TB(Data_Old)
  
  saveRDS(Data_Old, paste0(UData, 'download_ssi_stkvn_intraday_history.rds'))
  xs = WRITE_SUMMARY_FULLPATH(pFileName=paste0(UData, 'download_ssi_stkvn_intraday_history.rds'), ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)
  
  try(RELOAD_INSREF())
  
  UData = "U:/EFRC/DATA/"
  My_SSI = GET_SSI_DATA_TODAY(ToSave = '')
  saveRDS(My_SSI, paste0(UData, 'download_ssi_stkvn_ref.rds'))
  xs = WRITE_SUMMARY_FULLPATH(pFileName=paste0(UData, 'download_ssi_stkvn_ref.rds'), ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)
  
  Data_Old = readRDS(paste0(UData, 'download_ssi_stkvn_ref_history.rds'))
  Data_Old = unique(rbind(Data_Old, My_SSI, fill=T), by=c('code', 'date'))
  Data_Old$name = NULL
  Data_Old = merge(Data_Old, ins_ref[, .(code, name=short_name)], all.x=T, by='code')
  
  My.Kable.TB(Data_Old[order(date, code)][, .(code, market, name, date)])
  saveRDS(Data_Old, paste0(UData, 'download_ssi_stkvn_ref_history.rds'))
  xs = WRITE_SUMMARY_FULLPATH(pFileName=paste0(UData, 'download_ssi_stkvn_ref_history.rds'), ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)
}#=================================================================================================

#=================================================================================================
DEMO_SSI_STKVN = function()
{
  # try(DEMO_SSI_STKVN())
  try(DOWNLOAD_SSI_STKVN_INTRADAY())
  
  UData = 'U:/EFRC/DATA/'
  WRITE_SUMMARY_DATE (pFileName = paste0(UData, 'download_ssi_stkvn_ref.rds'))
  
  try(GET_SSI_REF_TODAY(pFolder = "U:/EFRC/DATA/", ToCheckDate=T, ToCheckTime=10))
  x = try(GET_SSI_DATA_TODAY())
  
  try(GET_SSI_PRICES_LIQUIDITY_TODAY(pFolder = "U:/EFRC/DATA/", ToCheckDate=T, ToCheckTime=17, ToForce=T))
  try(GET_SSI_PRICES_LIQUIDITY_TODAY(pFolder = CCPRData, ToCheckDate=T, ToCheckTime=10))
  
  Data_New = readRDS(paste0(UData, 'download_ssi_stkvn_prices.rds'))
  Data_Hst = readRDS(paste0(UData, 'download_ssi_stkvn_history.rds'))
  
  My.Kable.TB(Data_New[order(date)])
  My.Kable.TB(Data_Hst[order(date)])
  
  Data_Hst = unique(rbind(Data_Hst, Data_New, fill=T), by=c('code', 'date'))
  Data_Hst = Data_Hst[date!='1900-01-01']
  My.Kable.TB(Data_Hst[order(date)])
  saveRDS(Data_Hst, paste0(UData, 'download_ssi_stkvn_history.rds'))
  xs = WRITE_SUMMARY_FULLPATH(paste0(UData, 'download_ssi_stkvn_history.rds'))
  
}
#=================================================================================================
DOWNLOAD_VST_STKVN_MAJOR_SHAREHOLDERS_BY_CODE = function(codes = list())
{
  x.info     = data.table()
  data.info  = list()
  # i=1
  for (i in 1: length(codes))
  {
    pCode    = codes[[i]]
    pURL       = paste0('https://finance.vietstock.vn/', pCode,'/profile.htm?languageid=2')
    # pURL       ='https://finance.vietstock.vn/DL1/profile.htm?languageid=2'  
    CATln_Border(paste(i,'/',length(codes),pURL))
    
    content    = rvest::read_html(pURL)
    tables     = content %>% html_table(fill = TRUE)
    
    if (length(tables)>=3) { 
      # data    = SELECT_TABLE_WITH_FIELD(tables=tables, pField='First Trading Date', pContent="", like=T)
      data       = as.data.table(tables[[3]])
      data       = CLEAN_COLNAMES(data)
      # data       = data[, 3:6]
      if ('shareholder' %in% colnames(data)) {
        data$date   = as.Date( data$updatedate , '%m/%d/%Y')
        data[, ':='(codesource=pCode, source='VST')]
        data = data[order(-share)]
        data.info [[i]] = data[1]
        
        My.Kable.All(data.info [[i]]) 
      } else {data.info [[i]] = data.table(shareholder = as.character (NA), codesource = pCode, source = 'VST')}
    }
  }
  x.info = rbindlist(data.info, fill = T)
  x.info = x.info[,.(shareholder, codesource, source)]
  
  return(x.info)
}


CONVERT_FILE_PRICES = function(pFrom = paste0(ODDrive, 'BeQ/PEH/WHOLESALES/DOWNLOAD_VST_WHOLESALE_PRICES.rds'), 
                               pTo   = paste0(CCPRData, "CCPR_STKVN_PRICES_FOR_SECTORS.rds"))
{
  # FileName = 
  # 
  # pTo      = 
  # RData
  # CCPRData
  
  FileData = readRDS(pFrom)
  if ('ticker' %in% names(FileData) & ! "codesource" %in% names(FileData)) { FileData[, codesource:=ticker]}
  if ('codesource' %in% names(FileData) & ! "code" %in% names(FileData)) { FileData[, code:=paste0("STKVN", codesource)]}
  FileData[, rt:=(close/reference)-1, by='codesource']
  FileData[, close_unadj:=close]
  FileData = FileData[nchar(codesource)==3]
  FileData = merge(FileData[, -c('name')], ins_ref[type=='STK'] [, .(code, name=short_name)], all.x=T, by='code')
  My.Kable(FileData[, .(codesource, name, date, close, close_unadj, reference, rt, sharesout=sharesoutstanding, shareslis=listedshares)])
  str(FileData)
  if (file.exists(pTo))
  {
    Data_Old = readRDS(pTo)
    
  } else { Data_Old = data.table()}
  FileData = FileData[, .(codesource, code, name, date, close, close_unadj, reference, rt, sharesout=sharesoutstanding, shareslis=listedshares)]
  FileData = FileData[!is.na(sharesout), capiloc:=sharesout*close_unadj]
  FileData = FileData[is.na(sharesout) & !is.na(shareslis), capiloc:=shareslis*close_unadj]
  Data_Final = unique(rbind(Data_Old, FileData, fill=T), by=c('code', 'date'), fromLast=T)
  saveRDS(FileData, pTo)
  return(Data_Final)
}

CCPR_STKVN_PE_FOR_SECTORS = function (pList=list(), pFile=paste0(CCPRData,'download_exc_stkvn_ref_history.rds'), MinNb=100)
{
  FileName = paste0(CCPRData,'CCPR_STKVN_PE_FOR_SECTORS.rds')
  if (file.exists(FileName))
  {
    data_old = readRDS(FileName)
  } else {data_old = data.table()}
  
  data_final = data.table()
  if (length(pList) > 0)
  {
    my_list = pList
  } else {
    my_stk_list = readRDS(pFile)
    my_stk_list = unique (my_stk_list, by = 'codesource')
    if (nrow(data_old)>0)
    {
      List_Update = unique(data_old[order(codesource, -date)], by='codesource')[date==SYSDATETIME(9)]
      if (nrow(List_Update)>0) {my_stk_list = my_stk_list[!codesource %in% List_Update$codesource] }
    }
    my_list     = my_stk_list$codesource
    print(length(my_list))
  }
  
  data_list = list()
  if ( length(my_list) > 0) {
    Nb_ToDo = min(MinNb, length(my_list))
    for (i in 1: Nb_ToDo)
    {
      pcode = my_list[[i]]
      datax = try(DOWNLOAD_VST_STKVN_PE_BY_CODE(pCode= pcode))
      data_list[[i]]  = data.table(codesource = pcode,pe = datax, date = SYSDATETIME(9))
    }
    my_pe = rbindlist(data_list, fill =T)
    
    My.Kable(my_pe)
    
    data_final = unique( rbind(data_old, my_pe, fill = T), by = c('codesource', 'date'))
    # CATln_Border(FileName)
    My.Kable(data_final)
    saveRDS(data_final,FileName)
    CATln_Border(paste('SAVED FILE: ',FileName)) }
  return (data_final)
}

CCPR_STKVN_OWNERSHIP_FOR_SECTORS = function (pList=list(), pFile=paste0(CCPRData,'download_exc_stkvn_ref_history.rds'), MinNb=100)
{
  FileName = paste0(CCPRData,'CCPR_STKVN_OWNERSHIP_FOR_SECTORS.rds')
  if (file.exists(FileName))
  {
    data_old = readRDS(FileName)
  } else {data_old = data.table()}
  
  data_final = data.table()
  if (length(pList) > 0)
  {
    my_list = pList
  } else {
    my_stk_list = readRDS(pFile)
    my_stk_list = unique (my_stk_list, by = 'codesource')
    if (nrow(data_old)>0)
    {
      List_Update = unique(data_old[order(codesource, -date)], by='codesource')[date==SYSDATETIME(9)]
      if (nrow(List_Update)>0) {my_stk_list = my_stk_list[!codesource %in% List_Update$codesource] }
    }
    my_list     = my_stk_list$codesource
    print(length(my_list))
  }
  
  data_list = list()
  if ( length(my_list) > 0) {
    Nb_ToDo = min(MinNb, length(my_list))
    for (i in 1: Nb_ToDo)
    {
      # i =1
      pcode = my_list[[i]]
      datax = try(DOWNLOAD_VST_STKVN_OWNERSHIP_CATEGORY_BY_CODE (pCode= pcode))
      datax = datax[[1]]
      datax[, date := SYSDATETIME(9)]
      data_list[[i]]  = setDT(datax)
    }
    my_pe = rbindlist(data_list, fill =T)
    
    My.Kable(my_pe)
    
    data_final = unique( rbind(data_old, my_pe, fill = T), by = c('codesource', 'date'))
    # CATln_Border(FileName)
    My.Kable(data_final)
    saveRDS(data_final,FileName)
    CATln_Border(paste('SAVED FILE: ',FileName)) }
  return (data_final)
}

CCPR_STKVN_PRICES_FOR_SECTORS_DAY = function (pList=list(), pFile=paste0(CCPRData,'download_exc_stkvn_ref_history.rds'), MinNb=100)
{
  FileName = paste0(CCPRData,'CCPR_STKVN_PRICES_FOR_SECTORS_DAY.rds')
  
  data = readRDS(FileName)
  if (all(class(data)!='try-error'))
  {
    data_old = data
  } else {data_old = data.table()}
  
  data_final = data.table()
  if (length(pList) > 0)
  {
    my_list = pList
  } else {
    my_stk_list = readRDS(pFile)
    my_stk_list = unique (my_stk_list, by = 'codesource')
    if (nrow(data_old)>0)
    {
      List_Update = unique(data_old[order(codesource, -date)], by='codesource')[date==SYSDATETIME(9)]
      if (nrow(List_Update)>0) {my_stk_list = my_stk_list[!codesource %in% List_Update$codesource] }
    }
    my_list     = my_stk_list$codesource
    print(length(my_list))
  }
  
  data_list = list()
  if ( length(my_list) > 0) {
    Nb_ToDo = min(MinNb, length(my_list))
    for (i in 1: Nb_ToDo)
    {
      # i =1
      pcode = my_list[[i]]
      datax = try(GET_VST_PRICES (code = pcode))
      datax$codesource = datax$ticker
      My.Kable.MaxCols(datax)
      data_list[[i]]  = setDT(datax)
    }
    my_pe = rbindlist(data_list, fill =T)
    
    My.Kable(my_pe)
    
    data_final = unique( rbind(data_old, my_pe, fill = T), by = c('codesource', 'date'))
    # CATln_Border(FileName)
    My.Kable(data_final)
    saveRDS(data_final,FileName)
    CATln_Border(paste('SAVED FILE: ',FileName)) }
  return (data_final)
}

#=================================================================================================
DOWNLOAD_VST_STKVN_PE_BY_CODE = function(pCode='VND')
{
  Result = as.numeric(NA)
  Final_Data = data.table()
  pURL   = paste0('https://finance.vietstock.vn/', pCode, '/financials.htm?tab=CSTC')
  CATln_Border(pURL)
  # FileTempHTML = paste0('temp_', gsub('-|:|[.]','', gsub(' ','_', Sys.time())),'.html')
  # CATln_Border(FileTempHTML)
  # 
  # MyExecution = gsub('<URL>', pURL, gsub('<FILE>', FileTempHTML, 'python r:/python/savehtml.py "<URL>" "r:/python/temp/<FILE>"'))
  # CATln_Border(MyExecution)
  # shell(MyExecution)
  # 
  # FilePath = paste0('r:/python/temp/', FileTempHTML)
  # CATln_Border(FilePath)
  # 
  # content    = rvest::read_html(FilePath)
  # tables     = content %>% html_table(fill = TRUE)
  
  pWeb     = try(GET_CURL_FETCH(pURL))
  if (all(class(pWeb)!='try-error'))
  {
    # writeLines(pWeb, 'c:/temp/xxx.txt')
    Result = as.numeric(str.extract(str.extract(pWeb, '<p class=p8>P/E', '<p'), '>', '<'))
    LastTrading = CCPR_LAST_TRADING_DAY_VN(max(9, hour(Sys.time())), ToPrompt = T)
    Final_Data = data.table(source='VST', codesource=pCode, ticker=pCode, code=paste0('STKVN', pCode), date=LastTrading, dataset='PE', close=Result, updated=substr(Sys.time(),1,16))
    My.Kable.All(Final_Data)
  }
  CATln(paste('P/E of', pCode, '=', Result))
  return(Final_Data)
}
# 

DOWNLOAD_STB_STKVN_MAJOR_HOLDERS_PERCENTAGE_BY_CODE = function(pCode='VIC')
{
  # x = DOWNLOAD_STB_STKVN_MAJOR_HOLDERS_PERCENTAGE_BY_CODE(pCode='VIC')
  Final_Data = data.table()
  pURL = paste0('https://www.stockbiz.vn/Stocks/', pCode, '/MajorHolders.aspx')
  CATln_Border(pURL)
  content    = try(rvest::read_html(pURL))
  if (all(class(content)!='try-error'))
  {
    tables     = content %>% html_table(fill = TRUE)
    if (length(tables)>=13)
    {
      xData      = as.data.table(tables[[13]])
      xData      = CLEAN_COLNAMES(xData)
      # My.Kable(xData)
      LastTrading = CCPR_LAST_TRADING_DAY_VN(max(9, hour(Sys.time())), ToPrompt = T)
      Final_Data = xData[, .(source='STB', codesource=pCode, ticker=pCode, code=paste0("STKVN", pCode),
                             date = LastTrading,
                             people_name=tên, position=vịtrí,
                             shares=as.numeric(gsub('[.]','',sốcổphần)),
                             percentage=as.numeric(gsub(',','.', gsub('%','',tỷlệsởhữu))),
                             lastupdate=as.Date(substr(trimws(ngàycậpnhật),1,10), '%d/%m/%Y')
      )]
      Final_Data[position=='---', position:=as.character(NA)]
      RELOAD_INSREF()
      Final_Data = merge(Final_Data[, -c('name')], ins_ref[type=='STK'][, .(code, name=short_name)], all.x=T, by='code')
      Final_Data = Final_Data %>% select(source, code, codesource, ticker, date, name, everything())
      My.Kable.TB(Final_Data)
    }
  }
  return(Final_Data)
}

#=================================================================================================
DOWNLOAD_VST_STKVN_OWNERSHIP_CATEGORY_BY_CODE = function(pCode='GAS')
{
  
  x.category = data.table()
  x.info     = data.table()
  
  # pURL       = paste0('https://finance.vietstock.vn/', pCode, '/transaction-statistics.htm')
  pURL       = paste0('https://finance.vietstock.vn/', pCode,'/ownership-structure.htm?languageid=2')
  CATln_Border(pURL)
  content    = rvest::read_html(pURL)
  tables     = content %>% html_table(fill = TRUE)
  
  if (length(tables)>=3) { 
    # x.info    = SELECT_TABLE_WITH_FIELD(tables=tables, pField='First trading date', pContent="", like=T)
    x.info       = as.data.table(tables[[2]])
    x.info       = CLEAN_COLNAMES(x.info)
    x.info       = x.info[, 1:4]
    
    colnames(x.info) = c('date', 'sharesholder', 'share', 'percent')
    
    x.info$date   = as.Date( x.info$date , '%m/%d/%Y')
    x.info[, share  := as.numeric(gsub(',','', share))]
    x.info$category =as.character(NA)
    x.info[grepl('state', sharesholder, ignore.case = T), category:='STATE']
    x.info[grepl('foreign', sharesholder, ignore.case = T), category:='FOREIGN']
    x.info[is.na(category), category:='OTHER']
    x.info[, ':='(codesource=pCode, source='VST')]
    My.Kable.All(x.info)
    LastTrading = CCPR_LAST_TRADING_DAY_VN(max(9, hour(Sys.time())), ToPrompt = T)
    x.category  = x.info[, .(source='VST', codesource=pCode, code=paste0('STKVN',pCode), date_update=date[1], date=LastTrading,
                             share=sum(share, na.rm=T), 
                             percent=min(100, sum(percent, na.rm=T)), updated=substr(Sys.time(),1,19)), by='category']
    My.Kable.All(x.category)
    
  }
  return(list(x.category, x.info))
}
#=================================================================================================
DOWNLOAD_YAH_STK_SECTOR_INDUSTRY = function(pCode='AAPL')
{
  pURL     = paste0('https://finance.yahoo.com/quote/', pCode, '/profile?p=', pCode)
  CATln_Border(pURL)
  pWeb     = try(GET_CURL_FETCH(pURL))
  if (all(class(pWeb)!='try-error'))
  {
    # writeLines(pWeb, 'c:/temp/xx.txt')
    
    xSector = str.extract(pWeb, '<span>Sector(s)</span>', "</span>")
    xSector   =  sub(paste0(".*", pRef='>'), "", xSector)
    
    xIndustry = str.extract(pWeb, '<span>Industry</span>', "</span>")
    xIndustry   =  sub(paste0(".*", pRef='>'), "", xIndustry)
    Final_Data = data.table(source='YAH', codesource=pCode, sector=xSector, industry=xIndustry, date=Sys.Date())
    My.Kable.All(Final_Data)
  }
  return(Final_Data)
}
#=================================================================================================
FILE_SUMMARY_CODESOURCE_DATE = function(FileNames = list("R:/CCPR/DATA/YAH/YYYYMM/CCPR_YAH_STKTOP1000_CLOSE_202307.rds"))
{
  FileSummarys = data.table()
  FileReports  = data.table()
  for (k in 1:length(FileNames))
  {
    # k = 1
    FileName = FileNames[[k]]
    CATln_Border(FileName)
    if (file.exists(FileName))
    {
      FileData = try(CCPR_READ_RDS_WITH_ALTERNATIVE(FileFrm = FileNames, FileAlt = paste0(ODDrive, gsub(':',"",FileNames))) [[1]])
      if (all(class(FileData)!='try-error'))
      {
        if (all(c('codesource', 'date') %in% names(FileData)))
        {
          FileSummary = FileData[, .(startdate=date[1], enddate=date[.N], records=.N ), by=c('codesource')]
          FileSummary[, file:=FileName]
          
          FileReport = FileSummary[, .(file=FileName, codes=.N, startdate=min(startdate, na.rm=T),
                                       enddate=max(enddate, na.rm=T), records=sum(records, na.rm=T))]
          
          FileSummarys = rbind(FileSummarys, FileSummary, fill=T)
          FileReports = rbind(FileReports, FileReport, fill=T)
        }
      } else {     
        CATln_Border(paste('ERROR : ', FileName))
        FileReports = rbind(FileReports, data.table(file=FileName, status='ERROR DATA'), fill=T)
      }
    } else {
      FileReports = rbind(FileReports, data.table(file=FileName, status='NOT EXIST'), fill=T)
    }
  }
  My.Kable.All(FileReports)
  return(list(FileReports, FileSummarys))
}
#======================================================================================================
# FILE_SUMMARY_CODESOURCE_DATE = function(FileNames = list("R:/CCPR/DATA/YAH/YYYYMM/CCPR_YAH_STKTOP1000_CLOSE_202307.rds"))
# {
#   FileSummarys = data.table()
#   FileReports  = data.table()
#   for (k in 1:length(FileNames))
#   {
#     # k = 1
#     FileName = FileNames[[k]]
#     CATln_Border(FileName)
#     if (file.exists(FileName))
#     {
#       FileData = try(readRDS(FileName))
#       if (all(class(FileData)!='try-error'))
#       {
#         if (all(c('codesource', 'date') %in% names(FileData)))
#         {
#           FileSummary = FileData[, .(startdate=date[1], enddate=date[.N], records=.N ), by=c('codesource')]
#           FileSummary[, file:=FileName]
#           
#           FileReport = FileSummary[, .(file=FileName, codes=.N, startdate=min(startdate, na.rm=T),
#                                        enddate=max(enddate, na.rm=T), records=sum(records, na.rm=T))]
#           
#           FileSummarys = rbind(FileSummarys, FileSummary, fill=T)
#           FileReports = rbind(FileReports, FileReport, fill=T)
#         }
#       } else {     
#         CATln_Border(paste('ERROR : ', FileName))
#         FileReports = rbind(FileReports, data.table(file=FileName, status='ERROR DATA'), fill=T)
#       }
#     } else {
#       FileReports = rbind(FileReports, data.table(file=FileName, status='NOT EXIST'), fill=T)
#     }
#   }
#   My.Kable.All(FileReports)
#   return(list(FileReports, FileSummarys))
# }

#=================================================================================================
UPDATE_UPLOAD_CCPR_MARKET_LAST = function(UData=CCPRData, UList= paste0(CCPRData, 'YAH/LIST/','LIST_YAH_MARKET_LAST.txt'), 
                                          pSource = 'YAH',  xPeriod = '5m', toReset = F)
{
  #-------------------------------------------------------------------------------------------------
  # CATln_Border('LOADING INS_REF...')
  try(RELOAD_INSREF ())
  # ins_ref = readRDS(paste0('R:/CCPR/DATA/', 'efrc_ins_ref.rds'))
  # CCPRData = UData
  # ccpi_quote_slider
  # UPDATE_UPLOAD_CCPR_MARKET_LAST(UData=CCPRData, UList= paste0(CCPRData, 'YAH/LIST/','LIST_YAH_MARKET_LAST.txt'), 
  #                                           pSource = 'YAH',  xPeriod = '5m', toReset = F)
  xList = list()
  
  Data_To_Do = read.table(UList, header = FALSE, sep = "\t")
  setnames(Data_To_Do, as.character(Data_To_Do[1, ]))
  Data_To_Do = Data_To_Do[-1, ]
  Data_To_Do = data.table(Data_To_Do)
  # Data_To_Do = Data_To_Do[slider == 0]
  
  for (i in 1:nrow(Data_To_Do))
  {
    # i = 1
    CATln('')
    x     = try(DOWNLOAD_INTRADAY_YAH(pCode = Data_To_Do[i]$yah, pDur = xPeriod, NbDays = 250))
    if (all(class(x)!='try-error')) { 
      x = MERGE_CODE_FROM_INSREF(pData = x, ins_ref = ins_ref, source = pSource) 
      x[is.na(code), ':='(code=Data_To_Do[i]$code, name=Data_To_Do[i]$name)]
      x[is.na(type), type:= Data_To_Do[i]$type]
      x[scat=='CRYPTO',type:='CRY']
      x[is.na(country), country:= Data_To_Do[i]$country]
      x[is.na(continent), continent:= Data_To_Do[i]$continent]
      x[, slider:= Data_To_Do[i]$slider ]
      My.Kable.TB(x) 
      xList[[i]] = x
    }
  }
  
  xAll = rbindlist(xList, fill=T)
  My.Kable(xAll)
  Data_Old = data.table()
  Data_Final = rbind(Data_Old, xAll, fill=T)[, -c('codesource', 'source')]
  Data_Final[, datetime:=as.character(datetime)]   
  From_Time = as.POSIXct(Data_Final$datetime,tz = "Europe/London")
  To_Time   = as.POSIXct(From_Time, tz="UTC")
  To_Time   = as.character(substr(as.POSIXct(From_Time, tz="Asia/Bangkok"),1,19)); CATln(To_Time)
  Data_Final[, datetime_vn:= substr(To_Time, 1,16)] 
  
  My.Kable(Data_Final[, .(continent, country, slider, code, name, datetime_vn, close)])
  # str(Data_Final)
  if(!toReset){
    # Data_Old = readRDS(paste0(CCPRData, 'ccpi_market_last_intraday.rds')) D:\Onedrive\R\CCPR\DATA
    Data_Old = CCPR_READ_RDS_WITH_ALTERNATIVE (FileFrm = paste0(CCPRData, 'ccpi_market_last_intraday.rds'), FileAlt=paste0(ODDrive,'R/CCPR/DATA/', 'ccpi_market_last_intraday.rds')) [[1]]
    Data_Final = unique(rbind(Data_Final, Data_Old, fill=T), by=c('code', 'datetime'))
  }
  
  My.Kable.TB(Data_Final[order(-datetime_vn, code)][, .(slider, code, name, datetime_vn, close)])
  # My.Kable(Data_Final[order(-datetime_vn, code) & (slider == 1)][, .(slider, code, name, datetime_vn, close)])
  # saveRDS(Data_Final, paste0(CCPRData, 'ccpi_market_last_intraday.rds'))
  SAVERDS_WITH_ONEDRIVE (pData=Data_Final, FileFolder=CCPRData, FileName='ccpi_market_last_intraday.rds' , SaveOneDrive=T)
  try(IFRC.UPLOAD.RDS.2023(l.host = "ccpr.vn", l.tablename='ccpi_market_last_intraday',
                           l.filepath= tolower(paste0(CCPRData, 'ccpi_market_last_intraday.rds')),
                           CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
  
  Data_Final_Day = unique(Data_Final[order(code, -datetime)][!is.na(close)], by=c('code', 'date'))[order(code, date)]
  Data_Final_Day = Data_Final_Day[, ":="(pclose=shift(close), change=close-shift(close), varpc=100*((close/shift(close))-1)), by='code'][order(code, -date)]
  
  Data_Final_Last = unique(Data_Final_Day, by='code')[, .(slider, type, code, name, date, datetime, datetime_vn, tz, tzoffset, last=close,
                                                          previous = pclose, change, varpc, timestamp=paste(datetime, tz), cur,continent,country)]
  
  Data_Qoute_Slider = Data_Final_Last[slider == 1,.(type, code, name, date, datetime, datetime_vn, tz, tzoffset ,
                                                    last, previous, change, varpc, timestamp, cur)]
  
  My.Kable.All(Data_Qoute_Slider)
  # saveRDS(Data_Qoute_Slider, paste0(CCPRData, 'ccpi_quote_slider.rds'))
  SAVERDS_WITH_ONEDRIVE (pData=Data_Qoute_Slider, FileFolder=CCPRData, FileName='ccpi_quote_slider.rds' , SaveOneDrive=T)
  
  try(IFRC.UPLOAD.RDS.2023(l.host = "ccpr.vn", l.tablename='ccpi_quote_slider',
                           l.filepath= tolower(paste0(CCPRData, 'ccpi_quote_slider.rds')),
                           CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
  
  Data_Final_Last = Data_Final_Last[order(continent, country)]           
  
  My.Kable.All(Data_Final_Last)
  saveRDS(Data_Final_Last, paste0(CCPRData, 'ccpi_market_last.rds'))
  try(IFRC.UPLOAD.RDS.2023(l.host = "ccpr.vn", l.tablename='ccpi_market_last',
                           l.filepath= tolower(paste0(CCPRData, 'ccpi_market_last.rds')),
                           CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
  
  My.Kable.All(Data_Final_Last)
}

#=================================================================================================
QUICK_INDEX_CALCULATION = function ( pSource='YAH', FileSource='list_robotics', Index_Name='ROBOTICS INDEX (USD)', 
                                     Index_Code = 'INDROBPRUSD',Frequency='MONTH', pProcess=list('DOWNLOAD', 'CALCULATION', 'REPORT'),
                                     pOption = list('DOWNLOAD_YAH_PRICES','DOWNLOAD_YAH_DIV','DOWNLOAD_YAH_BOARD','DOWNLOAD_YAH_CAPIUSD'))
{
  # pSource='YAH'; FileSource='list_robotics'; Index_Name='ROBOTICS INDEX (USD)'
  # Index_Code = 'INDROBPRUSD';Frequency='MONTH'
  # pProcess=list('DOWNLOAD', 'MERGE' ,'CALCULATION', 'REPORT')
  # pOption = list('DOWNLOAD_YAH_PRICES','DOWNLOAD_YAH_DIV','DOWNLOAD_YAH_BOARD','DOWNLOAD_YAH_CAPIUSD')
  CATln_Border('                            WELCOME, YOUR CALCULATION CONFIGURATION')
  # CATln('SERVER FOLDER,  RDATA         | R:/CCPR/DATA/YAH/YYYYMM/                      |')
  My_Report = data.table()
  for (kProcess in 1:length(pProcess))
  {
    # kProcess = 1
    xProcess = pProcess[[kProcess]]
    shell('cls')
    CATln_Border(paste('QUICK_INDEX_CALCULATION, Process = ', xProcess))
    # xProcess = 'DOWNLOAD'
    if (xProcess == 'MERGE'){
      FileData = list()
      if (length(pOption)==4)
      {
        for (i in 1:length(pOption))
        {
          xOption        = pOption[[i]]
          FileName       = paste0('CCPR_',toupper(xOption), '_', Index_Code)
          FilePath       = paste0('R:/CCPR/DATA/YAH/YYYYMM/', FileName, '.rds')
          CATln_Border(paste("SAVE TO FILEPATH : ", FilePath))
          FileData[[i]] = readRDS(FilePath)
          # colnames(FileData[[i]])  = paste0(colnames(FileData[[i]]),'.')
        }
        merge12  = merge (FileData[[1]][,.(instype,codesource,date, close,index_name, index_code, cur)], FileData[[2]][,.(codesource, date, div = close)],all.x= T, by=c('codesource', 'date'))
        # merge12  = merge (FileData[[1]][,.(instype,codesource,date, close,index_name, index_code, cur)],FileData[[4]][,.(codesource,date, capiusd)],by = c('codesource'))
        merge124 = merge (merge12,unique(FileData[[4]][,.(codesource,date,capiloc, capiusd)], by = 'codesource'), by = c('codesource'))
        merge124 = unique(merge124, by = c('codesource', 'date.x','index_code'))
        colnames(merge124) = c ("codesource" ,"date" ,    "instype"  ,  "close" ,     "index_name" ,"index_code", "cur"  ,      "div" ,      
                                "updated" ,  'capiloc',  "capiusd" )
        merge124[, updated:= Sys.Date()]
        # unique(FileData[[4]]$codesource)
        saveRDS(merge124,paste0(CCPRData,"DATA/YAH/YYYYMM/CCPR_",Index_Code,".rds" ))
      }
    }
    
    if (xProcess == 'DOWNLOAD') {
      for (xOption in pOption)
      {
        File_List      = readRDS(paste0('R:/DATA/',FileSource,'.rds'))
        CATln_Border(File_List)
        List_Codes     = unique(File_List, by = 'codesource')
        My.Kable(List_Codes)
        # Index_Code   = 'STKROBUSD'
        # xOption        = 'DOWNLOAD_YAH_PRICES'
        FileName       = paste0('CCPR_',Index_Code)
        FilePath       = paste0(CCPRData,'DATA/YAH/YYYYMM/', FileName, '.rds')
        CATln_Border(paste("SAVE TO FILEPATH : ", FilePath))
        # ----------------------------------------------------------------------
        CATln_Border(FilePath)
        if (nrow(List_Codes)>0)
        {
          if (file.exists(FilePath))
          {
            Data_old = readRDS(FilePath)
            if (nrow(Data_old)>0) {
              Data_old = Data_old[!is.na(codesource)]
              
              List_Codes_ToDo = List_Codes
              My.Kable.MaxCols(Data_old)
            } else {
              List_Codes_ToDo = List_Codes
              
            }
          } else { 
            CATln(paste('FILE NO EXIST :', FilePath))
            Data_old = data.table() 
            List_Codes_ToDo = List_Codes
          }
          List_Codes_ToDo = List_Codes_ToDo[order(-codesource)]
          My.Kable(List_Codes_ToDo)
          
          if (nrow(List_Codes_ToDo)>0)
          {
            # x = GET_VST_BOARD_MEMBER_BY_CODE(pCode = 'YEG')
            Data_All = data.table()
            
            # FileName = paste0("LIST_YAH_", pISO3, '.rds') 
            # CATln_Border(FileName)
            Data_List = list()
            
            FileData = List_Codes_ToDo
            FileRow  = nrow(List_Codes_ToDo)
            Nb_Min   = 5000
            Nb_Group = 50
            Nb_TODO  = min(Nb_Min, FileRow)
            Nb_Total = FileRow
            
            FileToSave = FilePath
            Total_Time  = 0
            for (i in 1:Nb_TODO)
            {
              # i =1
              # i =5
              # setdiff(List_Codes$codesource, unique(merge124$codesource))
              Start.Time = Sys.time()
              pCode    = gsub(' ','',FileData[i]$codesource)
              CATln("")
              if (file.exists(FilePath)){
                k = as.numeric(Sys.Date() - max(Data_old[codesource == pCode] $updated))
                # k = 3
                k = replace(k, is.infinite(k),0)} else {k = 250000}
              # CATln_Border(paste(i, '/', Nb_TODO, ' = ', pCode))
              if (k != 0)
              {
                CATln_Border(paste('RUN_LOOP_BY_BROUP_AND_SAVE : ', pProcess,xOption , '=', i, paste0('(', Nb_Group,')'), '/', Nb_TODO, '/', Nb_Total, ' = ', pCode))
                
                switch(xOption,
                       'DOWNLOAD_YAH_PRICES'      = {
                         My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=k))
                         # My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=0))
                       },
                       'DOWNLOAD_YAH_DIV'         = {
                         My_Data = try(DOWNLOAD_YAH_STK_DIVIDEND(pCode=pCode))
                       },
                       'DOWNLOAD_YAH_CAPIUSD'         = {
                         My_Data = try(DOWNLOAD_YAH_STK_CAPIUSD_BY_CODE(pCode=pCode, EchoOn=T, ToKable=T))
                       }
                       
                )
                
                if (all(class(My_Data)!='try-error'))
                {
                  Data_List[[i]] = My_Data
                } 
                
                Sys.sleep(2)
                
                if (i %% Nb_Group ==0 | i == Nb_TODO)
                {
                  Data_All = rbindlist(Data_List, fill=T)
                  # Data_All = rbindlist(Data_List)
                  Data_All[, index_code:= Index_Code]
                  Data_All[, index_name:= Index_Name]
                  Data_final = unique(rbind(Data_old, Data_All, fill=T), by = c('codesource','date'))
                  saveRDS(Data_final, FileToSave)
                  CATln(paste('SAVED : ', FileToSave, '-', Format.Number(nrow(Data_final),0),'records'))
                  Sys.sleep(2)
                }
              } else {
                Data_final = Data_old
                
              }
              
              End.Time = Sys.time()
              Total_Time = as.numeric(difftime(End.Time, Start.Time, units = 'secs')) + Total_Time
              
              Avg_Time = Total_Time / i
              End_Forecast = Avg_Time * (Nb_TODO - i)
              Time_To_End = as.difftime(End_Forecast, units = "secs") + Sys.time()
              CATln_Border(paste('TIME TO END :', Time_To_End, '( after',format(End_Forecast, nsmall = 0),'secs )'))
              
              
            }
            My.Kable.TB(Data_final)
            
          } else {
            CATln_Border('DATA FULLY UPDATED')
            IFRC_SLEEP(2)
          }
        }
      }     
    }
    
    if (xProcess == 'CALCULATION') {
      FileName = paste0("R:/CCPR/DATA/YAH/YYYYMM/CCPR_",Index_Code,".rds" )
      CATln_Border(FileName)
      My_Data  = readRDS(FileName)
      
      # My_Board = readRDS(paste0('R:/CCPR/DATA/YAH/YYYYMM/', paste0('CCPR_','DOWNLOAD_YAH_BOARD', '.rds')))
      # My_WCEO  = My_Board [(ceo == 1) & (gender == 'F')]
      My_Data = My_Data [order(date)]
      My_Data = My_Data [!is.na(close)]
      My_Data[, rt:=(close/shift(close))-1, by='codesource']
      My_Data[, rt1 := rt +1]        
      My_Data = My_Data[!is.na(rt1)]
      My_Data$rt_1 = as.numeric (NA)
      my_code  = unique(My_Data, by = 'codesource')
      for (k in 1:nrow(my_code))
      {
        # k=1
        pCode = my_code[k]$codesource
        CATln_Border(paste0('CALCULATE RT1: ',pCode))
        My_Data[codesource == pCode]$rt_1 = cumprod(My_Data[codesource == pCode]$rt1)
      }
      
      My_Data[, adj_capi := rt_1*capiusd]
      sumcapi = My_Data[, .(sumcapi = sum(adj_capi, na.rm = T)), by = 'date']
      My_Data = merge(My_Data, sumcapi, all.x = T, by = 'date')
      My_Data[, ratio:= adj_capi/sumcapi]
      My_Data[, cw_rt := rt*ratio]
      My_Data[, year := year(date)]
      
      #CALCULATE CW TOTAL
      # YYYY = 2009
      All_Year = data.table()
      for (YYYY in 2009:2023)
      {
        Select_Data_Prices = My_Data[year == YYYY]
        Select_Data_Prices = Select_Data_Prices[, .(codesource, date, close,cw_rt)]
        My.Kable(Select_Data_Prices)
        Select_Data_Prices_CW = Select_Data_Prices[, .(cwmt=sum(cw_rt, na.rm=T)), by='date']
        Select_Data_Prices_CW = rbind(data.table(cwmt=0, date=as.Date(paste0(YYYY-1, '-12-31'))), Select_Data_Prices_CW, fill=T)
        Select_Data_Prices_CW[order(date)]
        
        All_Year = rbind(All_Year, Select_Data_Prices_CW, fill=T)
      }
      All_Year = unique(All_Year[order(date)], by='date')
      My.Kable(All_Year)
      
      All_Year[, index:=1000.00]
      Last_Index = 1000.00
      for (i in 1:nrow(All_Year))
      {
        xRMT = All_Year[i]$cwmt
        Last_Index = Last_Index*(1+xRMT)
        All_Year[i]$index = Last_Index
      }
      All_Year = All_Year[, .(date, rt = cwmt, index, name = paste0(str_sub(Index_Code,4,6), 'CW',substring(Index_Code,7)))]
      if (file.exists(paste0("R:/CCPR/DATA/YAH/YYYYMM/CCPR_",Index_Code,"_INDEXES.rds" )))
      {
        data_old = readRDS(paste0("R:/CCPR/DATA/YAH/YYYYMM/CCPR_",Index_Code,"_INDEXES.rds" ))
      } else { data_old = data.table()}
      data_final = rbind (All_Year, data_old)
      saveRDS(data_final,paste0(CCPRDrive,"CCPR/DATA/YAH/YYYYMM/CCPR_",Index_Code,"_INDEXES.rds" ))
      
      #CALCULATE EW TOTAL
      All_Year = data.table()
      for (YYYY in 2009:2023)
      {
        Select_Data_Prices = My_Data[year(date)==YYYY]
        Select_Data_Prices = Select_Data_Prices[, .(codesource, date, close,rt)]
        My.Kable(Select_Data_Prices)
        Select_Data_Prices_Avg = Select_Data_Prices[, .(rmt=mean(rt, na.rm=T)), by='date']
        Select_Data_Prices_Avg = rbind(data.table(rmt=0, date=as.Date(paste0(YYYY-1, '-12-31'))), Select_Data_Prices_Avg, fill=T)
        Select_Data_Prices_Avg[order(date)]
        
        All_Year = rbind(All_Year, Select_Data_Prices_Avg, fill=T)
      }
      All_Year = unique(All_Year[order(date)], by='date')
      My.Kable(All_Year)
      
      All_Year[, index:=1000.00]
      Last_Index = 1000.00
      for (i in 1:nrow(All_Year))
      {
        xRMT = All_Year[i]$rmt
        Last_Index = Last_Index*(1+xRMT)
        All_Year[i]$index = Last_Index
      }
      All_Year = All_Year[, .(date, rt = rmt, index, name = paste0(str_sub(Index_Code,4,6), 'EW',substring(Index_Code,7)))]
      data_old = readRDS(paste0(CCPRDrive,"CCPR/DATA/YAH/YYYYMM/CCPR_",Index_Code,"_INDEXES.rds" ))
      data_final = unique(rbind(data_old, All_Year), by = c('date','name'))
      saveRDS(data_final,paste0(CCPRDrive,"CCPR/DATA/YAH/YYYYMM/CCPR_",Index_Code,"_INDEXES.rds" ))
      # data_final = readRDS(paste0("R:/CCPR/DATA/YAH/YYYYMM/CCPR_",Index_Code,"_INDEXES.rds" ))
      # ggplot(data_final[,.(date,index,name)], aes(date, index) ) + geom_line(aes(colour=name))
    }
    
    if (xProcess == 'REPORT') {
      Data_Report = readRDS(paste0(CCPRDrive,"CCPR/DATA/YAH/YYYYMM/CCPR_",Index_Code,"_INDEXES.rds" ))
      Data_Report = Data_Report[,.(date,index, name)]
      My_Report  = spread(Data_Report, key = 'name', value ='index', fill = F)
      
      saveRDS(My_Report,paste0(CCPRDrive,"CCPR/DATA/YAH/YYYYMM/CCPR_",Index_Code,"_REPORT.rds" ) )
      My.Kable(My_Report)
    }
  }  
  
  return(My_Report)
}


#=================================================================================================
GET_SSI_REF_TODAY = function(pFolder = CCPRData, ToCheckDate=T, ToCheckTime=9, ToForce=F)
{
  CATln_Border(paste('GET_SSI_REF_TODAY', pFolder, ToCheckDate, ToCheckTime, '-', Sys.time()))
  # pFolder = 'U:/EFRC/DATA/'
  RELOAD_INSREF (pFolder = pFolder)
  pFileName = paste0(pFolder, 'download_ssi_stkvn_ref.rds')
  if (ToForce || (WRITE_SUMMARY_DATE(pFileName)<SYSDATETIME(ToCheckTime)))
  {
    MyData = list()
    MyData[[1]] = DOWNLOAD_SSI_EXCHANGE(pID="SSI_HOSE", ToCheckTime=ToCheckTime)
    MyData[[2]] = DOWNLOAD_SSI_EXCHANGE(pID="SSI_HNX", ToCheckTime=ToCheckTime)
    MyData[[3]] = DOWNLOAD_SSI_EXCHANGE(pID="SSI_UPCOM", ToCheckTime=ToCheckTime)
    All_Data = rbindlist(MyData, fill=T)
    My.Kable.MaxCols(All_Data)
    
    # UData = 'U:/EFRC/DATA/'
    
    # ins_ref = readRDS(paste0(UData, 'efrc_ins_ref.rds'))
    # assign("ins_ref", ins_ref, envirpFolder = .GlobalEnv)
    # My_SSI = GET_SSI_DATA_TODAY(ToSave = '')
    My_SSI = All_Data
    
    saveRDS(My_SSI, paste0(pFolder, 'download_ssi_stkvn_ref.rds'))
    xs = WRITE_SUMMARY_FULLPATH(pFileName=paste0(pFolder, 'download_ssi_stkvn_ref.rds'), ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)
    
    Data_Old = readRDS(paste0(pFolder, 'download_ssi_stkvn_ref_history.rds'))
    Data_Old = unique(rbind(Data_Old, My_SSI, fill=T), by=c('code', 'date'))
    My.Kable.TB(Data_Old[order(date, code)][, .(code, market, date)])
    saveRDS(Data_Old, paste0(pFolder, 'download_ssi_stkvn_ref_history.rds'))
    xs = WRITE_SUMMARY_FULLPATH(pFileName=paste0(pFolder, 'download_ssi_stkvn_ref_history.rds'), ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)
  }
}

GET_SSI_PRICES_LIQUIDITY_TODAY = function(pFolder = CCPRData, ToCheckDate=T, ToCheckTime=10, ToForce=F)
{
  CATln_Border(paste('GET_SSI_REF_TODAY', pFolder, ToCheckDate, ToCheckTime, '-', Sys.time()))
  # pFolder = 'U:/EFRC/DATA/'
  RELOAD_INSREF (pFolder = pFolder)
  All_Data = data.table()
  pFileName = paste0(pFolder, 'download_ssi_stkvn_ref.rds')
  if (WRITE_SUMMARY_DATE(pFileName)<SYSDATETIME(ToCheckTime) | ToForce)
  {
    MyData = list()
    MyData[[1]] = DOWNLOAD_SSI_EXCHANGE(pID="SSI_HOSE")
    MyData[[2]] = DOWNLOAD_SSI_EXCHANGE(pID="SSI_HNX")
    MyData[[3]] = DOWNLOAD_SSI_EXCHANGE(pID="SSI_UPCOM")
    All_Data = rbindlist(MyData, fill=T)
    My.Kable.MaxCols(All_Data)
  }
  
  # pFolder = 'U:/EFRC/DATA/'
  
  # ins_ref = readRDS(paste0(UData, 'efrc_ins_ref.rds'))
  # assign("ins_ref", ins_ref, envirpFolder = .GlobalEnv)
  # My_SSI = GET_SSI_DATA_TODAY(ToSave = '')
  #   My_SSI = All_Data
  #   
  #   
  # MyData = list()
  # MyData[[1]] = DOWNLOAD_SSI_EXCHANGE(pID="SSI_HOSE")
  # MyData[[2]] = DOWNLOAD_SSI_EXCHANGE(pID="SSI_HNX")
  # MyData[[3]] = DOWNLOAD_SSI_EXCHANGE(pID="SSI_UPCOM")
  # All_Data = rbindlist(MyData, fill=T)
  # My.Kable.MaxCols(All_Data)
  # 
  # UData = 'U:/EFRC/DATA/'
  
  # ins_ref = readRDS(paste0(UData, 'efrc_ins_ref.rds'))
  # assign("ins_ref", ins_ref, envir = .GlobalEnv)
  # # My_SSI = GET_SSI_DATA_TODAY(ToSave = '')
  if (nrow(All_Data)>0)
  {
    My_SSI = All_Data
    str(My_SSI)
    My_Prices = My_SSI[, .(date, code, market, codesource, high=highest, low=lowest, last=matchedprice, volume=nmtotaltradedqty , reference=refprice, average=avgprice, 
                           change=as.numeric(pricechange), varpc=as.numeric(pricechangepercent))]
    Sys.sleep(2)
    My_Prices[is.na(close), close:=reference]
    My_Prices[is.na(close), close:=reference]
    
    My.Kable(My_Prices)
    
    saveRDS(My_Prices, paste0(pFolder, 'download_ssi_stkvn_prices.rds'))
    xs = WRITE_SUMMARY_FULLPATH(pFileName=paste0(pFolder, 'download_ssi_stkvn_prices.rds'), ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)
    
    Data_Old = try(readRDS(paste0(pFolder, 'download_ssi_stkvn_history.rds')))
    Data_Old = unique(rbind(Data_Old, My_Prices, fill=T), by=c('code', 'date'))[!is.na(date)]
    My.Kable.TB(Data_Old[order(date, code)][, .(code, market, date, close)])
    saveRDS(Data_Old, paste0(pFolder, 'download_ssi_stkvn_history.rds'))
    xs = WRITE_SUMMARY_FULLPATH(pFileName=paste0(pFolder, 'download_ssi_stkvn_history.rds'), ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)
    
    saveRDS(My_Prices, paste0(pFolder, 'download_ssi_stkvn_liquidity.rds'))
    xs = WRITE_SUMMARY_FULLPATH(pFileName=paste0(pFolder, 'download_ssi_stkvn_liquidity.rds'), ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)
    
    if (file.exists(paste0(pFolder, 'download_ssi_stkvn_liquidity_history.rds')))
    {
      Data_Old = readRDS(paste0(pFolder, 'download_ssi_stkvn_liquidity_history.rds'))
    } else { Data_Old = data.table()}
    Data_Old = unique(rbind(Data_Old, My_Prices, fill=T), by=c('code', 'date'))
    My.Kable.TB(Data_Old[order(date, code)][, .(code, market, date, close)])
    saveRDS(Data_Old, paste0(pFolder, 'download_ssi_stkvn_liquidity_history.rds'))
    xs = WRITE_SUMMARY_FULLPATH(pFileName=paste0(pFolder, 'download_ssi_stkvn_liquidity_history.rds'), ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)
  }
}

# ==================================================================================================
DOWNLOAD_SSI_EXCHANGE = function(pID="SSI_HOSE", ToCheckTime) {
  # ------------------------------------------------------------------------------------------------
  LastTrading = CCPR_LAST_TRADING_DAY_VN(ToCheckTime, ToPrompt = T)
  FileName   = paste0('R:/PYTHON/GET_DATA/response/', pID, '.json')
  file.remove(FileName)
  # ...
  MyExcution = paste('python R:/PYTHON/GET_DATA/post_json_id.py iboard.xlsx', pID)
  shell(MyExcution)
  if (file.exists(FileName)) {
    x     = jsonlite::fromJSON(FileName)
    xData = as.data.table(x$data$stockRealtimes)
    xData = CLEAN_COLNAMES(xData)
    xData = xData %>% mutate(across(.cols=where(is.integer), .fns=as.numeric))
    # Xdata[, ':='(codesource=stocksymbol, code=paste0('STKVN', stocksymbol), close)]
    xData[exchange=='hose', market:='HSX']
    xData[exchange=='hnx', market:='HNX']
    xData[exchange=='upcom', market:='UPC']
    # str(Xdata)
    xData[, ':='(codesource=stocksymbol, code=paste0('STKVN',stocksymbol), source='SSI', close=lastmatchedprice)]
    xData[, date:=LastTrading]
    xData = xData %>% select(code, codesource, market, date, close, everything())
    
    My.Kable.MaxCols(xData)
  } else { xData = data.table()}
  # str(Xdata)
  return(xData)
}

#=================================================================================================
RELOAD_INSREF = function (pFolder = CCPRData, silent=T)
{
  if (exists("ins_ref")) { if (!silent) { CATln("INS_REF ALREADY LOADED") } } else { 
    # UData = CCPRData
    ins_ref = readRDS(paste0(pFolder, 'efrc_ins_ref.rds'))
    assign("ins_ref", ins_ref, envir = .GlobalEnv)
  }
}

#==================================================================================================
UPDATE_UPLOAD_CCPR_QUOTE_SLIDER = function(UData="R:/CCPR/DATA/"){
  #-------------------------------------------------------------------------------------------------
  CATln_Border('LOADING INS_REF...')
  ins_ref = readRDS(paste0(UData, 'efrc_ins_ref.rds'))
  # ins_ref = readRDS(paste0('R:/CCPR/DATA/', 'efrc_ins_ref.rds'))
  # CCPRData = UData
  
  # ccpi_quote_slider
  xPeriod = '5m'
  xList = list()
  
  x     = try(DOWNLOAD_INTRADAY_YAH(pCode='^GSPC', pDur=xPeriod, NbDays=250))
  if (all(class(x)!='try-error')) { 
    x = MERGE_CODE_FROM_INSREF(pData=x, ins_ref=ins_ref, source='YAH') 
    x[, type:='IND']
    My.Kable.TB(x) 
    xList[[1]] = x
  }
  
  x     = try(DOWNLOAD_INTRADAY_YAH(pCode='^NDX', pDur=xPeriod, NbDays=250))
  if (all(class(x)!='try-error')) { 
    x = MERGE_CODE_FROM_INSREF(pData=x, ins_ref=ins_ref, source='YAH') 
    x[, type:='IND']
    My.Kable.TB(x) 
    xList[[2]] = x
  }
  
  x     = try(DOWNLOAD_INTRADAY_YAH(pCode='EUR=X', pDur=xPeriod, NbDays=250))
  if (all(class(x)!='try-error')) { 
    x = MERGE_CODE_FROM_INSREF(pData=x, ins_ref=ins_ref, source='YAH') 
    x[, ':='(code='CURUSDEUR', name='EXCHANGE RATE USD/EUR')]
    x[, type:='CUR']
    My.Kable.TB(x) 
    xList[[3]] = x
  }
  
  x     = try(DOWNLOAD_INTRADAY_YAH(pCode='BTC-USD', pDur=xPeriod, NbDays=250))
  if (all(class(x)!='try-error')) { 
    x = MERGE_CODE_FROM_INSREF(pData=x, ins_ref=ins_ref, source='YAH') 
    x[, type:='CRY']
    My.Kable.TB(x) 
    xList[[4]] = x
  }
  
  x     = try(DOWNLOAD_INTRADAY_YAH(pCode='ETH-USD', pDur=xPeriod, NbDays=250))
  # xd    = try(DOWNLOAD_INTRADAY_YAH(pCode='ETH-USD', pDur='1d', NbDays=250))
  
  if (all(class(x)!='try-error')) { 
    x = MERGE_CODE_FROM_INSREF(pData=x, ins_ref=ins_ref, source='YAH') 
    x[, type:='CRY']
    My.Kable.TB(x) 
    xList[[5]] = x
  }
  
  x     = try(DOWNLOAD_INTRADAY_YAH(pCode='GC=F', pDur=xPeriod, NbDays=250))
  if (all(class(x)!='try-error')) { 
    x = MERGE_CODE_FROM_INSREF(pData=x, ins_ref=ins_ref, source='YAH') 
    x[, ':='(code='CMDGOLD', name='GOLD')]
    x[, type:='CMD']
    My.Kable.TB(x) 
    xList[[6]] = x
  }
  
  x     = try(DOWNLOAD_INTRADAY_YAH(pCode='AAPL', pDur=xPeriod, NbDays=250))
  if (all(class(x)!='try-error')) { 
    x = MERGE_CODE_FROM_INSREF(pData=x, ins_ref=ins_ref, source='YAH') 
    x[, ':='(code='STKUSAAPL', name='APPLE')]
    x[, type:='STK']
    My.Kable.TB(x) 
    xList[[7]] = x
  }
  x     = try(DOWNLOAD_INTRADAY_YAH(pCode='^HSI', pDur=xPeriod, NbDays=250))
  if (all(class(x)!='try-error')) { 
    x = MERGE_CODE_FROM_INSREF(pData=x, ins_ref=ins_ref, source='YAH') 
    x[, ':='(code='INDHSI', name='HSI')]
    x[, type:='IND']
    My.Kable.TB(x) 
    xList[[7]] = x
  }
  
  xAll = rbindlist(xList, fill=T)
  My.Kable(xAll)
  Data_Old = data.table()
  Data_Final = rbind(Data_Old, xAll, fill=T)[, -c('codesource', 'source')]
  Data_Final[, datetime:=as.character(datetime)]   
  From_Time = as.POSIXct(Data_Final$datetime,tz = "Europe/London")
  To_Time   = as.POSIXct(From_Time, tz="UTC")
  To_Time   = as.character(substr(as.POSIXct(From_Time, tz="Asia/Bangkok"),1,19)); CATln(To_Time)
  Data_Final[, datetime_vn:= To_Time] 
  My.Kable(Data_Final)
  saveRDS(Data_Final, paste0(CCPRData, 'ccpi_quote_slider_intraday.rds'))
  
  try(IFRC.UPLOAD.RDS.2023(l.host = "ccpr.vn", l.tablename='ccpi_quote_slider_intraday',
                           l.filepath= tolower(paste0(CCPRData, 'ccpi_quote_slider_intraday.rds')),
                           CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=T))
  
  Data_Final_Day = unique(Data_Final[order(code, -datetime)][!is.na(close)], by=c('code', 'date'))[order(code, date)]
  Data_Final_Day = Data_Final_Day[, ":="(pclose=shift(close), change=close-shift(close), varpc=100*((close/shift(close))-1)), by='code'][order(code, -date)]
  
  Data_Final_Last = unique(Data_Final_Day, by='code')[, .(type, code, name, date, datetime,datetime_vn, tz, tzoffset, last=close, 
                                                          previous=pclose, change, varpc, timestamp=paste(datetime, tz), cur)]
  My.Kable.All(Data_Final_Last)
  saveRDS(Data_Final_Last, paste0(CCPRData, 'ccpi_quote_slider.rds'))
  try(IFRC.UPLOAD.RDS.2023(l.host = "ccpr.vn", l.tablename='ccpi_quote_slider',
                           l.filepath= tolower(paste0(CCPRData, 'ccpi_quote_slider.rds')),
                           CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=T))
  My.Kable.All(Data_Final_Last)
}

#===============================================================================
COUNT_OUR_DATABASE = function(MyREF, Label, Group){
  #-------------------------------------------------------------------------------------------------
  MyStats = data.table(
    group     = Group, label = Label,
    datasets  = Format.Number(length(unique(MyREF[!is.na(code)]$code)),0), 
    records   = Format.Number(sum(MyREF[!is.na(records)]$records),0), 
    startdate = as.character(min(MyREF[!is.na(startdate)]$startdate)),
    enddate   = as.character(max(MyREF[!is.na(enddate)]$enddate)),
    countries = Format.Number(length(unique(MyREF[!is.na(country)]$country)),0)
  )
  My.Kable.All(MyStats)
  return(MyStats)
}

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# MAIN START
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

RUN_LOOP_BY_BROUP_AND_SAVE = function(pOption='VST_BOARD_MEMBER', Nb_Group = 50, Nb_Min = 50000, Frequency='MONTH', AddParam='')
{
  RData = 'R:/DATA/'
  # source(paste0('R:/R/', 'LIBRARIES/WCEO_LIBRARY.r'))
  Data_final = data.table()
  # pOption='VST_BOARD_MEMBER'
  # pOption='YAH_STK_SECTOR_WEBSITE'
  # Frequency='DAY'
  
  # Nb_Group = 5; Nb_Min = 10; Frequency='MONTH'
  switch(Frequency,
         'MONTH' = {CurrentFileDate = substr(gsub('-','', as.character(Sys.Date())),1,6)},
         'DAY'   = {CurrentFileDate = substr(gsub('-','', as.character(Sys.Date())),1,8)}
  )
  # CurrentYYYYMM = substr(gsub('-','', as.character(Sys.Date())),1,6)
  
  switch(pOption,
         # 'YAH_MUTUAL_FUND'      = {
         #   List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_MUTUAL_FUND.rds"))
         #   List_Codes     = unique(List_Companies[,.(codesource, name, code=paste0("MUTUAL_FUND",codesource ))], by ='codesource')
         #   My.Kable(List_Codes)
         #   FileName       = paste0('CCPR_YAH_MUTUAL_FUND')
         #   FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         # },
         'DOWNLOAD_CAF_STKVN_DAY'      = {
           # pOption='CLEANED_LIST_YAH_PARAM'
           File_List      = paste0('R:/CCPR/DATA/YAH/LIST/', "CLEAN_LIST_YAH_", AddParam, ".rds")
           CATln_Border(File_List)
           List_Companies = readRDS(File_List)
           List_Codes     = unique(List_Companies[,.(codesource, name, code)], by ='codesource')
           My.Kable(List_Codes)
           
           FileName       = paste0('CCPR_YAH_', AddParam)
           FilePath       = paste0('R:/CCPR/DATA/YAH/YYYYMM/', FileName, '_', CurrentFileDate, '.rds')
           CATln_Border(paste("SAVE TO FILEPATH : ", FilePath))
         },
         'CLEANED_LIST_YAH_PARAM'      = {
           # pOption='CLEANED_LIST_YAH_PARAM'
           File_List      = paste0('R:/CCPR/DATA/YAH/LIST/', "CLEAN_LIST_YAH_", AddParam, ".rds")
           CATln_Border(File_List)
           List_Companies = readRDS(File_List)
           List_Codes     = unique(List_Companies[,.(codesource, name, code)], by ='codesource')
           My.Kable(List_Codes)
           
           FileName       = paste0('CCPR_YAH_', AddParam)
           FilePath       = paste0('R:/CCPR/DATA/YAH/YYYYMM/', FileName, '_', CurrentFileDate, '.rds')
           CATln_Border(paste("SAVE TO FILEPATH : ", FilePath))
         },
         
         'YAH_FUTURES'      = {
           List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_FUTURES.rds"))
           List_Codes     = unique(List_Companies[,.(codesource, name, code=paste0("ETF",codesource ))], by ='codesource')
           My.Kable(List_Codes)
           FileName       = paste0('CCPR_YAH_FUTURES')
           FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         },
         # 'YAH_ETF'      = {
         #   List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_ETF.rds"))
         #   List_Codes     = unique(List_Companies[,.(codesource, name, code=paste0("ETF", gsub('-USD','',codesource )))], by ='codesource')
         #   My.Kable(List_Codes)
         #   FileName       = paste0('CCPR_YAH_ETF')
         #   FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         # },
         'YAH_CURCRY'      = {
           List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_CRYPTO.rds"))
           List_Codes     = unique(List_Companies[,.(codesource, name, code=paste0("CURCRY", gsub('-USD','',codesource )))], by ='codesource')
           My.Kable(List_Codes)
           FileName       = paste0('CCPR_YAH_CURCRY_PRICES')
           FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         },
         'YAH_STK_BY_SECTOR'      = {
           # AddParam = 'BSM'
           List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_STK_BY_SECTOR_ClEANED.rds"))
           List_Codes     = unique(List_Companies[,.(codesource, name, sector_code)][sector_code==AddParam], by ='codesource')
           My.Kable(List_Codes)
           FileName       = paste0('CCPR_YAH_STK_', AddParam, '_PRICES')
           FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         },
         'YAH_STK_DIV_BY_SECTOR'      = {
           # AddParam = 'BSM'
           List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_STK_BY_SECTOR_ClEANED.rds"))
           List_Codes     = unique(List_Companies[,.(codesource, name, sector_code)][sector_code==AddParam], by ='codesource')
           My.Kable(List_Codes)
           FileName       = paste0('CCPR_YAH_STK_', AddParam, '_DIV')
           FilePath       = paste0('R:/CCPR/DATA/YAH/YYYYMM/', FileName, '_', CurrentFileDate, '.rds')
         },
         
         'YAH_STK_BY_SECTOR_UNADJ'      = {
           # AddParam = 'BSM'
           List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_STK_BY_SECTOR_ClEANED.rds"))
           List_Codes     = unique(List_Companies[,.(codesource, name, sector_code)][sector_code==AddParam], by ='codesource')
           My.Kable(List_Codes)
           FileName       = paste0('CCPR_YAH_STK_', AddParam, '_PRICES_UNADDJ')
           FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         },
         
         'VST_BOARD_MEMBER'       = {
           List_Companies =  readRDS(paste0(RData, "SSI/download_ssi_stkvn_exc_ref.rds"))
           List_Codes  = unique(List_Companies[,.(code,source,codesource),by ='codesource'])
           My.Kable(List_Codes)
           FileName    = 'DOWNLOAD_VST_STKVN_BOARD_MEMBERS'
           FilePath    = paste0('R:/WOMEN_CEO/DATA/VST/', FileName, '_', CurrentFileDate, '.rds')
         },
         'CAF_STKVN_INTRADAY'       = {
           # pOption     = 'CAF_STKVN_INTRADAY'
           List_Companies =  readRDS(paste0(RData, "SSI/download_ssi_stkvn_exc_ref.rds"))
           List_Codes  = unique(List_Companies[,.(code,source,codesource),by ='codesource'])[order(codesource)]
           My.Kable(List_Codes)
           FileName    = 'DOWNLOAD_CAF_STKVN_INTRADAY_DAY'
           FilePath    = paste0(RData, 'CAF/DAY/', FileName, '_', CurrentFileDate, '.rds')
         },
         # 'YAH_STK_SECTOR_WEBSITE' = {
         #   CATln('Loading INS_REF ...')
         #   ins_ref     = setDT(readRDS('R:/data/efrc_ins_ref.rds'))
         #   List_Codes  = ins_ref[type=='STK' & !is.na(yah), .(type, iso2, iso3, continent, country, yah, code, name, startdate, enddate, records)]
         #   List_Codes[, codesource:=yah]
         #   My.Kable(List_Codes)
         #   FileName    = 'DOWNLOAD_YAH_STK_SECTOR_WEBSITE'
         #   FilePath    = paste0('R:/WOMEN_CEO/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         # } ,
         'YAH_STK_SECTOR_WEBSITES' = {
           CATln('Loading INS_REF ...')
           ins_ref     = setDT(readRDS(paste0('R:/WOMEN_CEO/DATA/YAH/', 'LIST_YAH_STK.rds')))
           List_Codes  = ins_ref[!is.na(marketcap), .(codesource, name, updated,iso3, country)]
           # List_Codes[, codesource:=yah]
           My.Kable(List_Codes)
           FileName    = 'DOWNLOAD_YAH_STK_SECTOR_WEBSITE'
           FilePath    = paste0('R:/WOMEN_CEO/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         }
  )
  
  CATln_Border(FilePath)
  
  if (nrow(List_Codes)>0)
  {
    if (file.exists(FilePath))
    {
      Data_old = readRDS(FilePath)
      if (nrow(Data_old)>0) {
        Data_old = Data_old[!is.na(codesource)]
        List_Codes_ToDo = List_Codes[!codesource %in% Data_old$codesource]
        My.Kable.MaxCols(Data_old)
      } else {
        List_Codes_ToDo = List_Codes
      }
    } else { 
      CATln(paste('FILE NO EXIST :', FilePath))
      Data_old = data.table() 
      List_Codes_ToDo = List_Codes
    }
    
    
    # Data_old$coxdesource = NULL
    # My.Kable.MaxCols(unique(Data_old[, .(codesource)], by='codesource'))
    # Data_old[, codesource:=ticker]
    # saveRDS(Data_old, FilePath)
    
    List_Codes_ToDo = List_Codes_ToDo[order(-codesource)]
    My.Kable(List_Codes_ToDo)
    
    if (nrow(List_Codes_ToDo)>0)
    {
      # x = GET_VST_BOARD_MEMBER_BY_CODE(pCode = 'YEG')
      Data_All = data.table()
      
      # FileName = paste0("LIST_YAH_", pISO3, '.rds') 
      # CATln_Border(FileName)
      Data_List = list()
      
      FileData = List_Codes_ToDo
      FileRow  = nrow(List_Codes_ToDo)
      
      Nb_TODO  = min(Nb_Min, FileRow)
      Nb_Total = FileRow
      
      FileToSave = FilePath
      Total_Time  = 0
      
      for (i in 1:Nb_TODO)
      {
        # i =1
        Start.Time = Sys.time()
        pCode    = FileData[i]$codesource
        CATln("")
        # CATln_Border(paste(i, '/', Nb_TODO, ' = ', pCode))
        CATln_Border(paste('RUN_LOOP_BY_BROUP_AND_SAVE : ', pOption, AddParam, '=', i, paste0('(', Nb_Group,')'), '/', Nb_TODO, '/', Nb_Total, ' = ', pCode))
        
        
        switch(pOption,
               'CLEANED_LIST_YAH_PARAM'      = {
                 My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))
               },
               'YAH_CURCRY'      = {
                 My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))
               },
               
               'YAH_FUTURES'      = {
                 My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))
               },
               
               'VST_BOARD_MEMBER'       = {
                 My_Data = try(GET_VST_BOARD_MEMBER_BY_CODE(pCode = pCode))
               },
               
               'YAH_STK_BY_SECTOR'       = {
                 My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))
               },
               
               'YAH_STK_DIV_BY_SECTOR'       = {
                 My_Data = try(DOWNLOAD_YAH_STK_DIVIDEND(pCode=pCode))
               },
               
               'YAH_STK_BY_SECTOR_UNADJ'       = {
                 My_Data = try(DOWNLOAD_YAH_INS_PRICES_UNADJ(p_nbdays_back=50000, tickers = pCode, freq.data="daily", p_saveto=''))
               },
               
               'YAH_STK_SECTOR_WEBSITE'       = {
                 My_Data = try(GET_YAH_STK_SECTOR_WEBSITE(pCode = pCode, ToPrint = F))
               },
               'CAF_STKVN_INTRADAY' = {
                 My_Data = try(DOWNLOAD_CAF_STKVN_INTRADAY_DAY(pCode=pCode, pDate=format.Date(as.Date(SYSDATETIME(9)), '%d/%m/%Y')))
               }
        )
        
        if (all(class(My_Data)!='try-error'))
        {
          Data_List[[i]] = My_Data
        }
        Sys.sleep(2)
        
        if (i %% Nb_Group ==0 | i == Nb_TODO)
        {
          Data_All = rbindlist(Data_List, fill=T)
          Data_final = rbind(Data_old, Data_All, fill=T)
          saveRDS(Data_final, FileToSave)
          CATln(paste('SAVED : ', FileToSave, '-', Format.Number(nrow(Data_final),0),'records'))
          Sys.sleep(2)
        }
        End.Time = Sys.time()
        Total_Time = as.numeric(difftime(End.Time, Start.Time, units = 'secs')) + Total_Time
        Avg_Time = Total_Time / i
        End_Forecast = Avg_Time * (Nb_TODO - i)
        Time_To_End = as.difftime(End_Forecast, units = "secs") + Sys.time()
        CATln_Border(paste('TIME TO END :', Time_To_End, '( after',format(End_Forecast, nsmall = 0),'secs )'))
      }
      
      My.Kable.TB(Data_final)
    } else {
      CATln_Border('DATA FULLY UPDATED')
      IFRC_SLEEP(2)
    }
  }
  return(Data_final)
}

#===============================================================================================
RUN_LOOP_BY_BROUP_AND_SAVE_V2 = function(pOption='VST_BOARD_MEMBER', Nb_Group = 50, Nb_Min = 50000, Frequency='MONTH', AddParam='')
{ #----------------------------------------------------------------------------------------------
  RData = 'R:/DATA/'
  ODDrive = 'D:/Onedrive/'
  # source(paste0('R:/R/', 'LIBRARIES/WCEO_LIBRARY.r'))
  Data_final = data.table()
  # pOption='VST_BOARD_MEMBER'
  # pOption='YAH_STK_SECTOR_WEBSITE'
  # Frequency='DAY'
  
  # pOption='DOWNLOAD_YAH_STK_TOP100'
  # Nb_Group = 5; Nb_Min = 10; Frequency='MONTH'
  switch(Frequency,
         'MONTH' = {
           CurrentFileDate = substr(gsub('-','', as.character(Sys.Date())),1,6)
           SaveFolder      = 'YYYYMM'
         },
         'DAY'   = {
           CurrentFileDate = substr(gsub('-','', as.character(Sys.Date())),1,8)
           SaveFolder      = 'YYYYMMDD'
         }
  )
  # CurrentYYYYMM = substr(gsub('-','', as.character(Sys.Date())),1,6)
  
  switch(pOption,
         # 'YAH_MUTUAL_FUND'      = {
         #   List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_MUTUAL_FUND.rds"))
         #   List_Codes     = unique(List_Companies[,.(codesource, name, code=paste0("MUTUAL_FUND",codesource ))], by ='codesource')
         #   My.Kable(List_Codes)
         #   FileName       = paste0('CCPR_YAH_MUTUAL_FUND')
         #   FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         # },
         
         'DOWNLOAD_YAH_STK_SIZE_PRICES'      = {
           # Frequency='MONTH'
           # pOption='DOWNLOAD_YAH_STK_SIZE'
           # AddParam = "MEGA,UNADJUST_ADJUST"
           xSize   = word(AddParam,1,1, sep = ',')
           xMethod = word(AddParam,2,2, sep = ',')
           
           File_List      = tolower(paste0(CCPRData, "YAH/LIST/LIST_YAH_STK_SCREENER.rds"))
           CATln_Border(File_List)
           List_Companies = setDT(readRDS(File_List))
           # List_Companies[, marketcapT:=as.numeric(gsub('T$|,','',marketcap))]
           List_Companies = List_Companies[!grepl('ETF|Fund', name) & size==xSize]
           List_Companies$capi_value = as.numeric(NA)
           List_Companies[, marketcap:=as.character(marketcap)]
           for (i in 1:nrow(List_Companies)) { List_Companies[i]$capi_value = CONVERT_IN_VALUE_KMBT(List_Companies[i]$marketcap) }
           My.Kable(List_Companies[, .(name, codesource, marketcap, capi_value)])
           
           #Top100 = List_Companies[1:100]
           #Top100 = List_Companies[1:100]
           #My.Kable(Top100[, .(name, codesource, marketcapT)])
           #My.Kable(Top100[, .(name, codesource, marketcapT)])
           
           #List_Codes     = unique(Top100[,.(codesource, name, marketcapT)], by ='codesource')
           List_Codes     = unique(List_Companies[,.(codesource, name)], by ='codesource')
           My.Kable(List_Codes)
           
           FileName       = paste0('DOWNLOAD_YAH_STK_SIZE_', xSize)
           #switch(AddParam,
           #'UNADJUST_ADJUST' = { FileName       = paste0('CCPR_YAH_STKTOP100')           },
           #'CLOSE'           = { FileName       = paste0('CCPR_YAH_STKTOP100_CLOSE')     }
           #)
           
           FilePath       = paste0('R:/CCPR/DATA/YAH/', SaveFolder, '/', FileName, '_', CurrentFileDate, '.rds')
           CATln_Border(paste("SAVE TO FILEPATH : ", FilePath))
         },
         
         'DOWNLOAD_YAH_STK_SECTOR_INDUSTRY'      = {
           # Frequency='MONTH'
           # pOption='CLEANED_LIST_YAH_PARAM'
           # AddParam = "UNADJUST_ADJUST"
           File_List      = tolower(paste0('R:/CCPR/', "LIST_YAH_LARGEST.rds"))
           CATln_Border(File_List)
           List_Companies = setDT(readRDS(File_List))
           List_Companies[, marketcapT:=as.numeric(gsub('T$|,','',marketcap))]
           List_Companies = List_Companies[!grepl('ETF|Fund', name) & !is.na(marketcapT)][order(-marketcapT)]
           My.Kable(List_Companies[, .(name, codesource, marketcapT)])
           #Top100 = List_Companies[1:100]
           #Top100 = List_Companies[1:100]
           #My.Kable(Top100[, .(name, codesource, marketcapT)])
           #My.Kable(Top100[, .(name, codesource, marketcapT)])
           
           #List_Codes     = unique(Top100[,.(codesource, name, marketcapT)], by ='codesource')
           List_Codes     = unique(List_Companies[,.(codesource, name, marketcapT)], by ='codesource')
           My.Kable(List_Codes)
           
           FileName       = paste0('DOWNLOAD_YAH_STK_SECTOR_INDUSTRY')
           #switch(AddParam,
           #'UNADJUST_ADJUST' = { FileName       = paste0('CCPR_YAH_STKTOP100')           },
           #'CLOSE'           = { FileName       = paste0('CCPR_YAH_STKTOP100_CLOSE')     }
           #)
           
           FilePath       = paste0('R:/CCPR/DATA/YAH/', SaveFolder, '/', FileName, '_', CurrentFileDate, '.rds')
           CATln_Border(paste("SAVE TO FILEPATH : ", FilePath))
         },
         'DOWNLOAD_YAH_STK_TOP100'      = {
           # Frequency='MONTH'
           # pOption='CLEANED_LIST_YAH_PARAM'
           # AddParam = "UNADJUST_ADJUST"
           File_List      = tolower(paste0('R:/CCPR/', "LIST_YAH_LARGEST.rds"))
           CATln_Border(File_List)
           List_Companies = setDT(readRDS(File_List))
           List_Companies[, marketcapT:=as.numeric(gsub('T$|,','',marketcap))]
           List_Companies = List_Companies[!grepl('ETF|Fund', name) & !is.na(marketcapT)][order(-marketcapT)]
           My.Kable(List_Companies[, .(name, codesource, marketcapT)])
           #Top100 = List_Companies[1:100]
           Top100 = List_Companies[1:100]
           #My.Kable(Top100[, .(name, codesource, marketcapT)])
           My.Kable(Top100[, .(name, codesource, marketcapT)])
           
           #List_Codes     = unique(Top100[,.(codesource, name, marketcapT)], by ='codesource')
           List_Codes     = unique(Top100[,.(codesource, name, marketcapT)], by ='codesource')
           My.Kable(List_Codes)
           
           #FileName       = paste0('CCPR_YAH_STKTOP100')
           switch(AddParam,
                  'UNADJUST_ADJUST' = { FileName       = paste0('CCPR_YAH_STKTOP100')           },
                  'CLOSE'           = { FileName       = paste0('CCPR_YAH_STKTOP100_CLOSE')     }
           )
           
           FilePath       = paste0('R:/CCPR/DATA/YAH/', SaveFolder, '/', FileName, '_', CurrentFileDate, '.rds')
           CATln_Border(paste("SAVE TO FILEPATH : ", FilePath))
         },
         'DOWNLOAD_YAH_STK_TOP1000'      = {
           # Frequency='MONTH'
           # pOption='CLEANED_LIST_YAH_PARAM'
           File_List      = tolower(paste0('R:/CCPR/', "LIST_YAH_LARGEST.rds"))
           CATln_Border(File_List)
           List_Companies = setDT(readRDS(File_List))
           List_Companies[, marketcapT:=as.numeric(gsub('T$|,','',marketcap))]
           List_Companies = List_Companies[!grepl('ETF|Fund', name) & !is.na(marketcapT)][order(-marketcapT)]
           My.Kable(List_Companies[, .(name, codesource, marketcapT)])
           #Top100 = List_Companies[1:100]
           Top1000 = List_Companies[1:1000]
           #My.Kable(Top100[, .(name, codesource, marketcapT)])
           My.Kable(Top1000[, .(name, codesource, marketcapT)])
           
           #List_Codes     = unique(Top100[,.(codesource, name, marketcapT)], by ='codesource')
           List_Codes     = unique(Top1000[,.(codesource, name, marketcapT)], by ='codesource')
           My.Kable(List_Codes)
           
           #FileName       = paste0('CCPR_YAH_STKTOP100')
           switch(AddParam,
                  'UNADJUST_ADJUST' = { FileName       = paste0('CCPR_YAH_STKTOP1000')           },
                  'CLOSE'           = { FileName       = paste0('CCPR_YAH_STKTOP1000_CLOSE')     }
           )
           
           FilePath       = paste0('R:/CCPR/DATA/YAH/', SaveFolder, '/', FileName, '_', CurrentFileDate, '.rds')
           CATln_Border(paste("SAVE TO FILEPATH : ", FilePath))
         },
         'CLEANED_LIST_YAH_PARAM'      = {
           # pOption='CLEANED_LIST_YAH_PARAM'
           File_List      = tolower(paste0('R:/CCPR/DATA/YAH/LIST/', "CLEAN_LIST_YAH_", AddParam, ".rds"))
           CATln_Border(File_List)
           List_Companies = readRDS(File_List)
           List_Codes     = unique(List_Companies[,.(codesource, name, code)], by ='codesource')
           My.Kable(List_Codes)
           
           FileName       = paste0('CCPR_YAH_', AddParam)
           FilePath       = paste0('R:/CCPR/DATA/YAH/', SaveFolder, '/', FileName, '_', CurrentFileDate, '.rds')
           CATln_Border(paste("SAVE TO FILEPATH : ", FilePath))
         },
         
         'YAH_FUTURES'      = {
           List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_FUTURES.rds"))
           List_Codes     = unique(List_Companies[,.(codesource, name, code=paste0("ETF",codesource ))], by ='codesource')
           My.Kable(List_Codes)
           FileName       = paste0('CCPR_YAH_FUTURES')
           FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         },
         # 'YAH_ETF'      = {
         #   List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_ETF.rds"))
         #   List_Codes     = unique(List_Companies[,.(codesource, name, code=paste0("ETF", gsub('-USD','',codesource )))], by ='codesource')
         #   My.Kable(List_Codes)
         #   FileName       = paste0('CCPR_YAH_ETF')
         #   FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         # },
         'YAH_CURCRY'      = {
           List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_CRYPTO.rds"))
           List_Codes     = unique(List_Companies[,.(codesource, name, code=paste0("CURCRY", gsub('-USD','',codesource )))], by ='codesource')
           My.Kable(List_Codes)
           FileName       = paste0('CCPR_YAH_CURCRY_PRICES')
           FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         },
         'YAH_STK_BY_SECTOR'      = {
           # AddParam = 'BSM'
           List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_STK_BY_SECTOR_ClEANED.rds"))
           List_Codes     = unique(List_Companies[,.(codesource, name, sector_code)][sector_code==AddParam], by ='codesource')
           My.Kable(List_Codes)
           FileName       = paste0('CCPR_YAH_STK_', AddParam, '_PRICES')
           FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         },
         'YAH_STK_DIV_BY_SECTOR'      = {
           # AddParam = 'BSM'
           List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_STK_BY_SECTOR_ClEANED.rds"))
           List_Codes     = unique(List_Companies[,.(codesource, name, sector_code)][sector_code==AddParam], by ='codesource')
           My.Kable(List_Codes)
           FileName       = paste0('CCPR_YAH_STK_', AddParam, '_DIV')
           FilePath       = paste0('R:/CCPR/DATA/YAH/YYYYMM/', FileName, '_', CurrentFileDate, '.rds')
         },
         
         'YAH_STK_BY_SECTOR_UNADJ'      = {
           # AddParam = 'BSM'
           List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_STK_BY_SECTOR_ClEANED.rds"))
           List_Codes     = unique(List_Companies[,.(codesource, name, sector_code)][sector_code==AddParam], by ='codesource')
           My.Kable(List_Codes)
           FileName       = paste0('CCPR_YAH_STK_', AddParam, '_PRICES_UNADDJ')
           FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         },
         
         'VST_BOARD_MEMBER'       = {
           List_Companies =  readRDS(paste0(RData, "SSI/download_ssi_stkvn_exc_ref.rds"))
           List_Codes  = unique(List_Companies[,.(code,source,codesource),by ='codesource'])
           My.Kable(List_Codes)
           FileName    = 'DOWNLOAD_VST_STKVN_BOARD_MEMBERS'
           FilePath    = paste0('R:/WOMEN_CEO/DATA/VST/', FileName, '_', CurrentFileDate, '.rds')
         },
         'CAF_STKVN_INTRADAY'       = {
           # pOption     = 'CAF_STKVN_INTRADAY'
           List_Companies =  readRDS(paste0(RData, "SSI/download_ssi_stkvn_exc_ref.rds"))
           List_Codes  = unique(List_Companies[,.(code,source,codesource),by ='codesource'])[order(codesource)]
           My.Kable(List_Codes)
           FileName    = 'DOWNLOAD_CAF_STKVN_INTRADAY_DAY'
           FilePath    = paste0(RData, 'CAF/DAY/', FileName, '_', CurrentFileDate, '.rds')
         },
         # 'YAH_STK_SECTOR_WEBSITE' = {
         #   CATln('Loading INS_REF ...')
         #   ins_ref     = setDT(readRDS('R:/data/efrc_ins_ref.rds'))
         #   List_Codes  = ins_ref[type=='STK' & !is.na(yah), .(type, iso2, iso3, continent, country, yah, code, name, startdate, enddate, records)]
         #   List_Codes[, codesource:=yah]
         #   My.Kable(List_Codes)
         #   FileName    = 'DOWNLOAD_YAH_STK_SECTOR_WEBSITE'
         #   FilePath    = paste0('R:/WOMEN_CEO/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         # } ,
         'YAH_STK_SECTOR_WEBSITES' = {
           CATln('Loading INS_REF ...')
           ins_ref     = setDT(readRDS(paste0('R:/WOMEN_CEO/DATA/YAH/', 'LIST_YAH_STK.rds')))
           List_Codes  = ins_ref[!is.na(marketcap), .(codesource, name, updated,iso3, country)]
           # List_Codes[, codesource:=yah]
           My.Kable(List_Codes)
           FileName    = 'DOWNLOAD_YAH_STK_SECTOR_WEBSITE'
           FilePath    = paste0('R:/WOMEN_CEO/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
         },
         'YAH_BOARD_MEMBER'      = {
           CATln('Loading INS_REF ...')
           ins_ref     = setDT(readRDS(paste0('R:/WOMEN_CEO/DATA/YAH/', 'LIST_YAH_STK.rds')))
           List_Codes  = ins_ref[!is.na(marketcap), .(codesource, name, updated,iso3, country)]
           # List_Current = setDT(readRDS(paste0('R:/WOMEN_CEO/DATA/YAH/', 'LIST_YAH_BOARD_ALL_20230527.rds')))
           My.Kable(List_Codes)
           FileName    = 'DOWNLOAD_YAH_BOARD'
           FilePath    = paste0('R:/WOMEN_CEO/DATA/YAH/BOARD/', FileName, '_', CurrentFileDate, '.rds')
         },
         'YAH_CAPI_SIZE_BOARD'      = {
           # AddParam = 'SMALL'
           CATln('Loading LIST CODES ...')
           ins_ref     = setDT(readRDS(paste0('R:/CCPR/DATA/', 'LIST_STK_SCREENER_BY_CAP_ALL.rds')))
           List_Codes  = ins_ref[capi_size == AddParam]
           # List_Current = setDT(readRDS(paste0('R:/WOMEN_CEO/DATA/YAH/', 'LIST_YAH_BOARD_ALL_20230527.rds')))
           My.Kable(List_Codes)
           FileName    = paste0('DOWNLOAD_YAH_STK_SIZE_',AddParam,'_BOARD_',str_sub(gsub('-','',Sys.Date()), 1,6),'.rds')
           FilePath    = paste0(CCPRData, FileName)
         },
         'YAH_CAPI_SIZE_DIV'      = {
           # AddParam = 'SMALL'
           CATln('Loading LIST CODES ...')
           ins_ref     = setDT(readRDS(paste0('R:/CCPR/DATA/', 'LIST_STK_SCREENER_BY_CAP_ALL.rds')))
           List_Codes  = ins_ref[capi_size == AddParam]
           # List_Current = setDT(readRDS(paste0('R:/WOMEN_CEO/DATA/YAH/', 'LIST_YAH_BOARD_ALL_20230527.rds')))
           My.Kable(List_Codes)
           FileName    = paste0('DOWNLOAD_YAH_STK_SIZE_',AddParam,'_DIV_',str_sub(gsub('-','',Sys.Date()), 1,6),'.rds')
           FilePath    = paste0(CCPRData, FileName)
         },
         'DOWNLOAD_YAH_STK_SIZE_WCEO_PRICES'      = {
           # Frequency='MONTH'
           # pOption='DOWNLOAD_YAH_STK_SIZE'
           # AddParam = "MEGA,UNADJUST_ADJUST"
           xSize   = word(AddParam,1,1, sep = ',')
           xMethod = word(AddParam,2,2, sep = ',')
           
           File_List      = tolower( paste0(CCPRData, 'LIST_WCEO_ALLSIZE.rds'))
           CATln_Border(File_List)
           List_Companies = setDT(readRDS(File_List))
           # List_Companies[, marketcapT:=as.numeric(gsub('T$|,','',marketcap))]
           List_Companies = List_Companies[!grepl('ETF|Fund', name)]
           List_Companies$capi_value = as.numeric(NA)
           # List_Companies[, marketcap:=as.character(marketcap)]
           # for (i in 1:nrow(List_Companies)) { List_Companies[i]$capi_value = CONVERT_IN_VALUE_KMBT(List_Companies[i]$marketcap) }
           My.Kable(List_Companies[, .(name, codesource)])
           
           List_Codes     = unique(List_Companies[,.(codesource, name)], by ='codesource')
           My.Kable(List_Codes)
           
           FileName       = paste0('DOWNLOAD_YAH_STK_SIZE_WCEO_', xSize, '_', xMethod)
           #switch(AddParam,
           #'UNADJUST_ADJUST' = { FileName       = paste0('CCPR_YAH_STKTOP100')           },
           #'CLOSE'           = { FileName       = paste0('CCPR_YAH_STKTOP100_CLOSE')     }
           #)
           
           FilePath       = paste0(CCPRData,   FileName, '_', CurrentFileDate, '.rds')
           CATln_Border(paste("SAVE TO FILEPATH : ", FilePath))
         }
         
  )
  
  CATln_Border(FilePath)
  
  if (nrow(List_Codes)>0)
  {
    if (file.exists(FilePath))
    {
      Data_old = readRDS(FilePath)
      if (nrow(Data_old)>0) {
        Data_old = Data_old[!is.na(codesource)]
        List_Codes_ToDo = List_Codes[!codesource %in% Data_old$codesource]
        My.Kable.MaxCols(Data_old)
      } else {
        List_Codes_ToDo = List_Codes
      }
    } else { 
      CATln(paste('FILE NO EXIST :', FilePath))
      Data_old = data.table() 
      List_Codes_ToDo = List_Codes
    }
    
    
    # Data_old$coxdesource = NULL
    # My.Kable.MaxCols(unique(Data_old[, .(codesource)], by='codesource'))
    # Data_old[, codesource:=ticker]
    # saveRDS(Data_old, FilePath)
    
    List_Codes_ToDo = List_Codes_ToDo[order(-codesource)]
    My.Kable(List_Codes_ToDo)
    
    if (nrow(List_Codes_ToDo)>0)
    {
      # x = GET_VST_BOARD_MEMBER_BY_CODE(pCode = 'YEG')
      Data_All = data.table()
      
      # FileName = paste0("LIST_YAH_", pISO3, '.rds') 
      # CATln_Border(FileName)
      Data_List = list()
      
      FileData = List_Codes_ToDo
      FileRow  = nrow(List_Codes_ToDo)
      
      Nb_TODO  = min(Nb_Min, FileRow)
      Nb_Total = FileRow
      
      FileToSave = FilePath
      Total_Time  = 0
      
      for (i in 1:Nb_TODO)
      {
        # i =1
        Start.Time = Sys.time()
        pCode    = FileData[i]$codesource
        CATln("")
        # CATln_Border(paste(i, '/', Nb_TODO, ' = ', pCode))
        CATln_Border(paste('RUN_LOOP_BY_BROUP_AND_SAVE : ', pOption, AddParam, '=', i, paste0('(', Nb_Group,')'), '/', Nb_TODO, '/', Nb_Total, ' = ', pCode))
        
        
        switch(pOption,
               
               'DOWNLOAD_YAH_STK_SIZE_PRICES'      = {
                 switch(xMethod,
                        'UNADJUST_ADJUST' = { My_Data = try(DOWNLOAD_YAH_INS_PRICES_UNADJ(p_nbdays_back=50000, tickers = pCode, 
                                                                                          freq.data="daily", p_saveto='')) }, 
                        'CLOSE'           = { My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))     }
                 )
               },
               'DOWNLOAD_YAH_STK_SIZE_WCEO_PRICES' = {
                 switch(xMethod,
                        'UNADJUST_ADJUST' = { My_Data = try(DOWNLOAD_YAH_INS_PRICES_UNADJ(p_nbdays_back=50000, tickers = pCode, 
                                                                                          freq.data="daily", p_saveto='')) }, 
                        'CLOSE'           = { My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))     }
                 )
               } ,
               
               'DOWNLOAD_YAH_STK_SECTOR_INDUSTRY'      = {
                 My_Data = try(DOWNLOAD_YAH_STK_SECTOR_INDUSTRY(pCode = pCode))
               },
               'DOWNLOAD_YAH_STK_TOP100'      = {
                 switch(AddParam,
                        'UNADJUST_ADJUST' = { My_Data = try(DOWNLOAD_YAH_INS_PRICES_UNADJ(p_nbdays_back=50000, tickers = pCode, freq.data="daily", p_saveto='')) }, 
                        'CLOSE'           = { My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))     }
                 )
               },
               
               'DOWNLOAD_YAH_STK_TOP1000'      = {
                 switch(AddParam,
                        'UNADJUST_ADJUST' = { My_Data = try(DOWNLOAD_YAH_INS_PRICES_UNADJ(p_nbdays_back=50000, tickers = pCode, freq.data="daily", p_saveto='')) }, 
                        'CLOSE'           = { My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))     }
                 )
               },
               
               'CLEANED_LIST_YAH_PARAM'      = {
                 My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))
               },
               'YAH_CURCRY'      = {
                 My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))
               },
               
               'YAH_FUTURES'      = {
                 My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))
               },
               
               'VST_BOARD_MEMBER'       = {
                 My_Data = try(GET_VST_BOARD_MEMBER_BY_CODE(pCode = pCode))
               },
               
               'YAH_STK_BY_SECTOR'       = {
                 My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))
               },
               
               'YAH_STK_DIV_BY_SECTOR'       = {
                 My_Data = try(DOWNLOAD_YAH_STK_DIVIDEND(pCode=pCode))
               },
               
               'YAH_STK_BY_SECTOR_UNADJ'       = {
                 My_Data = try(DOWNLOAD_YAH_INS_PRICES_UNADJ(p_nbdays_back=50000, tickers = pCode, freq.data="daily", p_saveto=''))
               },
               
               'YAH_STK_SECTOR_WEBSITE'       = {
                 My_Data = try(GET_YAH_STK_SECTOR_WEBSITE(pCode = pCode, ToPrint = F))
               },
               'CAF_STKVN_INTRADAY' = {
                 My_Data = try(DOWNLOAD_CAF_STKVN_INTRADAY_DAY(pCode=pCode, pDate=format.Date(as.Date(SYSDATETIME(9)), '%d/%m/%Y')))
               },
               'YAH_CAPI_SIZE_BOARD' = {
                 My_Data = try(DOWNLOAD_YAH_BOARD(pCode=pCode))
               },
               'YAH_CAPI_SIZE_DIV' =  {
                 My_Data = try(DOWNLOAD_YAH_STK_DIVIDEND(pCode=pCode))
               }
        )
        
        if (all(class(My_Data)!='try-error'))
        {
          Data_List[[i]] = My_Data
        }
        Sys.sleep(2)
        
        if (i %% Nb_Group ==0 | i == Nb_TODO)
        {
          Data_All = rbindlist(Data_List, fill=T)
          Data_final = rbind(Data_old, Data_All, fill=T)
          saveRDS(Data_final, FileToSave)
          # saveRDS(Data_final, paste0(ODDrive,FileName ) )
          CATln(paste('SAVED : ', FileToSave, '-', Format.Number(nrow(Data_final),0),'records'))
          Sys.sleep(2)
        }
        End.Time = Sys.time()
        Total_Time = as.numeric(difftime(End.Time, Start.Time, units = 'secs')) + Total_Time
        Avg_Time = Total_Time / i
        End_Forecast = Avg_Time * (Nb_TODO - i)
        Time_To_End = as.difftime(End_Forecast, units = "secs") + Sys.time()
        CATln_Border(paste('TIME TO END :', Time_To_End, '( after',format(End_Forecast, nsmall = 0),'secs )'))
      }
      
      My.Kable.TB(Data_final)
    } else {
      CATln_Border('DATA FULLY UPDATED')
      IFRC_SLEEP(2)
    }
  }
  return(Data_final)
}

#========================================================================================================
UPDATE_UPLOAD_CCPR_DATA_BASE = function(UData="R:/CCPR/DATA/")
{
  #------------------------------------------------------------------------------------------------------
  CATln_Border('LOADING INS_REF...')
  # ins_ref = readRDS(paste0(UData, 'efrc_ins_ref.rds'))
  RELOAD_INSREF(pFolder = UData)
  
  Data_Old = CCPR_LOAD_SQL_HOST(pTableSQL = 'design_our_database', ToDisconnect=T, ToKable=T)
  Data_Old$id = NULL
  Data_Old[,nr:= as.numeric(nr)]
  My.Kable.All(Data_Old)
  
  Data_List = list()
  nr = 0
  
  if(nrow(Data_Old)<=0){
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='header', group=NA , col1='Type', col2='Datasets', col3='Records', col4='Start Date', col5='End Date', col6='Countries')
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='group', group=NA , col1='Vietnam', col2=NA, col3=NA, col4=NA, col5=NA, col6=NA)
  }
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type=='STK' & country=='VIETNAM'], Label='Stocks',   Group='Vietnam')
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group & col1==x$label, ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type=='IND' & country=='VIETNAM'], Label='Indexes', Group='Vietnam')
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group & col1==x$label, ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type=='IND' & country=='VIETNAM' & grepl('IFRC', provider)], Label='BeQ/IFRC Indexes', Group='Vietnam')
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group & col1==x$label, ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type %in% list('BND', 'CUR', 'FUT', 'FND', 'CMD') & country=='VIETNAM'], Label='Other instruments', Group='Vietnam')
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group & grepl(x$label,col1), ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type %in% list('ECO', 'CLI', 'COV') & country=='VIETNAM'], Label='Economics/Environment', Group='Vietnam')
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group & grepl(x$label,col1), ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[!(type %in% list('STK','IND','BND','CUR','FUT','FND','CMD','ECO', 'CLI', 'COV')) & country=='VIETNAM'], Label='Other', Group='Vietnam')
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group & grepl(x$label,col1), ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  #==================================================================================================
  
  if(nrow(Data_Old)<=0){
    nr = nr+1
    Data_List[[nr]] = data.table(nr=nr,type='group', group=NA , col1='International', col2=NA, col3=NA, col4=NA, col5=NA, col6=NA)
  }
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type=='STK' & country!='VIETNAM'], Label='Stocks',   Group='International')
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group & col1==x$label, ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type=='IND' & country!='VIETNAM'], Label='Indexes', Group='International')
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group & col1==x$label, ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type=='IND' & country!='VIETNAM' & grepl('IFRC', provider)], Label='BeQ/IFRC Indexes', Group='International')
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group & col1==x$label, ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  # x = COUNT_OUR_DATABASE(MyREF = ins_ref[type %in% list('BND', 'CUR', 'FUT', 'FND', 'CMD') & country!='VIETNAM'], Label='Other instruments', 
  #                        Group='International')
  # Data_Old[type=='data' & group==x$group & grepl(x$label,col1), ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  # 
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type %in% list('BND') & country!='VIETNAM'], Label='Bonds', 
                         Group='International')
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group& col1==x$label, ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type %in% list('CUR') & country!='VIETNAM'], Label='Currencies', 
                         Group='International')
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group& col1==x$label, ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type %in% list('FUT') & country!='VIETNAM'], Label='Futures', 
                         Group='International')
  
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group & col1==x$label, ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type %in% list('FND') & country!='VIETNAM'], Label='Funds', 
                         Group='International')
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group & col1==x$label, ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type %in% list('CMD') & country!='VIETNAM'], Label='Commodities', 
                         Group='International')
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group & col1==x$label, ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type %in% list('FUT') & country!='VIETNAM'], Label='Derivatives',
                         Group='International')
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group & col1==x$label, ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref[type %in% list('ECO', 'CLI', 'COV') & country!='VIETNAM'], Label='Economics/Environment', Group='International')
  
  if(nrow(Data_Old)>0){
    Data_Old[type=='data' & group==x$group & grepl(x$label,col1), ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='data', group=x$group , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  # x = COUNT_OUR_DATABASE(MyREF = ins_ref[!(type %in% list('STK','IND','BND','CUR','FUT','FND','CMD','ECO', 'CLI', 'COV')) & country!='VIETNAM'], Label='Other', Group='International')
  # Data_Old[type=='data' & group==x$group & grepl(x$label,col1), ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  
  x = COUNT_OUR_DATABASE(MyREF = ins_ref, Label='Total', Group='International')
  
  if(nrow(Data_Old)>0){
    Data_Old[type=='group' & col1 == x$label, ":="(col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)]
  }else{
    nr = nr+1
    Data_List[[nr]] = data.table(nr = nr,type='group', group=NA , col1=x$label, col2=x$datasets, col3=x$records, col4=x$startdate, col5=x$enddate, col6=x$countries)
  }
  
  if(nrow(Data_Old)>0){
    Data_Final = Data_Old
  }else{
    Data_Final = bind_rows(Data_List)
  }
  
  
  saveRDS(Data_Final,paste0(CCPRData,'design_our_database.rds'))
  
  try(IFRC.UPLOAD.RDS.2023(l.host = "ccpr.vn", l.tablename='design_our_database',
                           l.filepath= tolower(paste0(CCPRDrive,'design_our_database.rds')),
                           CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=T))
}

#========================================================================================================
# RUN_LOOP_BY_BROUP_AND_SAVE = function(pOption='VST_BOARD_MEMBER', Nb_Group = 50, Nb_Min = 50000, Frequency='MONTH', AddParam='')
# {
#   RData = paste0(CCPRDrive, 'CCPR/')
#   source(paste0('R:/R/', 'LIBRARIES/WCEO_LIBRARY.r'))
#   Data_final = data.table()
#   # pOption='VST_BOARD_MEMBER'
#   # pOption='YAH_STK_SECTOR_WEBSITE'
#   # Frequency='DAY'
#   switch(Frequency,
#          'MONTH' = {CurrentFileDate = substr(gsub('-','', as.character(Sys.Date())),1,6)},
#          'DAY'   = {CurrentFileDate = substr(gsub('-','', as.character(Sys.Date())),1,8)}
#   )
#   # CurrentYYYYMM = substr(gsub('-','', as.character(Sys.Date())),1,6)
#   switch(pOption,
#          # 'YAH_MUTUAL_FUND'      = {
#          #   List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_MUTUAL_FUND.rds"))
#          #   List_Codes     = unique(List_Companies[,.(codesource, name, code=paste0("MUTUAL_FUND",codesource ))], by ='codesource')
#          #   My.Kable(List_Codes)
#          #   FileName       = paste0('CCPR_YAH_MUTUAL_FUND')
#          #   FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
#          # },
#          'YAH_FUTURES'      = {
#            List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_FUTURES.rds"))
#            List_Codes     = unique(List_Companies[,.(codesource, name, code=paste0("ETF",codesource ))], by ='codesource')
#            My.Kable(List_Codes)
#            FileName       = paste0('CCPR_YAH_FUTURES')
#            FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
#          },
#          # 'YAH_ETF'      = {
#          #   List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_ETF.rds"))
#          #   List_Codes     = unique(List_Companies[,.(codesource, name, code=paste0("ETF", gsub('-USD','',codesource )))], by ='codesource')
#          #   My.Kable(List_Codes)
#          #   FileName       = paste0('CCPR_YAH_ETF')
#          #   FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
#          # },
#          'YAH_CURCRY'      = {
#            List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_CRYPTO.rds"))
#            List_Codes     = unique(List_Companies[,.(codesource, name, code=paste0("CURCRY", gsub('-USD','',codesource )))], by ='codesource')
#            My.Kable(List_Codes)
#            FileName       = paste0('CCPR_YAH_CURCRY_PRICES')
#            FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
#          },
#          'YAH_STK_BY_SECTOR'      = {
#            # AddParam = 'BSM'
#            List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_STK_BY_SECTOR_ClEANED.rds"))
#            List_Codes     = unique(List_Companies[,.(codesource, name, sector_code)][sector_code==AddParam], by ='codesource')
#            My.Kable(List_Codes)
#            FileName       = paste0('CCPR_YAH_STK_', AddParam, '_PRICES')
#            FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
#          },
#          'YAH_STK_DIV_BY_SECTOR'      = {
#            # AddParam = 'BSM'
#            List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_STK_BY_SECTOR_ClEANED.rds"))
#            List_Codes     = unique(List_Companies[,.(codesource, name, sector_code)][sector_code==AddParam], by ='codesource')
#            My.Kable(List_Codes)
#            FileName       = paste0('CCPR_YAH_STK_', AddParam, '_DIV')
#            FilePath       = paste0('R:/CCPR/DATA/YAH/YYYYMM/', FileName, '_', CurrentFileDate, '.rds')
#          },
#          
#          'YAH_STK_BY_SECTOR_UNADJ'      = {
#            # AddParam = 'BSM'
#            List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_STK_BY_SECTOR_ClEANED.rds"))
#            List_Codes     = unique(List_Companies[,.(codesource, name, sector_code)][sector_code==AddParam], by ='codesource')
#            My.Kable(List_Codes)
#            FileName       = paste0('CCPR_YAH_STK_', AddParam, '_PRICES_UNADDJ')
#            FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
#          },
#          
#          'VST_BOARD_MEMBER'       = {
#            List_Companies =  readRDS(paste0(RData, "SSI/download_ssi_stkvn_exc_ref.rds"))
#            List_Codes  = unique(List_Companies[,.(code,source,codesource),by ='codesource'])
#            My.Kable(List_Codes)
#            FileName    = 'DOWNLOAD_VST_STKVN_BOARD_MEMBERS'
#            FilePath    = paste0('R:/WOMEN_CEO/DATA/VST/', FileName, '_', CurrentFileDate, '.rds')
#          },
#          'CAF_STKVN_INTRADAY'       = {
#            # pOption     = 'CAF_STKVN_INTRADAY'
#            List_Companies =  readRDS(paste0(RData, "SSI/download_ssi_stkvn_exc_ref.rds"))
#            List_Codes  = unique(List_Companies[,.(code,source,codesource),by ='codesource'])[order(codesource)]
#            My.Kable(List_Codes)
#            FileName    = 'DOWNLOAD_CAF_STKVN_INTRADAY_DAY'
#            FilePath    = paste0(RData, 'CAF/DAY/', FileName, '_', CurrentFileDate, '.rds')
#          },
#          # 'YAH_STK_SECTOR_WEBSITE' = {
#          #   CATln('Loading INS_REF ...')
#          #   ins_ref     = setDT(readRDS('R:/data/efrc_ins_ref.rds'))
#          #   List_Codes  = ins_ref[type=='STK' & !is.na(yah), .(type, iso2, iso3, continent, country, yah, code, name, startdate, enddate, records)]
#          #   List_Codes[, codesource:=yah]
#          #   My.Kable(List_Codes)
#          #   FileName    = 'DOWNLOAD_YAH_STK_SECTOR_WEBSITE'
#          #   FilePath    = paste0('R:/WOMEN_CEO/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
#          # } ,
#          'YAH_STK_SECTOR_WEBSITES' = {
#            CATln('Loading INS_REF ...')
#            ins_ref     = setDT(readRDS(paste0('R:/WOMEN_CEO/DATA/YAH/', 'LIST_YAH_STK.rds')))
#            List_Codes  = ins_ref[!is.na(marketcap), .(codesource, name, updated,iso3, country)]
#            # List_Codes[, codesource:=yah]
#            My.Kable(List_Codes)
#            FileName    = 'DOWNLOAD_YAH_STK_SECTOR_WEBSITE'
#            FilePath    = paste0('R:/WOMEN_CEO/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
#          }
#   )
#   
#   CATln_Border(FilePath)
#   if (nrow(List_Codes)>0)
#   {
#     if (file.exists(FilePath))
#     {
#       Data_old = readRDS(FilePath)
#       if (nrow(Data_old)>0) {
#         Data_old = Data_old[!is.na(codesource)]
#         List_Codes_ToDo = List_Codes[!codesource %in% Data_old$codesource]
#         My.Kable.MaxCols(Data_old)
#       } else {
#         List_Codes_ToDo = List_Codes
#       }
#     } else { 
#       Data_old = data.table() 
#       List_Codes_ToDo = List_Codes
#     }
#     
#     
#     # Data_old$coxdesource = NULL
#     # My.Kable.MaxCols(unique(Data_old[, .(codesource)], by='codesource'))
#     # Data_old[, codesource:=ticker]
#     # saveRDS(Data_old, FilePath)
#     
#     List_Codes_ToDo = List_Codes_ToDo[order(-codesource)]
#     My.Kable(List_Codes_ToDo)
#     if (nrow(List_Codes_ToDo)>0)
#     {
#       # x = GET_VST_BOARD_MEMBER_BY_CODE(pCode = 'YEG')
#       Data_All = data.table()
#       
#       # FileName = paste0("LIST_YAH_", pISO3, '.rds') 
#       # CATln_Border(FileName)
#       Data_List = list()
#       
#       FileData = List_Codes_ToDo
#       FileRow  = nrow(List_Codes_ToDo)
#       
#       Nb_TODO  = min(Nb_Min, FileRow)
#       Nb_Total = FileRow
#       
#       FileToSave = FilePath
#       Total_Time  = 0
#       for (i in 1:Nb_TODO)
#       {
#         # i =1
#         Start.Time = Sys.time()
#         pCode    = FileData[i]$codesource
#         CATln("")
#         # CATln_Border(paste(i, '/', Nb_TODO, ' = ', pCode))
#         CATln_Border(paste('RUN_LOOP_BY_BROUP_AND_SAVE : ', pOption, AddParam, '=', i, paste0('(', Nb_Group,')'), '/', Nb_TODO, '/', Nb_Total, ' = ', pCode))
#         
#         
#         switch(pOption,
#                'YAH_CURCRY'      = {
#                  My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))
#                },
#                
#                'YAH_FUTURES'      = {
#                  My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))
#                },
#                
#                'VST_BOARD_MEMBER'       = {
#                  My_Data = try(GET_VST_BOARD_MEMBER_BY_CODE(pCode = pCode))
#                },
#                
#                'YAH_STK_BY_SECTOR'       = {
#                  My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))
#                },
#                
#                'YAH_STK_DIV_BY_SECTOR'       = {
#                  My_Data = try(DOWNLOAD_YAH_STK_DIVIDEND(pCode=pCode))
#                },
#                
#                'YAH_STK_BY_SECTOR_UNADJ'       = {
#                  My_Data = try(DOWNLOAD_YAH_INS_PRICES_UNADJ(p_nbdays_back=50000, tickers = pCode, freq.data="daily", p_saveto=''))
#                },
#                
#                'YAH_STK_SECTOR_WEBSITE'       = {
#                  My_Data = try(GET_YAH_STK_SECTOR_WEBSITE(pCode = pCode, ToPrint = F))
#                },
#                'CAF_STKVN_INTRADAY' = {
#                  My_Data = try(DOWNLOAD_CAF_STKVN_INTRADAY_DAY(pCode=pCode, pDate=format.Date(as.Date(SYSDATETIME(9)), '%d/%m/%Y')))
#                }
#         )
#         
#         if (all(class(My_Data)!='try-error'))
#         {
#           Data_List[[i]] = My_Data
#         }
#         Sys.sleep(2)
#         
#         if (i %% Nb_Group ==0 | i == Nb_TODO)
#         {
#           Data_All = rbindlist(Data_List, fill=T)
#           Data_final = rbind(Data_old, Data_All, fill=T)
#           saveRDS(Data_final, FileToSave)
#           CATln(paste('SAVED : ', FileToSave, '-', Format.Number(nrow(Data_final),0),'records'))
#           Sys.sleep(2)
#         }
#         End.Time = Sys.time()
#         Total_Time = as.numeric(difftime(End.Time, Start.Time, units = 'secs')) + Total_Time
#         Avg_Time = Total_Time / i
#         End_Forecast = Avg_Time * (Nb_TODO - i)
#         Time_To_End = as.difftime(End_Forecast, units = "secs") + Sys.time()
#         CATln_Border(paste('TIME TO END :', Time_To_End, '( after',format(End_Forecast, nsmall = 0),'secs )'))
#       }
#       
#       My.Kable.TB(Data_final)
#     } else {
#       CATln_Border('DATA FULLY UPDATED')
#       IFRC_SLEEP(2)
#     }
#   }
#   return(Data_final)
# }

# ==================================================================================================
STR_RIGHT_AFTER = function(l.str, l.sep) {
  # ------------------------------------------------------------------------------------------------
  # l.sep ="\\/"
  # l.sep = "abc"
  # l.str = "D:/ONEDRIVE_IREEDS/OneDrive - ireeds.edu.vn/EFRC/DATA/STKEIK/efrc_eik_stkUSA_mdata.rds"
  x.out = unlist(str_split(l.str, l.sep))
  l.strres = x.out[[length(x.out)]]
  return(l.strres)
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
CATln = function(l.Str, top_border=F, p_border=F, pNbCar=120) {
  # ------------------------------------------------------------------------------------------------
  if (top_border) {catrep('=',pNbCar)}
  cat("\r", paste(l.Str, strrep(" ",max(0,pNbCar - nchar(l.Str)))), "\n")
  if (p_border) {catrep('-',pNbCar)}
}

# ==================================================================================================
DATACENTER_LINE_BORDER = function(l.msg="hello", l.justify="centre", clearscreen=F) {
  # ------------------------------------------------------------------------------------------------
  NbCarLine = 125
  if (clearscreen) {shell("cls")}
  CATln(strrep("=",NbCarLine))
  CATln(format(l.msg, width=NbCarLine, justify=l.justify))
  CATln(strrep("=",NbCarLine))
}

# ==================================================================================================
printrep = function(strchar,nchar) {
  # ------------------------------------------------------------------------------------------------
  print(paste(replicate(nchar,strchar), collapse = ""),quote = F)
}

# ==================================================================================================
My.Kable.All = function(pData, l.Text='') {
  My.Kable(pData, HeadOnly = T, Nb=nrow(pData), l.Text=l.Text)
}

# ==================================================================================================
My.Kable.Matrix = function(My_MATRIX) {
  # ------------------------------------------------------------------------------------------------
  First_Col = as.data.table(rownames(My_MATRIX)); colnames(First_Col) = "NAME"
  Other_Col = as.data.table(My_MATRIX)
  My.Kable.All(cbind(First_Col, Other_Col))
  CATln('* Updated TODAY')
}

# ==================================================================================================
str.extract = function(x,strt,stre) { # FUNCTION: EXTRACT SUB-STRING FROM A STRING
  # ------------------------------------------------------------------------------------------------
  num1 <- nchar(strt)
  num2 <- nchar(stre)
  pos1 <- regexpr(strt, x, fixed = T)
  z <- str.remove(x,strt,x)
  pos2 <- regexpr(stre, z, fixed = T) + pos1 + num1
  y <- substr(x, pos1 + num1, pos2 - 2)
  return(y)
}

# ==================================================================================================
efrc.str.extract.mid = function(x, x.begin, x.end) {
  # ------------------------------------------------------------------------------------------------
  l.res = gsub(paste0(".*",x.begin,"\\s*|",x.end,".*"), "", x)
  return(l.res)
}

# ==================================================================================================
efrc.str.extract.end = function(x, x.begin) {
  # ------------------------------------------------------------------------------------------------
  l.res = gsub(paste0(".*",x.begin), "", x)
  return(l.res)
}

# ==================================================================================================
efrc.str.extract.begin = function(x, x.end) {
  # ------------------------------------------------------------------------------------------------
  l.res = gsub(paste0(x.end,".*"), "", x)
  return(l.res)
}

# ==================================================================================================
str.remove = function(x,strt,stre) { # FUNCTION: REMOVE SUB-STRING FROM A STRING
  # ------------------------------------------------------------------------------------------------
  pos1 <- regexpr(strt, x, fixed = T)
  num1 <- nchar(strt)
  pos2 <- nchar(stre)
  y <- substr(x, num1 + pos1, pos2)
  return(y)
}

# 
# # ==================================================================================================
# My.Kable = function(l.Table, Nb=3, l.Text="", HeadOnly=F) {
#   # ------------------------------------------------------------------------------------------------
#   # l.Table = x.ins
#   cat("\r\r")
#   if (nrow(l.Table)>0)
#   {
#     if (nchar(l.Text)>0) { cat(l.Text) }
#     print(kable(format.args = list(decimal.mark = ".", big.mark = ","),
#                 head(l.Table, min(Nb,nrow(l.Table))), format="markdown"))
#     # cat("")
#     if (! HeadOnly)
#     {
#       print(kable(format.args = list(decimal.mark = ".", big.mark = ","),
#                   tail(l.Table, min(Nb,nrow(l.Table))), format="markdown"))
#     }
#     # print("", quote=F)
#     print(paste(Format.Number(nrow(l.Table),0), "records"), quote = F)
#   } else
#   { print("No data ...", quote=F)}
# }
# 
# # ==================================================================================================
# My.Kable.TB = function(l.Table, HeadOnly=F, Nb=3, l.Text="") {
#   # ------------------------------------------------------------------------------------------------
#   # l.Table =
#   library(knitr)
#   # l.Text=""; Nb=3
#   # l.Table = readRDS("u:/efrc/data/efrc_indipo_history.rds")
#   cat("\r\r")
#   if (nrow(l.Table)>0)
#   {
#     if (!HeadOnly) {l.Table.TB = rbind( head(l.Table, min(Nb,nrow(l.Table))), tail(l.Table, min(Nb,nrow(l.Table))), fill=T)} else {
#       l.Table.TB = head(l.Table, min(Nb,nrow(l.Table))) }
#     if (nchar(l.Text)>0) { cat(l.Text,"\n") }
#     print(kable(format.args = list(decimal.mark = ".", big.mark = ","), l.Table.TB, format="markdown"))
#     print(paste(Format.Number(nrow(l.Table),0), "records"), quote = F)
#   } else
#   { print("No data ...", quote=F)}
# }

# # ==================================================================================================
# DATACENTER_LINE_BORDER = function(l.msg="hello", l.justify="centre", clearscreen=F) {
#   # ------------------------------------------------------------------------------------------------
#   if (clearscreen) {shell("cls")}
#   CATln(strrep("=",125))
#   CATln(format(l.msg, width=125, justify=l.justify))
#   CATln(strrep("=",125))
# }
# # FROM OTHER LIBRARIES

# ==================================================================================================
CATrp = function(l.Str) {
  # ------------------------------------------------------------------------------------------------
  cat("\r", paste(l.Str, strrep(" ",max(0,120-nchar(l.Str)))), "")
}

# ==================================================================================================
str.extract = function(x,strt,stre) { # FUNCTION: EXTRACT SUB-STRING FROM A STRING
  # ------------------------------------------------------------------------------------------------
  num1 <- nchar(strt)
  num2 <- nchar(stre)
  pos1 <- regexpr(strt, x, fixed = T)
  z <- str.remove(x,strt,x)
  pos2 <- regexpr(stre, z, fixed = T) + pos1 + num1
  y <- substr(x, pos1 + num1, pos2 - 2)
  return(y)
}

# ==================================================================================================
# str.remove = function(x,strt,stre) { # FUNCTION: REMOVE SUB-STRING FROM A STRING
#   # ------------------------------------------------------------------------------------------------
#   pos1 <- regexpr(strt, x, fixed = T)
#   num1 <- nchar(strt)
#   pos2 <- nchar(stre)
#   y <- substr(x, num1 + pos1, pos2)
#   return(y)
# }

# ==================================================================================================
catrep = function(l.char, n.times) {
  # ------------------------------------------------------------------------------------------------
  cat("\r", strrep(l.char, n.times), "\n")
}

# ==================================================================================================
Format.Number = function(NumberStr, l.decimal) {
  # ------------------------------------------------------------------------------------------------
  return(format(NumberStr, digits = 2, nsmall = l.decimal, big.mark = ","))
}

# ==================================================================================================
CLEAN_COLNAMES = function(dt) {
  # ------------------------------------------------------------------------------------------------
  xt = dt
  colnames(xt) =  trimws(gsub("\\.","_", gsub("[*]", "", gsub('%','percent_', gsub(' ','',gsub('-','_',tolower(colnames(xt))))))))
  return(xt)
}

# ==================================================================================================
CATln = function(l.Str, top_border=F, p_border=F, pNbCar=120, clearscreen=F) {
  # ------------------------------------------------------------------------------------------------
  if (clearscreen) { shell('cls') }
  if (top_border) {catrep('=',pNbCar)}
  cat("\r", paste(l.Str, strrep(" ",max(0,pNbCar - nchar(l.Str)))), "\n")
  if (p_border) {catrep('-',pNbCar)}
}

# ==================================================================================================
CATln_Border = function(pStr) {
  # ------------------------------------------------------------------------------------------------
  CATln(pStr, top_border = T, p_border = T) 
}

# ==================================================================================================
GET_BOARD_INVESTING = function(pCodesource='equities/amazon-com-inc') {
  # ------------------------------------------------------------------------------------------------
  # pCodesource = 'equities/amazon-com-inc'
  # pCodesource = 'equities/2s-metal'
  pURL = paste0('https://www.investing.com/', pCodesource, '-company-profile')
  print(pURL)
  
  content  = rvest::read_html(pURL)
  tables   = content %>% html_table(fill = TRUE)
  rm(xData)
  xData = data.table()
  if ("Name" %in% names(tables[[1]]))
  {
    xData    = as.data.table(tables[[1]])
  }
  if ("Name" %in% names(tables[[2]]))
  {
    xData    = as.data.table(tables[[2]])
  }
  My.Kable(xData)
  if (nrow(xData)>0)
  {
    # tables
    
    # kable(xData)
    colnames(xData) = tolower(colnames(xData))
    
    xData$last_name = as.character(NA)
    # str(xData)
    xData[grepl('[.]', name), last_name:=trimws(sub(paste0(".*", '[.]'), "", name))]
    xData[grepl('  ', name) & is.na(last_name), last_name:=trimws(sub(paste0(".*", '  '), "", name))]
    xData[grepl(' ', name) & is.na(last_name), last_name:=trimws(sub(paste0(".*", ' '), "", name))]
    My.Kable(xData)
    
    # str(xData)
    
    xData$first_name = as.character(NA)
    for (k in 1:nrow(xData))
    {
      FName = trimws(sub(paste0(xData[k]$last_name,".*"), "", xData[k]$name))
      xData[k]$first_name = FName
    }
    My.Kable(xData)
    
    library(stringr)
    # word('Nha Quan', 1)
    xData[, first1:=word(first_name)]
    
    library(gender)
    # x  = gender('Christine')
    # x$gender
    
    xData$gender = as.character(NA)
    for (k in 1:nrow(xData))
    {
      # k = 1
      # xData[k]$first1
      FName = try(gender(xData[k]$first1))
      if (nrow(FName)>0)
      {
        print(paste(k, FName))
        GName = FName$gender
        xData[k]$gender = GName
      }
    }
    if (nrow(xData)>0)
    {
      xData[, ':='(source='INV', codesource=pCodesource)]
      xData$ceo=as.numeric(NA)
      xData$cob=as.numeric(NA)
      xData[grepl('CEO', title, ignore.case = T), ceo:=1]
      xData[grepl('Chairman|Chairwoman', title, ignore.case = T), cob:=1]
      xData[grepl('female', gender), gender:='F']
      xData[grepl('male', gender), gender:='M']
    }
    My.Kable(xData[ceo==1][, .(name, first_name, last_name, title)])
    My.Kable(xData)
  }
  return(xData)
}
# gender('Trang')
# gender('Minh')

# ==================================================================================================
RUN_ALL = function(FileName = 'country_indexes.xlsx', NbMin = 5) {
  # ------------------------------------------------------------------------------------------------
  My_list = setDT(openxlsx::read.xlsx(paste0(ODDrive, 'Beq/WOMEN_CEO/', 'country_indexes.xlsx')))
  My.Kable(My_list)
  str(My_list)
  My_country = unique(My_list, by='country')     
  My.Kable(My_country)
  
  
  for (k in 1:nrow(My_country))
  {
    # k = 2
    My_group = My_list[country == My_country[k]$country]
    My.Kable(My_group)
    pIndexCodes = as.list(My_group$index)           
    print(pIndexCodes)
    GET_BOARD_AIO_INV(pIndexCodes, NbMin=NbMin)
  }
}

# ==================================================================================================
GET_BOARD_AIO_INV = function(pIndexCodes, NbMin=1000) {
  # ------------------------------------------------------------------------------------------------
  Start = Sys.time()
  Data_all = data.table()
  for (k in 1:length(pIndexCodes))
  {
    # k = 1
    pIndexCode = pIndexCodes[k]
    pURL       = paste0('https://www.investing.com/', pIndexCode, '-components')
    # pURL     = 'https://www.investing.com/indices/investing.com-us-500-components'
    # pURL     = 'https://www.investing.com/indices/shanghai-composite-components'
    
    content  = rvest::read_html(pURL)
    tables   = content %>% html_table(fill = TRUE)
    # tables
    xData    = as.data.table(tables[[1]])
    xData    = xData[, -1]
    # xData    = xData[, -ncol(xData)]
    str(xData)
    colnames(xData)[ncol(xData)] = "x"
    xData = xData[, -c('x')]
    nrow(xData)
    
    links    = content %>% html_nodes(xpath='//a') %>% html_attr('href')
    tb_links = as.data.table(links)
    tb_links = tb_links[grepl('^/equities', links) & nchar(links)>nchar('/equities/')]
    tb_links[, id:=seq.int(1,.N)]
    tb_links = tb_links[order(id)][id>25]
    tb_links = tb_links[1:nrow(xData)]
    tb_links = setDT(tb_links)
    tb_links[, id:=seq.int(1,.N)]
    # View(tb_links)
    
    xData[, id:=seq.int(1,.N)]
    xData = setDT(xData)
    xData = merge(xData[, -c('links')], tb_links[, .(id, links)], all.x=T, by='id')
    xData[, date:=Sys.Date()]
    xData[, indexcode:=pIndexCode]
    str(xData)
    # View(xData)
    Data_all = rbind(Data_all, xData)
  }
  Data_all = unique(Data_all, by='links')
  My.Kable(Data_all)
  
  Stock_all = data.table()
  for (k in 1:min(NbMin, nrow(Data_all)))
  {
    # k = 1
    xcode = substr(Data_all[k]$links, 2, nchar(Data_all[k]$links)    )  
    CATln_Border(paste(k, xcode))
    z = GET_BOARD_INVESTING(pCodesource=xcode) 
    My.Kable(z)
    if (nrow(z)>0) { Stock_all = rbind(Stock_all, z, fill=T) }
    CATln("")
  }
  My.Kable(Stock_all)
  
  
  Data_People = 'WCEO_PPL_ASEAN.rds'
  File_People = paste0(ODDrive, 'Beq/WOMEN_CEO/', Data_People)
  if (file.exists(File_People))
  {
    Data_Old = readRDS(File_People)
  } else {
    Data_Old = data.table()
  }
  
  Data_Old = unique(rbind(Data_Old, Stock_all, fill=T))
  My.Kable(Data_Old)
  nrow(Data_Old)
  saveRDS(Data_Old,File_People)
  
  Data_Stock = 'WCEO_STK_ASEAN.rds'
  File_Stock = paste0(ODDrive, 'Beq/WOMEN_CEO/', Data_Stock)
  if (file.exists(File_Stock))
  {
    Data_Old = readRDS(File_Stock)
  } else {
    Data_Old = data.table()
  }
  
  Data_Old = unique(rbind(Data_Old, Data_all, fill=T))
  My.Kable(Data_Old)
  nrow(Data_Old)
  saveRDS(Data_Old,File_Stock)
  End = Sys.time()
  CATln_Border(difftime(End, Start, units='secs'))
}

# ==================================================================================================
GET_LIST_STOCK_EXCHANGE_MALAYSIA = function(RDataFolder='c:/rdata/') {
  # ------------------------------------------------------------------------------------------------
  
  # GET_LIST_STOCK_EXCHANGE_MALAYSIA()
  pURL = paste0('https://www.bursamarketplace.com/index.php?screentype=stocks&board=all&tpl=screener_ajax&type=getResult&action=listing&pagenum=',
                k, '&sfield=name&stype=desc')
  x = jsonlite::fromJSON(pURL)
  
  nb_pages = x$totalpage
  print(nb_pages)
  
  xList = list()
  for (k in 1:min(300, nb_pages))
  {
    # k = 1
    # k = 2
    # k = 3
    
    gc()
    # xURL = 'https://www.bursamarketplace.com'
    # 
    # content  = rvest::read_html(xURL)
    # 
    pURL = paste0('https://www.bursamarketplace.com/index.php?screentype=stocks&board=all&tpl=screener_ajax&type=getResult&action=listing&pagenum=',
                  k, '&sfield=name&stype=desc')
    print(paste(k, pURL))
    x = jsonlite::fromJSON(pURL)
    
    if (k == 1)
    {
      z = as.matrix(x$records)
      xDt = as.data.table(z)
      Final_Colname = colnames(xDt)
      
    } else {
      ncol(z)
      xDt = as.data.table(z)
    }
    My.Kable(xDt)
    
    xList[[k]] = xDt
    Sys.sleep(1)
  }
  
  xAll = rbindlist(xList, fill=T)
  xAll[, date:=Sys.Date()]
  kable(xAll)
  View(xAll)
  FileName = "list_stock_exchange_malaysia.rds"
  
  if (file.exists(paste0("c:/rdata/", FileName)))
  {
    Data_old = readRDS(paste0(RDataFolder, FileName))
  } else { Data_old = data.table()
  }
  Data_old = unique(rbind(Data_old, xAll, fill=T))
  saveRDS(Data_old, paste0(RDataFolder, FileName))
}

# ==================================================================================================
FINAL_DOWNLOAD_YAH_INS_BY_CODE = function(sCode, NbDaysBack=365, FillEmpty=F) {
  # ------------------------------------------------------------------------------------------------
  # sCode = "EURVND=X"; ForceCode="CUREURVND"
  # sCode = 'CL'; NbDaysBack = 100*365
  enddate   = as.numeric(as.POSIXct(Sys.Date()+1)); enddate
  startdate = as.numeric(as.POSIXct(Sys.Date()-NbDaysBack)); startdate
  # download.file(paste0("http://query1.finance.yahoo.com/v7/finance/download/", pCodesource, 
  #                      "?period1=", startdate, "&period2=", enddate, "&interval=1d&events=history"), "c:/temp/yah.csv")
  # dt_yah0 = copy(dt_yah)
  rm(dt_yah, x); GC_SILENT()
  dt_yah = data.table()
  x      = try(setDT(fread(paste0("http://query1.finance.yahoo.com/v7/finance/download/", sCode, 
                                  "?period1=", startdate, "&period2=", enddate, "&interval=1d&events=history"))))
  if (all(class(x)!='try-error'))
  {
    # dt_yah=dt_yah0
    dt_yah = CLEAN_COLNAMES(x)
    # colnames(dt_yah) = tolower(colnames(dt_yah))
    dt_yah = dt_yah[!is.na(date)]
    if ('adjclose' %in% names(dt_yah)) { setnames(dt_yah, 'adjclose', 'close_adj') }
    if ('open' %in% names(dt_yah)) { dt_yah[open==0, open:=as.numeric(NA)] }
    if ('close' %in% names(dt_yah)) { dt_yah[, close_unadj:=close] }
    if ('volume' %in% names(dt_yah)) { dt_yah[, volume:=as.numeric(volume)] }
    dt_yah[, date:=as.Date(date)]
    dt_yah = dt_yah[order(date)]
    
    dt_yah[, ':='(rt=(close_adj/shift(close_adj))-1)]
    dt_yah[, ':='(lnrt=log(close_adj/shift(close_adj)))]
    
    # str(dt_yah)
    dt_yah[, date:=as.Date(date)]
    dt_yah[, ":="(codesource=sCode, source="YAH")]
    xref   = ins_ref[yah==sCode]
    if (nrow(xref)==1) { pCode = xref$code } else { pCode = as.character(NA)}
    dt_yah[, code:=pCode]
    if (!is.na(pCode))
    {
      dt_yah[, ':='(iso2=xref$iso2, continent=xref$continent, country=xref$country, name=xref$short_name)]
    }
    
    My.Kable.TB(dt_yah[, -c('adjclose')])
  } else {
    if (FillEmpty)
    {
      dt_yah = data.table(date=as.Date(1900-01-01))
      dt_yah[, ":="(codesource=sCode, source="YAH")]
      dt_yah[, ':='(iso2=xref$iso2, continent=xref$continent, country=xref$country, name=xref$short_name)]
    }
  }
  return(dt_yah)
}

# ==================================================================================================
FINAL_DOWNLOAD_YAH_INS_BY_CODESOURCE = function(pCodesource='AAPL', NbDaysBack=20*365, ToSleep=2) {
  # ------------------------------------------------------------------------------------------------
  enddate   = as.numeric(as.POSIXct(Sys.Date()+1)); enddate
  startdate = as.numeric(as.POSIXct(Sys.Date()-NbDaysBack)); startdate
  # download.file(paste0("http://query1.finance.yahoo.com/v7/finance/download/", pCodesource, 
  #                      "?period1=", startdate, "&period2=", enddate, "&interval=1d&events=history"), "c:/temp/yah.csv")
  # dt_yah0 = copy(dt_yah)
  # rm(dt_yah)
  # GC_SILENT()
  pURL      = paste0("http://query1.finance.yahoo.com/v7/finance/download/", pCodesource, 
                     "?period1=", startdate, "&period2=", enddate, "&interval=1d&events=history")
  # CATln_Border(pURL)
  
  dt_yah    = setDT(fread(pURL, showProgress = F))
  if (nrow(dt_yah)>0)
  {
    # dt_yah=dt_yah0
    # colnames(dt_yah) = tolower(colnames(dt_yah))
    dt_yah = CLEAN_COLNAMES(dt_yah)
    # str(dt_yah)
    dt_yah[, date:=as.Date(date)]
    dt_yah[, 2:7]    = lapply(dt_yah[, 2:7], function(x) as.numeric(x))
    dt_yah[, ":="(codesource=pCodesource, source="YAH", code=as.character(NA))]
    # if (nchar(ForceCode)==0) { 
    #   dt_yah = transform(dt_yah, code=insref_yah$code[match(codesource, insref_yah$yah)])
    # } else {
    #   dt_yah[, code:=ForceCode]
    # }
    # str(dt_yah)
    dt_yah = dt_yah[!is.na(close)]
    
    if (nrow(dt_yah)>0)
    {
      if ("adjclose" %in% names(dt_yah))
      {
        dt_yah = dt_yah[order(date)]
        dt_yah[, rt:=(adjclose/shift(adjclose))-1, by=c("codesource")]
        dt_yah[, change:=(adjclose-shift(adjclose)), by=c("codesource")]
        dt_yah[, varpc:=100*rt, by=c("codesource")]
        dt_yah[, close_adj:=adjclose]
      }
      RELOAD_INSREF()
      dt_yah = merge(dt_yah[, -c('code', 'type', 'iso2', 'country', 'continent', 'name')], 
                     ins_ref[, .(type, code, country, iso2, continent, codesource=yah, name=short_name)], all.x=T, by='codesource')
      My.Kable.TB(dt_yah)
      # dt_yah[, codesource:=gsub("=", "-", codesource)]
      # My.Kable(dt_yah)
      # fcat = substr(dt_yah[1]$code,1,3)
      # dt_yah = MHM_DATACENTER_INTEGRATION_CODE_DATE_BYLIST(p_folder=UData,p_datatable=dt_yah,
      #                                                      p_method='all',p_codesource ="",
      #                                                      list_filefrom=list(),
      #                                                      p_fileto=paste0('download_yah_', fcat, '_history.rds'), ToTransform=T, 
      #                                                      ToAdjustWeekDay=T, HistorySTD = T)
    }
  }
  return(dt_yah)
}

#=================================================================================================
EXTRACT_FILES_LIST = function(Folder, PricesExt=".htm*")
  #-----------------------------------------------------------------------------
{
  CATln_Border(Folder)
  # FileName = paste0(ODDrive, 'BeQ/PEH/', PREFIX_SECTOR, '/', 'LISTFINAL_VST_', PREFIX_SECTOR)
  #folder = 'D:/OneDrive/Beq/PEH/BROKERAGE/'
  # CATln_Border(FileName)
  Files_List = as.data.table(list.files(Folder,PricesExt))
  Files_List[, ticker:=  sub(paste0(pRef='[.]',".*"), "", V1)]
  My.Kable(Files_List)
  return(Files_List)
}

# ==================================================================================================
GET_CURL_FETCH = function(pURL = "https://www.theice.com/products") {
  # ------------------------------------------------------------------------------------------------
  req <- curl::curl_fetch_memory(pURL)
  x = rawToChar(req$content); x
  writeLines(x, "c:/temp/GET_CURL_FETCH.r")
  return(x)
}

# ==================================================================================================
GET_CAF_SHARES = function(pURL='https://s.cafef.vn/hose/SSI-cong-ty-co-phan-chung-khoan-ssi.chn', StrSearch = "Vốn hóa thị trường") {
  # ------------------------------------------------------------------------------------------------
  # pURL       = pLink
  # content    = rvest::read_html(pURL)
  # tables     = content %>% html_table(fill = TRUE)
  # StrSearch = "Vốn hóa thị trường")
  pWeb       = GET_CURL_FETCH(pURL = pURL)
  # pWeb       = decodeVN(pWeb)
  xlist      = unlist(strsplit(pWeb, '<div class="l">'))
  # for (k in 2:length(xlist))
  # {
  #   catrep("-",100)
  #   CATln('');
  #   print(k);
  #   print(xlist[[k]])
  # }
  nr_found = grep(StrSearch, xlist)
  if (nr_found>0)
  {
    xfound = xlist[[nr_found]]
    value_shares = gsub('\r|\n|\"r\"', '', xfound)
    value_shares = trimws(str.extract(value_shares, '<div class=>', '</div>'))
    value_shares = as.numeric(gsub(",","", value_shares))
    CATln_Border(paste("SHARES = ", value_shares))
  } else {
    CATln_Border(paste("SHARES = ", 'NOT FOUND'))
    value_shares = as.numeric(NA)
  }
  return(value_shares)
}

# ==================================================================================================
GET_CAF_PRICES = function(pURL='https://s.cafef.vn/hose/SSI-cong-ty-co-phan-chung-khoan-ssi.chn', StrSearch = "Giá tham chiếu") {
  # ------------------------------------------------------------------------------------------------
  # pURL       = pLink
  # content    = rvest::read_html(pURL)
  # tables     = content %>% html_table(fill = TRUE)
  
  pWeb       = GET_CURL_FETCH(pURL = pURL)
  # pWeb       = decodeVN(pWeb)
  xlist      = unlist(strsplit(pWeb, '<div class="l">'))
  # for (k in 2:length(xlist))
  # {
  #   catrep("-",100)
  #   CATln('');
  #   print(k);
  #   print(xlist[[k]])
  # }
  nr_found = grep(StrSearch, xlist)
  if (nr_found>0)
  {
    xfound = xlist[[nr_found]]
    value = gsub('\r|\n|\"r\"', '', xfound)
    value = trimws(str.extract(value, '<div class=', '</div>'))
    # pLeft   = sub(paste0(pRef,".*"), "", pString)
    # pRight  = sub(paste0(".*", pRef), "", pString)
    value = sub(paste0(".*", ' '), "", value)
    value
    value = 1000*as.numeric(value)
    CATln_Border(paste("PRICE = ", value))
  } else {
    CATln_Border(paste("PRICE = ", 'NOT FOUND'))
    value = as.numeric(NA)
  }
  return(value)
}

# ==================================================================================================
GET_CAF_FIRSTDATE = function(pURL='https://s.cafef.vn/hose/SSI-cong-ty-co-phan-chung-khoan-ssi.chn') {
  # ------------------------------------------------------------------------------------------------
  # print(GET_CAF_FIRSTDATE(pURL='https://s.cafef.vn/hose/SSI-cong-ty-co-phan-chung-khoan-ssi.chn'))
  pWeb       = GET_CURL_FETCH(pURL = pURL)
  if (grepl( 'Ngày giao dịch đầu tiên', pWeb))
  {
    xWeb = trimws(str.extract(gsub('\r\n','',str.extract(pWeb, 'Ngày giao dịch đầu tiên', '</div>')), '<b>', '</b>'))
    xres = as.Date(xWeb, '%d/%m/%Y')
  } else {
    xres=as.Date(NA)
  }
  return(xres)
}

# ==================================================================================================
GET_CAF_NAME = function(pURL='https://s.cafef.vn/hose/SSI-cong-ty-co-phan-chung-khoan-ssi.chn') {
  # ------------------------------------------------------------------------------------------------
  # print(GET_CAF_NAME(pURL='https://s.cafef.vn/hose/SSI-cong-ty-co-phan-chung-khoan-ssi.chn'))
  pWeb       = GET_CURL_FETCH(pURL = pURL)
  if (grepl( '<div id="namebox" class="dlt-ten">', pWeb))
  {
    xWeb = trimws(gsub('<h1>&nbsp;', '', gsub('\r\n','',str.extract(pWeb, '<div id="namebox" class="dlt-ten">', '</div>'))))
    xres = xWeb
  } else {
    xres=as.character(NA)
  }
  return(xres)
}

# ==================================================================================================
GET_CAF_SAME_SECTOR = function(Ticker='AAT', FilePath = 'BROKERAGE/LIST_BROKERAGE', SinglePath='BROKERAGE/BROKERAGE_REFERENCE') {
  # ------------------------------------------------------------------------------------------------
  # Get list of stock in the same sector
  # "REALESTATE/LIST_REALESTATE.rds"
  # SinglePath='REALESTATE/REALESTATE_REFERENCE'
  pURL       = paste0('https://s.cafef.vn/Ajax/CungNganh/SameCategory.aspx?symbol=', Ticker, '&PageIndex=1&PageSize=100')
  content    = rvest::read_html(pURL)
  tables     = content %>% html_table(fill = TRUE)
  My_List    = as.data.table(tables[[1]])
  My.Kable.All(My_List)
  
  links    = content %>% html_nodes(xpath='//a') %>% html_attr('href')
  tb_links = as.data.table(links)
  tb_links[, id:=seq.int(1, .N)]
  My_List[, id:=seq.int(1, .N)]
  My.Kable.All(tb_links)
  
  if (nrow(My_List)==nrow(tb_links))
  {
    My_List = merge(My_List, tb_links, all.x=T, by='id')
    
    My_List = CLEAN_COLNAMES(My_List)
    colnames(My_List) = decodeVN(colnames(My_List), from='Unicode', to='Unicode', diacritics=F)
    My_List[, ticker:=mack]
    My_List[, market:= My_List$san]
    My.Kable.All(My_List)
    
    SinglePath = paste0(ODDrive, 'BEQ/PEH/', SinglePath, '.xlsx')
    SingleList = setDT(read.xlsx(SinglePath))
    SingleList = CLEAN_COLNAMES(SingleList)
    colnames(SingleList) = decodeVN(colnames(SingleList), from='Unicode', to='Unicode', diacritics=F)
    My.Kable.All(SingleList)
    AllList = unique(rbind(My_List, SingleList, fill=T), by='ticker')
    
    AllList[is.na(mack), mack:=ticker]
    My.Kable.All(AllList)
    # view(AllList)
    #AllList = AllList[,.(id, ticker, market,  )]
    write.xlsx(AllList, paste0(ODDrive, 'BEQ/PEH/', FilePath, '.xlsx'))
    saveRDS(AllList, paste0(ODDrive, 'BEQ/PEH/', FilePath, '.rds'))
    
  } else {
    # My_List = 
  }
  return(My_List)
}

# ==================================================================================================
UPDATE_SHARES_IN_TABLE = function(FilePath = 'BROKERAGE/LIST_BROKERAGE.rds') {
  # ------------------------------------------------------------------------------------------------
  FileFull = paste0(ODDrive, 'BEQ/PEH/', FilePath)
  CATln_Border(FileFull)
  file.exists(FileFull)
  My_List = readRDS(FileFull)
  My.Kable.All(My_List)
  My_List$shares = as.numeric(NA)
  
  for (k in 1:nrow(My_List))
  {
    # k = 1
    pLink = My_List[k]$links    
    x.shares = GET_CAF_SHARES(pURL=pLink, StrSearch = "KLCP đang lưu hành")
    CATln_Border(paste(My_List[k]$mãck, x.shares))
    My_List[k]$shares = x.shares
  }
  saveRDS(My_List, FileFull)
  My.Kable.All(My_List)
  return(My_List)
}

# ==================================================================================================
UPDATE_PRICES_IN_TABLE = function(FilePath = 'BROKERAGE/LIST_HEALTHCARE.rds') {
  # ------------------------------------------------------------------------------------------------
  FileFull = paste0(ODDrive, 'BEQ/PEH/', FilePath)
  CATln_Border(FileFull)
  file.exists(FileFull)
  My_List = readRDS(FileFull)
  My.Kable.All(My_List)
  My_List$close = as.numeric(NA)
  
  for (k in 1:nrow(My_List))
  {
    # k = 1
    pLink = My_List[k]$links    
    x.value = GET_CAF_PRICES(pURL=pLink, StrSearch = "Giá tham chiếu")
    CATln_Border(paste(My_List[k]$mãck, x.value))
    My_List[k]$close = x.value
  }
  saveRDS(My_List, FileFull)
  My.Kable.All(My_List)
  return(My_List)
}

# ==================================================================================================
UPDATE_FIRSTDATE_IN_TABLE = function(FilePath = 'BROKERAGE/LIST_BROKERAGE.rds') {
  # ------------------------------------------------------------------------------------------------
  FileFull = paste0(ODDrive, 'BEQ/PEH/', FilePath)
  CATln_Border(FileFull)
  file.exists(FileFull)
  My_List = readRDS(FileFull)
  My.Kable.All(My_List)
  My_List$firstdate = NULL
  My_List$firstdate = as.Date(NA)
  str(My_List)
  
  for (k in 1:nrow(My_List))
  {
    # k = 1
    pLink = My_List[k]$links    
    x.value = GET_CAF_FIRSTDATE(pURL=pLink)
    CATln_Border(paste(My_List[k]$mãck, x.value))
    My_List[k]$firstdate = x.value
  }
  saveRDS(My_List, FileFull)
  My.Kable.All(My_List)
  return(My_List)
}

# ==================================================================================================
UPDATE_NAME_IN_TABLE = function(FilePath = 'BROKERAGE/LIST_BROKERAGE.rds') {
  # ------------------------------------------------------------------------------------------------
  # FilePath = 'REALESTATE/LIST_REALESTATE.rds'
  FileFull = paste0(ODDrive, 'BEQ/PEH/', FilePath)
  CATln_Border(FileFull)
  file.exists(FileFull)
  My_List = readRDS(FileFull)
  My.Kable.All(My_List)
  #View(My_List)
  My_List$name = NULL
  My_List$short_name = NULL
  My_List$name = as.character(NA)
  My_List$short_name = as.character(NA)
  str(My_List)
  
  for (k in 1:nrow(My_List))
  {
    # k = 1
    pLink = My_List[k]$links    
    x.value = GET_CAF_NAME(pURL=pLink)
    CATln_Border(paste(My_List[k]$ticker, x.value))
    My_List[k]$name = x.value
    # str(x.value)
  }
  str(My_List)
  saveRDS(My_List, FileFull)
  My.Kable.All(My_List)
  return(My_List)
}

# ==================================================================================================
UPDATE_OWNERSHIP_IN_TABLE = function(FilePath = 'BROKERAGE/LIST_BROKERAGE.rds') {
  # ------------------------------------------------------------------------------------------------
  FileFull = paste0(ODDrive, 'BEQ/PEH/', FilePath)
  CATln_Border(FileFull)
  file.exists(FileFull)
  My_List = readRDS(FileFull)
  My.Kable.All(My_List)
  My_List$name = as.character(NA)
  My_List$own_state   = as.numeric(NA)
  My_List$own_foreign = as.numeric(NA)
  My_List$own_other   = as.numeric(NA)
  
  for (k in 1:nrow(My_List))
  {
    # k = 1
    pLink = My_List[k]$links  
    pTicker = My_List[k]$mack   
    CATln_Border(paste(k, pTicker))
    x.value = GET_STB_OWNERSHIP(pCode = pTicker)
    
    if (length(x.value)==2)
    {
      x.state   = as.numeric(gsub(',','.', gsub('[%]','', x.value[[1]][X1=='Sở hữu nhà nước']$X2)))
      x.foreign = as.numeric(gsub(',','.', gsub('[%]','', x.value[[1]][X1=='Sở hữu nước ngoài']$X2)))
      x.other   = as.numeric(gsub(',','.', gsub('[%]','', x.value[[1]][X1=='Sở hữu khác']$X2)))
      My_List[k]$own_state = x.state
      My_List[k]$own_foreign = x.foreign
      My_List[k]$own_other = x.other
    }
    
  }
  saveRDS(My_List, FileFull)
  My.Kable.All(My_List)
  return(My_List)
}

# ==================================================================================================
GET_STB_OWNERSHIP = function(pCode = 'PSI') {
  # ------------------------------------------------------------------------------------------------
  # x = GET_STB_OWNERSHIP(pCode = 'PSI')
  
  pURL  = paste0('https://www.stockbiz.vn/Stocks/', pCode, '/MajorHolders.aspx')
  
  content    = rvest::read_html(pURL)
  tables     = content %>% html_table(fill = TRUE)
  
  Ownership_overview = as.data.table(tables[[12]])
  My.Kable.All(Ownership_overview)
  
  Ownership_detail = as.data.table(tables[[13]])
  colnames(Ownership_detail) = c('name', 'position', 'shares', 'percentage', 'updated_data')
  Ownership_detail[, shares      :=as.numeric(gsub('[.]','', shares))]
  Ownership_detail[, percentage  :=as.numeric(gsub(",",".", gsub('[%]','', percentage )))]
  Ownership_detail[, updated_data     :=as.Date(updated_data, '%d/%m/%Y')]
  Ownership_detail[, ticker:=pCode]
  Ownership_detail[, updated:=Sys.Date()]
  My.Kable.All(Ownership_detail)
  return(list(Ownership_overview, Ownership_detail))
}

#================================================================================
GET_VST_FIRSTTRADE = function(pTicker = 'NVL')
  #-----------------------------------------------
{
  # pTicker = pTICKER
  x.firsttrade = as.Date(NA)
  pURL       = paste0('https://finance.vietstock.vn/', pTicker, '/ho-so-doanh-nghiep.htm?languageid=2')
  
  # pURL       = paste0('https://finance.vietstock.vn/NVL/ownership-structure.htm?languageid=2')
  CATln_Border(pURL)
  # pURL       = 'https://finance.vietstock.vn/L14/transaction-statistics.htm'
  content    = rvest::read_html(pURL)
  tables     = content %>% html_table(fill = TRUE)
  
  
  
  if (length(tables)>=5) { 
    x.info    = SELECT_TABLE_WITH_FIELD(tables=tables, pField='First trading date', pContent="", like=T)
    # x.info       = as.data.table(tables[[5]])
    x.firsttrade = x.info[X1=='First trading date']$X2
    x.firsttrade = as.Date(x.firsttrade, '%m/%d/%Y')
    CATln_Border(paste(pTicker, x.firsttrade))
  }
  
  return(x.firsttrade)
}

#===================================================================================================
CALCULATE_PERFORMANCE_VST_XLS_IN_FOLDER = function(File_folder = paste0(ODDrive,'BeQ/PEH/BROKERAGE/prices/'), FileFull= paste0(ODDrive,'BeQ/PEH/BROKERAGE/PERFORMANCE.rds'))
  #--------------------------------------------------------------------------------------------------
{
  
  List_files = list.files(File_folder, '.xls')
  Data_all = list()
  
  for (i in 1:length(List_files))
  {
    # i = 1
    My_file = List_files[[i]]
    # i = 1; My_file = 'VND.htm'
    # i = 1; My_file = 'PSI.htm'
    pURL       = paste0(File_folder, My_file)
    ticker     = tolower(gsub('.xls', '', My_file))
    pCode      = toupper(ticker)
    CATln_Border(paste(i, ticker, pURL))
    # library("readxl")
    # xls files
    # my_data <- read_excel(pURL)
    my_data = readHTMLTable(pURL)
    price_data = setDT(my_data[[2]])
    My.Kable(price_data)
    # ncol(price_data)
    # str(price_data)
    my_cols = c('date',	'listed_shares',	'shares_outstanding',	'reference',	'ceiling',	'floor',	'total_share_volume',	'total_value','market_capitalization',	'open',	'Close',	'High',	'Low',	'Change',	'Average',	'Adjusted_Price',	'Change',	'percentage_change',	'Average_Buy',	'average_sell',	'remain_bid',	'remain_ask',		
                'order_Vol',	'order_Val',	'order_Buy',	'order_Sell',	'order_Buy_Sell',	'volumn_Buy',	'volumn_Sell',	'volumn_Buy_Sell',	'po_Vol',	'po_Val' )
    my_cols = tolower(my_cols)
    length(my_cols)
    colnames(price_data) = my_cols
    # str(price_data)
    price_data[, ticker:=toupper(ticker)]
    Data_all[[i]] = price_data 
  }
  Data_Prices = rbindlist(Data_all)
  str(Data_Prices)
  
  Data_Prices[, shares_outstanding  := as.numeric(gsub(',', '', Data_Prices$shares_outstanding))]
  Data_Prices[, close               := as.numeric(gsub(',', '', Data_Prices$close))]
  Data_Prices[, reference           := as.numeric(gsub(',', '', Data_Prices$reference))]
  Data_Prices[, adjusted_price      := as.numeric(gsub(',', '', Data_Prices$adjusted_price))]
  Data_Prices[, capivnd             := shares_outstanding*reference/1000000000]
  Data_Prices[, ticker              := toupper(ticker)]
  Data_Prices[, date                := as.Date(date, "%m/%d/%Y")]
  
  Data_Prices = Data_Prices[order(ticker, date)]
  Data_Prices[, rtd:=(adjusted_price/shift(adjusted_price))-1, by='ticker']
  Data_Prices[, lnrt:=log(adjusted_price/shift(adjusted_price)), by='ticker']
  Data_Prices[, date_end:=max(date), by='ticker']
  Data_Prices$M1 = NULL
  Data_Prices$M3 = NULL
  Data_Prices$M6 = NULL
  Data_Prices$Y1 = NULL
  
  Data_Prices[date>=date_end-30,  M1:=1, by='ticker']
  Data_Prices[date>=date_end-91,  M3:=1, by='ticker']
  Data_Prices[date>=date_end-182, M6:=1, by='ticker']
  
  My.Kable(Data_Prices[, .(ticker, date, close, adjusted_price, rtd, lnrt)])
  My.Kable(Data_Prices[, .(ticker, date, close, adjusted_price, rtd, lnrt)][ticker=='SSI'])
  My.Kable(Data_Prices[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6)][ticker=='SSI'])
  My.Kable(Data_Prices[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6)][ticker=='AGR'])
  
  Data_Prices_MX = Data_Prices[M1==1]
  My.Kable(Data_Prices_MX[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6)][ticker=='SSI'])
  Data_Prices_MXRT = Data_Prices_MX[, .(period='M1', date=date[1], close1=adjusted_price[1], closeN=adjusted_price[.N], 
                                        rt=(adjusted_price[.N]/adjusted_price[1])-1, N1=.N), by='ticker']
  Data_Prices_M1RT = Data_Prices_MXRT
  My.Kable.All(Data_Prices_M1RT)
  
  Data_Prices_MX = Data_Prices[M3==1]
  My.Kable(Data_Prices_MX[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6)][ticker=='SSI'])
  # My.Kable(Data_Prices_MX[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6)][ticker=='AGR'])
  Data_Prices_MXRT = Data_Prices_MX[, .(period='M3', date=date[1], close1=adjusted_price[1], closeN=adjusted_price[.N], 
                                        rt=(adjusted_price[.N]/adjusted_price[1])-1, N3=.N), by='ticker']
  Data_Prices_M3RT = Data_Prices_MXRT
  My.Kable.All(Data_Prices_M3RT)
  
  Data_Prices_MX = Data_Prices[M6==1]
  My.Kable(Data_Prices_MX[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1, M3, M6)][ticker=='SSI'])
  Data_Prices_MXRT = Data_Prices_MX[, .(period='M6', date=date[1], close1=adjusted_price[1], closeN=adjusted_price[.N], 
                                        rt=(adjusted_price[.N]/adjusted_price[1])-1, N6=.N), by='ticker']
  Data_Prices_M6RT = Data_Prices_MXRT
  My.Kable.All(Data_Prices_M6RT)
  
  Data_Prices_MX = Data_Prices
  My.Kable(Data_Prices_MX[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6)][ticker=='SSI'])
  My.Kable(Data_Prices_MX[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6)][ticker=='AGR'])
  Data_Prices_MXRT = Data_Prices_MX[, .(period='Y1', date=date[1], close1=adjusted_price[1], closeN=adjusted_price[.N], 
                                        rt=(adjusted_price[.N]/adjusted_price[1])-1, NY1=.N), by='ticker']
  Data_Prices_Y1RT = Data_Prices_MXRT
  My.Kable.All(Data_Prices_Y1RT)
  
  X1  = merge(Data_Prices_M1RT[, .(ticker, M1=rt, N1)], Data_Prices_M3RT[, .(ticker, M3=rt, N3)], by='ticker')
  X2  = merge(Data_Prices_M6RT[, .(ticker, M6=rt, N6)], Data_Prices_Y1RT[, .(ticker, Y1=rt, NY1)], by='ticker')
  X12 = merge(X1, X2, by='ticker')
  X12 = merge(X12, unique(Data_Prices, by='ticker')[, .(ticker, date_end)], by='ticker')
  My.Kable.All(X12)  
  
  saveRDS(X12, FileFull)
  return(X12)
  # write.xlsx(X12, 'D:/ONEDRIVE/BeQ/PEH/BROKERAGE/PERFORMANCE_1205.XLSX')
}

#==========================================================================================

CALCULATE_VOLATILITY_VST_XLS_IN_FOLDER = function(File_folder = 'D:/ONEDRIVE/BeQ/PEH/BROKERAGE/prices/', FileFull= paste0(ODDrive,'BeQ/PEH/BROKERAGE/VOLATILITY.rds'))
  #----------------------------------------------------------------------------------------
{
  List_files = list.files(File_folder, '.xls')
  Data_all = list()
  
  for (i in 1:length(List_files))
  {
    # i = 1
    My_file = List_files[[i]]
    # i = 1; My_file = 'VND.htm'
    # i = 1; My_file = 'PSI.htm'
    pURL       = paste0(File_folder, My_file)
    ticker     = tolower(gsub('.xls', '', My_file))
    pCode      = toupper(ticker)
    CATln_Border(paste(ticker, pURL))
    # library("readxl")
    # xls files
    # my_data <- read_excel(pURL)
    my_data = readHTMLTable(pURL)
    price_data = setDT(my_data[[2]])
    My.Kable(price_data)
    # ncol(price_data)
    # str(price_data)
    my_cols = c('date',	'listed_shares',	'shares_outstanding',	'reference',	'ceiling',	'floor',	'total_share_volume',	'total_value','market_capitalization',	'open',	'Close',	'High',	'Low',	'Change',	'Average',	'Adjusted_Price',	'Change',	'percentage_change',	'Average_Buy',	'average_sell',	'remain_bid',	'remain_ask',		
                'order_Vol',	'order_Val',	'order_Buy',	'order_Sell',	'order_Buy_Sell',	'volumn_Buy',	'volumn_Sell',	'volumn_Buy_Sell',	'po_Vol',	'po_Val' )
    my_cols = tolower(my_cols)
    length(my_cols)
    colnames(price_data) = my_cols
    # str(price_data)
    price_data[, ticker:=toupper(ticker)]
    Data_all[[i]] = price_data 
  }
  Data_Prices = rbindlist(Data_all)
  str(Data_Prices)
  
  Data_Prices[, shares_outstanding  := as.numeric(gsub(',', '', Data_Prices$shares_outstanding))]
  Data_Prices[, close               := as.numeric(gsub(',', '', Data_Prices$close))]
  Data_Prices[, reference           := as.numeric(gsub(',', '', Data_Prices$reference))]
  Data_Prices[, adjusted_price      := as.numeric(gsub(',', '', Data_Prices$adjusted_price))]
  Data_Prices[, capivnd             := shares_outstanding*reference/1000000000]
  Data_Prices[, ticker              := toupper(ticker)]
  Data_Prices[, date                := as.Date(date, "%m/%d/%Y")]
  
  Data_Prices = Data_Prices[order(ticker, date)]
  Data_Prices[, rtd:=(adjusted_price/shift(adjusted_price))-1, by='ticker']
  Data_Prices[, lnrt:=log(adjusted_price/shift(adjusted_price)), by='ticker']
  Data_Prices[, date_end:=max(date), by='ticker']
  Data_Prices$M1 = NULL
  Data_Prices$M3 = NULL
  Data_Prices$M6 = NULL
  Data_Prices$Y1 = NULL
  
  Data_Prices[date>=date_end-30,  M1:=1, by='ticker']
  Data_Prices[date>=date_end-91,  M3:=1, by='ticker']
  Data_Prices[date>=date_end-182, M6:=1, by='ticker']
  
  My.Kable(Data_Prices[, .(ticker, date, close, adjusted_price, rtd, lnrt)])
  My.Kable(Data_Prices[, .(ticker, date, close, adjusted_price, rtd, lnrt)][ticker=='SSI'])
  My.Kable(Data_Prices[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6)][ticker=='SSI'])
  
  Data_Prices_MX = Data_Prices[M1==1]
  My.Kable(Data_Prices_MX[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6)][ticker=='SSI'])
  Data_Prices_MXRT = Data_Prices_MX[, .(period='M1', date=date[1], close1=adjusted_price[1], closeN=adjusted_price[.N], 
                                        st=sqrt(250)*sqrt(var(lnrt, na.rm = T)), N1=.N), by='ticker']
  Data_Prices_M1RT = Data_Prices_MXRT
  My.Kable.All(Data_Prices_M1RT)
  
  Data_Prices_MX = Data_Prices[M3==1]
  My.Kable(Data_Prices_MX[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6)][ticker=='SSI'])
  Data_Prices_MXRT = Data_Prices_MX[, .(period='M3', date=date[1], close1=adjusted_price[1], closeN=adjusted_price[.N], 
                                        st=sqrt(250)*sqrt(var(lnrt, na.rm = T)), N3=.N), by='ticker']
  Data_Prices_M3RT = Data_Prices_MXRT
  My.Kable.All(Data_Prices_M3RT)
  
  Data_Prices_MX = Data_Prices[M6==1]
  My.Kable(Data_Prices_MX[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1, M3, M6)][ticker=='SSI'])
  Data_Prices_MXRT = Data_Prices_MX[, .(period='M6', date=date[1], close1=adjusted_price[1], closeN=adjusted_price[.N], 
                                        st=sqrt(250)*sqrt(var(lnrt, na.rm = T)), N6=.N), by='ticker']
  Data_Prices_M6RT = Data_Prices_MXRT
  My.Kable.All(Data_Prices_M6RT)
  
  Data_Prices_MX = Data_Prices
  My.Kable(Data_Prices_MX[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6)][ticker=='SSI'])
  Data_Prices_MXRT = Data_Prices_MX[, .(period='Y1', date=date[1], close1=adjusted_price[1], closeN=adjusted_price[.N], 
                                        st=sqrt(250)*sqrt(var(lnrt, na.rm = T)), NY1=.N), by='ticker']
  Data_Prices_Y1RT = Data_Prices_MXRT
  My.Kable.All(Data_Prices_Y1RT)
  
  X1  = merge(Data_Prices_M1RT[, .(ticker, M1=st, N1)], Data_Prices_M3RT[, .(ticker, M3=st, N3)], by='ticker')
  X2  = merge(Data_Prices_M6RT[, .(ticker, M6=st, N6)], Data_Prices_Y1RT[, .(ticker, Y1=st, NY1)], by='ticker')
  X12 = merge(X1, X2, by='ticker')
  X12 = merge(X12, unique(Data_Prices, by='ticker')[, .(ticker, date_end)], by='ticker')
  My.Kable.All(X12)  
  
  saveRDS(X12, FileFull)
  return(X12)
  
}

#=======================================================================================
CALCULATE_LIQUIDITY_VST_XLS_IN_FOLDER = function(File_folder = 'D:/ONEDRIVE/BeQ/PEH/BROKERAGE/prices/', FileFull= paste0(ODDrive,'BeQ/PEH/BROKERAGE/LIQUIDITY.rds'))
  #-------------------------------------------------------------------------------------
{
  List_files = list.files(File_folder, '.xls')
  Data_all = list()
  
  for (i in 1:length(List_files))
  {
    # i = 1
    My_file = List_files[[i]]
    # i = 1; My_file = 'VND.htm'
    # i = 1; My_file = 'PSI.htm'
    pURL       = paste0(File_folder, My_file)
    ticker     = tolower(gsub('.xls', '', My_file))
    pCode      = toupper(ticker)
    CATln_Border(paste(ticker, pURL))
    # library("readxl")
    # xls files
    # my_data <- read_excel(pURL)
    my_data = readHTMLTable(pURL)
    price_data = setDT(my_data[[2]])
    My.Kable(price_data)
    # ncol(price_data)
    # str(price_data)
    my_cols = c('date',	'listed_shares',	'shares_outstanding',	'reference',	'ceiling',	'floor',	'total_share_volume',	'total_value','market_capitalization',	'open',	'Close',	'High',	'Low',	'Change',	'Average',	'Adjusted_Price',	'Change',	'percentage_change',	'Average_Buy',	'average_sell',	'remain_bid',	'remain_ask',		
                'order_Vol',	'order_Val',	'order_Buy',	'order_Sell',	'order_Buy_Sell',	'volumn_Buy',	'volumn_Sell',	'volumn_Buy_Sell',	'po_Vol',	'po_Val' )
    my_cols = tolower(my_cols)
    length(my_cols)
    colnames(price_data) = my_cols
    # str(price_data)
    price_data[, ticker:=toupper(ticker)]
    Data_all[[i]] = price_data 
  }
  Data_Prices = rbindlist(Data_all)
  str(Data_Prices)
  
  Data_Prices[, total_share_volume  := as.numeric(gsub(',', '', Data_Prices$total_share_volume))]
  Data_Prices[, shares_outstanding  := as.numeric(gsub(',', '', Data_Prices$shares_outstanding))]
  Data_Prices[, close               := as.numeric(gsub(',', '', Data_Prices$close))]
  Data_Prices[, reference           := as.numeric(gsub(',', '', Data_Prices$reference))]
  Data_Prices[, adjusted_price      := as.numeric(gsub(',', '', Data_Prices$adjusted_price))]
  Data_Prices[, capivnd             := shares_outstanding*reference/1000000000]
  Data_Prices[, ticker              := toupper(ticker)]
  Data_Prices[, date                := as.Date(date, "%m/%d/%Y")]
  
  Data_Prices = Data_Prices[order(ticker, date)]
  Data_Prices[, rtd:=(adjusted_price/shift(adjusted_price))-1, by='ticker']
  Data_Prices[, lnrt:=log(adjusted_price/shift(adjusted_price)), by='ticker']
  Data_Prices[, date_end:=max(date), by='ticker']
  Data_Prices$M1 = NULL
  Data_Prices$M3 = NULL
  Data_Prices$M6 = NULL
  Data_Prices$Y1 = NULL
  
  Data_Prices[date>=date_end-30,  M1:=1, by='ticker']
  Data_Prices[date>=date_end-91,  M3:=1, by='ticker']
  Data_Prices[date>=date_end-182, M6:=1, by='ticker']
  
  My.Kable(Data_Prices[, .(ticker, date, close, adjusted_price, rtd, lnrt)])
  My.Kable(Data_Prices[, .(ticker, date, close, adjusted_price, rtd, lnrt)][ticker=='SSI'])
  My.Kable(Data_Prices[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6)][ticker=='SSI'])
  
  Data_Prices_MX = Data_Prices[M1==1 & !is.na(shares_outstanding) & !is.na(total_share_volume)]
  My.Kable(Data_Prices_MX[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6, total_share_volume, shares_outstanding)][ticker=='SSI'])
  Data_Prices_MXRT = Data_Prices_MX[, .(period='M1', date=date[1], close1=adjusted_price[1], closeN=adjusted_price[.N], 
                                        lt=250*100*mean(total_share_volume/shares_outstanding, na.rm = T), N1=.N), by='ticker']
  Data_Prices_M1RT = Data_Prices_MXRT
  My.Kable.All(Data_Prices_M1RT)
  
  Data_Prices_MX = Data_Prices[M3==1 & !is.na(shares_outstanding) & !is.na(total_share_volume)]
  My.Kable(Data_Prices_MX[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6)][ticker=='SSI'])
  Data_Prices_MXRT = Data_Prices_MX[, .(period='M3', date=date[1], close1=adjusted_price[1], closeN=adjusted_price[.N], 
                                        lt=250*100*mean(total_share_volume/shares_outstanding, na.rm = T), N3=.N), by='ticker']
  Data_Prices_M3RT = Data_Prices_MXRT
  My.Kable.All(Data_Prices_M3RT)
  
  Data_Prices_MX = Data_Prices[M6==1 & !is.na(shares_outstanding) & !is.na(total_share_volume)]
  My.Kable(Data_Prices_MX[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1, M3, M6)][ticker=='SSI'])
  Data_Prices_MXRT = Data_Prices_MX[, .(period='M6', date=date[1], close1=adjusted_price[1], closeN=adjusted_price[.N], 
                                        lt=250*100*mean(total_share_volume/shares_outstanding, na.rm = T), N6=.N), by='ticker']
  Data_Prices_M6RT = Data_Prices_MXRT
  My.Kable.All(Data_Prices_M6RT)
  
  Data_Prices_MX = Data_Prices[!is.na(shares_outstanding) & !is.na(total_share_volume)]
  My.Kable(Data_Prices_MX[, .(ticker, date_end, date, close, adjusted_price, rtd, lnrt, M1,M3, M6)][ticker=='SSI'])
  Data_Prices_MXRT = Data_Prices_MX[, .(period='Y1', date=date[1], close1=adjusted_price[1], closeN=adjusted_price[.N], 
                                        lt=250*100*mean(total_share_volume/shares_outstanding, na.rm = T), NY1=.N), by='ticker']
  Data_Prices_Y1RT = Data_Prices_MXRT
  My.Kable.All(Data_Prices_Y1RT)
  
  X1  = merge(Data_Prices_M1RT[, .(ticker, M1=lt, N1)], Data_Prices_M3RT[, .(ticker, M3=lt, N3)], by='ticker')
  X2  = merge(Data_Prices_M6RT[, .(ticker, M6=lt, N6)], Data_Prices_Y1RT[, .(ticker, Y1=lt, NY1)], by='ticker')
  X12 = merge(X1, X2, by='ticker')
  X12 = merge(X12, unique(Data_Prices, by='ticker')[, .(ticker, date_end)], by='ticker')
  My.Kable.All(X12)  
  
  saveRDS(X12, FileFull)
  return(X12)
  
}

#==================================================================================
GET_STB_OWNERSHIP = function(pCode = 'PSI')
  #--------------------------------------
{
  pURL  = paste0('https://www.stockbiz.vn/Stocks/', pCode, '/MajorHolders.aspx')
  
  content    = rvest::read_html(pURL)
  tables     = content %>% html_table(fill = TRUE)
  
  Ownership_overview = as.data.table(tables[[12]])
  Ownership_overview[X1 == 'Sở hữu nhà nước', ownership:='state']
  Ownership_overview[X1 == 'Sở hữu nước ngoài', ownership:='foreign']
  Ownership_overview[X1 == 'Sở hữu khác', ownership:='other']
  Ownership_overview[, percentage:=as.numeric(gsub(',','.', gsub('%','',X2)))]
  Ownership_overview[, ":="(source='STB', ticker=pCode, updated=Sys.Date())]
  My.Kable.All(Ownership_overview)
  
  Ownership_detail = as.data.table(tables[[13]])
  colnames(Ownership_detail) = c('name', 'position', 'shares', 'percentage', 'updated')
  Ownership_detail[, shares      :=as.numeric(gsub('[.]','', shares))]
  Ownership_detail[, percentage  :=as.numeric(gsub(",",".", gsub('[%]','', percentage )))]
  Ownership_detail[, updated     :=as.Date(updated, '%d/%m/%Y')]
  Ownership_detail[, ticker:=pCode]
  Ownership_detail[, source:='STB']
  Ownership_detail[, name_en:=vietnameseConverter::decodeVN(name, from='Unicode', to='Unicode', diacritics=F)]
  My.Kable.All(Ownership_detail)
  return(list(Ownership_overview, Ownership_detail))
}

CONVERT_STR_TO_NUMBER = function(l.str) {
  # ------------------------------------------------------------------------------------------------
  # l.str = "6,800,000.23"
  l_str = l.str
  l_str = gsub(",","", l_str)
  return(as.numeric(l_str))
}

# ==================================================================================================
SYSDATETIME = function(l.hour, WeekDateTime=T) {
  # ------------------------------------------------------------------------------------------------
  # print(SYSDATETIME(15))
  # l.hour = 16
  if (hour(Sys.time())>=l.hour) {l.res=Sys.Date()} else {l.res = Sys.Date()-1}
  if (WeekDateTime)
  {
    while ( weekdays(l.res) %in% c("Saturday", "Sunday")) {
      l.res = l.res - 1
    }
  }
  return(l.res)
}

DATA_OLD = function(FileNameFull)
{
  # FileNameFull = paste0(ODDrive, 'BeQ/PEH/DATA/', 'CCPR_CAC_STKVN_PRICES_HISTORY.rds')
  if (file.exists(FileNameFull))
  {
    Data_Old = readRDS(FileNameFull)
  } else { Data_Old = data.table() }
  return(Data_Old )
}

#===============================================================================
GET_VST_PRICES = function(code)
{#------------------------------------------------------------------
  # code = "OIL"
  
  URL = paste0('https://finance.vietstock.vn/data/ExportTradingResult?Code=',code,'&OrderBy=&OrderDirection=desc&PageIndex=1&PageSize=10&FromDate=2013-05-08&ToDate=',Sys.Date(),'&ExportType=excel&Cols=KLNY%2CKLCPDLH%2CGTC%2CT%2CS%2CTKLGD%2CTGTGD%2CVHTT%2CMC%2CTGG%2CLDM%2CDC%2CTGPTG%2CLDB%2CCN%2CBQM%2CLDMB%2CTN%2CBQB%2CKLDM%2CGYG%2CDM%2CKLDB%2CBQ%2CDB%2CKLDMB%2CGDC%2CKLGDKL%2C%2C%2CGTGDKL%2CKLGDTT%2CGTGDTT&ExchangeID=1&languageid=2')
  req <- read_html(URL)
  body_nodes <- req %>% 
    html_node("body") %>% 
    html_children()
  
  chart <- body_nodes %>%
    # rvest::html_nodes("body") %>%
    xml2::xml_find_all("//table")
  xdt = as.data.table(html_table(chart[[2]], header=T, fill = T))
  xdt <- xdt[-c(1),]
  # view(xdt)
  # my_header = fread(paste0(my_folder, "vietstock_colnames.txt"))
  # colnames(xdt) = my_header$code
  colnames(xdt) = c('Date'	,'Listed shares',	'Shares outstanding',	'Reference',	'Ceiling',	'Floor'	,'Total Share Volume',	'Total Value',	'Market Capitalization'	,'Open'	,'Close',	'High',	'Low',	'Change',	'Average',	'Adjusted Price',	'Adjusted Change',	'% Change',	'Average Buy',	'Average Sell',	'Remain Bid',	'Remain Ask',	
                    'Order matching Vol.',	'Order matching Val.',	'Total Order Buy',	'Total Order Sell',	'Total Order Buy - Sell'	,'Total Volume Buy',	'Total Volume Sell',	'Total Volume Buy - Sell',	'Put-through Vol.','	Put-through Val.')
  colnames(xdt) = tolower(colnames(xdt))
  xdt = CLEAN_COLNAMES(xdt)
  xdt$date = as.Date(xdt$date, "%m/%d/%Y")
  # xdt[order(-date)]
  # xdt = xdt %>% mutate_if(is.character, as.numeric)
  convert_numeric = function(x)
  {
    return(as.numeric(gsub(",", "", x)))
  }
  # str(xdt)
  xdt = xdt %>% mutate_if(is.character, convert_numeric)
  xdt[, turnover:= 1000000*ordermatchingval_]
  xdt[, totalvalueconverted:= 1000000*totalvalue]
  xdt[, marketcapi := 1000000*marketcapitalization]
  xdt[, ptvalue := 1000000*put_throughval_]
  xdt[, ticker:= code]
  # My.Kable.All(xdt)
  return(xdt)
}

#===============================================================================
LIST_STK_YAH_COUNTRY_DOWNLOAD = function(pOption='PRICES', pISO3='SAU', FileList='LIST_YAH_STK_COUNTRY.xlsx', 
                                         Nb_Group = 10, NbMin=21, ToDo, ToSleep=2, ToSort='ASC', Active=0) {
  #---------------------------------------------------------------
  # pOption='BOARD'; pISO3='IRL'; FileList=''; Nb_Group = 50; NbMin=1100; ToDo='COMPLETE'; ToSleep=2; ToSort='ASC'
  # pOption='PRICES'; pISO3='SAU'; FileList=''; Nb_Group = 10; NbMin=21; ToDo='COMPLETE'; ToSleep=2; ToSort='ASC'
  # NbMin = 1000
  ToInit = F
  switch(pOption,
         
         'BOARD' = {
           CATln_Border(paste('LIST_STK_YAH_COUNTRY_DOWNLOAD >>>', pOption))
           if (nchar(FileList)>0)
           {
             List_iso3 = setDT(read.xlsx(paste0(ODDrive, 'BeQ/PEH/DATA/', FileList)))
             if (ToSort == 'DESC') {List_iso3 = List_iso3[order(-iso3)] } else { List_iso3 = List_iso3[order(iso3)] }
             if (Active==1) {List_iso3 = List_iso3[active==1] }
             My.Kable.TB(List_iso3)
             List_ISO3 = as.list(List_iso3$iso3)
           } else { List_ISO3 = pISO3 }
           print(List_ISO3)
           if (length(List_ISO3)>0)
           {
             for (il in 1:length(List_ISO3))
             {
               # il = 1
               xISO3      = List_ISO3[[il]]
               CATln_Border(paste('LIST_STK_YAH_COUNTRY_DOWNLOAD', pOption, xISO3))
               # LIST_YAH_STATISTICSLAST_BEL
               FileToSave = paste0("LIST_YAH_BOARD_", xISO3, '.rds')
               FileFull   = paste0(ODDrive, 'BeQ/PEH/DATA/', FileToSave)
               
               # ToDo = 'CHECKFILE'
               # ToDo = 'RESET'
               # ToDo = 'COMPLETE'
               
               switch(ToDo,
                      'CHECKFILE' = {To_Do = !file.exists(FileFull) } ,
                      'RESET'     = {To_Do = T } ,
                      'COMPLETE'  = {
                        if (!file.exists(FileFull))
                        { 
                          To_Do   = T
                          ToInit  = T
                        } else {
                          STK_LIST   = setDT(readRDS(paste0(ODDrive,'BeQ/PEH/DATA/', 'LIST_YAH_', xISO3, '.rds')))
                          
                          STK_LIST   = STK_LIST[!grepl(' ETF| ETN| Index| Fund', name)]
                          STK_LIST[, codesource:=symbol]
                          CATln_Border('FULL LIST')
                          My.Kable(STK_LIST)
                          FileToSave = paste0("LIST_YAH_BOARD_", xISO3, '.rds')
                          STK_PRICES = setDT(readRDS(paste0(ODDrive,'BeQ/PEH/DATA/', FileToSave)))
                          
                          STK_SELECT = STK_LIST[!codesource %in% STK_PRICES$codesource]
                          My.Kable(STK_SELECT)
                          STK_LIST   = STK_SELECT
                          CATln_Border('SELECT LIST')
                          My.Kable(STK_LIST)
                          To_Do = T
                        }
                      }
               )
               
               if (To_Do)
               {
                 if (ToDo !='COMPLETE' | ToInit)
                 {
                   STK_LIST   = setDT(readRDS(paste0(ODDrive,'BeQ/PEH/DATA/', 'LIST_YAH_', xISO3, '.rds')))
                   STK_LIST   = STK_LIST[!grepl(' ETF| ETN| Index| Fund', name)]
                   STK_PRICES = data.table()
                 }
                 My.Kable.TB(STK_LIST)
                 My.Kable.TB(STK_PRICES)
                 
                 FileToSave = paste0("LIST_YAH_BOARD_", xISO3, '.rds')
                 My.Kable(STK_LIST)
                 
                 # Nb_Group = 25
                 Nb_TODO = min(NbMin, nrow(STK_LIST))
                 if (Nb_TODO>0)
                 {
                   Data_List = list()
                   
                   for (i in 1:Nb_TODO)
                   {
                     # i =1
                     pCode    = STK_LIST[i]$ symbol
                     CATln("")
                     CATln_Border(paste(i, '/', Nb_TODO, ' = ', pCode))
                     # x = try(FINAL_DOWNLOAD_YAH_INS_BY_CODESOURCE(pCodesource='9553-SABE.SR', NbDaysBack=10*365, ToSleep=2))
                     # .................................................................................
                     # x = try(GET_YAH_BOARD(pCodesource=pCode, ToKable=T))
                     # x = try(GET_YAH_STK_SHARES_FLOAT(pCode = pCode))
                     x = try(GET_YAH_BOARD(pCodesource=pCode, ToKable=T))
                     # x = try(FINAL_DOWNLOAD_YAH_INS_BY_CODESOURCE(pCodesource=pCode, NbDaysBack=10*365, ToSleep=2))
                     # .................................................................................
                     if (all(class(x)!='try-error'))
                     {
                       Data_List[[i]] = x
                       My.Kable.TB(x)
                     }
                     Sys.sleep(ToSleep)
                     if (i %% Nb_Group ==0 | i == Nb_TODO)
                     {
                       Data_All = unique(rbind(STK_PRICES, rbindlist(Data_List, fill=T), fill=T), by=c('codesource', 'fullname', 'date'), fromLast=T)
                       My.Kable.TB(STK_PRICES)
                       My.Kable.TB(Data_All)
                       saveRDS(Data_All, paste0(ODDrive, 'BeQ/PEH/DATA/', FileToSave))
                       CATln_Border(paste('SAVED :', FileToSave))
                       CATln('')
                     }
                   }
                 }
                 
               }
               Sys.sleep(ToSleep)
             }
           }
         },
         
         'SHARESFLOAT' = {
           CATln_Border(paste('LIST_STK_YAH_COUNTRY_DOWNLOAD >>>', pOption))
           if (nchar(FileList)>0)
           {
             List_iso3 = setDT(read.xlsx(paste0(ODDrive, 'BeQ/PEH/DATA/', FileList)))
             if (ToSort == 'DESC') {List_iso3 = List_iso3[order(-iso3)] } else { List_iso3 = List_iso3[order(iso3)] }
             if (Active==1) {List_iso3 = List_iso3[active==1] }
             My.Kable.TB(List_iso3)
             List_ISO3 = as.list(List_iso3$iso3)
           } else { List_ISO3 = pISO3 }
           print(List_ISO3)
           if (length(List_ISO3)>0)
           {
             for (il in 1:length(List_ISO3))
             {
               # il = 1
               xISO3      = List_ISO3[[il]]
               CATln_Border(paste('LIST_STK_YAH_COUNTRY_DOWNLOAD', pOption, xISO3))
               # LIST_YAH_STATISTICSLAST_BEL
               FileToSave = paste0("LIST_YAH_SHARESFLOAT_", xISO3, '.rds')
               FileFull   = paste0(ODDrive, 'BeQ/PEH/DATA/', FileToSave)
               
               # ToDo = 'CHECKFILE'
               # ToDo = 'RESET'
               # ToDo = 'COMPLETE'
               
               switch(ToDo,
                      'CHECKFILE' = {To_Do = !file.exists(FileFull) } ,
                      'RESET'     = {To_Do = T } ,
                      'COMPLETE'  = {
                        if (!file.exists(FileFull))
                        { 
                          To_Do  = T
                          ToInit = T
                        } else {
                          STK_LIST   = setDT(readRDS(paste0(ODDrive,'BeQ/PEH/DATA/', 'LIST_YAH_', xISO3, '.rds')))
                          
                          STK_LIST   = STK_LIST[!grepl(' ETF| ETN| Index| Fund', name)]
                          STK_LIST[, codesource:=symbol]
                          CATln_Border('FULL LIST')
                          My.Kable(STK_LIST)
                          FileToSave = paste0("LIST_YAH_SHARESFLOAT_", xISO3, '.rds')
                          STK_PRICES = setDT(readRDS(paste0(ODDrive,'BeQ/PEH/DATA/', FileToSave)))
                          
                          STK_SELECT = STK_LIST[!codesource %in% STK_PRICES$codesource]
                          My.Kable(STK_SELECT)
                          STK_LIST   = STK_SELECT
                          CATln_Border('SELECT LIST')
                          My.Kable(STK_LIST)
                          To_Do = T
                        }
                      }
               )
               
               if (To_Do)
               {
                 if (ToDo !='COMPLETE' | ToInit)
                 {
                   STK_LIST   = setDT(readRDS(paste0(ODDrive,'BeQ/PEH/DATA/', 'LIST_YAH_', xISO3, '.rds')))
                   STK_LIST   = STK_LIST[!grepl(' ETF| ETN| Index| Fund', name)]
                   STK_PRICES = data.table()
                 }
                 My.Kable.TB(STK_LIST)
                 My.Kable.TB(STK_PRICES)
                 
                 FileToSave = paste0("LIST_YAH_SHARESFLOAT_", xISO3, '.rds')
                 My.Kable(STK_LIST)
                 
                 # Nb_Group = 25
                 Nb_TODO = min(NbMin, nrow(STK_LIST))
                 if (Nb_TODO>0)
                 {
                   Data_List = list()
                   
                   for (i in 1:Nb_TODO)
                   {
                     # i =1
                     pCode    = STK_LIST[i]$ symbol
                     CATln("")
                     CATln_Border(paste(i, '/', Nb_TODO, ' = ', pCode))
                     # x = try(FINAL_DOWNLOAD_YAH_INS_BY_CODESOURCE(pCodesource='9553-SABE.SR', NbDaysBack=10*365, ToSleep=2))
                     # .................................................................................
                     # x = try(GET_YAH_BOARD(pCodesource=pCode, ToKable=T))
                     x = try(GET_YAH_STK_SHARES_FLOAT(pCode = pCode))
                     
                     # x = try(FINAL_DOWNLOAD_YAH_INS_BY_CODESOURCE(pCodesource=pCode, NbDaysBack=10*365, ToSleep=2))
                     # .................................................................................
                     if (all(class(x)!='try-error'))
                     {
                       Data_List[[i]] = x
                       My.Kable.TB(x)
                     }
                     Sys.sleep(ToSleep)
                     if (i %% Nb_Group ==0 | i == Nb_TODO)
                     {
                       Data_All = unique(rbind(STK_PRICES, rbindlist(Data_List, fill=T), fill=T), by=c('codesource', 'date'), fromLast=T)
                       My.Kable.TB(STK_PRICES)
                       My.Kable.TB(Data_All)
                       saveRDS(Data_All, paste0(ODDrive, 'BeQ/PEH/DATA/', FileToSave))
                       CATln_Border(paste('SAVED :', FileToSave))
                       CATln('')
                     }
                   }
                 }
                 
               }
               Sys.sleep(ToSleep)
             }
           }
         },
         
         'STATISTICSLAST' = {
           CATln_Border(paste('LIST_STK_YAH_COUNTRY_DOWNLOAD >>>', pOption))
           if (nchar(FileList)>0)
           {
             List_iso3 = setDT(read.xlsx(paste0(ODDrive, 'BeQ/PEH/DATA/', FileList)))
             if (ToSort == 'DESC') {List_iso3 = List_iso3[order(-iso3)] } else { List_iso3 = List_iso3[order(iso3)] }
             if (Active==1) {List_iso3 = List_iso3[active==1] }
             My.Kable.TB(List_iso3)
             List_ISO3 = as.list(List_iso3$iso3)
           } else { List_ISO3 = pISO3 }
           print(List_ISO3)
           if (length(List_ISO3)>0)
           {
             for (il in 1:length(List_ISO3))
             {
               # il = 1
               xISO3      = List_ISO3[[il]]
               CATln_Border(paste('LIST_STK_YAH_COUNTRY_DOWNLOAD', pOption, xISO3))
               # LIST_YAH_STATISTICSLAST_BEL
               FileToSave = paste0("LIST_YAH_STATISTICSLAST_", xISO3, '.rds')
               FileFull   = paste0(ODDrive, 'BeQ/PEH/DATA/', FileToSave)
               
               # ToDo = 'CHECKFILE'
               # ToDo = 'RESET'
               # ToDo = 'COMPLETE'
               
               switch(ToDo,
                      'CHECKFILE' = {To_Do = !file.exists(FileFull) } ,
                      'RESET'     = {To_Do = T } ,
                      'COMPLETE'  = {
                        if (!file.exists(FileFull))
                        { 
                          To_Do  = T
                          ToInit = T
                        } else {
                          STK_LIST   = setDT(readRDS(paste0(ODDrive,'BeQ/PEH/DATA/', 'LIST_YAH_', xISO3, '.rds')))
                          
                          STK_LIST   = STK_LIST[!grepl(' ETF| ETN| Index| Fund', name)]
                          STK_LIST[, codesource:=symbol]
                          CATln_Border('FULL LIST')
                          My.Kable(STK_LIST)
                          FileToSave = paste0("LIST_YAH_STATISTICSLAST_", xISO3, '.rds')
                          STK_PRICES = setDT(readRDS(paste0(ODDrive,'BeQ/PEH/DATA/', FileToSave)))
                          
                          STK_SELECT = STK_LIST[!codesource %in% STK_PRICES$codesource]
                          My.Kable(STK_SELECT)
                          STK_LIST   = STK_SELECT
                          CATln_Border('SELECT LIST')
                          My.Kable(STK_LIST)
                          To_Do = T
                        } 
                      }
               )
               
               if (To_Do)
               {
                 if (ToDo !='COMPLETE' | ToInit)
                 {
                   STK_LIST   = setDT(readRDS(paste0(ODDrive,'BeQ/PEH/DATA/', 'LIST_YAH_', xISO3, '.rds')))
                   STK_LIST   = STK_LIST[!grepl(' ETF| ETN| Index| Fund', name)]
                   STK_PRICES = data.table()
                 }
                 My.Kable.TB(STK_LIST)
                 My.Kable.TB(STK_PRICES)
                 
                 FileToSave = paste0("LIST_YAH_STATISTICSLAST_", xISO3, '.rds')
                 My.Kable(STK_LIST)
                 
                 # Nb_Group = 25
                 Nb_TODO = min(NbMin, nrow(STK_LIST))
                 if (Nb_TODO>0)
                 {
                   Data_List = list()
                   
                   for (i in 1:Nb_TODO)
                   {
                     # i =1
                     pCode    = STK_LIST[i]$ symbol
                     CATln("")
                     CATln_Border(paste(i, '/', Nb_TODO, ' = ', pCode))
                     # x = try(FINAL_DOWNLOAD_YAH_INS_BY_CODESOURCE(pCodesource='9553-SABE.SR', NbDaysBack=10*365, ToSleep=2))
                     # .................................................................................
                     # x = try(GET_YAH_BOARD(pCodesource=pCode, ToKable=T))
                     x = try(GET_YAH_STATISTICS_PRICE(pCode=pCode))
                     # x = try(FINAL_DOWNLOAD_YAH_INS_BY_CODESOURCE(pCodesource=pCode, NbDaysBack=10*365, ToSleep=2))
                     # .................................................................................
                     if (all(class(x)!='try-error'))
                     {
                       Data_List[[i]] = x
                       My.Kable.TB(x)
                     }
                     Sys.sleep(ToSleep)
                     if (i %% Nb_Group ==0 | i == Nb_TODO)
                     {
                       Data_All = unique(rbind(STK_PRICES, rbindlist(Data_List, fill=T), fill=T), by=c('codesource', 'date'), fromLast=T)
                       My.Kable.TB(STK_PRICES)
                       My.Kable.TB(Data_All)
                       saveRDS(Data_All, paste0(ODDrive, 'BeQ/PEH/DATA/', FileToSave))
                       CATln_Border(paste('SAVED :', FileToSave))
                       CATln('')
                     }
                   }
                 }
               }
               Sys.sleep(ToSleep)
             }
           }
         },
         
         'PRICES' = {
           CATln_Border(paste('LIST_STK_YAH_COUNTRY_DOWNLOAD >>>', pOption))
           if (nchar(FileList)>0)
           {
             List_iso3 = setDT(read.xlsx(paste0(ODDrive, 'BeQ/PEH/DATA/', FileList)))
             if (ToSort == 'DESC') {List_iso3 = List_iso3[order(-iso3)] } else { List_iso3 = List_iso3[order(iso3)] }
             if (Active==1) {List_iso3 = List_iso3[active==1] }
             My.Kable.TB(List_iso3)
             List_ISO3 = as.list(List_iso3$iso3)
           } else { List_ISO3 = pISO3 }
           print(List_ISO3)
           if (length(List_ISO3)>0)
           {
             for (il in 1:length(List_ISO3))
             {
               # il = 1
               xISO3      = List_ISO3[[il]]
               CATln_Border(paste('LIST_STK_YAH_COUNTRY_DOWNLOAD', pOption, xISO3))
               FileToSave = paste0("LIST_YAH_PRICES_", xISO3, '.rds')
               FileFull   = paste0(ODDrive, 'BeQ/PEH/DATA/', FileToSave)
               
               # ToDo = 'CHECKFILE'
               # ToDo = 'RESET'
               # ToDo = 'COMPLETE'
               
               switch(ToDo,
                      'CHECKFILE' = {To_Do = !file.exists(FileFull) } ,
                      'RESET'     = {To_Do = T } ,
                      'COMPLETE'  = {
                        if (!file.exists(FileFull))
                        { 
                          To_Do  = T
                          ToInit = T
                        } else {
                          STK_LIST   = setDT(readRDS(paste0(ODDrive,'BeQ/PEH/DATA/', 'LIST_YAH_', xISO3, '.rds')))
                          
                          STK_LIST   = STK_LIST[!grepl(' ETF| ETN| Index| Fund', name)]
                          STK_LIST[, codesource:=symbol]
                          CATln_Border('FULL LIST')
                          My.Kable(STK_LIST)
                          FileToSave = paste0("LIST_YAH_PRICES_", xISO3, '.rds')
                          STK_PRICES = setDT(readRDS(paste0(ODDrive,'BeQ/PEH/DATA/', FileToSave)))
                          
                          STK_SELECT = STK_LIST[!codesource %in% STK_PRICES$codesource]
                          My.Kable(STK_SELECT)
                          STK_LIST   = STK_SELECT
                          CATln_Border('SELECT LIST')
                          My.Kable(STK_LIST)
                          To_Do = T
                        } 
                      }
               )
               
               if (To_Do)
               {
                 if (ToDo !='COMPLETE' | ToInit)
                 {
                   STK_LIST   = setDT(readRDS(paste0(ODDrive,'BeQ/PEH/DATA/', 'LIST_YAH_', xISO3, '.rds')))
                   STK_LIST   = STK_LIST[!grepl(' ETF| ETN| Index| Fund', name)]
                   STK_PRICES = data.table()
                 }
                 My.Kable.TB(STK_LIST)
                 My.Kable.TB(STK_PRICES)
                 
                 FileToSave = paste0("LIST_YAH_PRICES_", xISO3, '.rds')
                 My.Kable(STK_LIST)
                 
                 # Nb_Group = 25
                 Nb_TODO = min(NbMin, nrow(STK_LIST))
                 if (Nb_TODO>0)
                 {
                   Data_List = list()
                   
                   for (i in 1:Nb_TODO)
                   {
                     # i =1
                     pCode    = STK_LIST[i]$ symbol
                     CATln("")
                     CATln_Border(paste(i, '/', Nb_TODO, ' = ', pCode))
                     # x = try(FINAL_DOWNLOAD_YAH_INS_BY_CODESOURCE(pCodesource='9553-SABE.SR', NbDaysBack=10*365, ToSleep=2))
                     x = try(FINAL_DOWNLOAD_YAH_INS_BY_CODESOURCE(pCodesource=pCode, NbDaysBack=30*365, ToSleep=2))
                     if (all(class(x)!='try-error'))
                     {
                       Data_List[[i]] = x
                       My.Kable.TB(x)
                     }
                     Sys.sleep(ToSleep)
                     if (i %% Nb_Group ==0 | i == Nb_TODO)
                     {
                       Data_All = unique(rbind(STK_PRICES, rbindlist(Data_List, fill=T), fill=T), by=c('codesource', 'date'), fromLast=T)
                       My.Kable.TB(STK_PRICES)
                       My.Kable.TB(Data_All)
                       saveRDS(Data_All, paste0(ODDrive, 'BeQ/PEH/DATA/', FileToSave))
                       CATln_Border(paste('SAVED :', FileToSave))
                       CATln('')
                     }
                   }
                   
                 }
               }
               Sys.sleep(ToSleep)
             }
           }
         }
  )
}

#===============================================================================
GET_YAH_STK_SHARES_FLOAT = function(pCode = 'IBM')
  #-----------------------------------------------------------------------------
{
  pURL  = paste0('https://finance.yahoo.com/quote/', pCode, '/key-statistics?p=',pCode)
  CATln_Border(pURL)
  content  = rvest::read_html(pURL)
  tables   = content %>% html_table(fill = TRUE)
  
  xData    = SELECT_TABLE_WITH_FIELD(tables=tables, pField='x1', pContent="Shares Outstanding 5", like=T)
  My.Kable.All(xData)
  if (nrow(xData)>0)
  {
    shares_outstanding = GET_VALUE_IN_TABLE_x1x2(pTable=xData, pDataset='Shares Outstanding 5')
    shares_outstanding = CONVERT_IN_VALUE_KMBT(shares_outstanding)
    print(shares_outstanding)
    shares_float       = GET_VALUE_IN_TABLE_x1x2(pTable=xData, pDataset='Float 8') 
    shares_float = CONVERT_IN_VALUE_KMBT(shares_float)
    print(shares_float)
    table.res = data.table(codesource=pCode, sharesout=shares_outstanding, sharesffl=shares_float, date=Sys.Date(), updated=Sys.Date())
  } else {
    table.res = data.table()
  }
  My.Kable.All(table.res)
  return(table.res)
}

#===============================================================================
CONVERT_IN_VALUE_KMBT = function(x = '10.23K', ToPrompt=F)
  #-----------------------------------------------------------------------------
{
  # x = '10.2365B'
  X1 = substr(x, nchar(x), nchar(x))
  x2 = substr(x, 1 , nchar(x)-1)
  switch (X1, 
          'K' = {m = 1000},
          'M' = {m = 1000000},
          'B' = {m = 1000000000},
          'T' = {m = 1000000000000},
          { m = 1 }
  )
  value = m*as.numeric(gsub(',','', x2))
  if (ToPrompt) {print(value)}
  return(value)
}

#==============================================================================
GET_VALUE_IN_TABLE_x1x2 = function(pTable=xData, pDataset='Shares Outstanding 5')
  #----------------------------------------------------------------------------
{
  xres = pTable[x1==pDataset]
  if (nrow(xres)==1) { 
    xvalue = xres[1]$x2 } else { xvalue = as.character(NA) }
  return(xvalue)
}

#===============================================================================
LIST_STK_YAH_COUNTRY_ADJUST = function(pOption='PRICES', pISO3='BEL', FileList='LIST_YAH_STK_COUNTRY.xlsx')
  #------------------------------------------------------------------------
{
  switch(pOption,
         'PRICES' = {
           if (nchar(FileList)>0)
           {
             List_iso3 = setDT(read.xlsx(paste0(ODDrive, 'BeQ/PEH/DATA/', FileList)))
             My.Kable.TB(List_iso3)
             List_ISO3 = as.list(List_iso3$iso3)
           } else { List_ISO3 = pISO3 }
           print(List_ISO3)
           for (il in 1:length(List_ISO3))
           {
             xISO3      = List_ISO3[[il]]
             CATln_Border(paste('LIST_STK_YAH_COUNTRY_ADJUST', pOption, xISO3))
             FileToSave = paste0("LIST_YAH_PRICES_", xISO3, '.rds')
             FileFull   = paste0(ODDrive, 'BeQ/PEH/DATA/', FileToSave)
             if (file.exists(FileFull))
             {
               dt_yah     = readRDS(FileFull)
               
               dt_yah     = CLEAN_COLNAMES(dt_yah)
               str(dt_yah)
               dt_yah = dt_yah[!is.na(close)][order(codesource, date)]
               
               if (nrow(dt_yah)>0)
               {
                 if ("adjclose" %in% names(dt_yah))
                 {
                   dt_yah = dt_yah[order(codesource, date)]
                   dt_yah[, rt:=(adjclose/shift(adjclose))-1, by=c("codesource")]
                   dt_yah[, change:=(adjclose-shift(adjclose)), by=c("codesource")]
                   dt_yah[, varpc:=100*rt, by=c("codesource")]
                 }
                 My.Kable.TB(dt_yah)
                 saveRDS(dt_yah, FileFull)
                 CATln_Border(paste('SAVED : ', FileFull, '-', nrow(unique(dt_yah, by='codesource')), 'codes', '/', nrow(dt_yah), 'records'))
               }
             }
           }
         }
  )
}

#===============================================================================
LIST_STK_YAH_COUNTRY_SUMMARYALL = function()
  #-----------------------------------------------------------------------------
{
  my_list   = LIST_STK_YAH_COUNTRY_SUMMARY()
  My.Kable.All(my_list)
  
  my_result = COUNT_YAH_STK_COLUMNS_BY_LIST(FileList='LIST_YAH_STK_COUNTRY.xlsx')
  My.Kable.All(my_result)
  
  my_list =merge(my_list, my_result, by='iso3', all.x=T)
  My.Kable.All(my_list)
  saveRDS(my_list, paste0(ODDrive, 'BeQ/PEH/DATA/', 'LIST_STK_YAH_COUNTRY_SUMMARYALL.rds'))
  write.xlsx(my_list, paste0(ODDrive, 'BeQ/PEH/DATA/', 'LIST_STK_YAH_COUNTRY_SUMMARYALL.xlsx'))
  return(my_list)
}

#===============================================================================
LIST_STK_YAH_COUNTRY_SUMMARY = function()
  #-----------------------------------------------------------------------------
{
  # List_country = setDT(read.xlsx ('C:/Users/Admin/OneDrive/BeQ/DATA/INV/ALL/Country_checklist.xlsx', sheetIndex = 1))
  List_country = setDT(read.xlsx( paste0(ODDrive, 'BeQ/PEH/DATA/', 'LIST_YAH_STK_COUNTRY.xlsx')))
  My.Kable(List_country)
  Data_list = list()
  for (i in 1:nrow(List_country))
  {
    # shell('cls')
    # i =4
    CATln_Border(paste(i, List_country[i]$iso3))
    CATln('')
    FileName = paste0("LIST_YAH_", List_country[i]$iso3, '.rds') 
    CATln_Border(FileName)
    if (file.exists(paste0(ODDrive, 'BeQ/PEH/DATA/', FileName)))
    {
      FileData = readRDS(paste0(ODDrive, 'BeQ/PEH/DATA/', FileName))
      FileRow  = nrow(FileData)
      Data_list[[i]] = data.table(country=FileData[1]$country, iso3 = List_country[i]$iso3, nbcompanies=FileRow)
    }
    # LIST_YAH_STK_BY_COUNTRY(pISO3=List_country[i]$iso3, pURLx = List_country[i]$link, ToCheckFile = T)
  }
  Data_All = rbindlist(Data_list, fill=T)[order(-nbcompanies)]
  My.Kable.All(Data_All)
  write.xlsx(Data_All, paste0(ODDrive, 'BeQ/PEH/DATA/', 'LIST_STK_YAH_COUNTRY_SUMMARY.xlsx'))
  return(Data_All)
}

#===============================================================================
EXECUTION_LIST_STK_YAH = function(pExecution='SCREENING')
  #-----------------------------------------------------------------------------
{
  switch(pExecution,
         'SCREENING' = {
           List_country = setDT(read.xlsx( paste0(ODDrive, 'BeQ/PEH/DATA/', 'LIST_YAH_STK_COUNTRY.xlsx')))
           My.Kable(List_country)
           
           for (i in 1:nrow(List_country))
           {
             # shell('cls')
             # i =1
             # pISO3 = 'TWN'
             CATln_Border(paste(i, List_country[i]$iso3))
             CATln('')
             LIST_YAH_STK_BY_COUNTRY(pISO3=List_country[i]$iso3, pURLx = List_country[i]$link, ToCheckFile = T)
           }
         },
         'PRICES' = {
           
         },
         "BOARD" = {
           
         }
  )
}

#===============================================================================
COUNT_YAH_STK_COLUMNS_BY_LIST = function(FileList='LIST_YAH_STK_COUNTRY.xlsx')
  #-----------------------------------------------------------------------------
{
  List_iso3 = setDT(read.xlsx(paste0(ODDrive, 'BeQ/PEH/DATA/', FileList)))
  My.Kable.TB(List_iso3)
  List_ISO3 = as.list(List_iso3$iso3)
  Data_list = list()
  
  for (il in 1:length(List_ISO3))
  {
    # il = 1
    xISO3      = List_ISO3[[il]]
    shell('cls')
    CATln_Border(paste('GET_YAH_STK_STATISTICS_PRICE_BY_LIST', xISO3))
    x = try(COUNT_YAH_STK_COLUMNS_BY_COUNTRY(pISO3=xISO3))
    if (all(class(x)!='try-error')) # No ERROR
    {
      Data_list[[il]] = x
    }
  }
  Data_All = rbindlist(Data_list, fill=T)
  My.Kable.All(Data_All)
  return(Data_All)
}

#===============================================================================
COUNT_YAH_STK_COLUMNS_BY_COUNTRY = function(pISO3 = 'USA') {
  #-----------------------------------------------------------------------------
  
  File_prices = paste0(ODDrive, 'BeQ/PEH/DATA/', 'LIST_YAH_PRICES_', pISO3, '.rds')
  if (file.exists(File_prices))
  {
    File_Data = setDT(readRDS(File_prices))   
    str(File_Data)
    Nb_prices = nrow(unique(File_Data, by='codesource'))
  } else { Nb_prices = as.numeric(NA) }
  CATln_Border(paste(pISO3, 'prices =', Nb_prices))
  start_date = min(File_Data$date)
  # LIST_YAH_BOARD_BEL.rds
  File_Data = paste0(ODDrive, 'BeQ/PEH/DATA/', 'LIST_YAH_BOARD_', pISO3, '.rds')
  CATln_Border(File_Data)
  if (file.exists(File_Data))
  {
    File_Data = setDT(readRDS(File_Data))   
    str(File_Data)
    My.Kable(File_Data)
    Nb_board = nrow(unique(File_Data, by='codesource'))
  } else { Nb_board = as.numeric(NA) }
  CATln_Border(paste(pISO3, 'board =', Nb_board))
  
  # LIST_YAH_STATISTICSLAST_BEL
  File_Data = paste0(ODDrive, 'BeQ/PEH/DATA/', 'LIST_YAH_STATISTICSLAST_', pISO3, '.rds')
  CATln_Border(File_Data)
  if (file.exists(File_Data))
  {
    File_Data = setDT(readRDS(File_Data))   
    str(File_Data)
    if (all(c("last", "change", "var") %in% names(File_Data)))
    {
      My.Kable(File_Data)
      Nb_statslast = nrow(unique(File_Data, by='codesource'))
    } else { 
      file.remove(File_Data)
      Nb_statslast = as.numeric(NA)
    }
    
  } else { Nb_statslast = as.numeric(NA) }
  CATln_Border(paste(pISO3, 'statslast =', Nb_statslast))
  
  # LIST_YAH_SHARESFLOAT_BEL
  File_Data = paste0(ODDrive, 'BeQ/PEH/DATA/', 'LIST_YAH_SHARESFLOAT_', pISO3, '.rds')
  CATln_Border(File_Data)
  if (file.exists(File_Data))
  {
    File_Data = setDT(readRDS(File_Data))   
    str(File_Data)
    if (all(c("sharesout", "sharesffl") %in% names(File_Data)))
    {
      My.Kable(File_Data)
      Nb_sharesfloat = nrow(unique(File_Data, by='codesource'))
    } else { 
      file.remove(File_Data)
      Nb_sharesfloat = as.numeric(NA)
    }
    
  } else { Nb_sharesfloat = as.numeric(NA) }
  CATln_Border(paste(pISO3, 'Nb_sharesfloat  =', Nb_sharesfloat))
  
  tab.res = data.table(iso3=pISO3, prices=Nb_prices,startdate = start_date, board=Nb_board, statslast=Nb_statslast, sharesfloat=Nb_sharesfloat)
  My.Kable(tab.res)
  return(tab.res)
}

#===============================================================================
GET_YAH_STK_STATISTICS_PRICE_BY_LIST = function(FileList='LIST_YAH_STK_COUNTRY.xlsx', NbMin=2000, Nb_Group=20, ToSleep=2) {
  #-------------------------------------------------------------------------------
  
  List_iso3 = setDT(read.xlsx(paste0(ODDrive, 'BeQ/PEH/DATA/', FileList)))
  My.Kable.TB(List_iso3)
  List_ISO3 = as.list(List_iso3$iso3)
  for (il in 1:length(List_ISO3))
  {
    # il = 1
    xISO3      = List_ISO3[[il]]
    shell('cls')
    CATln_Border(paste('GET_YAH_STK_STATISTICS_PRICE_BY_LIST', xISO3))
    try(GET_YAH_STK_STATISTICS_PRICE_BY_COUNTRY(pISO3=xISO3, NbMin=20000, Nb_Group=20, ToSleep=2))
  }
}

#===============================================================================
GET_YAH_STK_STATISTICS_PRICE_BY_COUNTRY = function(pISO3='BEL', NbMin=2000, Nb_Group=20, ToSleep=2)
  #---------------------------------------------------------
{
  FileName = paste0('LIST_YAH_', pISO3, '.rds')
  FileFull = paste0(ODDrive, 'BeQ/PEH/DATA/', FileName)
  if (file.exists(FileFull))
  {
    STK_LIST = readRDS(FileFull)
    
    FileToSave = paste0("LIST_YAH_STATISTICSLAST_", pISO3, '.rds')
    My.Kable(STK_LIST)
    
    # Nb_Group = 25
    Nb_TODO = min(NbMin, nrow(STK_LIST))
    Data_List = list()
    
    for (i in 1:Nb_TODO)
    {
      # i =1
      pCode    = STK_LIST[i]$ symbol
      CATln("")
      CATln_Border(paste(i, '/', Nb_TODO, ' = ', pCode))
      
      # x = try(FINAL_DOWNLOAD_YAH_INS_BY_CODESOURCE(pCodesource='9553-SABE.SR', NbDaysBack=10*365, ToSleep=2))
      x = try(GET_YAH_STK_STATISTICS_PRICE(pCode = pCode))
      # x = try(FINAL_DOWNLOAD_YAH_INS_BY_CODESOURCE(pCodesource=pCode, NbDaysBack=10*365, ToSleep=2))
      if (all(class(x)!='try-error'))
      {
        Data_List[[i]] = x
        My.Kable.TB(x)
      }
      Sys.sleep(ToSleep)
      
      if (i %% Nb_Group ==0 | i == Nb_TODO)
      {
        Data_All = unique(rbindlist(Data_List, fill=T), by=c('codesource', 'updated'), fromLast=T)
        My.Kable.TB(Data_All)
        saveRDS(Data_All, paste0(ODDrive, 'BeQ/PEH/DATA/', FileToSave))
        CATln_Border(paste('SAVED :', FileToSave))
        CATln('')
      }
    }
  }
}

#================================================================================
SELECT_TABLE_WITH_FIELD = function(tables, pField='x1', pContent="EPS", like=F)
  #---------------------------------------------------------------------------------
{
  x.res = data.table()
  # pField='shareholder'; pContent=""
  # pField='x1'; pContent="First trading date"
  length(tables)
  if (nchar(pContent)==0)
  {
    nr = 0
    for (itab in 1:length(tables))
    {
      # itab = 50
      x.info = as.data.table(tables[[itab]])
      x.info = CLEAN_COLNAMES(x.info)
      colnames(x.info) = vietnameseConverter::decodeVN(colnames(x.info), from='Unicode', to='Unicode', diacritics=F)
      if (pField %in% colnames(x.info))
      {
        nr = itab
        # str(x.info)  
        # CATln_Border(paste("FOUND :", pField, nr))
        x.res = x.info
      }
    }
  } else {
    nr = 0
    for (itab in 1:length(tables))
    {
      # itab = 50
      # like = T
      x.info = as.data.table(tables[[itab]])
      x.info = CLEAN_COLNAMES(x.info)
      colnames(x.info) = vietnameseConverter::decodeVN(colnames(x.info), from='Unicode', to='Unicode', diacritics=F)
      
      if (pField %in% colnames(x.info))
      {
        setnames(x.info, pField, 'myfield')
        if (like)
        {
          # pContent = 'First Trading'
          nlike = (nrow(x.info[grepl(trimws(pContent), myfield, ignore.case = T)])==1)
        } else {
          nlike = (nrow(x.info[myfield==pContent])==1)
        }
        
        if (nlike)
        {
          nr = itab
          # str(x.info)  
          # CATln_Border(paste("FOUND :", pField, pContent, nr))
          setnames(x.info, 'myfield', pField)
          x.res = x.info
        }
      }
    }
  }
  return(x.res)
}

#==============================================================================
GET_YAH_BOARD=function(pCodesource='AWK.SI', ToKable=T)
  #----------------------------------------------------------------------------
{
  #pCodesource = BOD_MISSING[1]
  # pCodesource = '586.SI'
  # pURL = 'https://finance.yahoo.com/quote/SGD%3DX/history?p=SGD%3DX'
  pURL = paste0('https://finance.yahoo.com/quote/', pCodesource, '/profile?p=', pCodesource)
  CATln_Border(pURL)
  
  content  = rvest::read_html(pURL)
  tables   = content %>% html_table(fill = TRUE)
  
  if (length(tables)>0)
  {
    xData    = as.data.table(tables[[1]])
    # kable(xData)
    colnames(xData) = tolower(colnames(xData))
    # str(xData)
    xData[, codesource:=pCodesource]
    xData[, date:=Sys.Date()]
    xData[, updated:=Sys.Date()]
    xData$gender=as.character(NA)
    xData[grepl('^Mr.', name), gender:='M'] #
    xData[grepl('^Ms.', name), gender:='F']
    xData[grepl('^Mrs.', name), gender:='F']
    # kable(xData)
    xData$fullname=as.character(NA)
    xData[,fullname:=trimws(gsub('^Mr.|^Ms.|^Mrs.', '', name))]
    # kable(xData[is.na(gender)])
    
    xData[,fullname:=gsub(' A.C.S., ACIS',"" ,fullname)]
    xData$lastname=as.character(NA)
    xData[,lastname:= trimws(sub(paste0(".*", '  '), "", fullname))] #
    xData[lastname==fullname,lastname:= sub(paste0(".*", ' '), "", fullname)] #
    
    xData[,fullname:=gsub('  '," " ,fullname)]
    # xData$firstname=as.character(NA)
    # xData[,firstname:=sub(paste0(paste0(" ", lastname),".*"), "", fullname)] #
    # View(xData)
    xData$ceo=as.numeric(NA)
    xData[grepl('Chief Exec. Officer|CEO', title),ceo:=1]
    
    if (nrow(xData[ceo==1])>1)
    {
      xData[, nr:=seq.int(1, .N)]
      minCEO = min(xData[ceo==1]$nr)
      xData[nr>minCEO & ceo==1, ceo:=as.numeric(NA)]
      xData$nr = NULL
    }
    xData$cob=as.numeric(NA)
    xData[grepl('Chairman|Chairwoman', title),cob:=1]
    # str(xData)
    xData=xData[,-c('name')]
  } else {
    xData = data.table()
  }
  if (ToKable) { My.Kable(xData)}
  xData$codesource = pCodesource
  return(xData)
}


#==================================================================================
GET_STK_YAH_BOARD_BY_COUNTRY = function(pISO3 = 'BEL', Nb_Group = 10, Nb_Min = 20)
{
  Data_All = data.table()
  FileName = paste0("LIST_YAH_", pISO3, '.rds') 
  CATln_Border(FileName)
  Data_List = list()
  if (file.exists(paste0(ODDrive,'BeQ/PEH/DATA/', FileName)))
  {
    FileData = readRDS(paste0(ODDrive,'BeQ/PEH/DATA/', FileName))
    FileRow  = nrow(FileData)
    
    Nb_TODO  = min(Nb_Min, FileRow)
    
    FileToSave = paste0("LIST_YAH_BOARD_", pISO3, '.rds')
    for (i in 1:Nb_TODO)
    {
      # i =51
      pCode    = FileData[i]$ symbol
      CATln("")
      CATln_Border(paste(i, '/', Nb_TODO, ' = ', pCode))
      My_Board = try(GET_YAH_BOARD(pCodesource=pCode, ToKable=T))
      if (all(class(My_Board)!='try-error'))
      {
        Data_List[[i]] = My_Board
      }
      Sys.sleep(2)
      if (i %% Nb_Group ==0 | i == Nb_TODO)
      {
        Data_All = rbindlist(Data_List, fill=T)
        view(Data_All)
        saveRDS(Data_All, paste0(ODDrive, 'BeQ/PEH/DATA/', FileToSave))
        CATln('SAVED')
        
      }
    }
  }
  return(Data_All)
}

# ==================================================================================================
IFRC_LEFTRIGHT_OF_STRING = function(pOption="LEFT", pString, pRef, ToPrompt=T, RemoveHTML=T) {
  # ------------------------------------------------------------------------------------------------
  # pString = "hello xxx other stuff"
  # pRef    = "xxx"
  pLeft   = sub(paste0(pRef,".*"), "", pString)
  # sub(".*xxx ", "", x)  
  pRight  = sub(paste0(".*", pRef), "", pString)
  pRes    = pString
  switch(pOption,
         "LEFT"  = {pRes = pLeft},
         "RIGHT" = {pRes = pRight}
  )
  if (RemoveHTML) { 
    pRes = gsub('"','',pRes) 
    pRes = gsub('&amp;','&',pRes) 
  }
  if (ToPrompt) { CATln(pRes)}
  return(pRes)
}


#===================================================================================================
GET_BARRONS_BOARD = function(pSymbol='ONE',  pISO2="CA")
  #-------------------------------------------------------------------------------------------------
{
  # pSymbol = 'HOME'
  # pISO2   = 'US'
  pURL    = paste0('https://www.barrons.com/market-data/stocks/', pSymbol, '?countrycode=', pISO2)
  CATln_Border(pURL)
  
  pWeb   = GET_CURL_FETCH(pURL)
  writeLines(pWeb, 'c:/temp/x.txt')
  x = str.extract(pWeb, '<script id="__NEXT_DATA__" type="application/json">', '"isRealTime"')
  
  xRows = unlist(strsplit(pWeb, '<div data-id="People_index" class="People__Person-sc-1sjcbb1-3 bOccoz">'))
  length(xRows)
  
  if (length(xRows) >1 )
  {
    xRow = xRows[[2]]
    Data_List = list()
    for (k in 2:(length(xRows) - 1))
    {
      xRow = xRows[[k]]
      p.Name     = IFRC_LEFTRIGHT_OF_STRING(pOption="RIGHT", pString=str.extract(xRow, 'People__Name', '</div>'), pRef='>', ToPrompt=F, RemoveHTML=T) 
      p.Name     = gsub(',','',p.Name)
      p.Position = IFRC_LEFTRIGHT_OF_STRING(pOption="RIGHT", pString=str.extract(xRow, 'People__Position', '</div>'), pRef='>', ToPrompt=F, RemoveHTML=T)
      Data_List[[k]]=data.table(name=p.Name, position = p.Position)
    }
  } else {
    Data_List = list()
  }
  
  Data_All=rbindlist(Data_List, fill = T)
  if (nrow(Data_All)>0)
  {
    Data_All = Data_All[!is.na(name)]
    Data_All = Data_All[nchar(name)>0]
    Data_All[grepl('^Chairman ', position) | position=='Chairman', cob:=1]
    Data_All[grepl('Chief Executive Officer', position), ceo:=1]
    Data_All[grepl(' Jr.$', name, ignore.case = T), name:=gsub(' Jr.$','', name, ignore.case = T)]
    Data_All[grepl(' MBA$', name, ignore.case = T), name:=gsub(' MBA$','', name, ignore.case = T)]
    Data_All[grepl(' PhD$', name, ignore.case = T), name:=gsub(' PhD$','', name, ignore.case = T)]
    Data_All[grepl(' LLD$', name, ignore.case = T), name:=gsub(' LLD$','', name, ignore.case = T)]
    Data_All[grepl(' EMBA$', name, ignore.case = T), name:=gsub(' EMBA$','', name, ignore.case = T)]
    Data_All[grepl(' MD$', name, ignore.case = T), name:=gsub(' MD$','', name, ignore.case = T)]
    Data_All[, ':='(iso2=pISO2, symbol=pSymbol)]
    
    # gender()
    Data_All$fullname=as.character(NA)
    Data_All[,fullname:=trimws(gsub('^Mr.|^Ms.|^Mrs.| Jr.$', '', name))]
    # kable(xData[is.na(gender)])
    
    Data_All[,fullname:=gsub(' A.C.S., ACIS| MBA$| CPA$| phD$| LLD$| EMBA$| MD$',"" ,fullname, ignore.case = T)]
    Data_All$lastname=as.character(NA)
    Data_All[,lastname:= trimws(sub(paste0(".*", '  '), "", fullname))] #
    Data_All[lastname==fullname,lastname:= sub(paste0(".*", ' '), "", fullname)] #
    Data_All[grepl(' II$| III$| IV$', fullname), lastname:= trimws(word(fullname, -2, -1))]
    
    # word(strings, -2, -1)
    
    Data_All[, firstname:=trimws(substr(fullname,1,nchar(fullname)-nchar(lastname)-1))]
    # Data_All[,firstname:=sub(paste0(paste0(" ", lastname),".*"), "", fullname)] #
    # View(xData)
    My.Kable.All(Data_All)
    
    Data_All$gender = as.character(NA)
    
    for (k in 1:nrow(Data_All))
    {
      # k = 1
      # Data_All[k]$first1
      # CATln(k)
      myname       = Data_All[k]$fullname
      onename      = word(myname,1)
      my_lastname  = word(myname,-1)
      my_firstname = trimws(gsub(my_lastname, '', myname))
      
      My_Gender    = try(CCPR_GENDER(FirstName=stri_trans_general(str = onename, id = "Latin-ASCII"), min_ratio=0.8))
      if (all(class(My_Gender)!='try-error'))
      {
        Data_All[k]$gender = My_Gender
      } else { My_Gender = as.character(NA)}
    }
    
    Data_All[, updated:=Sys.Date()]
    Data_All[, date:=Sys.Date()]
    Data_All[, codesource:=symbol]
    Data_All = unique(Data_All, by=c('name', 'position'))
  } else {
    Data_All = data.table()
  }
  My.Kable.All(Data_All)
  return(Data_All)
}

#===================================================================================================
GET_BARRONS_COMPANIES_BY_COUNTRY = function(pCountry = 'switzerland', iso3 = "CHE")
  #-------------------------------------------------------------------------------
{
  ToContinu  = T
  pPage      = 1
  All_Companies = data.table()
  
  while (ToContinu)
  {
    pURL = paste0('https://www.barrons.com/market-data/company-list/country/', pCountry, '/', pPage)
    CATln_Border(pURL)
    content  = rvest::read_html(pURL)
    tables   = content %>% html_table(fill = TRUE)
    xName    = as.data.table(tables[[1]])
    nrow(xName)
    if (nrow(xName)>1)
    {
      pWeb   = GET_CURL_FETCH(pURL)
      writeLines(pWeb, 'c:/temp/x.txt')
      xBody  = str.extract(pWeb, 'BACK TO BROWSE</a></div>', '</table></div></div></div></div>') 
      
      writeLines(xBody, 'c:/temp/xbody.txt')
      
      xRows  = unlist(strsplit(xBody, '<tr data-id="CompanyListTable_Table">'))
      length(xRows)
      ToPrompt = F
      NbSymbol = as.numeric(floor(length(xRows)/2)-1)
      Data_List = list()
      nr = 0
      for (k in 1:NbSymbol)
      {
        # k = 3
        symbol = as.character(NA)
        xRow = xRows[[k+2]]
        link = gsub('"','', str.extract(xRow, '<a href=\"', '">'))
        symbol = str.extract(link, 'stocks/', '?')
        iso2 = sub(paste0(".*", pRef='countrycode='), "", link) 
        name = str.extract(gsub('"','', str.extract(xRow, '<a href=\"', '</td>')), '>', '<')
        if (!is.na(symbol) & nchar(symbol)>0)
        {
          nr = nr +1
          Data_List[[k]] = data.table(nr, symbol, iso2, name, link=paste0('https://www.barrons.com', link))
          if (ToPrompt) { My.Kable.All(Data_List[[k]]) }
        }
      }
      Data_All1 = rbindlist(Data_List, fill=T)
      
      Data_List2 = list()
      nr = 0
      for (k in 1:NbSymbol)
      {
        # k = 1
        symbol = as.character(NA)
        xRow = xRows[[NbSymbol+k+2]]
        # link = gsub('"','', str.extract(xRow, '<a href=\"', '">'))
        country = str.extract(xRow, '"CompanyListTable_Table\">', '<')
        exchange = str.extract(xRow, '"CompanyListTable_Table_19\">', '<')
        supersector = str.extract(xRow, '"CompanyListTable_Table_20\">', '<')
        
        nr = nr +1
        Data_List2[[k]] = data.table(nr, country, exchange, supersector)
        if (ToPrompt) { My.Kable.All(Data_List2[[k]]) }
      }
      Data_All2 = rbindlist(Data_List2, fill=T)
      Data_All12 = merge(Data_All1, Data_All2, by='nr')
      Data_All12[, name:=toupper(name)]
      My.Kable.TB(Data_All12)
      All_Companies = rbind(All_Companies, Data_All12, fill=T)
    } else { ToContinu = F }
    pPage = pPage+1
  }
  
  All_Companies[!(nchar(supersector)==0 | 
                    grepl('Exchange-Traded Funds', supersector, ignore.case = T) | 
                    grepl('Investment Advisors', supersector, ignore.case = T)
  ), stk:=1]
  My.Kable.TB(All_Companies)
  My.Kable.TB(All_Companies[stk==1])
  
  My_List   = All_Companies[stk==1]
  File_List = paste0(ODDrive, paste0('BeQ/PEH/DATA/BARRONS/LIST_BAR_STK_',iso3,'.rds'))
  saveRDS(My_List, File_List)
  
  return(All_Companies[stk==1])
}

#===================================================================================================
CCPR_GENDER = function(FirstName='Timothy', min_ratio=0.8, ToPrompt=F)
  #--------------------------------------------------------------------------------------------------
{
  x.gender = as.character(NA)
  xGender = as.data.table(gender(FirstName))
  if (nrow(xGender)==1)
  {
    x.gender = xGender$gender
    if (x.gender=='male') { x.ratio = xGender$proportion_male }
    if (x.gender=='female') { x.ratio = xGender$proportion_female  }
    if (x.ratio < min_ratio) { x.gender = as.character(NA)}
  }
  if (is.na(x.gender))
  {
    xGender = try(setDT(jsonlite::fromJSON(paste0('https://api.genderize.io/?name=', FirstName))))
    if (all(class(xGender)!='try-error'))
    {
      if (nrow(xGender)==1)
      {
        x.gender = xGender$gender
        x.ratio = xGender$probability
        
        if (x.ratio < min_ratio) { x.gender = as.character(NA)}
      }
    }
  }
  if (ToPrompt) { CATln_Border(paste('GENDER OF ', FirstName, ' = ', x.gender, 100*x.ratio,'%')) }
  return(x.gender)
}

#===================================================================================================
GET_RTS_BOARD_BY_CODE = function(pCode='APL.PS')
  #-------------------------------------------------------------------------------------------------
{
  # z = GET_RTS_BOARD_BY_CODE(pCode='APL.PS')
  # pURL = 'https://www.reuters.com/markets/companies/APL.PS/'
  pURL = paste0('https://www.reuters.com/markets/companies/', pCode, '/')
  CATln_Border(pURL)
  # content  = rvest::read_html(pURL)
  # tables   = content %>% html_table(fill = TRUE)
  # xData    = as.data.table(tables[[1]])
  # nrow(xData)
  
  pWeb   = GET_CURL_FETCH(pURL)
  # writeLines(pWeb, 'c:/temp/x.txt')
  if (grepl('<dl title="Executive Leadership"', pWeb))
  {
    xBody = str.extract(pWeb, '<dl title="Executive Leadership"', '</dl>')
    xRows = unlist(strsplit(xBody, '<dt data-testid="Heading"'))
    
    
    Data_List = list()
    for (k in 2:length(xRows))
    {
      xRow = xRows[[k]]
      xCols = unlist(strsplit(xRow, '>'))
      name      = sub(paste0(pRef='<',".*"), "", xCols[[2]])
      position  = sub(paste0(pRef='<',".*"), "", xCols[[4]])
      Data_List[[k]] = data.table(codesource=pCode, date=Sys.Date(), name, position)
    }
    Data_All = rbindlist(Data_List, fill=T)
    My.Kable.All(Data_All)
  }
  return(Data_All)
}

#===============================================================================
GET_SRC_BOARD_BY_CODE = function(pSource='ENX', pCode='BE0003723377-XBRU', pISO2)
  #-----------------------------------------------------------------------------
{
  switch(pSource,
         'BAR' = { MyData = try(GET_BARRONS_BOARD(pSymbol=pCode, pISO2 = pISO2))},
         'ENX' = { MyData = try(GET_ENX_BOARD_BY_CODE(pCode=pCode))},
         'RTS' = { MyData = try(GET_RTS_BOARD_BY_CODE(pCode=pCode))},
         'INV' = { MyData = try(GET_INV_BOARD_BY_CODE(pCode=pCode))}
  )
  if (all(class(MyData)!='try-error'))
  {
    MyResult = MyData
  } else { MyResult = data.table()}
  My.Kable.All(MyResult)
  return(MyResult)
}

#===============================================================================
GET_RTS_BOARD_BY_CODE = function(pCode='APL.PS')
  #-----------------------------------------------------------------------------
{
  # z = GET_RTS_BOARD_BY_CODE(pCode='APL.PS')
  # pURL = 'https://www.reuters.com/markets/companies/APL.PS/'
  pURL = paste0('https://www.reuters.com/markets/companies/', pCode, '/')
  CATln_Border(pURL)
  # content  = rvest::read_html(pURL)
  # tables   = content %>% html_table(fill = TRUE)
  # xData    = as.data.table(tables[[1]])
  # nrow(xData)
  
  pWeb   = GET_CURL_FETCH(pURL)
  # writeLines(pWeb, 'c:/temp/x.txt')
  if (grepl('<dl title="Executive Leadership"', pWeb))
  {
    xBody = str.extract(pWeb, '<dl title="Executive Leadership"', '</dl>')
    xRows = unlist(strsplit(xBody, '<dt data-testid="Heading"'))
    
    
    Data_List = list()
    for (k in 2:length(xRows))
    {
      xRow = xRows[[k]]
      xCols = unlist(strsplit(xRow, '>'))
      name      = sub(paste0(pRef='<',".*"), "", xCols[[2]])
      position  = sub(paste0(pRef='<',".*"), "", xCols[[4]])
      Data_List[[k]] = data.table(codesource=pCode, date=Sys.Date(), name, position)
    }
    Data_All = rbindlist(Data_List, fill=T)
    My.Kable.All(Data_All)
  }
  return(Data_All)
}

#===============================================================================
GET_INV_BOARD_BY_CODE = function(pCode='equities/bank-of-america-corp')
  #-----------------------------------------------------------------------------
{
  # z = GET_RTS_BOARD_BY_CODE(pCode='APL.PS')
  # pURL = ' https://www.investing.com/equities/jumia-tech-drc-company-profile '
  # pCode = 'equities/pan-american-silver-drc-ba'
  # pCode='equities/bank-of-america-corp'
  pURL = paste0('https://www.investing.com/', pCode, '-company-profile')
  # pURL = 'https://www.investing.com/equities/bank-of-america-corp'
  CATln_Border(pURL)
  content  = rvest::read_html(pURL)
  tables   = content %>% html_table(fill = TRUE)
  xData    = SELECT_TABLE_WITH_FIELD(tables=tables, pField='title', pContent="", like=T)
  if (nrow(xData)>0)
  {
    xData = CLEAN_COLNAMES(xData)
    xData[, ':='(source='INV', codesource=pCode, position=title)]
    My.Kable.TB(xData)
    My.Kable.All(xData)
  }
  return(xData)
}


#===============================================================================
GET_ENX_BOARD_BY_CODE = function(pCode    = 'BE0003723377-XBRU')
  #-----------------------------------------------------------------------------
{
  # z = GET_ENX_BOARD_BY_CODE(pCode    = 'BE0003723377-XBRU')
  pURL     = paste0('https://live.euronext.com/en/cofisem-public/', pCode)
  CATln_Border(pURL)
  content  = rvest::read_html(pURL)
  tables   = content %>% html_table(fill = TRUE)
  if (length(tables)>0)
  {
    # xData    = SELECT_TABLE_WITH_FIELD(tables=tables, pField='title', pContent="", like=T)
    xData    = as.data.table(tables[[1]])
    xData    = CLEAN_COLNAMES(xData)
    xData = xData[-c(nrow(xData))][, .(x1, x2)]
    colnames(xData) = c('position', 'name')
    xData[, ':='(date=Sys.Date(), source='ENX', codesource=pCode)]
    xData$ceo=as.numeric(NA)
    xData$cob=as.numeric(NA)
    xData[position=='Chief Executive Officer', ceo:=1]
    My.Kable.All(xData)
  }
  return(xData)
}
#========================================================================================
GET_YAH_STATISTICS_PRICE = function(pCode = '^SPX')
{
  # pCode = '8834.KL'
  # pCode = 'AC.PA'
  # pCode = 'IBM'
  pURL  = paste0('https://finance.yahoo.com/quote/', pCode, '?p=', pCode)
  pWeb  = GET_CURL_FETCH(pURL)
  
  Str.Last  = str.extract(pWeb, '<div class="D(ib) Mend(20px)">', '</div></div>')
  xList     = unlist(strsplit(Str.Last, 'fin-streamer'))
  # xList
  
  Str.Cur = str.extract(pWeb, '<div class="C($tertiaryColor) Fz(12px)">', '</div>')
  if (grepl('Currency in', Str.Cur)) { x.Currency = str.extract(Str.Cur, 'Currency in ', '</span>')} else { x.Currency = as.character(NA)}
  # data.table(cur=x.Currency)
  
  Str.price = str.extract(xList[[2]],'value=','<')
  Str.price = CONVERT_NUMBER(sub(paste0(".*", pRef='>'), "", Str.price))
  
  #as.numeric(trimws(substr(Str.price,2, nchar(Str.price)-1)))
  
  Str.change = str.extract(xList[[4]],'value=','<')
  Str.change = sub(paste0(pRef=" ",".*"), "", Str.change)
  Str.change = as.numeric(trimws(substr(Str.change,2, nchar(Str.change)-1)))
  
  Str.var = str.extract(xList[[6]],'value=','<')
  Str.var = sub(paste0(pRef=" ",".*"), "", Str.var)
  Str.var = as.numeric(trimws(substr(Str.var,2, nchar(Str.var)-1)))
  
  Str.time = str.extract(xList[[11]],'<span>','</span>')
  Str.times = unlist(strsplit(Str.time, '  '))
  if (grepl('At close', Str.times[[1]])) { x.Status = 'CLOSE'} else { x.Status = 'OPEN' }
  x.Time  =  Str.times[[2]]
  tab.res = data.table(codesource=pCode, source='YAH', cur=x.Currency, last=Str.price, change=Str.change, var=Str.var, 
                       datetime=paste(Sys.Date(), as.character(gsub(x.Time,'. Market open.',''))), status=x.Status, updated=Sys.Date())
  if (x.Status=='CLOSE')
  {
    
  }
  My.Kable.All(tab.res)
  return(tab.res)
  # END.
}

# #===============================================================================
# My_CAPIUSD = DOWNLOAD_YAH_STK_CAPIUSD_BY_CODE(pCodesource='PTT.BK', EchoOn=T, ToKable=T)

# ==================================================================================================
DOWNLOAD_YAH_STK_CAPIUSD_BY_CODE = function(pCodesource='CYBQF', EchoOn=T, ToKable=T) {
  # ------------------------------------------------------------------------------------------------
  xRES     = data.table()
  pURL     = paste0('https://finance.yahoo.com/quote/', pCodesource)
  CATln_Border(pURL)
  content  = rvest::read_html(pURL)
  tables   = content %>% html_table(fill = TRUE)
  if (length(tables)>=2)
  { ToContinu = T } else { ToContinu = F }
  
  if (ToContinu)
  {
    xDATA    = as.data.table(tables[[2]])
    if (EchoOn) { My.Kable.All(xDATA) }
    
    if (nrow(xDATA[X1=="Market Cap"])==1) 
    { 
      temp  = trimws(xDATA[X1=="Market Cap"]$X2)
      xunit = substr(temp,nchar(temp), nchar(temp))
      switch(xunit,
             'T' = {capi_unit = 1000000000000},
             'B' = {capi_unit = 1000000000},
             'M' = {capi_unit = 1000000}
      )
      if (EchoOn) { print(paste("IN : ", temp, "/", xunit))}
      
      pWeb     = GET_CURL_FETCH(pURL)
      if (grepl('Currency in', pWeb)) { cur = str.extract(pWeb, 'Currency in ', '</span>') } else { cur = as.character(NA) }
      rm(xRES)
      xRES = data.table(source='YAH', dataset='CAPITALISATION', codesource=pCodesource, date=SYSDATETIME(23), 
                        close_unit = as.numeric(gsub(',','', substr(temp,1,nchar(temp)-1))),
                        unit = capi_unit, cur = cur)
      xRES[, close:=close_unit*unit]
      
      if (!is.na(cur) && nchar(cur)>0)
      {
        CATln(paste("CUR = ", cur))
        cur_iso = cur ; cur_ratio = 1
        if (cur=='GBp (0.01 GBP)') { cur_iso = 'GBP'; cur_ratio = 100 }
        if (cur=='ILA (0.01 ILS)') { cur_iso = 'ILS'; cur_ratio = 100 }
        xCURS       = try(CUR_CONVERT_XE(cur_fr=cur_iso, cur_to='USD'))
        if (all(class(xCURS)!='try-error'))
        {
          cur_convert = (as.numeric(xCURS[1]))
          xRES[, capiloc:=close]
          xRES[, ':='(cur_convert=cur_convert, capiusd=close*cur_convert, cur_iso=cur_iso)]
          # xRES = MERGE_CODESOURCE(pData=xRES, pSource='YAH')
          # xRES = UPDATE_FIELD_HOME(MERGE_DATASTD_BYCODE(xRES))
          if (ToKable) { 
            # CATln(pCodesource)
            My.Kable.All(xRES[, -c('continent', 'country', 'type', 'fcat', 'dataset', 'close')])
          }
        }
      }
    }
  }
  return(xRES)
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
CUR_CONVERT_XE = function(cur_fr='USD', cur_to='JPY') {
  # ------------------------------------------------------------------------------------------------
  cur_convert = as.numeric(NA)
  cur_data    = data.table()
  xURL     = paste0('https://www.xe.com/currencyconverter/convert/?Amount=1&From=', cur_fr, '&To=', cur_to)
  content  = rvest::read_html(xURL)
  tables   = content %>% html_table(fill = TRUE)
  xCUR     = as.data.table(tables[[1]])
  if (nrow(xCUR)>=10)
  {
    # str(xCUR)
    colnames(xCUR) = c('cur_fr', 'cur_to')
    cur_convert    = as.numeric(gsub(',','', sub(paste0(' ',".*"), "", xCUR[1]$cur_to)))
    cur_data       = data.table(source='XE', code=paste0("CUR", cur_fr, cur_to), date=Sys.Date(), cur_convert=cur_convert)
  }
  return(c(cur_convert, cur_data))
}

#================================================================================
EXECUTION_LIST_LOOP_CODESOURCE_DAY = function(pAction='YAH_CAPIUSD', MyList, SaveFile=paste0(UData, 'DAY/DOWNLOAD_YAH_INSSTK_'), 
                                              SaveHistory, NbMin=21, Nb_Group=10, pSleep=0, ToKablek=T)
  #-----------------------------------------------------------------------------
{
  # MyList     = List_STK
  Nb_Total   = nrow(MyList)
  MyDate     = Sys.Date()
  SaveFileTo = paste0(SaveFile, gsub('-', '', MyDate), '.rds')
  CATln_Border(paste('SAVE TO =', SaveFileTo))
  
  if (file.exists(SaveFileTo))
  {
    CATln_Border(paste(SaveFileTo, '=', file.exists(SaveFileTo)))
    Data_Old = readRDS(SaveFileTo)
    My.Kable.MaxCols(Data_Old)
    MyList   = MyList[!codesource %in% Data_Old$codesource]
  } else { Data_Old = data.table() }
  
  Data_List = list()
  Nb_Todo = min(NbMin, nrow(MyList))
  
  CATln_Border(paste('Nb_Todo =', Nb_Todo))
  
  for (k in 1:Nb_Todo)
  {
    # k = 1
    pCode = MyList[k]$codesource
    CATln('')
    CATln_Border(paste(k, '/', Nb_Todo, '/', Nb_Total, '=', pCode))
    
    switch(pAction,
           'YAH_CAPIUSD' = {    x = try(DOWNLOAD_YAH_STK_CAPIUSD_BY_CODE(pCodesource=pCode, EchoOn=F, ToKable=T)) }
    )
    Sys.sleep(pSleep)
    if (!is.null(x) && all(class(x)!='try-error') && nrow(x)>0)
    {
      x[, updated:=Sys.time()]
      x = CLEAN_COLNAMES(x)
      if (ToKablek) { My.Kable.TB(x[, -c('updated')]) }
      Data_List[[k]] = x
    }
    if (k %% Nb_Group == 0 | k==Nb_Todo)
    {
      CATln_Border("SAVING ...")
      Data_All = rbindlist(Data_List, fill=T)
      Data_Old = rbind(Data_Old, Data_All, fill=T)
      My.Kable.MaxCols(Data_Old)
      saveRDS(Data_Old, SaveFileTo)
    }
  }
}


# ==================================================================================================
MERGE_CODESOURCE = function(pData, pSource='YAH', ToKable=F) {
  
  # My.Kable.INSREF(ins_ref[iso2=='FR' & type=='STK' & grepl('BNP', name)])
  # 
  # ins_ref[!is.na(yah)][trimws(yah)=='BNP:EPA'][, .(codesource=yah, code, name=short_name)]
  
  # pData = xRES
  switch(pSource,
         'YAH' = { pData = merge(pData[, -c('code', 'name')], ins_ref[!is.na(yah)][, .(codesource=yah, code, name=short_name)], all.x=T, by='codesource')} ,
         'BLG' = { pData = merge(pData[, -c('code', 'name')], ins_ref[!is.na(blg)][, .(codesource=blg, code, name=short_name)], all.x=T, by='codesource')} ,
         'RTS' = { pData = merge(pData[, -c('code', 'name')], ins_ref[!is.na(rts)][, .(codesource=rts, code, name=short_name)], all.x=T, by='codesource')} ,
         'EIK' = { pData = merge(pData[, -c('code', 'name')], ins_ref[!is.na(eik)][, .(codesource=eik, code, name=short_name)], all.x=T, by='codesource')} ,
         'INV' = { pData = merge(pData[, -c('code', 'name')], ins_ref[!is.na(inv)][, .(codesource=inv, code, name=short_name)], all.x=T, by='codesource')} ,
         'BIN' = { pData = merge(pData[, -c('code', 'name')], ins_ref[!is.na(bin)][, .(codesource=bin, code, name=short_name)], all.x=T, by='codesource')} ,
         'GOG' = { pData = merge(pData[, -c('code', 'name')], ins_ref[!is.na(gog)][, .(codesource=gog, code, name=short_name)], all.x=T, by='codesource')} ,
         'QDL' = { pData = merge(pData[, -c('code', 'name')], ins_ref[!is.na(qdl)][, .(codesource=qdl, code, name=short_name)], all.x=T, by='codesource')} ,
         'ENX' = { pData = merge(pData[, -c('code', 'name')], ins_ref[!is.na(enx)][, .(codesource=enx, code, name=short_name)], all.x=T, by='codesource')} ,
         'NSD' = { pData = merge(pData[, -c('code', 'name')], ins_ref[!is.na(nsd)][, .(codesource=nsd, code, name=short_name)], all.x=T, by='codesource')} 
  )
  pData = pData %>% select(code , name, everything())
  if (ToKable) { ToKable = T; My.Kable.MaxCols(pData) }
  return(pData)
}


# ==================================================================================================
UPDATE_FIELD_HOME = function(pData) {
  # ------------------------------------------------------------------------------------------------
  # if ('stkhome' %in% names(pData) & 'indhome' %in% names(pData))
  # {
  #   pData[!is.na(stkhome), home:=stkhome]
  #   pData[!is.na(indhome), home:=indhome]
  #   if ('curhome' %in% names(pData))  { pData[!is.na(curhome), home:=curhome]}
  #   pData = pData[, -c('indhome', 'stkhome', 'curhome')]
  # }
  if ('stksample' %in% names(pData)) { pData[!is.na(stksample), sample:=stksample] }
  if ('indsample' %in% names(pData)) { pData[!is.na(indsample), sample:=indsample] }
  
  if ('curhome'   %in% names(pData)) { pData[!is.na(curhome),   home:=curhome] }
  if ('stkhome'   %in% names(pData)) { pData[!is.na(stkhome),   home:=stkhome] }
  if ('indhome'   %in% names(pData)) { pData[!is.na(indhome),   home:=indhome] }
  
  pData = pData[, -c('indhome', 'stkhome', 'curhome', 'stksample', 'indsample')]
  return(pData)
}

# ==================================================================================================
MERGE_DATASTD_BYCODE = function(pData, pOption='STD', ToKable=F) {
  # ------------------------------------------------------------------------------------------------
  switch(pOption,
         "SHORT" = {
           pData = merge(pData[, -c('iso2', 'country', 'continent', 'name', 'type', 'fcat')], 
                         ins_ref[, .(code, iso2, country, continent, name=short_name, type, fcat)], all.x=T, by='code')
         },
         "STD" = {
           pData = merge(pData[, -c('iso2', 'country', 'continent', 'name', 'type', 'fcat', 'indhome', 'stkhome', 'indsample', 'stksample', 'capiusd')], 
                         ins_ref[, .(code, iso2, country, continent, name=short_name, type, fcat, indhome=as.numeric(indhome), 
                                     stkhome, indsample, stksample, capiusd)], all.x=T, by='code')
         },
         "ADVANCED" = {
           pData = merge(pData[, -c('iso2', 'country', 'continent', 'name', 'type', 'fcat', 'indhome', 'stkhome', "isin", 'symbol', 'provider', 'cur', 'capiusd', 
                                    'indsample', 'stksample')], 
                         ins_ref[, .(code, iso2, country, continent, name=short_name, type, fcat, indhome=as.numeric(indhome), 
                                     stkhome, indsample, stksample, isin, symbol, provider, cur, capiusd)], all.x=T, by='code')
         }
  )
  # str(pData)
  pData[, name:=trimws(toupper(name))]
  pData = UPDATE_FIELD_HOME(pData)
  # library(data.table)
  # pData = as.data.table(pData)
  pData = pData %>% dplyr::select('iso2', 'country', 'continent', 'type', 'name', 'code', 'date', everything())
  if (ToKable) {  My.Kable.MaxCols(pData) }
  return(pData)
}

#===============================================================================
LIST_YAH_STK_BY_COUNTRY = function(pISO3='TWN', pURLx = 'https://finance.yahoo.com/screener/unsaved/be6ca3f8-c271-4ff4-b08d-64329db7159d', ToCheckFile=T)
  #-------------------------------------------------
{
  # ToCheckFile = T
  ToSkip = F
  if (ToCheckFile)
  {
    # pISO3='SGP'
    FileName = paste0("LIST_YAH_", pISO3, '.rds')       #'THA': SYMBOL CUA THAILAND, CACH TIM SYMBOL: ISO3 ....
    FileNameFull = paste0(ODDrive, 'BeQ/DATA/YAH/LIST/', FileName)
    CATln_Border(FileNameFull)
    if (file.exists(FileNameFull))
    {
      ToSkip = T
    } else { ToSkip = F}
  }
  ToSkip
  if (!ToSkip)
  {
    k = -1
    ToContinu = T
    Data_All = data.table()
    while (ToContinu)
    {
      k = k+1
      pURL     = paste0(pURLx, '?count=100&offset=', k*100)
      CATln('')
      CATln_Border(paste(k+1, ">>>", pURL))
      content  = rvest::read_html(pURL)
      tables   = content %>% html_table(fill = TRUE)
      if (length(tables)>0)
      {
        xData    = try(as.data.table(tables[[1]]))
        Sys.sleep(1)
        if (all(class(xData)!='try-eror'))
        {
          xData[, updated:=Sys.Date()]
          xData[, iso3:=pISO3]
          xData[, country:=countrycode(pISO3, origin = 'iso3c', destination = 'country.name')[1]]
          if (nrow(xData)>1)
          {
            ToContinu = T
            xData = CLEAN_COLNAMES(xData)
            My.Kable(xData)
            Data_All = rbind(Data_All, xData, fill=T)
          } else { ToContinu = F}
        }
      } else { ToContinu = F}
    }
    My.Kable(Data_All)
    CATln_Border(FileName) 
    saveRDS(Data_All, paste0(ODDrive, 'BeQ/PEH/DATA/', FileName))
  }
}

#==============================================================================
CHECK_CONNEXION = function(pURL = 'www.beqholdingsx.com', ToKable=T)
  #----------------------------------------------------------------------------
{
  pWeb   = try(GET_CURL_FETCH(pURL))
  if (!is.null(pWeb) && all(class(pWeb)!='try-error'))
  {
    pResult = data.table(url=pURL, connexion='SUCCESSFUL')
  } else { pResult = data.table(url=pURL, connexion='FAILED') }
  if (ToKable) { My.Kable.All(pResult) }
  return(pResult)
}
# My.Connextion = CHECK_CONNEXION(pURL = 'www.beqholdingsx.com', ToKable = T)
# My.Connextion = CHECK_CONNEXION(pURL = 'www.beqholdings.com', ToKable = T)


# ==================================================================================================
IFRC.CONNEXTION.2023 = function(lhost) {
  # ------------------------------------------------------------------------------------------------
  switch(lhost,
         "ccpr.vn"             = {lmy.connexion = try(dbConnect(MySQL(), user = "ccpr_user",       password = "Ifrc@2023_ccpr",    host = '27.71.235.40', dbname = "ifrc_ccpr"))},
         "ifrc.vn"             = {lmy.connexion = try(dbConnect(MySQL(), user = "dbifrc",          password = "@ifrcDB2022",       host = '27.71.235.71', dbname = "db_ifrcvn"))},
         "datacenter.ifrc.vn"  = {lmy.connexion = try(dbConnect(MySQL(), user = "datacenter_user", password = "@ifrcDB2022",       host = '27.71.235.71', dbname = "wp_datacenter"))},
         "board.womenceoworld" = {lmy.connexion = try(dbConnect(MySQL(), user = "womenworld_user", password = "admin@beqholdings", host = '27.71.235.71', dbname = "db_womenceoworld"))}
  )
  # database_name: db_womenceoworld
  # database_user: womenworld_user
  # database_pasword: admin@beqholdings
  # host: 27.71.235.71
  
  return(lmy.connexion)
}

# ==================================================================================================
IFRC.UPLOAD.RDS.2023 = function(l.host = "ifrc.vn", l.tablename, l.filepath, CheckField=T, ToPrint = F,
                                ToForceUpload=F, ToForceStructure=F) {
  # ------------------------------------------------------------------------------------------------
  # l.host = "strategy.vnefrc.com"; l.tablename = "efrc_str_ifrclab_last"; l.filepath = paste0(UData,"strategy/efrc_str_ifrclab_last.rds")
  # l.host = "ccpr.vn"; l.tablename = "ccpr_indals_history"; l.filepath = paste0(UData,"ccpr_indals_history.rds")
  # l.host = "ccpr.vn"; l.tablename = "ccpr_indcur_history"; l.filepath = paste0(UData,"ccpr_indcur_history.rds")
  
  # CheckField=T; ToPrint = T;  ToForceUpload=T; ToForceStructure=T
  # ToPrint = T
  DATACENTER_LINE_BORDER(paste('IFRC.UPLOAD.RDS = ', l.host, ' >>>', l.tablename))
  if (ToForceUpload) { ToUploadAll=T } else { source("T:/R/CONFIG/UPLOAD_ALL.R", echo = F) }
  if (ToUploadAll)
  {
    ToUploadAll = T
    t1 = Sys.time()
    CATrp("Connecting...")
    conn = try(IFRC.CONNEXTION.2023(l.host),silent = T)
    # print(conn)
    if (all(class(conn)!="try-error"))
    {
      l.data = readRDS(l.filepath)
      
      check_table_exist = setDT(dbGetQuery(conn,statement = paste0('SHOW TABLES LIKE "',l.tablename, '"')))
      dbDisconnect(conn)
      
      if (nrow(check_table_exist)>0)
      {
        colnames(check_table_exist) = c("tablename")
        check_table_exist = check_table_exist[tablename == l.tablename]
      }
      nb = nrow(check_table_exist)
      CATln(paste('nb = ', nb))
      
      # try(dbDisconnect(conn),silent = T)
      if (nb == 1) 
      {
        if (ToForceStructure) { UPLOAD_STRUCTURE_RDS_TO_SQL.2023(FileRDS="", DataRDS=l.data, TableSQL=l.tablename, host=l.host, ToForce=T) }
        if (CheckField)
        {
          conn = try(IFRC.CONNEXTION.2023(l.host),silent = T)
          CATrp("Read Table...")
          host_dt = dbReadTable(conn,l.tablename)
          host_dt = setDT(host_dt)
          ncols0  = colnames(host_dt)
          ncols   = setdiff(ncols0, "id")
          ncols   = (l.data[,colnames(l.data)[which(gsub(" ","\\.",colnames(l.data)) %in% ncols)]])
          l.data  = l.data[, ..ncols]
          try(dbDisconnect(conn),silent = T)
          CATln("Close connexion.")
          # My.Kable(l.data)
        }
        My.Kable.MaxCols(l.data)
        str(l.data)
        if ('updated' %in% names(l.data)) { l.data[, updated:=as.character(updated)] }
        if ('ord' %in% names(l.data)) { l.data[, ord:=as.numeric(ord)] }
        x = IFRC.DROP.CREATE.TABLE.LIKE.ONE.2023(l.host, l.tablename, l.data)
        
      } else if (nb == 0) {
        conn = try(IFRC.CONNEXTION.2023(l.host),silent = T)
        try(UPLOAD_STRUCTURE_RDS_TO_SQL.2023(FileRDS="", DataRDS=l.data, TableSQL=l.tablename, host=l.host, ToForce=T)     )   
        x = IFRC.DROP.CREATE.TABLE.LIKE.ONE.2023(l.host, l.tablename, l.data)
        try(dbDisconnect(conn),silent = T)
        CATln("Close connexion.")
        
      }
      print(paste("IFRC.UPLOAD.RDS >>> path :", l.filepath, "| OK"))
    } else {
      # print(paste("IFRC.UPLOAD.RDS >>> path :", l.filepath, "| NOT OK"))
      CATln(paste0("CONNEXION NOT AVAILABLE"))
      try(dbDisconnect(conn),silent = T) 
    }
    printrep('.',100)
    if(ToPrint) {
      efrc.print.time("Elapsed time = ", t1)
    }
    try(dbDisconnect(conn),silent = T) 
  }
  try(dbDisconnect(conn),silent = T) 
}

# ==================================================================================================
UPLOAD_STRUCTURE_RDS_TO_SQL.2023 = function(FolderStructure=RData, FileRDS, DataRDS, TableSQL, host='ifrc.vn', ToForce=F){
  # ------------------------------------------------------------------------------------------------
  # FileRDS=paste0(UData,"efrc_cmd_history.rds"); DataRDS=""; TableSQL="efrc_cmd_history"; host="ifrc.vn"
  DATACENTER_LINE_BORDER(paste('UPLOAD_STRUCTURE_RDS_TO_SQL = ', host, ' >>>', TableSQL))
  
  if (nchar(FileRDS)>0) { x = DATACENTER_LOADDTRDS(FileRDS) }else{ x = DataRDS }
  # xSTR  = as.data.table(str(x))
  # My.Kable(xSTR)
  # as.list(str(x))
  # typeof(str(x))
  # 
  # typeof(x$iso2)
  # xNAMES = names(x)
  # xNAMES
  xs = sapply(x,class)
  # colnames(x) = tolower(colnames(x))
  str_std = DATACENTER_LOAD_TEXTFILE(paste0(FolderStructure),"sql_structure_standard.txt")
  
  StrField = paste0('CREATE TABLE ',TableSQL,' ( id int NOT NULL AUTO_INCREMENT, ')
  for (k in 1:length(xs))
  {
    # k = 12
    xName = names(xs[k])
    xType = xs[[k]][1]
    switch(xType,
           
           "character" = {
             x1 = x[, ..xName]
             setnames(x1, xName, 'field')
             if (nrow(x1[!is.na(field)])==0) { nLen=1 } else { nLen = ifelse(nrow(str_std[field==xName])==1,
                                                                             max(str_std[field==xName]$len,nchar(x1[!is.na(field)]$field)),
                                                                             max(nchar(x1[!is.na(field)]$field))) }
             # nLen = floor(1.25*nLen)
             xEnd = ifelse(k < length(xs), ", ", ", PRIMARY KEY (id))")
             Str_One = paste0('`',xName,'`', ' varchar', '(', nLen, ') ', xEnd )
             StrField = paste0(StrField, Str_One)
             # Buu lam tiep duoc khong?
           },
           
           "Date" = {
             x1 = x[, ..xName]
             setnames(x1, xName, 'field')
             xEnd = ifelse(k < length(xs), ", ", ", PRIMARY KEY (id))")
             Str_One = paste0('`',xName,'`', ' date', xEnd )
             StrField = paste0(StrField, Str_One)
             # Buu lam tiep duoc khong?
           },
           
           "numeric" = {
             x1 = x[, ..xName]
             setnames(x1, xName, 'field')
             xEnd = ifelse(k < length(xs), ", ", ", PRIMARY KEY (id))")
             Str_One = paste0('`',xName,'`', ' float', xEnd )
             StrField = paste0(StrField, Str_One)
             # Buu lam tiep duoc khong?
           },
           {
             x1 = x[, ..xName]
             setnames(x1, xName, 'field')
             if (nrow(x1[!is.na(field)])==0) { nLen=1 } else { nLen = ifelse(nrow(str_std[field==xName])==1,
                                                                             max(str_std[field==xName]$len,nchar(x1[!is.na(field)]$field)),
                                                                             max(nchar(x1[!is.na(field)]$field))) }
             # nLen = floor(1.25*nLen)
             xEnd = ifelse(k < length(xs), ", ", ", PRIMARY KEY (id))")
             Str_One = paste0('`',xName,'`', ' varchar', '(', nLen, ') ', xEnd )
             StrField = paste0(StrField, Str_One)
           }
    )
  }
  
  CATln(StrField)
  
  conn = try(IFRC.CONNEXTION.2023(host),silent = T)
  if (ToForce) { stm1 = paste0("drop table if exists ", TableSQL); try(dbGetQuery(conn,statement = stm1)) }
  try(dbGetQuery(conn,statement = StrField))
  
}

# ==================================================================================================
IFRC.DROP.CREATE.TABLE.LIKE.ONE.2023 = function(l.host = "commo.ifrc.vn", l.tablename, l.data, ToPrint = T) {
  # ------------------------------------------------------------------------------------------------
  my_time = as.character(as.numeric(Sys.time())*100000)
  stm1 = paste0("drop table if exists tempx_", l.tablename, "_", my_time)
  stm2 = paste0("create table tempx_", l.tablename, "_", my_time, " like ", l.tablename)
  stm3 = paste0("insert into tempx_", l.tablename, "_", my_time)
  stm4 = paste0("drop table if exists ", l.tablename)
  stm5 = paste0("rename table tempx_", l.tablename, "_", my_time, " to ", l.tablename)
  conn = IFRC.CONNEXTION.2023(l.host)
  if (ToPrint) {print(stm1, quote = F)};dbGetQuery(conn, statement = stm1)
  if (ToPrint) {print(stm2, quote = F)};dbGetQuery(conn, statement = stm2)
  if (ToPrint) {print(stm3, quote = F)};dbWriteTable(conn, paste0("tempx_", l.tablename, "_", my_time), l.data, field.types = NULL, row.names = F, append = TRUE)
  
  if (ToPrint) {print(stm4, quote = F)};dbGetQuery(conn, statement = stm4)
  if (ToPrint) {print(stm5, quote = F)};dbGetQuery(conn, statement = stm5)
  dbDisconnect(conn)
  print(paste("DROP > CREATE LIKE > NAME >", l.tablename, "HOST: ",l.host, "...DONE..."))
}

# ==================================================================================================
IFRC_RELEASE_MEMORY = function(MyText, ToPrint=T) {
  # ------------------------------------------------------------------------------------------------
  Mem.Init  = memory.size()
  if (!is.infinite(Mem.Init))
  {
    Mem.GC    = gc()
    Mem.Final = memory.size()
    Mem.Msg   = paste(MyText, Mem.Init, " >> ", Mem.Final)
    if (ToPrint)  {print(Mem.Msg, quote = F)}
  }
}

#===================================================================================================
IFRC_SLEEP = function(NbSeconds) {
  # ------------------------------------------------------------------------------------------------
  # IFRC_SLEEP(NbSeconds=20)
  # time1 = Sys.time()
  for (i in NbSeconds:1)
  {
    # i=2; NbSeconds = 10
    CATrp(paste("TIMER = ", FormatNS(i,0,10), FormatNS(NbSeconds,0,10)))
    Sys.sleep(1)
  }
  # CATln(Sys.time()-time1)
  CATrp("")
}

# ==================================================================================================
FormatNS = function(l.Num, l.Dec, l.Wid, l.Sep = "|") {
  # ------------------------------------------------------------------------------------------------
  l.res = paste(format(Format.Number(l.Num, l.Dec), width = l.Wid, justify = "right"), l.Sep)
  return(l.res)
}

#==================================================================================================
GET_VST_BOARD_MEMBER_BY_CODE = function(pCode = 'VND')
  
{
  Start.time = Sys.time()
  Data_Final = data.table()
  pURL  = paste0('https://finance.vietstock.vn/', pCode, '/board-of-management.htm?languageid=2')
  CATln_Border(pURL)
  content  = rvest::read_html(pURL)
  tables   = content %>% html_table(fill = TRUE)
  # length(tables)
  if (length(tables)>1)
  {
    Data_List = list()
    for (k in 2:length(tables))
    {
      Data_List[[k]] = as.data.table(tables[[k]])
    }
    Data_All = rbindlist(Data_List, fill=T)
    Data_All = CLEAN_COLNAMES(Data_All)
    colnames(Data_All)[1]='date'
    Data_All[, date:=as.Date(date, '%m/%d/%Y')]
    Data_All$gender = as.character(NA)
    Data_All[grepl('^Ông ', fullname), gender:='M']
    Data_All[grepl('^Bà ', fullname), gender:='F']
    Data_All[, fullname:=gsub('^Bà ','', fullname)] 
    Data_All[, fullname:=gsub('^Ông ','', fullname)] 
    # My.Kable(Data_All)
    # str(Data_All)
    Data_All[, positions:=trimws(positions)]
    Data_All[nchar(trimws(education))==0, education:=as.character(NA)]
    Data_All[grepl('N/a|NA', education), education:=as.character(NA)]
    Data_All[grepl('NA|-', yearofbirth), yearofbirth:=as.character(NA)]
    
    Data_All[grepl('N/A', time), time:=as.character(NA)]
    Data_All$independence=as.numeric(NA)
    Data_All[grepl('Independence', time), independence:=1]
    Data_All[grepl('Independence', time), time:=gsub('Independence', "", time)]
    Data_All[nchar(shares)>0, shares:=gsub(',','',shares)]
    Data_All[nchar(shares)==0, shares:=as.character(NA)]
    Data_All[, shares:=as.numeric(shares)]
    #My.Kable(Data_All)
    
    Data_Final = Data_All[, .(date, codesource=pCode, ticker=pCode, source='VST', gender, fullname_lc=fullname, positions, since=time, 
                              birthyear=as.numeric(as.character(yearofbirth)), education=as.character(education), independence)]
    Data_Final[, fullname:=trimws(decodeVN(fullname_lc, from='Unicode', to='Unicode', diacritics=F))] 
    Data_Final[, lastname:=toupper(stringr::word(fullname, 1))]
    Data_Final[, firstname:=substr(fullname, nchar(lastname)+1, nchar(fullname))]
    
    My.Kable.TB(Data_Final[, -c('fullname', 'independence')])
    End.time = Sys.time()
    CATln(paste('Duration = ', Format.Number(difftime(End.time, Start.time, units='sec'),2)))
  }
  
  return(Data_Final)
}

# ==================================================================================================
DATACENTER_LOAD_TEXTFILE = function(p_folder, p_filename, p_active=T, EchoOn=F) {
  # ------------------------------------------------------------------------------------------------
  # p_folder = "u:/efrc/data/"; p_filename="list_demo_yah.txt"; p_active=T
  l_fullname = paste0(p_folder, p_filename)
  if (EchoOn) { CATrp(paste('Loading : ', l_fullname)) }
  if (file.exists(l_fullname))
  {
    l_data = setDT(fread(l_fullname, sep="\t"))
    if (EchoOn) { CATln(paste(paste(l_fullname), '=', nrow(l_data), "rows")) } else { CATln('')}
    # colnames(l_data) = tolower(colnames(l_data))
    if ("active" %in% colnames(l_data) & p_active) { l_data = l_data[active==1]}
    if (EchoOn) { My.Kable.TB(l_data) }
  } else {
    DATACENTER_LINE_BORDER(paste("DATACENTER_LOAD_TEXTFILE :", l_fullname, " - FILE DOES NOT EXIST"))
    l_data=data.table()
  }
  CATrp('')
  return(l_data)
}
#============================================================================================

RUN_LOOP_VST_BOARD_MEMBER = function()
{
  List_Companies =  readRDS("R:/DATA/download_exc_stkvn_ref_history.rds")
  List_Codes = unique(List_Companies[,.(code,source,codesource,name),by ='codesource'])
  My.Kable(List_Codes)
  
  CurrentYYYYMM = substr(gsub('-','', as.character(Sys.Date())),1,6)
  FileName    = 'DOWNLOAD_VST_STKVN_BOARD_MEMBERS'
  FilePath    = paste0('R:/WOMEN_CEO/DATA/VST/',FileName, '_', CurrentYYYYMM, '.rds')
  CATln_Border(FilePath)
  
  if (file.exists(FilePath))
  {
    Data_old = readRDS(FilePath)
    List_Codes_ToDo = List_Codes[!codesource %in% Data_old$ticker]
  } else { 
    Data_old = data.table() 
    List_Codes_ToDo = List_Codes
  }
  
  List_Codes_ToDo = List_Codes_ToDo[order(codesource)]
  My.Kable(List_Codes_ToDo)
  
  # x = GET_VST_BOARD_MEMBER_BY_CODE(pCode = 'YEG')
  
  Nb_Group = 50; Nb_Min = 50000
  
  Data_All = data.table()
  
  # FileName = paste0("LIST_YAH_", pISO3, '.rds') 
  # CATln_Border(FileName)
  Data_List = list()
  
  FileData = List_Codes_ToDo
  FileRow  = nrow(List_Codes_ToDo)
  
  Nb_TODO  = min(Nb_Min, FileRow)
  
  FileToSave = FilePath
  for (i in 1:Nb_TODO)
  {
    # i =1
    pCode    = FileData[i]$codesource
    CATln("")
    CATln_Border(paste(i, '/', Nb_TODO, ' = ', pCode))
    My_Board = try(GET_VST_BOARD_MEMBER_BY_CODE(pCode = pCode))
    if (all(class(My_Board)!='try-error'))
    {
      Data_List[[i]] = My_Board
    }
    Sys.sleep(2)
    if (i %% Nb_Group ==0 | i == Nb_TODO)
    {
      Data_All = rbindlist(Data_List, fill=T)
      saveRDS(Data_All, FileToSave)
      CATln('SAVED')
      Sys.sleep(2)
    }
  }
  My.Kable(Data_All)
}
#==================================================================================
GET_YAH_STK_SECTOR_WEBSITE = function(pCode='TSLA', ToPrint = T, ToKable=T)
{
  # x = GET_YAH_STK_SECTOR_WEBSITE(pCode='GOOG', ToPrint = T)
  # x = GET_YAH_STK_SECTOR_WEBSITE(pCode='IBM', ToPrint = T)
  # x = GET_YAH_STK_SECTOR_WEBSITE(pCode='29M.AX', ToPrint = T)
  # x = GET_YAH_STK_SECTOR_WEBSITE(pCode='VMC', ToPrint = T)
  # YAHOO WEBSITE, SECTOR :
  # paste('A', 'B')
  # paste0('A', 'B')
  # pCode    = 'XXX'
  # MyRes    = data.table()
  pURL     = paste0('https://finance.yahoo.com/quote/', pCode, '/profile?p=', pCode)
  CATln_Border(pURL)
  pWeb     = GET_CURL_FETCH(pURL)
  pTable_Info = str.extract(pWeb,'<div class="Pt(10px) smartphone_Pt(20px) Lh(1.7)" data-test="qsp-profile">','<div id="defaultLREC-sizer" class="darla-container D-n D(n) lrec-ldrb-ad darla-nonlrec34-ad darla-nonlrec34ldrb-ad darla-lrec-ad"')
  
  # pPhone = str.extract(pTable_Info,'<a href="tel:','" class="C($linkColor)">')
  # print(pPhone)
  
  xEmployee = gsub(',','',str.extract(pTable_Info,'<span class="Fw(600)"><span>','</span>'))
  
  if (nchar(xEmployee)>30) {    xEmployee =as.character(NA)  }
  if (ToPrint) { print(xEmployee) }
  xCountry = paste0(str.extract(pTable_Info,'<br/>','<br/><a href=\"tel:'),'<br/><a href=\"tel:')
  xCountry = str.extract(xCountry,'<br/>','<br/><a href=\"tel:')
  
  # pLeft   = sub(paste0(pRef,".*"), "", pString)
  # pRight  = sub(paste0(".*", pRef), "", pString)
  # writeLines(pWeb, 'c:/temp/vst.txt')
  
  if (grepl('>', xCountry)) { xCountry = sub(paste0(".*", pRef='>'), "", xCountry)}
  if (ToPrint) { print(xCountry) }
  
  xSector  = str.extract(pWeb, '<span>Sector(s)</span>', '</span>')
  xSector  = sub(paste0(".*", pRef='>'), "", xSector)
  
  if (grepl('>', xSector)) { xSector = sub(paste0(".*", pRef='>'), "", xSector)}
  if (!grepl('%', xSector) & nchar(xSector)<100) { } else { xSector = as.character(NA)}
  if (ToPrint) { print(xSector) }
  
  xWeb = str.extract(pWeb,'rel="noopener noreferrer" target="_blank" class="C($linkColor)" title="">', '</a>' )
  if (grepl('http', xWeb) & nchar(xWeb)<100) { } else { xWeb = as.character(NA)}
  if (ToPrint) { print(xWeb) }
  
  xName_Ticker = str.extract(pWeb,'<meta property="twitter:title" content="',' Company Profile &amp')
  xTicker = str.extract(xName_Ticker,'(',')')
  if (ToPrint) { print(xTicker) }
  
  xName = trim(gsub("\\(.*", "", xName_Ticker))
  if (ToPrint) { print(xName) }
  MyRes   = data.table(codesource=pCode, source='YAH', sector=xSector, website=xWeb,ticker=xTicker,companyname=xName, country=xCountry,employees=as.numeric(xEmployee), updated=Sys.Date())
  if (ToKable) { My.Kable.All(MyRes)}
  return(MyRes)
  # if (grepl('http', xWeb) & nchar(xWeb)<100) { } else { xWeb = as.character(NA)}
  # xResult = c(xSector, xWeb)
  # if (ToPrint) { print (xResult) }
  # if (ToKable) { My.Kable.All(MyRes)}
  # return(MyRes)
}

DOWNLOAD_CAF_STKVN_INTRADAY_DAY = function(pCode="VIC", pDate=format.Date(as.Date(SYSDATETIME(9)), '%d/%m/%Y'), CreateBlank=T)
{
  # x = DOWNLOAD_CAF_STKVN_INTRADAY_DAY(pCode="VIC", pDate=format.Date(as.Date(SYSDATETIME(9)), '%d/%m/%Y'), CreateBlank=T)
  # pDate    = format.Date(as.Date(Sys.Date()-250), '%d/%m/%Y')
  
  # pURL     = 'https://s.cafef.vn/ajax/khoplenh.aspx?order=time&dir=up&symbol=VNM&date=27%2F06%2F2023'
  Final_Data = data.table()
  pURL     = paste0('https://s.cafef.vn/ajax/khoplenh.aspx?order=time&dir=up&symbol=', pCode, '&date=',pDate)
  CATln_Border(pURL)
  content  = rvest::read_html(pURL)
  tables   = content %>% html_table(fill = TRUE)
  xData    = as.data.table(tables[[1]])
  if (ncol(xData)==5 && nrow(xData)>0)
  {
    xData    = CLEAN_COLNAMES(xData)
    Final_Data = xData[, .(codesource=pCode, code=paste0('STKVN', pCode), source='CAF', 
                           date=as.Date(pDate, '%d/%m/%Y'), time=x1, volume=x3, volume_total=as.numeric(gsub(',','', x4)),
                           last=1000*as.numeric(gsub(',','', word(x2, 1, 1))), 
                           change=1000*as.numeric(gsub(',','', word(x2, 2, 2))), 
                           varpc=as.numeric(gsub(',','', gsub('\\(|)|%','', word(x, 3, 3)))) )]
    My.Kable.TB(Final_Data)
  } else { 
    if (CreateBlank) {
      Final_Data = data.table[, .(codesource=pCode, code=paste0('STKVN', pCode), source='CAF', 
                                  date=as.Date(pDate, '%d/%m/%Y'))]
      My.Kable.TB(Final_Data)
    }
  }
  return(Final_Data)
}

DOWNLOAD_YAH_CUR_UNIC = function()
{
  country_ref = readRDS(paste0('R:/CCPR/DATA/', 'efrc_country_ref.rds'))
  My.Kable(country_ref)
  str(country_ref)
  
  List_CUR = unique(country_ref[!is.na(uniccur) & nchar(uniccur)==3]$uniccur)
  print(List_CUR)
  
  Data_list = list()
  for (k in 1:length(List_CUR))
  {
    pCode = paste0(List_CUR[k], '=X')
    CATln_Border(paste(k, pCode))
    My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))
    if (all(class(My_Data)!='try-error'))
    {
      Data_list[[k]] = My_Data
    }
  }
  Data_All = rbindlist(Data_list, fill=T)
  Data_All[, code:=paste0('CURUSD', substr(codesource ,1,3))]
  Data_All = Data_All[!is.na(close) & ! is.na(date)]
  My.Kable.TB(Data_All)
  saveRDS(Data_All, paste0("R:/CCPR/DATA/YAH/CCPR_YAH_CUR_UNIC_PRICES.rds"))
  try(CCPR_INTEGRATION(FileFolder='R:/CCPR/DATA/YAH/', 
                       FileFrom='CCPR_YAH_CUR_UNIC_PRICES.rds', 
                       FileTo='CCPR_YAH_CUR_UNIC_PRICES_HISTORY.rds', GroupBy='codesource x date'))
}

#===============================================================================
# RUN_LOOP_BY_BROUP_AND_SAVE = function(pOption='VST_BOARD_MEMBER', Nb_Group = 50, Nb_Min = 50000, Frequency='MONTH', AddParam='')
# {
#   RData = 'R:/DATA/'
#   source(paste0('R:/R/', 'LIBRARIES/WCEO_LIBRARY.r'))
#   
#   # pOption='VST_BOARD_MEMBER'
#   # pOption='YAH_STK_SECTOR_WEBSITE'
#   # Frequency='DAY'
#   switch(Frequency,
#          'MONTH' = {CurrentFileDate = substr(gsub('-','', as.character(Sys.Date())),1,6)},
#          'DAY'   = {CurrentFileDate = substr(gsub('-','', as.character(Sys.Date())),1,8)}
#   )
#   # CurrentYYYYMM = substr(gsub('-','', as.character(Sys.Date())),1,6)
#   switch(pOption,
#          'YAH_CURCRY'      = {
#            # AddParam = 'BSM'
#            List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_CRYPTO.rds"))
#            List_Codes     = unique(List_Companies[,.(codesource, name, code=paste0("CURCRY", gsub('-USD','',codesource )))], by ='codesource')
#            My.Kable(List_Codes)
#            FileName       = paste0('CCPR_YAH_CURCRY_PRICES')
#            FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
#          },
#          'YAH_STK_BY_SECTOR'      = {
#            # AddParam = 'BSM'
#            List_Companies = readRDS(paste0('R:/CCPR/DATA/', "LIST_YAH_STK_BY_SECTOR_ClEANED.rds"))
#            List_Codes     = unique(List_Companies[,.(codesource, name, sector_code)][sector_code==AddParam], by ='codesource')
#            My.Kable(List_Codes)
#            FileName       = paste0('CCPR_YAH_STK_', AddParam, '_PRICES')
#            FilePath       = paste0('R:/CCPR/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
#          },
#          
#          'VST_BOARD_MEMBER'       = {
#            List_Companies =  readRDS(paste0(RData, "SSI/download_ssi_stkvn_exc_ref.rds"))
#            List_Codes  = unique(List_Companies[,.(code,source,codesource),by ='codesource'])
#            My.Kable(List_Codes)
#            FileName    = 'DOWNLOAD_VST_STKVN_BOARD_MEMBERS'
#            FilePath    = paste0('R:/WOMEN_CEO/DATA/VST/', FileName, '_', CurrentFileDate, '.rds')
#          },
#          'CAF_STKVN_INTRADAY'       = {
#            # pOption     = 'CAF_STKVN_INTRADAY'
#            List_Companies =  readRDS(paste0(RData, "SSI/download_ssi_stkvn_exc_ref.rds"))
#            List_Codes  = unique(List_Companies[,.(code,source,codesource),by ='codesource'])[order(codesource)]
#            My.Kable(List_Codes)
#            FileName    = 'DOWNLOAD_CAF_STKVN_INTRADAY_DAY'
#            FilePath    = paste0(RData, 'CAF/DAY/', FileName, '_', CurrentFileDate, '.rds')
#          },
#          # 'YAH_STK_SECTOR_WEBSITE' = {
#          #   CATln('Loading INS_REF ...')
#          #   ins_ref     = setDT(readRDS('R:/data/efrc_ins_ref.rds'))
#          #   List_Codes  = ins_ref[type=='STK' & !is.na(yah), .(type, iso2, iso3, continent, country, yah, code, name, startdate, enddate, records)]
#          #   List_Codes[, codesource:=yah]
#          #   My.Kable(List_Codes)
#          #   FileName    = 'DOWNLOAD_YAH_STK_SECTOR_WEBSITE'
#          #   FilePath    = paste0('R:/WOMEN_CEO/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
#          # } ,
#          'YAH_STK_SECTOR_WEBSITES' = {
#            CATln('Loading INS_REF ...')
#            ins_ref     = setDT(readRDS(paste0('R:/WOMEN_CEO/DATA/YAH/', 'LIST_YAH_STK.rds')))
#            List_Codes  = ins_ref[!is.na(marketcap), .(codesource, name, updated,iso3, country)]
#            # List_Codes[, codesource:=yah]
#            My.Kable(List_Codes)
#            FileName    = 'DOWNLOAD_YAH_STK_SECTOR_WEBSITE'
#            FilePath    = paste0('R:/WOMEN_CEO/DATA/YAH/', FileName, '_', CurrentFileDate, '.rds')
#          }
#   )
#   
#   CATln_Border(FilePath)
#   
#   if (file.exists(FilePath))
#   {
#     Data_old = readRDS(FilePath)
#     Data_old = Data_old[!is.na(codesource)]
#     List_Codes_ToDo = List_Codes[!codesource %in% Data_old$codesource]
#   } else { 
#     Data_old = data.table() 
#     List_Codes_ToDo = List_Codes
#   }
#   
#   My.Kable.MaxCols(Data_old)
#   # Data_old$coxdesource = NULL
#   # My.Kable.MaxCols(unique(Data_old[, .(codesource)], by='codesource'))
#   # Data_old[, codesource:=ticker]
#   # saveRDS(Data_old, FilePath)
#   
#   List_Codes_ToDo = List_Codes_ToDo[order(-codesource)]
#   My.Kable(List_Codes_ToDo)
#   
#   # x = GET_VST_BOARD_MEMBER_BY_CODE(pCode = 'YEG')
#   Data_All = data.table()
#   
#   # FileName = paste0("LIST_YAH_", pISO3, '.rds') 
#   # CATln_Border(FileName)
#   Data_List = list()
#   
#   FileData = List_Codes_ToDo
#   FileRow  = nrow(List_Codes_ToDo)
#   
#   Nb_TODO  = min(Nb_Min, FileRow)
#   Nb_Total = FileRow
#   
#   FileToSave = FilePath
#   for (i in 1:Nb_TODO)
#   {
#     # i =1
#     pCode    = FileData[i]$codesource
#     CATln("")
#     # CATln_Border(paste(i, '/', Nb_TODO, ' = ', pCode))
#     CATln_Border(paste('RUN_LOOP_BY_BROUP_AND_SAVE : ', i, paste0('(', Nb_Group,')'), '/', Nb_TODO, '/', Nb_Total, ' = ', pCode))
#     
#     
#     switch(pOption,
#            'YAH_CURCRY'      = {
#              My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))
#            },
#            
#            'VST_BOARD_MEMBER'       = {
#              My_Data = try(GET_VST_BOARD_MEMBER_BY_CODE(pCode = pCode))
#            },
#            
#            'YAH_STK_BY_SECTOR'       = {
#              My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))
#            },
# 
#            'YAH_STK_SECTOR_WEBSITE'       = {
#              My_Data = try(GET_YAH_STK_SECTOR_WEBSITE(pCode = pCode, ToPrint = F))
#            },
#            'CAF_STKVN_INTRADAY' = {
#              My_Data = try(DOWNLOAD_CAF_STKVN_INTRADAY_DAY(pCode=pCode, pDate=format.Date(as.Date(SYSDATETIME(9)), '%d/%m/%Y')))
#            }
#     )
#     
#     if (all(class(My_Data)!='try-error'))
#     {
#       Data_List[[i]] = My_Data
#     }
#     Sys.sleep(2)
#     
#     if (i %% Nb_Group ==0 | i == Nb_TODO)
#     {
#       Data_All = rbindlist(Data_List, fill=T)
#       Data_final = rbind(Data_old, Data_All, fill=T)
#       saveRDS(Data_final, FileToSave)
#       CATln(paste('SAVED : ', FileToSave, '-', Format.Number(nrow(Data_final),0),'records'))
#       
#       Sys.sleep(2)
#     }
#   }
#   My.Kable.TB(Data_final)
#   return(Data_final)
# }




RUN_NOSTOP_BY_PC = function(pMyPC)
{
  List_Sector = list(
    c('BSM', 'Basic Materials', 'https://finance.yahoo.com/screener/predefined/ms_basic_materials/'),
    c('CMS', 'Communication Services', 'https://finance.yahoo.com/screener/predefined/ms_communication_services/'),
    c('CNC', 'Consumer Cyclical', 'https://finance.yahoo.com/screener/predefined/ms_consumer_cyclical/'),
    c('CND', 'Consumer Defensive', 'https://finance.yahoo.com/screener/predefined/ms_consumer_defensive/'),
    c('ENY', 'Energy', 'https://finance.yahoo.com/screener/predefined/ms_energy/'),
    c('FNS', 'Financial Services', 'https://finance.yahoo.com/screener/predefined/ms_financial_services/'),
    c('HTC', 'Healthcare', 'https://finance.yahoo.com/screener/predefined/ms_healthcare/'),
    c('IND', 'Industrials', 'https://finance.yahoo.com/screener/predefined/ms_industrials/'),
    c('REA', 'Real Estate', 'https://finance.yahoo.com/screener/predefined/ms_real_estate/'),
    c('TEC', 'Technology', 'https://finance.yahoo.com/screener/predefined/ms_technology/'),
    c('UTI', 'Utilities', 'https://finance.yahoo.com/screener/predefined/ms_utilities/')
  )
  # pMyPC = 'IREEDS'
  switch(pMyPC,
         'IFRC-3630'  = {  
         },
         'IREEDS'  = {   x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='YAH_STK_SIZE_BOARD', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
                                                               AddParam='MEGA'));
         x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='YAH_CAPI_SIZE_DIV', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
                                               AddParam='MEGA'))
         },
         
         '103'     = {  
         },
         
         'BeQ-RD1' = {  
           
           
         },
         
         'BeQ-RD2' = {     x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='YAH_STK_SIZE_BOARD', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
                                                                 AddParam='SMALL')) ;
         x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='YAH_CAPI_SIZE_DIV', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
                                               AddParam='SMALL'))
         
         },
         
         'BeQ-RD3' = {   x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='YAH_STK_SIZE_BOARD', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
                                                               AddParam='LARGE'));
         x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='YAH_CAPI_SIZE_DIV', Nb_Group = 100, Nb_Min = 100000, Frequency='MONTH',AddParam='LARGE'))
         
         },
         
         'BeQ-RD4' = {  
           
         }, 
         'BeQ-RD5' = {
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           #                                       AddParam='LARGE,UNADJUST_ADJUST'))
           x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_BOARD', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
                                                 AddParam='MID')) ;
           x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='YAH_CAPI_SIZE_DIV', Nb_Group = 100, Nb_Min = 100000, Frequency='MONTH',AddParam='MID'))
           
         },
         {
           x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_TOP100', Nb_Group = 100, Nb_Min = 100000, Frequency='DAILY',AddParam='MID'))
         }
  )
  rm(FN_Name_BATCH_FUNCTION)
  FN_Name_BATCH_FUNCTION    = match.call()[[1]]
  FINAL_BATCH_FUNCTION(FNName=FN_Name_BATCH_FUNCTION)
}

# ==================================================================================================
RUN_NOSTOP_BY_PC_CCPR = function(pMyPC, ToForce=F) {
  # ------------------------------------------------------------------------------------------------
  List_Sector = list(
    c('BSM', 'Basic Materials', 'https://finance.yahoo.com/screener/predefined/ms_basic_materials/'),
    c('CMS', 'Communication Services', 'https://finance.yahoo.com/screener/predefined/ms_communication_services/'),
    c('CNC', 'Consumer Cyclical', 'https://finance.yahoo.com/screener/predefined/ms_consumer_cyclical/'),
    c('CND', 'Consumer Defensive', 'https://finance.yahoo.com/screener/predefined/ms_consumer_defensive/'),
    c('ENY', 'Energy', 'https://finance.yahoo.com/screener/predefined/ms_energy/'),
    c('FNS', 'Financial Services', 'https://finance.yahoo.com/screener/predefined/ms_financial_services/'),
    c('HTC', 'Healthcare', 'https://finance.yahoo.com/screener/predefined/ms_healthcare/'),
    c('IND', 'Industrials', 'https://finance.yahoo.com/screener/predefined/ms_industrials/'),
    c('REA', 'Real Estate', 'https://finance.yahoo.com/screener/predefined/ms_real_estate/'),
    c('TEC', 'Technology', 'https://finance.yahoo.com/screener/predefined/ms_technology/'),
    c('UTI', 'Utilities', 'https://finance.yahoo.com/screener/predefined/ms_utilities/')
  )
  # pMyPC = 'IREEDS'
  rm(FN_Name_BATCH_FUNCTION)
  FN_Name_BATCH_FUNCTION    = match.call()[[1]]
  FINAL_BATCH_FUNCTION(FNName=FN_Name_BATCH_FUNCTION)
  
  switch(pMyPC,
         'IFRC-3630'  = {  
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2 (pOption='YAH_CAPI_SIZE_BOARD', Nb_Group = 50, Nb_Min = 10000, Frequency='MONTH', AddParam='LARGE'))
           
           
           STK_DATA = try(LOOP_DOWNLOAD_YAH_STK(Dataset='CLOSE', 
                                                Folder_List=CCPRData, File_List='LIST_YAH_STK_GBL_WCEO', 
                                                Folder_Out=CCPRData, Nb_Min=10000, Nb_Group=50))
           
           x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH',
                                                 AddParam='MEGA,CLOSE'))
         },
         
         'IFRC-IREEDS'  = { 
           if (ToForce | (CHECK_TIMEBETWEEN("19:30", "23:00") & (! lubridate::wday(Sys.Date()) %in% list(7,1))))
           {
             
           }
           
           x = try(RUN_LOOP_STKVN (pOption='FINAL_DOWNLOAD_CAF_STKVN_SHARES', Nb_Group = 50, Nb_Min = 5000,
                                   AddParam='', FillBlank=T, ToForce=F) )
           x = try(RUN_LOOP_STKVN (pOption='FINAL_DOWNLOAD_C68_STKVN_SHARES', Nb_Group = 50, Nb_Min = 5000,
                                   AddParam='', FillBlank=T, ToForce=F) )
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2 (pOption='YAH_CAPI_SIZE_BOARD', Nb_Group = 50, Nb_Min = 10000, Frequency='MONTH', AddParam='MEGA'))
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           #                                       AddParam='MEGA,UNADJUST_ADJUST'))
           
           
           STK_DATA = try(LOOP_DOWNLOAD_YAH_STK(Dataset='CLOSEADJ', 
                                                Folder_List=CCPRData, File_List='LIST_YAH_STK_GBL_WCEO', 
                                                Folder_Out=CCPRData, Nb_Min=10000, Nb_Group=50))
           
         },
         
         'IFRC-103'     = {  
         },
         
         'BeQ-RD1' = {  
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           #                                       AddParam='SMALL,UNADJUST_ADJUST'))
           # 
           x = RUN_LOOP_STKVN (pOption='DOWNLOAD_HNX_STKVN_SHARES_BY_CODE', Nb_Group = 50, Nb_Min = 20000, AddParam='', FillBlank=T, ToForce=F, ToCheckTime=7)
           x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='YAH_CAPI_SIZE_DIV', Nb_Group = 55, Nb_Min = 50000, Frequency='MONTH', AddParam='MID'))
           # SAVERDS_WITH_ONEDRIVE (pData=x, FileFolder=CCPRData, FileName='DOWNLOAD_YAH_STK_SIZE_SMALL_MID_202308.rds' , SaveOneDrive=T)
         },
         
         'BeQ-RD2' = {  
           z = try(RUN_LOOP_STKVN(pOption='DOWNLOAD_SSI_STKVN_PRICES', Nb_Group = 20, Nb_Min = 2000, AddParam='', FillBlank=T, ToForce=F, ToCheckTime=16))
           
           z = try(RUN_LOOP_STKVN(pOption='DOWNLOAD_STB_STKVN_PRICES_BY_CODE', Nb_Group = 20, Nb_Min = 2000, AddParam='', FillBlank=T, ToForce=F, ToCheckTime=16)) # 20
           # STK_DATA = try(LOOP_DOWNLOAD_YAH_STK(Dataset='DIV', 
           #                                      Folder_List=CCPRData, File_List='LIST_YAH_STK_GBL_WCEO', 
           #                                      Folder_Out=CCPRData, Nb_Min=10000, Nb_Group=50))
           x = try(RUN_LOOP_STKVN (pOption='DOWNLOAD_STKVN_OWNERSHIP_FOR_ALL', Nb_Group = 50, Nb_Min = 5000,
                                   AddParam='', FillBlank=T, ToForce=F) )
           
           # UPDATE_UPLOAD_CCPR_MARKET_LAST(UData="R:/CCPR/DATA/", UList= paste0('R:/CCPR/DATA/YAH/LIST/','LIST_YAH_MARKET_LAST.txt'), 
           #                                pSource = 'YAH',  xPeriod = '5m', toReset = F)
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           #                                       AddParam='LARGE,CLOSE'))
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_WCEO_PRICES', Nb_Group = 55, Nb_Min = 50000, Frequency='MONTH', AddParam='ALL,CLOSE'))
           # SAVERDS_WITH_ONEDRIVE (pData=x, FileFolder=CCPRData, FileName='DOWNLOAD_YAH_STK_WCEO_PRICES_CLOSE.rds' , SaveOneDrive=T)
           # Sys.sleep(1600)
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='YAH_CAPI_SIZE_DIV', Nb_Group = 55, Nb_Min = 50000, Frequency='MONTH', AddParam='SMALL'))
           # SAVERDS_WITH_ONEDRIVE (pData=x, FileFolder=CCPRData, FileName='DOWNLOAD_YAH_STK_SIZE_SMALL_DIV_202308.rds' , SaveOneDrive=T)
         },
         
         'BeQ-RD3' = {  
           z = try(RUN_LOOP_STKVN(pOption='FINAL_DOWNLOAD_STB_STKVN_SHARES', Nb_Group = 20, Nb_Min = 2000, AddParam='', FillBlank=T, ToForce=F, ToCheckTime=16))
           # STK_DATA = try(LOOP_DOWNLOAD_YAH_STK(Dataset='CAPIUSD', 
           #                                      Folder_List=CCPRData, File_List='LIST_YAH_STK_GBL_WCEO', 
           #                                      Folder_Out=CCPRData, Nb_Min=10000, Nb_Group=50))
           
           x = try(RUN_LOOP_STKVN (pOption='DOWNLOAD_STB_STKVN_RATIOS_VALUATION', Nb_Group = 50, Nb_Min = 5000,
                                   AddParam='', FillBlank=T, ToForce=F) )
           # x = try(RUN_LOOP_STKVN (pOption='DOWNLOAD_STKVN_SHAREHOLDER_FOR_ALL', Nb_Group = 50, Nb_Min = 5000,
           #                         AddParam='', FillBlank=T, ToForce=F) )
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2 (pOption='YAH_CAPI_SIZE_BOARD', Nb_Group = 50, Nb_Min = 10000, Frequency='MONTH', AddParam='SMALL'))
           # 
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           #                                       AddParam='LARGE,UNADJUST_ADJUST'))
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_WCEO_PRICES', Nb_Group = 55, Nb_Min = 50000, Frequency='MONTH', AddParam='ALL,UNADJUST_ADJUST'))
           # SAVERDS_WITH_ONEDRIVE (pData=x, FileFolder=CCPRData, FileName='DOWNLOAD_YAH_STK_WCEO_PRICES_UNADJ.rds' , SaveOneDrive=T)
           # 
           # Sys.sleep(1600)
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_WCEO_PRICES', Nb_Group = 55, Nb_Min = 50000, Frequency='MONTH', AddParam='ALL,CLOSE'))
           # SAVERDS_WITH_ONEDRIVE (pData=x, FileFolder=CCPRData, FileName='DOWNLOAD_YAH_STK_WCEO_PRICES_CLOSE.rds' , SaveOneDrive=T)
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='YAH_CAPI_SIZE_DIV', Nb_Group = 55, Nb_Min = 50000, Frequency='MONTH', AddParam='LARGE'))
           # SAVERDS_WITH_ONEDRIVE (pData=x, FileFolder=CCPRData, FileName='DOWNLOAD_YAH_STK_SIZE_SMALL_LARGE_202308.rds' , SaveOneDrive=T)
           
         },
         
         'BeQ-RD4' = {  
           
           # STK_DATA = try(LOOP_DOWNLOAD_YAH_STK(Dataset='BOARD', 
           #                                      Folder_List=CCPRData, File_List='LIST_YAH_STK_GBL_WCEO', 
           #                                      Folder_Out=CCPRData, Nb_Min=10000, Nb_Group=50))
           # x = RUN_LOOP_STKVN (pOption='DOWNLOAD_STKVN_PRICES', Nb_Group = 50, Nb_Min = 2000, AddParam='', FillBlank=T, ToForce=F, ToCheckTime=17)
           x = try(RUN_LOOP_STKVN (pOption='DOWNLOAD_CAF_STKVN_ANNUAL_RATIOS_BY_CODE', Nb_Group = 50, Nb_Min = 5000,
                                   AddParam='', FillBlank=T, ToForce=F) )
           x = try(RUN_LOOP_STKVN (pOption='DOWNLOAD_STKVN_PRICES_FOR_ALL', Nb_Group = 50, Nb_Min = 5000,
                                   AddParam='', FillBlank=T, ToForce=F) )
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           #                                       AddParam='MID,CLOSE'))
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='YAH_CAPI_SIZE_DIV', Nb_Group = 55, Nb_Min = 50000, Frequency='MONTH', AddParam='MEGA'))
           # SAVERDS_WITH_ONEDRIVE (pData=x, FileFolder=CCPRData, FileName='DOWNLOAD_YAH_STK_SIZE_SMALL_MEGA_202308.rds' , SaveOneDrive=T)
           # 
         }, 
         'BeQ-RD5' = {
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           #                                       AddParam='LARGE,UNADJUST_ADJUST'))
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2 (pOption='YAH_CAPI_SIZE_BOARD', Nb_Group = 50, Nb_Min = 10000, Frequency='MONTH', AddParam='MID'))
           # 
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           # AddParam='MID,UNADJUST_ADJUST'))
           x = try(RUN_LOOP_STKVN (pOption='DOWNLOAD_STKVN_PRICES_FOR_ALL', Nb_Group = 50, Nb_Min = 5000,
                                   AddParam='', FillBlank=T, ToForce=F) )
           x = try(RUN_LOOP_STKVN (pOption='FINAL_DOWNLOAD_C68_STKVN_SHARES', Nb_Group = 50, Nb_Min = 5000,
                                   AddParam='', FillBlank=T, ToForce=F) )
           
           
           # x = try(RUN_LOOP_STKVN (pOption='DOWNLOAD_STKVN_PE_FOR_ALL', Nb_Group = 50, Nb_Min = 5000,
           #                         AddParam='', FillBlank=T, ToForce=F) )
         },
         {
           x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_TOP100', Nb_Group = 10, Nb_Min = 200, Frequency='DAY',
                                                 AddParam='UNADJUST_ADJUST'))
           x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_TOP100', Nb_Group = 10, Nb_Min = 200, Frequency='DAY',
                                                 AddParam='CLOSE'))
           
         }
  )
  rm(FN_Name_BATCH_FUNCTION)
  FN_Name_BATCH_FUNCTION    = match.call()[[1]]
  FINAL_BATCH_FUNCTION(FNName=FN_Name_BATCH_FUNCTION)
}

# ==================================================================================================
RUN_NOSTOP_BY_PC_CCPR_SPECIAL = function(pMyPC) {
  # ------------------------------------------------------------------------------------------------
  
  # pMyPC = 'IREEDS'
  switch(pMyPC,
         'BeQ-RD3' = {
           try(CCPR_UPDATE_MONITOR_CCPR_FILES())
           if (CHECK_TIMEBETWEEN("15:00", "23:00"))
           {
             x = RUN_LOOP_STKVN (pOption='DOWNLOAD_HNX_STKVN_SHARES_BY_CODE', Nb_Group = 20, Nb_Min = 400, AddParam='', FillBlank=T, ToForce=F, ToCheckTime=17) 
           }
         },
         
         'BeQ-RD5' = {
           try(CCPR_UPDATE_MONITOR_CCPR_FILES())
           if (CHECK_TIMEBETWEEN("10:00", "14:00"))
           {
             x = RUN_LOOP_STKVN (pOption='DOWNLOAD_HNX_STKVN_SHARES_BY_CODE', Nb_Group = 20, Nb_Min = 400, AddParam='', FillBlank=T, ToForce=F, ToCheckTime=17) 
           }
           for (i in 1:10)
           {
             x = try(RUN_LOOP_STKVN (pOption='FINAL_DOWNLOAD_CAF_STKVN_SHARES', Nb_Group = 50, Nb_Min = 10000,
                                     AddParam='', FillBlank=T, ToForce=F, ToCheckTime = 9) )
           }
           
           
           # DOWNLOAD_STB_STKVN_RATIOS_VALUATION
         },
         
         'BeQ-RD4' = {
           try(RUN_LOOP_STKVN(pOption='NEW_DOWNLOAD_C68_STKVN_PERFORMANCE', Nb_Group = 50, Nb_Min = 10000, AddParam='', FillBlank=T, ToForce=F)) 
           for (i in 1:10)
           {
             x = try(RUN_LOOP_STKVN (pOption='FINAL_DOWNLOAD_VST_STKVN_SHARES', Nb_Group = 50, Nb_Min = 10000,
                                     AddParam='', FillBlank=T, ToForce=F, ToCheckTime = 9) )
           }
         },
         
         'IFRC-IREEDS' = {
           if (!wday(Sys.Date()) %in% c(1,7) | CHECK_TIMEBETWEEN("07:00", "09:00"))
             # try(CCPR_UPDATE_MONITOR_CCPR_FILES())
             if (wday(Sys.Date()) %in% c(1,7) && (CHECK_TIMEBETWEEN("18:30", "23:30") | CHECK_TIMEBETWEEN("00:30", "07:30")))
             {
               z = try(RUN_LOOP_STKVN(pOption='NEW_DOWNLOAD_CAF_STKVN_ORDER', Nb_Group = 20, Nb_Min =10000, 
                                      AddParam='', FillBlank=T, ToForce=F, ToCheckTime=16)) # 20
               
               x = try(RUN_LOOP_STKVN(pOption='DOWNLOAD_C68_STKVN_INTRADAY_DAY', Nb_Group = 20, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F))
               
               x = try(RUN_LOOP_STKVN (pOption='FINAL_DOWNLOAD_CAF_STKVN_SHARES', Nb_Group = 20, Nb_Min = 10000,
                                       AddParam='', FillBlank=T, ToForce=F, ToCheckTime = 9) )
               
             }
           z = try(RUN_LOOP_STKVN(pOption='DOWNLOAD_STB_STKVN_MAJOR_HOLDERS_PERCENTAGE', Nb_Group = 50, Nb_Min = 1000, AddParam='', FillBlank=T, ToForce=F, ToCheckTime=9))
           
           if (CHECK_TIMEBETWEEN("18:30", "23:30") | CHECK_TIMEBETWEEN("01:30", "07:30"))
           {
             z = try(RUN_LOOP_STKVN(pOption='DOWNLOAD_STB_STKVN_PRICES_BY_CODE', Nb_Group = 50, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F))
           }
           
           if (CHECK_TIMEBETWEEN("18:30", "23:30") | CHECK_TIMEBETWEEN("11:30", "13:30"))
           {
             z = try(RUN_LOOP_STKVN(pOption='FINAL_DOWNLOAD_VST_STKVN_SHARES', Nb_Group = 50, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F))
             z = try(RUN_LOOP_STKVN(pOption='FINAL_DOWNLOAD_STB_STKVN_SHARES', Nb_Group = 50, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F))
             z = try(RUN_LOOP_STKVN(pOption='FINAL_DOWNLOAD_C68_STKVN_SHARES', Nb_Group = 50, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F))
           }
           # z = try(RUN_LOOP_STKVN(pOption='FINAL_DOWNLOAD_STB_STKVN_SHARES', Nb_Group = 50, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F, ToCheckTime=10)) 
           xd = try(RUN_LOOP_STKVN(pOption='DOWNLOAD_C68_STKVN_INTRADAY', Nb_Group = 50, Nb_Min = 200, AddParam='',FillBlank=T, ToForce=F))
           
         },
         
         'IFRC-3630' = {
           
           try(CCPR_UPDATE_MONITOR_CCPR_FILES())
           
           if (CHECK_TIMEBETWEEN("18:30", "23:30") | CHECK_TIMEBETWEEN("11:30", "13:30"))
           {
             z = try(RUN_LOOP_STKVN(pOption='FINAL_DOWNLOAD_C68_STKVN_SHARES', Nb_Group = 50, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F))
             z = try(RUN_LOOP_STKVN(pOption='FINAL_DOWNLOAD_VST_STKVN_SHARES', Nb_Group = 50, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F))
             z = try(RUN_LOOP_STKVN(pOption='FINAL_DOWNLOAD_STB_STKVN_SHARES', Nb_Group = 50, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F))
           }
           
           if (CHECK_TIMEBETWEEN("18:30", "23:30"))
           {
             z = try(RUN_LOOP_STKVN(pOption='NEW_DOWNLOAD_CAF_STKVN_PRICES',   Nb_Group = 50, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F))
             z = try(RUN_LOOP_STKVN(pOption='FINAL_DOWNLOAD_C68_STKVN_SHARES', Nb_Group = 50, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F))
             z = try(RUN_LOOP_STKVN(pOption='FINAL_DOWNLOAD_STB_STKVN_SHARES', Nb_Group = 50, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F))
             
             z = try(RUN_LOOP_STKVN(pOption='DOWNLOAD_STB_STKVN_PRICES_BY_CODE', Nb_Group = 50, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F))
             z = try(RUN_LOOP_STKVN(pOption='FINAL_DOWNLOAD_VST_STKVN_SHARES', Nb_Group = 50, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F))
             
           }
           
           for (i in 1:10)
           {
             x = try(RUN_LOOP_STKVN (pOption='FINAL_DOWNLOAD_VST_STKVN_SHARES', Nb_Group = 50, Nb_Min = 10000,
                                     AddParam='', FillBlank=T, ToForce=F, ToCheckTime = 9) )
           }
           for (i in 1:10)
           {
             x = try(RUN_LOOP_STKVN (pOption='FINAL_DOWNLOAD_CAF_STKVN_SHARES', Nb_Group = 50, Nb_Min = 10000,
                                     AddParam='', FillBlank=T, ToForce=F, ToCheckTime = 9) )
           }
           for (i in 1:10)
           {
             x = try(RUN_LOOP_STKVN (pOption='FINAL_DOWNLOAD_C68_STKVN_SHARES', Nb_Group = 50, Nb_Min = 10000,
                                     AddParam='', FillBlank=T, ToForce=F, ToCheckTime = 9) )
           }
           
           # z = try(RUN_LOOP_STKVN(pOption='FINAL_DOWNLOAD_STB_STKVN_SHARES', Nb_Group = 50, Nb_Min = 5000, AddParam='', FillBlank=T, ToForce=F, ToCheckTime=10)) 
           xd = try(RUN_LOOP_STKVN(pOption='DOWNLOAD_C68_STKVN_INTRADAY', Nb_Group = 50, Nb_Min = 200, AddParam='',FillBlank=T, ToForce=F))
           
         },
         
         {
           # xd = try(RUN_LOOP_STKVN(pOption='DOWNLOAD_C68_STKVN_INTRADAY', Nb_Group = 50, Nb_Min = 2000, AddParam='',FillBlank=T, ToForce=F))
         }
  )
  rm(FN_Name_BATCH_FUNCTION)
  FN_Name_BATCH_FUNCTION    = match.call()[[1]]
  FINAL_BATCH_FUNCTION(FNName=FN_Name_BATCH_FUNCTION)
  IFRC_SLEEP(5)
}

# ==================================================================================================
RUN_NOSTOP_BY_PC_CCPR_QUICK = function(pMyPC) {
  # ------------------------------------------------------------------------------------------------
  List_Sector = list(
    c('BSM', 'Basic Materials', 'https://finance.yahoo.com/screener/predefined/ms_basic_materials/'),
    c('CMS', 'Communication Services', 'https://finance.yahoo.com/screener/predefined/ms_communication_services/'),
    c('CNC', 'Consumer Cyclical', 'https://finance.yahoo.com/screener/predefined/ms_consumer_cyclical/'),
    c('CND', 'Consumer Defensive', 'https://finance.yahoo.com/screener/predefined/ms_consumer_defensive/'),
    c('ENY', 'Energy', 'https://finance.yahoo.com/screener/predefined/ms_energy/'),
    c('FNS', 'Financial Services', 'https://finance.yahoo.com/screener/predefined/ms_financial_services/'),
    c('HTC', 'Healthcare', 'https://finance.yahoo.com/screener/predefined/ms_healthcare/'),
    c('IND', 'Industrials', 'https://finance.yahoo.com/screener/predefined/ms_industrials/'),
    c('REA', 'Real Estate', 'https://finance.yahoo.com/screener/predefined/ms_real_estate/'),
    c('TEC', 'Technology', 'https://finance.yahoo.com/screener/predefined/ms_technology/'),
    c('UTI', 'Utilities', 'https://finance.yahoo.com/screener/predefined/ms_utilities/')
  )
  # pMyPC = 'IREEDS'
  switch(pMyPC,
         'IFRC-3630'  = {  
           try(CCPR_CHECK_MYPC(pSleep=round(runif(1,5,10))))
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2 (pOption='YAH_CAPI_SIZE_BOARD', Nb_Group = 50, Nb_Min = 10000, Frequency='MONTH', AddParam='LARGE'))
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           #                                       AddParam='MEGA,CLOSE'))
         },
         'IFRC-IREEDS'  = { 
           try(CCPR_CHECK_MYPC(pSleep=round(runif(1,5,10))))
           # try(UPDATE_UPLOAD_CCPR_MARKET_LAST(UData=CCPRData, UList= paste0('R:/CCPR/DATA/YAH/LIST/','LIST_YAH_MARKET_LAST.txt'), 
           #                                    pSource = 'YAH',  xPeriod = '5m', toReset = F))
           # IFRC_SLEEP(60*1)
         },
         
         'IFRC-103'     = {  
           try(CCPR_CHECK_MYPC(pSleep=round(runif(1,50,100))))
         },
         
         'BeQ-RD1' = {  
           try(CCPR_CHECK_MYPC(pSleep=round(runif(1,50,100))))
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           #                                       AddParam='SMALL,UNADJUST_ADJUST'))
           # 
           # CATln_Border('HELLO')
         },
         
         'BeQ-RD2' = {  
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           #                                       AddParam='LARGE,CLOSE'))
           try(CCPR_CHECK_MYPC(pSleep=round(runif(1,50,100))))
           try(UPDATE_UPLOAD_CCPR_MARKET_LAST(UData=CCPRData, UList= paste0(CCPRData, 'YAH/LIST/','LIST_YAH_MARKET_LAST.txt'), 
                                              pSource = 'YAH',  xPeriod = '5m', toReset = F))
         },
         
         'BeQ-RD3' = {  
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2 (pOption='YAH_CAPI_SIZE_BOARD', Nb_Group = 50, Nb_Min = 10000, Frequency='MONTH', AddParam='SMALL'))
           # 
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           #                                       AddParam='LARGE,UNADJUST_ADJUST'))
           try(CCPR_CHECK_MYPC(pSleep=round(runif(1,50,100))))
           try(UPDATE_UPLOAD_CCPR_MARKET_LAST(UData=CCPRData, UList= paste0(CCPRData, 'YAH/LIST/','LIST_YAH_MARKET_LAST.txt'), 
                                              pSource = 'YAH',  xPeriod = '5m', toReset = F))
           try(XLS_UPDATE_BATCH_FILES_MONITOR_CCPR_RDS())
         },
         
         'BeQ-RD4' = {  
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           #                                       AddParam='MID,CLOSE'))
           try(CCPR_CHECK_MYPC(pSleep=60))
           
         }, 
         'BeQ-RD5' = {
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           #                                       AddParam='LARGE,UNADJUST_ADJUST'))
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2 (pOption='YAH_CAPI_SIZE_BOARD', Nb_Group = 50, Nb_Min = 10000, Frequency='MONTH', AddParam='MID'))
           # 
           # x = try(RUN_LOOP_BY_BROUP_AND_SAVE_V2(pOption='DOWNLOAD_YAH_STK_SIZE_PRICES', Nb_Group = 100, Nb_Min = 20000, Frequency='MONTH', 
           # AddParam='MID,UNADJUST_ADJUST'))
           try(CCPR_CHECK_MYPC(pSleep=60))
           try(XLS_UPDATE_BATCH_FILES_MONITOR_CCPR_RDS())
         },
         {
           try(CCPR_CHECK_MYPC(pSleep=60))
           try(UPDATE_UPLOAD_CCPR_MARKET_LAST(UData=CCPRData, UList= paste0(CCPRData, 'YAH/LIST/','LIST_YAH_MARKET_LAST.txt'), 
                                              pSource = 'YAH',  xPeriod = '5m', toReset = F))
           IFRC_SLEEP(60*1)
         }
  )
  # INSERT 
  rm(FN_Name_BATCH_FUNCTION)
  FN_Name_BATCH_FUNCTION    = match.call()[[1]]
  FINAL_BATCH_FUNCTION(FNName=FN_Name_BATCH_FUNCTION)
}

# ==================================================================================================
RUN_NOSTOP_BY_PC_CCPR_TEST = function(pMyPC) {
  # ------------------------------------------------------------------------------------------------
  List_Sector = list(
    c('BSM', 'Basic Materials', 'https://finance.yahoo.com/screener/predefined/ms_basic_materials/'),
    c('CMS', 'Communication Services', 'https://finance.yahoo.com/screener/predefined/ms_communication_services/'),
    c('CNC', 'Consumer Cyclical', 'https://finance.yahoo.com/screener/predefined/ms_consumer_cyclical/'),
    c('CND', 'Consumer Defensive', 'https://finance.yahoo.com/screener/predefined/ms_consumer_defensive/'),
    c('ENY', 'Energy', 'https://finance.yahoo.com/screener/predefined/ms_energy/'),
    c('FNS', 'Financial Services', 'https://finance.yahoo.com/screener/predefined/ms_financial_services/'),
    c('HTC', 'Healthcare', 'https://finance.yahoo.com/screener/predefined/ms_healthcare/'),
    c('IND', 'Industrials', 'https://finance.yahoo.com/screener/predefined/ms_industrials/'),
    c('REA', 'Real Estate', 'https://finance.yahoo.com/screener/predefined/ms_real_estate/'),
    c('TEC', 'Technology', 'https://finance.yahoo.com/screener/predefined/ms_technology/'),
    c('UTI', 'Utilities', 'https://finance.yahoo.com/screener/predefined/ms_utilities/')
  )
  # pMyPC = 'IREEDS'
  switch(pMyPC,
         'IFRC-3630'  = {  
           
         },
         'IFRC-IREEDS'  = { 
           
         },
         
         'IFRC-103'     = {  
           
         },
         
         'BeQ-RD1' = {  
           
         },
         
         'BeQ-RD2' = {  
           
         },
         
         'BeQ-RD3' = {  
           
         },
         
         'BeQ-RD4' = {  
           
           
         }, 
         'BeQ-RD5' = {
           
         },
         {
           
         }
  )
  # INSERT 
  rm(FN_Name_BATCH_FUNCTION)
  FN_Name_BATCH_FUNCTION    = match.call()[[1]]
  FINAL_BATCH_FUNCTION(FNName=FN_Name_BATCH_FUNCTION)
  # rm(FN_Name_BATCH_FUNCTION)
  # FN_Name_BATCH_FUNCTION    = match.call()[[1]]
  # To_Update  = UPDATE_PC_FUNCTION(FN_Name=as.character(FN_Name_BATCH_FUNCTION))
}

FINAL_BATCH_FUNCTION = function(FNName)
{
  # FNName = 'TEST'
  # print(FN_Name_BATCH_FUNCTION)
  # assign("FN_Name_BATCH_FUNCTION", FN_Name_BATCH_FUNCTION, envir = .GlobalEnv)
  # Sys.sleep(1)
  # MyPC       = GET_MYPC()
  # My_Updated = data.table(pc=MYPC, function_name=FN_Name, updated=substr(Sys.time(),1,19))
  # 
  To_Update  = UPDATE_PC_FUNCTION(FN_Name=as.character(FNName))
  # To_Update = My_Updated
  # My.Kable.All(To_Update)
  # Batch_One = TEST_FUNCTION()
  File_Batch = 'BATCH_MYPC.rds'
  File_Full  = paste0(CCPRData, 'MONITOR/', File_Batch)
  if (file.exists(File_Full))
  {
    Data_Batch = CCPR_READRDS(paste0(CCPRData, 'MONITOR/'), File_Batch, ToKable = T, ToRestore = T)
  } else { Data_Batch = data.table()}
  
  if (nrow(To_Update)>0)
  {
    Data_Batch = setDT(rbind(Data_Batch, To_Update, fill=T))
    Data_Batch = unique(Data_Batch, fromLast = T, by=c('pc', 'function_name'))[order(updated)]
    # My.Kable(Data_Batch)
    SAVERDS_WITH_ONEDRIVE(Data_Batch, paste0(CCPRData, 'MONITOR/'), File_Batch, SaveOneDrive = T, ToPrompt = F)
  }
}


CONVERT_HTML_INSIDER_TRADING = function(pSource='VST', pURL = 'R:/INSIDER/VST/VND VNDirect Securities Corporation - VNDIRECT VietstockFinance.htm')
{
  xData    = data.table()
  switch(pSource,
         'VST' = {
           content  = rvest::read_html(pURL)
           tables   = content %>% html_table(fill = TRUE)
           if (length(tables) >= 2)
           {
             xData = as.data.table(tables[[2]])
             xData = xData[-1,]
             colnames(xData) = c('nr', 'codesource', 'transactor', 'type', 'shares_before', 'pc_before', 'shares_volume', 'date_from', 'date_to', 'shares_after', 'pc_after')
             xData[, updated := Sys.Date()]
             xData[, source := 'VST']
             xData[, shares_before := as.numeric(gsub(',', '', shares_before))]
             xData[, pc_before := as.numeric(gsub(',', '', pc_before))]
             xData[, shares_volume := as.numeric(gsub(',', '', shares_volume))]
             xData[, shares_after := as.numeric(gsub(',', '', shares_after))]
             xData[, pc_after := as.numeric(pc_after)]
             xData[, nr := as.numeric(nr)]
             xData[, date_from := as.Date(date_from, format = "%m/%d/%Y")]
             xData[, date_to := as.Date(date_to, format = "%m/%d/%Y")]
             
             My.Kable.TB(xData)
           }
         }
  )
  return(xData)
}

#================================================================================
CCPR_LOAD_SQL_HOST = function(pTableSQL = 'wcw_directory', ToDisconnect=T, ToKable=T) {
  # ------------------------------------------------------------------------------------------------
  host_dt      = data.table()
  my.connexion = try(dbConnect(MySQL(), user = "ccpr_user",       password = "Ifrc@2023_ccpr", host = '27.71.235.40', dbname = "ifrc_ccpr"))
  print(my.connexion)
  
  if (all(class(my.connexion)!='try-error'))
  {
    CATrp("Read Table...")
    host_dt = try(dbReadTable(my.connexion,pTableSQL))
    
    if (all(class(host_dt)!='try-error'))
    {
      OutDt = setDT(host_dt)
      My.Kable(host_dt)
      str(host_dt)
    } 
  } else {
    CATln(paste(pHost, ": NO CONNEXION."))
  }
  
  CATln(paste('CCPR_LOAD_SQL_HOST : ', pTableSQL, ' - DONE, ', Sys.time()))
  if (ToKable) { My.Kable.MaxCols(host_dt) }
  if (ToDisconnect) { try(dbDisconnect(my.connexion),silent = T) }
  return(host_dt)
}

#===============================================================================
EURONEXT_DOWNLOAD_COMPANY_LIST = function (IndexCode='QS0010989141-XPAR', File_source = 'wcw_directory_enx_fr', Date = '20230619')
{
  pURL     = paste0('https://live.euronext.com/en/ajax/getIndexCompositionFull/', IndexCode)
  content  = rvest::read_html(pURL)
  tables   = content %>% html_table(fill = TRUE)
  
  links    = content %>% html_nodes(xpath='//a') %>% html_attr('href')
  tb_links = as.data.table(links)
  tb_links[, id:=seq.int(1,.N)]
  My.Kable(tb_links)
  
  tb_info  = as.data.table(tables[[1]])
  tb_info[, id:=seq.int(1,.N)]
  My.Kable(tb_info)
  
  tb_companies = merge(tb_info, tb_links, all.x=T, by='id')
  tb_companies[, codesource:=gsub('/en/product/equities/','', links)]
  My.Kable.TB(tb_companies)
  
  FileName = paste0(RD_SHARED, 'DATA/',File_source,'_', Date,'.rds')
  if (file.exists(FileName))
  {
    Data_old = readRDS (FileName)
  } else { Data_old = data.table()}
  
  Data_All = unique (rbind (Data_old, tb_companies, fill = T), by = 'codesource')
  My.Kable (Data_All)
  saveRDS(Data_All, FileName )
  
  return (Data_All)
}

#==============================================================================
EURONEXT_DOWNLOAD_BOARD_LIST  = function ( FileSource = 'wcw_directory_enx_all', File_path = 'wcw_directory_enx_fr_board', 
                                           Date = '20230619', option = 'FL', MinNb = 5)
{
  library(gender)
  data_final = data.table()
  FileName = paste0(RD_SHARED,'DATA/', FileSource,'_', Date,'.rds')
  if (file.exists(FileName))
  {
    tb_companies = try(readRDS(FileName))
    Data_List = list()
    for (k in 1:min(MinNb, nrow(tb_companies)))
    {
      # k = 675
      # k = 1
      # pCode = 'FR0010557264-XPAR'
      pCode = tb_companies[k]$codesource
      pURL1 = paste0('https://live.euronext.com/en/cofisem-public/', pCode)
      CATln('')
      CATln_Border(paste(k, nrow(tb_companies), pCode, pURL1))
      content1  = try(rvest::read_html(pURL1))
      if ( length (content1)> 1 ) {
        if (all(class(content1)!='try-error'))
        {
          tables1   = content1 %>% html_table(fill = TRUE)
          
          if ( length (tables1) >0) {
            
            if ( 'X1' %in% colnames(as.data.table(tables1[[1]])))
            {
              
              tb_board  = try(as.data.table(tables1[[1]])[, .(X1, X2)])
              
              Sys.sleep(1)
              tb_board  = tb_board[-nrow(tb_board)]
              colnames(tb_board) = c('position', 'fullname')
              tb_board[, ':='(source='ENX', codesource=pCode, updated=Sys.Date(), id=seq.int(1,.N))]
              tb_board[, ceo:=as.numeric(NA)]
              tb_board[, cob:=as.numeric(NA)]
              tb_board[id==1 & grepl('CEO$|Chief Executive Officer$', position), ceo:=1]
              tb_board[id==1 & grepl('^Chairman', position), cob:=1]
              tb_board[, company:= tb_companies[k]$Component]
              tb_board = DIRECTORY_ADJUST_FIRST_LAST_NAME (tb_board, option = option)
              
              tb_board$gender = as.character (NA)
              for (i in 1:nrow(tb_board))
              {
                # i = 1
                # xData[k]$first1
                xname = decodeVN(tb_board[i]$firstname, from='Unicode', to='Unicode', diacritics=F)
                FName = try(setDT(gender(xname)))
                if (nrow(FName)>0)
                {
                  print(paste(tb_board, FName))
                  GName = FName$gender
                  tb_board[i]$gender = GName
                }
              }
              tb_board[grepl('female', gender), gender:='F']
              tb_board[grepl('male', gender), gender:='M']
              My.Kable.All(tb_board)
              
              Data_List[[k]] = tb_board }
            else { Data_List[[k]] = data.table()}
          }  
          
        }
      }
    }
    data_final = rbindlist(Data_List)
    
    My.Kable (data_final)
    
    
    # data_final [(ceo ==1) & (gender == 'F')]
    # data_final [(ceo ==1)]
    # unique(data_final , by = 'codesource')
    
    MyFile = paste0(RD_SHARED, 'DATA/', File_path,'_', Date,'.rds')
    # x = readRDS(MyFile)
    CATln_Border(MyFile)
    
    if (file.exists(MyFile))
    {
      Data_old = readRDS (MyFile)
    } else { Data_old = data.table()}
    
    Data_All = unique (rbind (Data_old, data_final, fill = T), by = c('codesource', 'fullname', 'position'))
    My.Kable (Data_All)
    saveRDS(Data_All, MyFile)
  }
  return (Data_All)
}

# ==================================================================================================
UPLOAD_STRUCTURE_RDS_TO_SQL_2023 = function(FileRDS, DataRDS, TableSQL, host='ifrc.vn', ToForce=F){
  # ------------------------------------------------------------------------------------------------
  # FileRDS=paste0(UData,"efrc_cmd_history.rds"); DataRDS=""; TableSQL="efrc_cmd_history"; host="ifrc.vn"
  DATACENTER_LINE_BORDER(paste('UPLOAD_STRUCTURE_RDS_TO_SQL = ', host, ' >>>', TableSQL))
  
  if (nchar(FileRDS)>0) { x = DATACENTER_LOADDTRDS(FileRDS) }else{ x = DataRDS }
  # xSTR  = as.data.table(str(x))
  # My.Kable(xSTR)
  # as.list(str(x))
  # typeof(str(x))
  # 
  # typeof(x$iso2)
  # xNAMES = names(x)
  # xNAMES
  xs = sapply(x,class)
  # colnames(x) = tolower(colnames(x))
  str_std = DATACENTER_LOAD_TEXTFILE(paste0(UData),"sql_structure_standard.txt")
  
  StrField = paste0('CREATE TABLE ',TableSQL,' ( id int NOT NULL AUTO_INCREMENT, ')
  for (k in 1:length(xs))
  {
    # k = 12
    xName = names(xs[k])
    xType = xs[[k]][1]
    switch(xType,
           
           "character" = {
             x1 = x[, ..xName]
             setnames(x1, xName, 'field')
             if (nrow(x1[!is.na(field)])==0) { nLen=1 } else { nLen = ifelse(nrow(str_std[field==xName])==1,
                                                                             max(str_std[field==xName]$len,nchar(x1[!is.na(field)]$field)),
                                                                             max(nchar(x1[!is.na(field)]$field))) }
             # nLen = floor(1.25*nLen)
             xEnd = ifelse(k < length(xs), ", ", ", PRIMARY KEY (id))")
             Str_One = paste0('`',xName,'`', ' varchar', '(', nLen, ') ', xEnd )
             StrField = paste0(StrField, Str_One)
             # Buu lam tiep duoc khong?
           },
           
           "Date" = {
             x1 = x[, ..xName]
             setnames(x1, xName, 'field')
             xEnd = ifelse(k < length(xs), ", ", ", PRIMARY KEY (id))")
             Str_One = paste0('`',xName,'`', ' date', xEnd )
             StrField = paste0(StrField, Str_One)
             # Buu lam tiep duoc khong?
           },
           
           "numeric" = {
             x1 = x[, ..xName]
             setnames(x1, xName, 'field')
             xEnd = ifelse(k < length(xs), ", ", ", PRIMARY KEY (id))")
             Str_One = paste0('`',xName,'`', ' float', xEnd )
             StrField = paste0(StrField, Str_One)
             # Buu lam tiep duoc khong?
           },
           {
             x1 = x[, ..xName]
             setnames(x1, xName, 'field')
             if (nrow(x1[!is.na(field)])==0) { nLen=1 } else { nLen = ifelse(nrow(str_std[field==xName])==1,
                                                                             max(str_std[field==xName]$len,nchar(x1[!is.na(field)]$field)),
                                                                             max(nchar(x1[!is.na(field)]$field))) }
             # nLen = floor(1.25*nLen)
             xEnd = ifelse(k < length(xs), ", ", ", PRIMARY KEY (id))")
             Str_One = paste0('`',xName,'`', ' varchar', '(', nLen, ') ', xEnd )
             StrField = paste0(StrField, Str_One)
           }
    )
  }
  
  CATln(StrField)
  
  conn = try(IFRC.CONNEXTION.2023(host),silent = T)
  if (ToForce) { stm1 = paste0("drop table if exists ", TableSQL); try(dbGetQuery(conn,statement = stm1)) }
  try(dbGetQuery(conn,statement = StrField))
  
}

# ==================================================================================================
efrc.print.time = function(MyText, ttime) {
  # ------------------------------------------------------------------------------------------------
  print(paste(MyText, format(Sys.time() - ttime, digits = 2, nsmall = 2, big.mark = ",")), quote = F)
}

#====================================================================================================
GET_YAH_BOARD_MEMBER = function(pCode = 'FRA'){
  #--------------------------------------------------------------------------------------------------
  Start.time = Sys.time()
  Data_Final = data.table()
  
  # List_Files  = as.data.table(list.files(paste0(ODDriveData, pSource, '/', pDataset, '/'), '*.rds'))
  # List_Files  = List_Files[grepl(paste0('^', pPrefix), V1)] 
  # My.Kable.TB(List_Files)
  
  pSource     = 'YAH'
  pDataset    = 'BOARD'
  pPrefix     = paste0('LIST_', pSource, '_')
  FullName    = tolower(paste0(UData, pSource, '/', pDataset, '/', pCode))
  # FullName = 'd:/onedrive/beq/data/yah/board/list_yah_board_egy.rds  '
  CATln_Border(FullName)
  xData       = readRDS(FullName)
  pISO3       = substr(pCode, nchar(pCode)-6, nchar(pCode)-4)
  # My.Kable.TB(xData)
  if (nrow(xData) > 0){
    Data_All = unique (xData [,. (title, codesource,date, updated, gender, fullname, ceo, cob)], by = c('fullname', 'codesource'))
    # unique(Data_All$codesource)
    Data_All = CLEAN_COLNAMES(Data_All)
    Data_All[, positions:=trimws(title)]
    # Data_All[, fullname:=trimws(decodeVN(fullname_lc, from='Unicode', to='Unicode', diacritics=F))] 
    Data_All[, firstname:=stringr::word(fullname, 1)]
    Data_All[, lastname:=toupper(substr(fullname, nchar(firstname)+1, nchar(fullname)))]
    # Data_All [(ceo == 1) ]
    Data_List = data.table (iso3 =pISO3 ,companies = length(unique(Data_All$codesource)), ceo_companies =length (unique(Data_All[ceo ==1]$codesource)), 
                            x_companies = length(unique(Data_All$codesource)) -  length (unique(Data_All[ceo ==1]$codesource)) ,
                            WCEO = nrow(Data_All [(ceo == 1) & (gender == 'F')]), MCEO = nrow(Data_All [(ceo == 1) & (gender == 'M')]),
                            XCEO = nrow(Data_All [(ceo == 1) & (is.na(gender))]), codesource = pCode)
    My.Kable.TB(Data_List )
  } else {Data_List = data.table(iso3 = pISO3, codesource = pCode)}
  
  
  End.time = Sys.time()
  CATln(paste('Duration = ', Format.Number(difftime(End.time, Start.time, units='sec'),2)))
  
  
  return(Data_List)
}

#=================================================================================================
GET_VST_BOARD_MEMBER_AVATAR_BY_CODE = function(pCode = 'VND')
{
  # pCode = 'VNM'
  pURL  = paste0('https://finance.vietstock.vn/', pCode, '/profile.htm?languageid=2')
  pWeb     = GET_CURL_FETCH(pURL)
  writeLines(pWeb, 'c:/temp/vst.txt')
  pBody    = str.extract(pWeb, 'board-of-management.htm', '</table>')
  print(pBody)
  xRows    = unlist(strsplit(pBody, '<div class=div-bom-avatar>'))
  for (k in 2:length(xRows))
  {
    xRow     = xRows[[k]]
    xImage   = str.extract(xRow, 'src=', ' '); CATln(xImage)
    LImage   = gsub('Small', 'Large', xImage); CATln(LImage)
    
    xName    = str.extract(xRow, 'alt=', '>'); CATln(xName)
    eName    = gsub('"',"", trimws(decodeVN(xName, from='Unicode', to='Unicode', diacritics=F))); CATln(eName)
    renameImage = tolower(paste0(pCode, '_', gsub(' ','',eName), '.png')) ; CATln(renameImage)
    
    download.file(url=LImage, destfile=paste0('R:/WOMEN_CEO/DOWNLOADS/VST/avatar/', renameImage), mode = 'wb')
  }
}

#=============================================================================================
CCPR_MERGE_LIST_FILES = function(pFolder='R:/WOMEN_CEO/DATA/YAH/LIST/', pPrefix     = 'LIST')
{ #-------------------------------------------------------------------------------------------
  List_Files  = as.data.table(list.files(pFolder, '*.rds'))
  List_Codes  = List_Files[grepl(paste0('^', pPrefix), V1)] 
  
  Data_List   = list() 
  for (k in 1:nrow(List_Codes))
  {
    # k = 1
    Data_List[[k]] = try(readRDS(paste0(pFolder, List_Codes[k]$V1)))
    CATln_Border(paste(k, paste0('R:/WOMEN_CEO/DATA/YAH/LIST/',List_Codes[k]$V1)))
  }
  Data_All = rbindlist(Data_List, fill=T)
  My.Kable(Data_All)
  return(Data_All)
}

#================================================================================
DIRECTORY_ADJUST_FIRST_LAST_NAME = function (MyData, option = 'FL')
{
  switch(option,
         'FL'= {
           # MyData2 = MyData [iso2 == 'FR']
           # MyData2 [,. (firstname, lastname, fullname)]
           MyData[, firstname:=trimws(str_to_title(word(fullname,1)))]
           MyData[, lastname:= toupper(substr(fullname, nchar(firstname)+1, nchar(fullname)))]
         },
         'LF' = {
           # MyData2 = MyData [iso2 == 'FR']
           MyData[, firstname:= str_to_title(substr(fullname, nchar(lastname)+1, nchar(fullname)))]
           MyData[, lastname:=trimws(toupper(word(fullname,1)))]
         })
  return (MyData)
}
#===============================================================================
DIRECTORY_ADJUST_POSITION = function(MyData, Position_From, Position_To)
{
  # MyData = Data_Selected
  MyData[tolower(position)==tolower(Position_From), position:=Position_To]
  return (MyData)
}

#===================================================================================================
WRITE_SUMMARY_FULLPATH = function(pFileName, ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T) {
  # ------------------------------------------------------------------------------------------------
  # xs = WRITE_SUMMARY(UData, "efrc_indexp_history.rds")
  
  x.rds     = try(readRDS(pFileName))
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

#=========================================================================================
# MyData = DOWNLOAD_INTRADAY_YAH(pCode='SGD=X', pDur='1d', NbDays=250000)
DOWNLOAD_INTRADAY_YAH = function(pCode='SGO.PA', pDur='1m', NbDays=250){
  #---------------------------------------------------------------------------------------
  xData = data.table()
  switch(pDur,
         '5m' = { pDuration = 30*24*60*60},
         '1m' = { pDuration = 7*24*60*60},
         '1d' = { pDuration = NbDays*24*60*60}
  )
  
  Epoch_Now    = floor(as.numeric(Sys.time()))
  Epoch_Before = floor(as.numeric(Sys.time()-pDuration))
  lubridate::as_datetime(Epoch_Now)
  lubridate::as_datetime(Epoch_Before)
  
  pURL = 'https://query1.finance.yahoo.com/v8/finance/chart/SGO.PA?symbol=SGO.PA&period1=1687378200&period2=1687841183&useYfid=true&interval=5m&includePrePost=true&events=div%7Csplit%7Cearn&lang=en-US&region=US&crumb=GlxsM.YMaan&corsDomain=finance.yahoo.com'
  pURL  = paste0('https://query1.finance.yahoo.com/v8/finance/chart/', pCode, '?symbol=', pCode, '&period1=', 
                 Epoch_Before, '&period2=', Epoch_Now, '&useYfid=true&interval=', pDur, 
                 '&includePrePost=true&events=div%7Csplit%7Cearn&lang=en-US&region=US&crumb=GlxsM.YMaan&corsDomain=finance.yahoo.com')
  CATln_Border(pURL)
  x = jsonlite::fromJSON(pURL)
  
  # x$chart$result$timestamp
  xCUR      = as.character(x$chart$result$meta$currency)
  xTZ       = as.character(x$chart$result$meta$timezone)
  xEXC      = as.character(x$chart$result$meta$exchangeName)
  xTZEXC    = as.character(x$chart$result$meta$exchangeTimezoneName)
  xOFFSET   = as.character(x$chart$result$meta$gmtoffset)
  xTYPE     = as.character(x$chart$result$meta$instrumentType)
  y = as.vector(x$chart$result$indicators$quote)
  
  z = as.data.table(y)
  x.timestamp = as.data.table(x$chart$result$timestamp)
  x.high   = as.data.table(z$high)
  x.low    = as.data.table(z$low)
  x.open   = as.data.table(z$open)
  x.close  = as.data.table(z$close)
  x.volume = as.data.table(z$volume)
  xData    = setDT(cbind(x.timestamp, x.high, x.low, x.open, x.close, x.volume))
  colnames(xData) = c('timestamp', 'high', 'low', 'open', 'close', 'volume')
  xData[, datetime:=lubridate::as_datetime(timestamp)]
  
  xData[, ":="(codesource=pCode, source='YAH', cur=xCUR, tz=xTZ, tzoffset=as.numeric(xOFFSET), instype=xTYPE, date=as.Date(substr(datetime,1,10)))]
  xData = xData %>% select(instype, codesource, source, timestamp, datetime, tz, tzoffset, everything())
  # xData[, datetime:=as.Date(as.POSIXct(timestamp, origin="1970-01-01"))]
  My.Kable(xData)
  return(xData)
}

#===================================================================================================
CCPR_DO_IT = function(ToDo){
  #-------------------------------------------------------------------------------------------------
  CCPRData = 'R:/CCPR/DATA/'
  RData    = 'R:/DATA/'
  CATln_Border(paste('CCPR_DO_IT : ', ToDo))
  switch(ToDo,
         'DOWNLOAD_CAF_STKVN_DAY' = {
           My_List = readRDS(paste0(RData, 'SSI/', 'download_ssi_stkvn_exc_ref.rds'))
         },
         'UPDATE_UPLOAD_CCPR_HELP_LINKS' = {
           My_Excel = setDT(openxlsx::read.xlsx('R:/CCPR/ccpr_helps.xlsx', sheet='ccpr_help_links'))
           str(My_Excel)
           My_Excel[is.na(URL), URL:=as.character(NA)]
           My.Kable(My_Excel)
           saveRDS(My_Excel, paste0('R:/CCPR/ccpr_help_links.rds'))
           
           try(IFRC.UPLOAD.RDS.2023(l.host = "ccpr.vn", l.tablename='ccpr_help_links',
                                    l.filepath= tolower('R:/CCPR/ccpr_help_links.rds'),
                                    CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=T))
         },
         'UPDATE_UPLOAD_CCPR_HELP_FAQ' = {
           My_Excel = setDT(openxlsx::read.xlsx('R:/CCPR/ccpr_helps.xlsx', sheet='ccpr_help_faq'))
           str(My_Excel)
           My_Excel[is.na(URL), URL:=as.character(NA)]
           My.Kable(My_Excel)
           saveRDS(My_Excel, paste0('R:/CCPR/ccpr_help_faq.rds'))
           
           try(IFRC.UPLOAD.RDS.2023(l.host = "ccpr.vn", l.tablename='ccpr_help_faq',
                                    l.filepath= tolower('R:/CCPR/ccpr_help_faq.rds'),
                                    CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=T))
         },
         
         'UPDATE_UPLOAD_CCPR_HELP_GLOSSARY' = {
           My_Excel = setDT(openxlsx::read.xlsx('R:/CCPR/ccpr_helps.xlsx', sheet='ccpr_help_glossary'))
           str(My_Excel)
           My_Excel[is.na(URL), URL:=as.character(NA)]
           My.Kable(My_Excel)
           saveRDS(My_Excel, paste0('R:/CCPR/ccpr_help_glossary.rds'))
           
           try(IFRC.UPLOAD.RDS.2023(l.host = "ccpr.vn", l.tablename='ccpr_help_glossary',
                                    l.filepath= tolower('R:/CCPR/ccpr_help_glossary.rds'),
                                    CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=T))
         },
         
         'UPDATE_UPLOAD_CCPR_LIBRARIES_ALL' = {
           pURL = 'https://cran.r-project.org/web/packages/available_packages_by_name.html'
           content  = rvest::read_html(pURL)
           tables   = content %>% html_table(fill = TRUE)
           xDATA    = as.data.table(tables[[1]])
           colnames(xDATA) = c('name', 'description')
           xDATA    = xDATA[!is.na(name) & !is.na(description)]
           xDATA[, language:='R']
           xDATA[, updated:=Sys.Date()]
           My.Kable(xDATA)
           saveRDS(xDATA, paste0(CCPR_Folder, 'ccpr_libraries_all.rds'))
           
           IFRC.UPLOAD.RDS.2023(l.host = "ccpr.vn", l.tablename='ccpr_libraries_all', l.filepath=paste0(CCPR_Folder, 'ccpr_libraries_all.rds'), 
                                CheckField=T, ToPrint = F, ToForceUpload=T, ToForceStructure=T)
         })
  CATln_Border(paste('CCPR_DO_IT : ', ToDo, '- DONE.'))
}

#=========================================================================================
DOWNLOAD_YAH_INS_PRICES = function(p_nbdays_back, tickers, freq.data, p_saveto="") {
  # ------------------------------------------------------------------------------------------------
  # p_nbdays_back=100; tickers = ('^TYX'); freq.data="daily"; p_saveto=paste0(UData, "download_yah_bnd_prices.rds")
  # p_nbdays_back=10000; tickers = ('IBM'); freq.data="daily"; p_saveto=paste0(UData, "download_yah_stkin_prices.rds")
  # p_nbdays_back=10000; tickers = ('^N225'); freq.data="daily"; p_saveto=paste0(UData, "download_yah_indin_prices.rds")
  # p_nbdays_back=100; tickers = ('JPY=X'); freq.data="daily"; p_saveto=paste0(UData, "download_yah_cur_prices.rds")
  # p_nbdays_back=100; tickers = ('BTC-USD'); freq.data="daily"; p_saveto=paste0(UData, "download_yah_curcry_prices.rds")
  # dt = DOWNLOAD_YAH_INS_PRICES(p_nbdays_back=100, tickers=('^TYX'), freq.data="daily", p_saveto=paste0(UData, "download_yah_bnd_prices.rds"))
  gc()
  first.date <- Sys.Date() - p_nbdays_back
  last.date <- Sys.Date()
  file_dir = list.files(path=file.path(tempdir(), 'BGS_Cache'), "*.rds$", full.names = T)
  file.remove(file_dir)
  l.out = try(yfR::yf_get(tickers = tickers, 
                          first_date = first.date,
                          last_date  = last.date, 
                          freq_data  = freq.data,
                          be_quiet = T,
                          cache_folder = file.path(tempdir(), 'BGS_Cache') )) # cache in tempdir()
  # l.out = try(BatchGetSymbols(tickers = tickers, 
  #                             first.date = first.date,
  #                             last.date = last.date, 
  #                             freq.data = freq.data,
  #                             cache.folder = file.path(tempdir(), 'BGS_Cache') )) # cache in tempdir()
  # l.out = try(BatchGetSymbols(tickers = tickers, first.date = first.date, last.date = last.date, 
  #                             freq.data = freq.data))
  if (all(class(l.out)!="try-error"))
  {
    my_data = setDT(l.out$df.tickers)
    My.Kable(my_data)
    # str(my_data)
    if (nrow(my_data)>0)
    {
      colnames(my_data) = gsub("price.","", colnames(my_data))
      setnames(my_data, "ref.date", "date")
      my_data[, ":="(source="YAH", codesource=ticker, code=as.character(NA))]
      My.Kable(my_data)
      list_first = c( "date", "close", "source", "codesource")
      setcolorder(my_data, c(list_first, setdiff(colnames(my_data), list_first)))
      
    } else { my_data=data.table()}
  } else { my_data=data.table()}
  return(my_data)
}

#==================================================================================================
READ_DATA_SSI_JSON = function(pCode ='hose', ToSave=''){
  #------------------------------------------------------------------------------------------------
  MyData = data.table()
  x = readr::read_file('R:/PYTHON/template.py')
  x = gsub('EXCHANGE', pCode, x)
  writeLines(x,paste0('R:/PYTHON/stock_ex.py'))
  MyFile = paste0('R:/PYTHON/GET_DATA/response/test', pCode,'.json')
  CATln_Border(MyFile)
  MyExcution = paste0('python R:/PYTHON/stock_ex.py > ', MyFile)
  shell(MyExcution)
  
  y = readr::read_file(MyFile)
  y = gsub('\r\n','',y)
  y = gsub("'",'"',y)
  y = gsub("None",'null',y)
  print(substr(y,1,60))
  x = jsonlite::fromJSON(y)
  MyData = setDT(x$data$stockRealtimes)
  MyData = CLEAN_COLNAMES(MyData)
  MyData[, date:=SYSDATETIME(10)]
  My.Kable.MaxCols(MyData)
  if (nchar(ToSave)>0) { saveRDS(MyData, ToSave)}
  return(MyData)
}

#===============================================================================
GET_SSI_DATA_TODAY = function(ToSave = ''){
  #-----------------------------------------------------------------------------
  # x = GET_SSI_DATA_TODAY(ToSave = paste0('R:/DATA/SSI/exc_',gsub('-','', Sys.Date()), '.rds'))
  My_SSI = list()
  My_SSI[[1]] = try(READ_DATA_SSI_JSON(pCode ='hose', ToSave=paste0('R:/DATA/SSI/hose_', gsub('-','', Sys.Date()), '.rds')))
  My_SSI[[2]] = try(READ_DATA_SSI_JSON(pCode ='hnx', ToSave=paste0('R:/DATA/SSI/hnx_', gsub('-','', Sys.Date()), '.rds')))
  My_SSI[[3]] = try(READ_DATA_SSI_JSON(pCode ='upcom', ToSave=paste0('R:/DATA/SSI/upc_', gsub('-','', Sys.Date()), '.rds')))
  All_Data = rbindlist(My_SSI, fill=T)
  All_Data[exchange=='hose', market:='HSX'] 
  All_Data[exchange=='hnx', market:='HNX'] 
  All_Data[exchange=='upcom', market:='UPC'] 
  str(All_Data)
  All_Data[, ':='(codesource=stocksymbol, code=paste0('STKVN',stocksymbol), source='SSI', close=lastmatchedprice)] 
  All_Data = All_Data %>% select(code, codesource, market, date, close, everything())
  
  My.Kable.MaxCols(All_Data, pMax=10)
  
  if (nchar(ToSave)) { 
    CATln_Border(paste(ToSave, nrow(All_Data)))
    saveRDS(All_Data, ToSave)
  }
  return(All_Data)
}

#============================================================================================
EPOCH_NOW = function(pBase=1000){
  #------------------------------------------------------------------------------------------
  n = Sys.time()
  epoch.now = as.numeric(n)*pBase
  return(epoch.now)
}


#===================================================================================================
CHECK_TIMEBETWEEN = function(pTimeInf="00:30", pTimeSup="23:30") {
  # ------------------------------------------------------------------------------------------------
  CurrentTime = substr(as.character(Sys.time()), 12,16)
  if (CurrentTime>=pTimeInf & CurrentTime<=pTimeSup) {TimeBetweenRes=T} else { TimeBetweenRes=F}
  return(TimeBetweenRes)
}
# CHECK_TIMEBETWEEN(pTimeInf="01:00", pTimeSup="06:00")
# 
# #===================================================================================================
# DOWNLOAD_SSI_EXCHANGE = function(pID="SSI_HOSE"){
#   #-------------------------------------------------------------------------------------------------
#   FileName   = paste0('R:/PYTHON/GET_DATA/response/', pID, '.json')
#   file.remove(FileName)
#   # ...
#   MyExcution = paste('python R:/PYTHON/GET_DATA/post_json_id.py iboard.xlsx', pID)
#   shell(MyExcution)
#   if (file.exists(FileName)) {
#     x     = jsonlite::fromJSON(FileName)
#     xData = as.data.table(x$data$stockRealtimes)
#     xData = CLEAN_COLNAMES(xData)
#     xData = xData %>% mutate(across(.cols=where(is.integer), .fns=as.numeric))
#     # Xdata[, ':='(codesource=stocksymbol, code=paste0('STKVN', stocksymbol), close)]
#     xData[exchange=='hose', market:='HSX']
#     xData[exchange=='hnx', market:='HNX']
#     xData[exchange=='upcom', market:='UPC']
#     # str(Xdata)
#     xData[, ':='(codesource=stocksymbol, code=paste0('STKVN',stocksymbol), source='SSI', close=lastmatchedprice)]
#     xData[, date:=SYSDATETIME(10)]
#     xData = xData %>% select(code, codesource, market, date, close, everything())
#     
#     My.Kable.MaxCols(xData)
#   } else { xData = data.table()}
#   # str(Xdata)
#   return(xData)
#   
# }

#======================================================================================================
LIST_YAH_BY_TYPE = function(pCode='crypto', pURLx = 'https://finance.yahoo.com/crypto/', ToCheckFile=T){
  #-------------------------------------------------
  
  # ToCheckFile = T
  ToSkip = F
  FileName = paste0("LIST_YAH_", toupper(pCode), '.rds')       #'THA': SYMBOL CUA THAILAND, CACH TIM SYMBOL: ISO3 ....
  FileNameFull = paste0('R:/DATA/YAH/', FileName)
  CATln_Border(FileNameFull)
  
  if (!ToSkip)
  {
    k = -1
    ToContinu = T
    Data_All = data.table()
    while (ToContinu)
    {
      k = k+1
      pURL     = paste0(pURLx, '?count=100&offset=', k*100)
      CATln('')
      CATln_Border(paste(k+1, ">>>", pURL))
      content  = rvest::read_html(pURL)
      tables   = content %>% html_table(fill = TRUE)
      if (length(tables)>0)
      {
        xData    = try(as.data.table(tables[[1]]))
        Sys.sleep(1)
        if (all(class(xData)!='try-eror'))
        {
          xData[, updated:=Sys.Date()]
          xData[, source:='YAH']
          
          # xData[, iso3:=pISO3]
          # xData[, country:=countrycode(pISO3, origin = 'iso3c', destination = 'country.name')[1]]
          if (nrow(xData)>1)
          {
            ToContinu = T
            xData = CLEAN_COLNAMES(xData)
            My.Kable(xData)
            Data_All = rbind(Data_All, xData, fill=T)
            Data_All[, codesource:= symbol]
            
            Sys.sleep(1)
            FileToSave = FileNameFull
            CATln_Border(paste0('SAVE FILE TO:   ',FileToSave) )
            saveRDS(Data_All, FileToSave)
          } else { ToContinu = F}
          Sys.sleep(2)
        }
      } else { ToContinu = F}
    }
    My.Kable(Data_All)
    # Data_All[, codesource:= symbol]
    # CATln_Border(FileName) 
    # Sys.sleep(1)
    # FileToSave = FileNameFull
    # saveRDS(Data_All, FileToSave)
  }
  return(Data_All)
}

#==============================================================================================
RUN_LOOP_BY_BROUP_AND_SAVE_DATA_FREE = function(pOption='FUTURES', Nb_Group = 5, Nb_Min = 50000, Frequency='MONTH')
{ #--------------------------------------------------------------------------------------------
  RData = 'R:/DATA/'
  # pOption='VST_BOARD_MEMBER'
  # pOption='YAH_STK_SECTOR_WEBSITE'
  # Frequency='DAY'
  FilePath = paste0(CCPRData,'YAH/', 'LIST_YAH_DATA_FREE_PRICES.rds')
  data = setDT(read_sheet(ss = 'https://docs.google.com/spreadsheets/d/1l6XECtOjhZFZAE-fZ2_OvcW4FFDWInCN9MeQ29-kngY/edit#gid=1159323064',
                          sheet = 'YAH'))
  switch(pOption,
         'INDEXS'      = {
           List_Codes  = unique(data[!is.na(YAH_INDEX)]$YAH_INDEX)
         },
         'FUTURES'     = {
           List_Codes  = unique(data[!is.na(YAH_FUTURES)]$YAH_FUTURES)
         },
         'ETFS'        = {
           List_Codes  = unique(data[!is.na(YAH_ETF)]$YAH_ETF)
         }
  )
  switch(Frequency,
         'MONTH' = {CurrentFileDate = substr(gsub('-','', as.character(Sys.Date())),1,6)},
         'DAY'   = {CurrentFileDate = substr(gsub('-','', as.character(Sys.Date())),1,8)}
  )
  # CurrentYYYYMM = substr(gsub('-','', as.character(Sys.Date())),1,6)
  CATln_Border(FilePath)
  Nb_TODO  = min(Nb_Min, length(List_Codes))
  Nb_Total = length(List_Codes)
  FileToSave = FilePath
  data_list = list()
  for (i in 1:Nb_TODO)
  {
    # i =1
    pCur     = List_Codes[i]
    CATln("")
    CATln_Border(paste('DOWNLOAD_CURRENCY_AND_SAVE : ', i,  '/', Nb_TODO, '/', Nb_Total, ' = ', pCur))
    
    switch(pOption,
           'INDEXES'       = {
             My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCur, pDur='1d', NbDays=250000))
           },
           'FUTURES'       = {
             My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCur, pDur='1d', NbDays=250000))
           },
           'ETFS' = {
             My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCur, pDur='1d', NbDays=250000))
           }
    )
    
    if (all(class(test)!='try-error'))
    {
      test = setDT(test)
      test = CLEAN_COLNAMES(test)
      data_list[[i]] = My_Data
      # str(data_update)
    }
    data_all   = rbindlist(data_list)
    Sys.sleep(1)
    
    if (i %% Nb_Group ==0 | i == Nb_TODO)
    {
      if (file.exists(FileToSave))
      {
        data_old = readRDS(FileToSave)
      }  else {data_old = data.table()}
      
      data_final = unique(rbind(data_all,data_old), by = c('codesource','date'))
      saveRDS(data_final,FileToSave )
      CATln(paste('SAVED : ', FileToSave, '-', Format.Number(nrow(data_final),0),'records'))
      
      Sys.sleep(2)
    }
  }
  My.Kable.TB(data_final)
  return(data_final)
}

#=========================================================================================
DOWNLOAD_YAH_INS_PRICES_UNADJ = function(p_nbdays_back, tickers, freq.data, p_saveto="", NbTry=10) {
  # ------------------------------------------------------------------------------------------------
  # p_nbdays_back=100; tickers = ('^TYX'); freq.data="daily"; p_saveto=paste0(UData, "download_yah_bnd_prices.rds")
  # p_nbdays_back=10000; tickers = ('IBM'); freq.data="daily"; p_saveto=paste0(UData, "download_yah_stkin_prices.rds")
  # p_nbdays_back=10000; tickers = ('^N225'); freq.data="daily"; p_saveto=paste0(UData, "download_yah_indin_prices.rds")
  # p_nbdays_back=100; tickers = ('JPY=X'); freq.data="daily"; p_saveto=paste0(UData, "download_yah_cur_prices.rds")
  # p_nbdays_back=100; tickers = ('BTC-USD'); freq.data="daily"; p_saveto=paste0(UData, "download_yah_curcry_prices.rds")
  # dt = DOWNLOAD_YAH_INS_PRICES(p_nbdays_back=100, tickers=('^TYX'), freq.data="daily", p_saveto=paste0(CCPRData, "download_yah_bnd_prices.rds"))
  
  gc()
  # NbTry=1
  # p_nbdays_back=100000; tickers = ('RXRX'); freq.data="daily"; p_saveto=''
  Divider = 2
  
  NrTry       = 0
  NbDays_ToDo = p_nbdays_back*Divider
  ToContinu   = T
  
  while (ToContinu & NrTry<=NbTry)
  {
    NrTry       = NrTry+1
    NbDays_ToDo = floor(NbDays_ToDo/(Divider))
    print(paste('try ', NrTry, '=', NbDays_ToDo))
    
    first.date <- Sys.Date() - NbDays_ToDo
    last.date <- Sys.Date()
    file_dir = list.files(path=file.path(tempdir(), 'BGS_Cache'), "*.rds$", full.names = T)
    file.remove(file_dir)
    l.out = try(yfR::yf_get(tickers = tickers, 
                            first_date = first.date,
                            last_date  = last.date, 
                            freq_data  = 'daily',
                            be_quiet = T,
                            cache_folder = file.path(tempdir(), 'BGS_Cache') )) # cache in tempdir
    nrow(l.out)
    if (length(l.out)>0 && nrow(l.out)>0) { ToContinu = F }
  }
  
  if (all(class(l.out)!="try-error"))
  {
    my_data = setDT(l.out)
    
    # My.Kable(my_data)
    # str(my_data)
    if (nrow(my_data)>0)
    {
      my_data = CLEAN_COLNAMES(my_data)
      colnames(my_data) = gsub("price_","", colnames(my_data))
      my_data = CCPR_SETNAMES(my_data, "ref_date", "date")
      my_data[, ":="(source="YAH", codesource=ticker, code=as.character(NA))]
      
      my_data = CCPR_SETNAMES(my_data, "ret_closing_prices", "ret_closing")
      my_data = CCPR_SETNAMES(my_data, "ret_adjusted_prices", "ret_adjusted")
      my_data = CCPR_SETNAMES(my_data, "cumret_adjusted_prices", "cumret_adjusted")
      my_data = CCPR_SETNAMES(my_data, "adjusted", "closeadj")
      
      list_first = c( "date", "close", "source", "codesource")
      setcolorder(my_data, c(list_first, setdiff(colnames(my_data), list_first)))
      My.Kable.TB(my_data)
    } else { my_data=data.table()}
  } else { my_data=data.table()}
  # str(my_data)
  return(my_data)
}

CCPR_SETNAMES = function(pData, OldName, NewName)
{
  # pData = my_data
  if (OldName %in% names(pData)) { setnames(pData, OldName, NewName)}
  return(pData)
}

# ==================================================================================================
DOWNLOAD_YAH_STK_DIVIDEND = function(pCode = 'AAPL') {
  # ------------------------------------------------------------------------------------------------
  # pCode = 'X12345'
  # pCode = 'XRXR'
  Epoch_Now = floor(EPOCH_NOW())
  # pURL = 'https://query1.finance.yahoo.com/v8/finance/chart/AAPL?region=US&lang=en-US&includePrePost=false&interval=1mo&useYfid=true&range=1d&corsDomain=finance.yahoo.com&.tsrc=finance'
  pURL  = paste0('https://finance.yahoo.com/quote/', pCode, '/history?period1=0&period2=', Epoch_Now, '&interval=capitalGain%7Cdiv%7Csplit&filter=div&frequency=1d&includeAdjustedClose=true')
  # pURL  = paste0('https://finance.yahoo.com/quote/', pCode, '/history?period1=962409600&period2=1688169600&interval=capitalGain%7Cdiv%7Csplit&filter=div&frequency=1d&includeAdjustedClose=true')
  # pURL = 'https://finance.yahoo.com/quote/IBM/history?period1=962409600&period2=1688169600&interval=capitalGain%7Cdiv%7Csplit&filter=div&frequency=1d'
  CATln_Border(pURL)
  content  = rvest::read_html(pURL)
  tables   = content %>% html_table(fill = TRUE)
  # rm(d.dates)
  d.dates  = try(as.data.table(tables[[1]]$Date), silent=T)
  if (! ( is.null(nrow(d.dates)) || nrow(d.dates)==0) )
  {
    class(d.dates)
    dates = d.dates[, .(date = as.Date(V1, '%B %d, %Y'))][!is.na(date)]
    
    d.dividends  = as.data.table(tables[[1]]$Dividends)
    dividends    = d.dividends[, .(dividend = as.numeric(word(V1,1,1)))][!is.na(dividend)]
    
    MyData       = cbind(dates, dividends)
    MyData[, ':='(source='YAH', codesource=pCode, dataset='DIVIDEND', updated=Sys.Date())]
    setnames(MyData, 'dividend', 'close')
    MyData       = MyData %>% select(source, codesource, dataset, date, close, everything())
    MyData       = MyData[order(date)]
    My.Kable.TB(MyData)
  } else {
    CATln('NO DATA')
    MyData = data.table() 
  }
  return(MyData)
}

MERGE_FILES_YAH_SECTOR_BY_MONTH = function(YYYYMM = '202306', FilePrefix = '_PRICES_', SubFolder='YYYYMM/', AddDate=F)
{
  Data_List = list()
  for (k in 1:length(List_Sector))
  {
    # k = 5
    FileName = paste0('CCPR_YAH_STK_', List_Sector[k][[1]][[1]], FilePrefix, YYYYMM)
    FullName = paste0('R:/CCPR/DATA/YAH/', SubFolder, FileName, '.rds')
    if (file.exists(FullName))
    {
      CATln_Border(FullName)
      NrRepeat  = 1
      ToContinu = T
      while (ToContinu & NrRepeat<=3)
      {
        x = try(readRDS(FullName))
        if (all(class(x)!='try-error'))
        {
          ToContinu = F
        } else {
          NrRepeat = NrRepeat+1
        }
      }
      
      if (all(class(x)!='try-error'))
      {
        Data_List[[k]]  = x
        ncodes   = nrow(unique(x, by=('codesource')))
        ndates   = nrow(unique(x, by=('date')))
        nrecords = nrow(x)
        CATln(paste('DATA (codes, dates, records) =', ncodes, ndates, nrecords))
      } else {
        CATln('DATA ERROR')
        file.remove(FullName)
        CATln(paste('REMOVE FILE =', FullName))
      }
    }
  }
  
  CATln_Border("DATA ALL")
  Data_All = rbindlist(Data_List, fill=T)
  Data_All = Data_All[!is.na(codesource) & !is.na(date) & !is.na(close)]
  My.Kable(Data_All)
  str(Data_All)
  
  ncodes   = nrow(unique(Data_All, by=('codesource')))
  ndates   = nrow(unique(Data_All, by=('date')))
  mindate  = min(unique(Data_All, by=('date'))$date)
  maxdate  = max(unique(Data_All, by=('date'))$date)
  nrecords = nrow(Data_All)
  CATln(paste('DATA (codes, dates, records) =', Format.Number(ncodes,0), Format.Number(ndates,0), 
              mindate, maxdate, Format.Number(nrecords,0)))
  FileName = paste0('CCPR_YAH_STK_ALL_SECTORS', FilePrefix, YYYYMM)
  FullName = paste0('R:/CCPR/DATA/YAH/', FileName, '.rds')
  CATln_Border(FullName)
  saveRDS(Data_All, FullName)
  CATln('SAVED')
  if (AddDate)
  {
    FileName = paste0('CCPR_YAH_STK_ALL_SECTORS', FilePrefix, YYYYMM, '_', gsub('-','',Sys.Date()))
    FullName = paste0('R:/CCPR/DATA/YAH/', FileName, '.rds')
    CATln_Border(FullName)
    saveRDS(Data_All, FullName)
    CATln('SAVED')
  }
  # xs = WRITE_SUMMARY_FULLPATH(pFileName=FullName, ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)
  rm(Data_List)
  return(Data_All)
}

#==================================================================================
MERGE_CODE_FROM_INSREF = function(pData, ins_ref, source){
  #---------------------------------------------------------------------------------
  switch(source,
         'YAH' = { pData = merge(pData[, -c('type', 'code','name', 'continent', 'country','scat')], 
                                 ins_ref[, .(type, code,name,codesource=yah, continent, country,scat)], all.x=T, by='codesource') }
  )
  return(pData)
}

#==================================================================================
COUNT_OUR_DATABASE = function(MyREF, Label, Group){
  #--------------------------------------------------------------------------------
  MyStats = data.table(
    group     = Group, label = Label,
    datasets  = Format.Number(length(unique(MyREF[!is.na(code)]$code)),0), 
    records   = Format.Number(sum(MyREF[!is.na(records)]$records),0), 
    startdate = as.character(min(MyREF[!is.na(startdate)]$startdate)),
    enddate   = as.character(max(MyREF[!is.na(enddate)]$enddate)),
    countries = Format.Number(length(unique(MyREF[!is.na(country)]$country)),0)
  )
  My.Kable.All(MyStats)
  return(MyStats)
}

#=================================function_loop_1
DOWNLOAD_YAH_BOARD = function(pCode)
{
  
  
  My_Board = try(GET_YAH_BOARD(pCodesource=pCode, ToKable=T))
  Sys.sleep(3)
  My_Board[, codesource:= pCode]
  My_Board[grepl('CEO$|Chief Executive Officer$', title), ceo:=1]
  My_Board[grepl('^Chairman', title), cob:=1]
  
  if (all(class(My_Board)!='try-error'))
  {
    Data_List = My_Board
  }
  
  Sys.sleep(2)
  return (Data_List)
}

# ==================================================================================================
WEB_SAVE_AS = function(pURL = 'https://api.nasdaq.com/api/quote/AAPL/historical?assetclass=stocks&fromdate=2013-09-09&limit=3600&todate=2023-07-19', SaveFolder='c:/python/downloads/')
{
  FileTempHTML = paste0('temp_', gsub('-|:|[.]','', gsub(' ','_', Sys.time())),'.html')
  # MyExecution = gsub('<CODE>', pCode, 'python r:/python/savehtml.py "http://en.stockbiz.vn/Stocks/<CODE>/HistoricalQuotes.aspx" "r:/python/stb.html"')
  MyExecution = gsub('<URL>', pURL, gsub('<FOLDER>', SaveFolder, gsub('<FILE>', FileTempHTML, 'py r:/python/savehtml_edit.py "<URL>" "<FOLDER><FILE>"')))
  CATln_Border(MyExecution)
  # MyExecution = gsub('<CODE>', pCode, 'python r:/python/savehtml.py "http://en.stockbiz.vn/Stocks/<CODE>/Snapshot.aspx" "r:/python/stb.html"')
  shell(MyExecution)
  
  if (file.exists(paste0(SaveFolder,FileTempHTML)))
  {
    My_HTML = readr::read_file(paste0(SaveFolder,FileTempHTML))
  }
  # content  = try(rvest::read_html("r:/python/stb.html"))
  return(My_HTML) 
}

# ==================================================================================================
WEB_SAVE_AS_OLD = function(pURL = 'https://api.nasdaq.com/api/quote/AAPL/historical?assetclass=stocks&fromdate=2013-09-09&limit=3600&todate=2023-07-19', SaveFolder='c:/python/downloads/')
{
  FileTempHTML = paste0('temp_', gsub('-|:|[.]','', gsub(' ','_', Sys.time())),'.html')
  # MyExecution = gsub('<CODE>', pCode, 'python r:/python/savehtml.py "http://en.stockbiz.vn/Stocks/<CODE>/HistoricalQuotes.aspx" "r:/python/stb.html"')
  MyExecution = gsub('<URL>', pURL, gsub('<FOLDER>', SaveFolder, gsub('<FILE>', FileTempHTML, 'py r:/python/savehtml.py "<URL>" "<FOLDER><FILE>"')))
  CATln_Border(MyExecution)
  # MyExecution = gsub('<CODE>', pCode, 'python r:/python/savehtml.py "http://en.stockbiz.vn/Stocks/<CODE>/Snapshot.aspx" "r:/python/stb.html"')
  shell(MyExecution)
  
  if (file.exists(paste0(SaveFolder,FileTempHTML)))
  {
    My_HTML = readr::read_file(paste0(SaveFolder,FileTempHTML))
  }
  # content  = try(rvest::read_html("r:/python/stb.html"))
  return(My_HTML) 
}

# ==================================================================================================
CCPR_CHECK_MYPC = function(pSleep = 10) {
  # ------------------------------------------------------------------------------------------------
  # try(CCPR_CHECK_MYPC(pSleep=10))
  MyPC = ""
  if (file.exists('c:/r/my_pc.txt'))
  {
    MyPC = as.character(readLines('c:/r/my_pc.txt'))
  }
  FileName = 'CHECK_MYPC.rds'
  pFolder  = paste0(CCPRData, 'MONITOR/')
  ODDrive_Data = paste0(ODDrive, substr(pFolder, 1, 1), word(pFolder, 2, 2, ":"))
  CATln(ODDrive_Data)
  
  if (!file.exists(ODDrive_Data))
  {
    try(dir.create(ODDrive_Data))
  }
  FullPath_Local = paste0(pFolder, FileName)
  FullPath_ODDrive = paste0(ODDrive_Data, FileName)
  
  Has_Config      = ifelse(file.exists('c:/r/config.r'), 'Y', 'N')
  Has_MyPC        = ifelse(file.exists('c:/r/my_pc.txt'), 'Y', 'N')
  Has_Py_Download = ifelse(file.exists('c:/r/my_pc.txt'), 'Y', 'N')
  Has_CCPR        = if_else(file.exists(CCPRData),'Y','N')
  Has_Udata       = if_else(file.exists(UData),'Y','N')
  
  Data_One = data.table(
    mypc        = MyPC,
    date        = Sys.Date(),
    updated     = substr(Sys.time(), 1, 19),
    config      = Has_Config,
    my_pc       = Has_MyPC,
    py_download = Has_Py_Download,
    ccprdata    = Has_CCPR,
    udata       = Has_Udata
  )
  if (!file.exists(FullPath_Local)) {
    Data_MYPC = data.table()
  } else {
    Data_MYPC = CCPR_READ_RDS_WITH_ALTERNATIVE(FileFrm = FullPath_Local, FileAlt = FullPath_ODDrive)[[1]]
    
  }
  Data_MYPC = rbind(Data_MYPC, Data_One, fill = T)
  Data_MYPC$x = NULL
  Data_MYPC = unique(Data_MYPC[order(mypc,-updated)], by = 'mypc')[!is.na(mypc)]
  My.Kable.All(Data_MYPC)
  
  try(SAVERDS_WITH_ONEDRIVE(pData=Data_MYPC, FileFolder=paste0(CCPRData, 'MONITOR/'), FileName=FileName , SaveOneDrive=T))
  IFRC_SLEEP(pSleep)
}
# ==================================================================================================

LOOP_DOWNLOAD_YAH_STK = function(Dataset='CLOSE', 
                                 Folder_List=CCPRData, File_List='LIST_YAH_STK_GBL_WCEO', 
                                 Folder_Out=CCPRData, Nb_Min, Nb_Group)
{
  Data_final = data.table()
  CurMonth          = year(Sys.Date())*100 + month(Sys.Date())
  switch(Dataset,
         'CLOSE'   = {
           File_Out = paste0('DOWNLOAD_', gsub('^LIST_','',File_List), '_PRICES_CLOSE_',CurMonth)
         },
         'CLOSEADJ'   = {
           File_Out = paste0('DOWNLOAD_', gsub('^LIST_','',File_List), '_PRICES_CLOSEADJ_',CurMonth)
         },
         
         'DIV'   = {
           File_Out = paste0('DOWNLOAD_', gsub('^LIST_','',File_List), '_DIV_',CurMonth)
         },
         
         'CAPIUSD'   = {
           File_Out = paste0('DOWNLOAD_', gsub('^LIST_','',File_List), '_CAPIUSD_',CurMonth)
         },
         
         'BOARD'   = {
           File_Out = paste0('DOWNLOAD_', gsub('^LIST_','',File_List), '_BOARD_',CurMonth)
         }
  )
  
  Data_List = CCPR_READ_RDS_WITH_ALTERNATIVE(FileFrm=paste0(CCPRData, File_List, '.rds'), FileAlt='')[[1]]
  My.Kable(Data_List)
  List_Codes = Data_List
  
  FilePath = paste0(Folder_Out, File_Out, '.rds')
  CATln_Border(FilePath)
  
  if (nrow(List_Codes)>0)
  {
    if (file.exists(FilePath))
    {
      CATrp('Loading Data ...')
      Data_old = readRDS(FilePath)
      CATln('Loaded')
      IFRC_SLEEP(2)
      if (nrow(Data_old)>0) {
        Data_old = Data_old[!is.na(codesource)]
        List_Codes_ToDo = List_Codes[!codesource %in% Data_old$codesource]
        My.Kable.MaxCols(Data_old)
      } else {
        List_Codes_ToDo = List_Codes
      }
    } else { 
      CATln(paste('FILE NO EXIST :', FilePath))
      Data_old = data.table() 
      List_Codes_ToDo = List_Codes
    }
    
    
    # Data_old$coxdesource = NULL
    # My.Kable.MaxCols(unique(Data_old[, .(codesource)], by='codesource'))
    # Data_old[, codesource:=ticker]
    # saveRDS(Data_old, FilePath)
    
    
    
    if (nrow(List_Codes_ToDo)>0)
    {
      List_Codes_ToDo = List_Codes_ToDo[order(-codesource)]
      My.Kable(List_Codes_ToDo)
      # x = GET_VST_BOARD_MEMBER_BY_CODE(pCode = 'YEG')
      Data_All = data.table()
      
      # FileName = paste0("LIST_YAH_", pISO3, '.rds') 
      # CATln_Border(FileName)
      Data_List = list()
      
      FileData = List_Codes_ToDo
      FileRow  = nrow(List_Codes_ToDo)
      
      Nb_TODO  = min(Nb_Min, FileRow)
      Nb_Total = FileRow
      
      FileToSave = FilePath
      Total_Time  = 0
      
      for (i in 1:Nb_TODO)
      {
        # i =1
        Start.Time = Sys.time()
        pCode    = FileData[i]$codesource
        CATln("")
        # CATln_Border(paste(i, '/', Nb_TODO, ' = ', pCode))
        CATln_Border(paste('RUN_LOOP_BY_BROUP_AND_SAVE : ', '=', i, paste0('(', Nb_Group,')'), '/', Nb_TODO, '/', Nb_Total, ' = ', pCode))
        
        
        switch(Dataset,
               'CLOSE' = {
                 My_Data = try(DOWNLOAD_INTRADAY_YAH(pCode=pCode, pDur='1d', NbDays=250000))  
               },
               'CLOSEADJ' = {
                 My_Data = try(DOWNLOAD_YAH_INS_PRICES_UNADJ(p_nbdays_back=50000, tickers = pCode, 
                                                             freq.data="daily", p_saveto=''))
               },
               'DIV' = {
                 My_Data = try(DOWNLOAD_YAH_STK_DIVIDEND(pCode=pCode))
               }, 
               'CAPIUSD' = {
                 My_Data = try(DOWNLOAD_YAH_STK_CAPIUSD_BY_CODE(pCode=pCode))
               },
               'BOARD' = {
                 My_Data = try(DOWNLOAD_YAH_BOARD(pCode=pCode))
               }
        )
        
        if (all(class(My_Data)!='try-error'))
        {
          Data_List[[i]] = My_Data
        }
        Sys.sleep(2)
        
        if (i %% Nb_Group ==0 | i == Nb_TODO)
        {
          Data_All = rbindlist(Data_List, fill=T)
          Data_final = rbind(Data_old, Data_All, fill=T)
          CATrp('Saving Data ...')
          
          # saveRDS(Data_final, FileToSave)
          
          FileTo=paste0(File_Out, '.rds')
          pFolder = Folder_Out
          ODDrive_Data = paste0(ODDrive, substr(pFolder,1,1), word(pFolder,2,2,":"))
          if (!file.exists(ODDrive_Data))
          {
            try(dir.create(ODDrive_Data))
          }
          FullPath_Local = paste0(pFolder,FileTo)
          FullPath_ODDrive = paste0(ODDrive_Data,FileTo)
          
          SAVERDS_WITH_ONEDRIVE(pData=Data_final, FileFolder=Folder_Out, FileName=FileTo , SaveOneDrive=T)
          
          
          
          CATln('Saved Data ...')
          # saveRDS(Data_final, paste0(ODDrive,FileName ) )
          CATln(paste('SAVED : ', FileToSave, '-', Format.Number(nrow(Data_final),0),'records'))
          Sys.sleep(2)
        }
        End.Time = Sys.time()
        Total_Time = as.numeric(difftime(End.Time, Start.Time, units = 'secs')) + Total_Time
        Avg_Time = Total_Time / i
        End_Forecast = Avg_Time * (Nb_TODO - i)
        Time_To_End = as.difftime(End_Forecast, units = "secs") + Sys.time()
        CATln_Border(paste('TIME TO END :', Time_To_End, '( after',format(End_Forecast, nsmall = 0),'secs )'))
      }
      
      My.Kable.TB(Data_final)
    } else {
      CATln_Border('DATA FULLY UPDATED')
      IFRC_SLEEP(2)
    }
  }
  My.Kable(Data_final)
  return(Data_final)
}

DOWNLOAD_STB_STKVN_PRICES_BY_CODE = function(pCode = 'VND')
{
  pURL = paste0('http://en.stockbiz.vn/Stocks/', pCode, '/HistoricalQuotes.aspx')
  CATln_Border(pURL)
  content    = try(rvest::read_html(pURL))
  if (all(class(content)!='try-error'))
  {
    tables     = content %>% html_table(fill = TRUE)
    xTable     = as.data.table(tables[[23]])
    xData      = xTable[, 1:9]
    xHeader    = gsub(' ','', tolower(as.list(xData[1,])))
    xHeader[1]
    colnames(xData)     = xHeader
    xData      = xData[2:(nrow(xData)-1), ]
    # My.Kable(xData)
    Final_Data = xData[, .(codesource=pCode, source='STB', code=paste0('STKVN', pCode),
                           date  = as.Date(date, '%m/%d/%Y'), 
                           open  = 1000*as.numeric(gsub(',','',open)),
                           high  = 1000*as.numeric(gsub(',','',high)),
                           low   = 1000*as.numeric(gsub(',','',low)),
                           close = 1000*as.numeric(gsub(',','',close)),
                           average = 1000*as.numeric(gsub(',','',average)),
                           close_adj = 1000*as.numeric(gsub(',','',adjustedclose )),
                           change  = 1000*as.numeric(gsub(',','',word(change,1,1,'/') )),
                           volume = as.numeric(gsub(',','',volume ))
    )]
    Final_Data = Final_Data[order(date)]
    Final_Data[, reference:=(close-change)]
    Final_Data[, rt_calculated:=(change)/reference]
    # Final_Data[, change:=]
    My.Kable.TB(Final_Data)
  }
  return(Final_Data)
}

FINAL_DOWNLOAD_STB_STKVN_SHARES_BY_CODE = function(pCode = 'VND')
{
  # pCode = 'VND'
  Final_Data = data.table()
  pURL  = paste0('https://www.stockbiz.vn/Stocks/', pCode, '/Snapshot.aspx')
  x  = WEB_SAVE_AS(pURL = pURL, SaveFolder = 'c:/python/downloads/')
  # writeLines(x, 'c:/temp/x.txt')
  # grepl('1.217.844.009',x)
  SharesLis = gsub('  ',' ', trimws(gsub('\n|</td>','', str.extract(x, 'KL đang niêm yết', '</tr'))))
  SharesLis = REPLACE_LOOP_ALL(SharesLis, '  ', ' ')
  SharesLis = as.numeric(gsub('[.]','', trimws(word(SharesLis,2,2,'>'))))
  Final_Data = data.table(source='STB', ticker=pCode, codesource=pCode, code=paste0('STKVN', pCode), date=SYSDATETIME(10), 
                          shareslis=SharesLis, updated=substr(as.character(Sys.time()),1,19))
  My.Kable.All(Final_Data)
  return(Final_Data)
}


#===============================================================================================
RUN_LOOP_STKVN = function(pOption='VST_BOARD_MEMBER', Nb_Group = 50, Nb_Min = 50000, AddParam='', FillBlank=T, ToForce=F, ToCheckTime=17) { 
  #----------------------------------------------------------------------------------------------
  # RUN_LOOP_STKVN(pOption='DOWNLOAD_C68_STKVN_INTRADAY', Nb_Group = 5, Nb_Min = 15, AddParam='',FillBlank=T, ToForce=F)
  # xd = RUN_LOOP_STKVN(pOption='DOWNLOAD_C68_STKVN_INTRADAY', Nb_Group = 50, Nb_Min = 2000, AddParam='',FillBlank=T, ToForce=F)
  
  # Nb_Group = 5; Nb_Min = 10; AddParam=''; FillBlank=T; ToForce=F; ToCheckTime=17
  rm(FN_Name_BATCH_FUNCTION)
  FN_Name_BATCH_FUNCTION    = match.call()[[1]]
  FINAL_BATCH_FUNCTION(FNName=FN_Name_BATCH_FUNCTION)
  RData = CCPRData
  # RData = paste0(CCPRData)
  ODDrive = ODDrive
  # source(paste0('R:/R/', 'LIBRARIES/WCEO_LIBRARY.r'))
  Data_final = data.table()
  # pOption='VST_BOARD_MEMBER'
  # pOption='YAH_STK_SECTOR_WEBSITE'
  # Frequency='DAY'
  
  # pOption='DOWNLOAD_YAH_STK_TOP100'
  # Nb_Group = 5; Nb_Min = 10; Frequency='MONTH'
  
  # ................................................................................................
  # LOAD LIST_CODES
  # ................................................................................................
  RELOAD_INSREF()
  RELOAD_CALENDAR_VN() # idx_calendar.vn
  CATln('Loading REF ...')
  LastTrading = CCPR_LAST_TRADING_DAY_VN(ToCheckTime, ToPrompt = T)
  File_List      = tolower(paste0(CCPRData, "download_exc_stkvn_ref_history.rds"))
  CATln_Border(File_List)
  List_Companies = setDT(readRDS(File_List))
  List_Companies = unique(List_Companies[order(codesource, -date)], by='codesource')[nchar(codesource)==3]
  List_Companies = merge(List_Companies[, -c('name')], ins_ref[type=='STK', .(code, name=short_name)], all.x=T, by='code')
  My.Kable(List_Companies[, .(name, codesource)])
  CATln('Loaded REF .')
  List_Codes     = unique(List_Companies[,.(codesource, name)], by ='codesource')
  My.Kable(List_Codes)
  FileNameDay = ''
  
  switch(pOption,
         
         'DOWNLOAD_STB_STKVN_MAJOR_HOLDERS_PERCENTAGE' = {
           FileName    =  paste0('DOWNLOAD_STB_STKVN_MAJOR_HOLDERS_PERCENTAGE_', gsub('-','', LastTrading))
           FileNameDay =  paste0('DOWNLOAD_STB_STKVN_MAJOR_HOLDERS_PERCENTAGE_DAY')
         },
         
         'NEW_DOWNLOAD_CAF_STKVN_INTRADAY' = {
           FileName    =  paste0('NEW_DOWNLOAD_CAF_STKVN_INTRADAY_', gsub('-','', LastTrading))
           FileNameDay =  paste0('NEW_DOWNLOAD_CAF_STKVN_INTRADAY_DAY')
         },
         
         'NEW_DOWNLOAD_C68_STKVN_PRICES' = {
           FileName    =  paste0('NEW_DOWNLOAD_C68_STKVN_PRICES_', gsub('-','', LastTrading))
           FileNameDay =  paste0('NEW_DOWNLOAD_C68_STKVN_PRICES_DAY')
         },
         
         'DOWNLOAD_SSI_STKVN_PRICES' = {
           FileName    =  paste0('DOWNLOAD_SSI_STKVN_PRICES_', gsub('-','', LastTrading))
           FileNameDay =  paste0('DOWNLOAD_SSI_STKVN_PRICES')
         },
         'NEW_DOWNLOAD_CAF_STKVN_PRICES' = {
           FileName    =  paste0('NEW_DOWNLOAD_CAF_STKVN_PRICES_', gsub('-','', LastTrading))
           FileNameDay =  paste0('NEW_DOWNLOAD_CAF_STKVN_PRICES_DAY')
         },
         
         'NEW_DOWNLOAD_CAF_STKVN_ORDER' = {
           FileName    =  paste0('NEW_DOWNLOAD_CAF_STKVN_ORDER_', gsub('-','', LastTrading))
           FileNameDay =  paste0('NEW_DOWNLOAD_CAF_STKVN_ORDER_DAY')
         },
         
         'NEW_DOWNLOAD_CAF_STKVN_FOREIGN' = {
           FileName    =  paste0('NEW_DOWNLOAD_CAF_STKVN_FOREIGN_', gsub('-','', LastTrading))
           FileNameDay =  paste0('NEW_DOWNLOAD_CAF_STKVN_FOREIGN_DAY')
         },
         
         'FINAL_DOWNLOAD_STB_STKVN_SHARES' = {
           FileName    =  paste0('FINAL_DOWNLOAD_STB_STKVN_SHARES_', gsub('-','', LastTrading))
           FileNameDay =  paste0('DOWNLOAD_STB_STKVN_SHARES_DAY')
         },
         
         'DOWNLOAD_STB_STKVN_SECTOR' = {
           pOption = 'DOWNLOAD_STB_STKVN_SECTOR'
           FileName =  paste0('DOWNLOAD_STB_STKVN_SECTOR_', gsub('-','', LastTrading))
         }, 
         'DOWNLOAD_C68_STKVN_INTRADAY' = {
           pOption = 'DOWNLOAD_C68_STKVN_INTRADAY'
           FileName =  paste0('DOWNLOAD_C68_STKVN_INTRADAY_DAY_', gsub('-','', LastTrading))
         }, 
         'FINAL_DOWNLOAD_CAF_STKVN_SHARES' = {
           FileName    =  paste0('FINAL_DOWNLOAD_CAF_STKVN_SHARES_', gsub('-','', LastTrading))
           FileNameDay =  paste0('DOWNLOAD_CAF_STKVN_SHARES_DAY')
         }, 
         'FINAL_DOWNLOAD_C68_STKVN_SHARES' = {
           FileName =  paste0('FINAL_DOWNLOAD_C68_STKVN_SHARES_', gsub('-','', LastTrading))
           FileNameDay =  paste0('DOWNLOAD_C68_STKVN_SHARES_DAY')
         }, 
         'FINAL_DOWNLOAD_VST_STKVN_SHARES' = {
           FileName =  paste0('FINAL_DOWNLOAD_VST_STKVN_SHARES_', gsub('-','', LastTrading))
           FileNameDay =  paste0('DOWNLOAD_VST_STKVN_SHARES_DAY')
         }, 
         'DOWNLOAD_STB_STKVN_PRICES_BY_CODE' = {
           FileName = paste0('DOWNLOAD_STB_STKVN_PRICES_BY_CODE_', gsub('-','', LastTrading))
         },
         'DOWNLOAD_STKVN_PRICES_FOR_ALL' = { 
           
           FileName = paste0('CCPR_STKVN_PRICES_FOR_ALL_DAY_', gsub('-','', LastTrading))
         },
         'DOWNLOAD_STKVN_OWNERSHIP_FOR_ALL' = { 
           FileName = paste0('CCPR_STKVN_OWNERSHIP_FOR_ALL_DAY_', gsub('-','', LastTrading))
         },
         'DOWNLOAD_STKVN_PE_FOR_ALL' = {
           FileName = paste0('CCPR_STKVN_PE_FOR_ALL_DAY_', gsub('-','', LastTrading))
         },
         'DOWNLOAD_STKVN_SHAREHOLDER_FOR_ALL' = {
           FileName = paste0('CCPR_STKVN_SHAREHOLDER_FOR_ALL_DAY_', gsub('-','', LastTrading))
         },
         'DOWNLOAD_CAF_STKVN_ANNUAL_RATIOS_BY_CODE' = {
           FileName = paste0('DOWNLOAD_CAF_STKVN_ANNUAL_RATIOS_DAY_', gsub('-','', LastTrading))
         },
         'DOWNLOAD_STKVN_PRICES'   = {
           FileName = paste0('CCPR_STKVN_PRICES_ALL_DAY_', gsub('-','', LastTrading))
         },
         'DOWNLOAD_STB_STKVN_RATIOS_VALUATION'  = {
           FileName = paste0('CCPR_STKVN_RATIOS_VALUATION_DAY_', gsub('-','', LastTrading))
         }, 
         'NEW_DOWNLOAD_C68_STKVN_PERFORMANCE'  = {
           FileName = paste0('CCPR_STKVN_C68_PERFORMANCE_DAY_', gsub('-','', LastTrading))
         }, 
         'FINAL_DOWNLOAD_C68_STKVN_INTRADAY_BY_CODE'  = {
           FileName = paste0('CCPR_STKVN_C68_INTRADAY_BY_CODE_DAY_', gsub('-','', LastTrading))
         }, 
         'NEW_DOWNLOAD_CAF_STKVN_TRADE_BY_CODE'  = {
           FileName = paste0('NEW_DOWNLOAD_CAF_STKVN_TRADE_DAY_', gsub('-','', LastTrading))
         }, 
         'NEW_DOWNLOAD_CAF_STKVN_FOREIGN_BY_CODE'  = {
           FileName = paste0('NEW_DOWNLOAD_CAF_STKVN_FOREIGN_DAY_', gsub('-','', LastTrading))
         }, 
         'DOWNLOAD_HNX_STKVN_SHARES_BY_CODE'  = {
           FileName    =  paste0('DOWNLOAD_HNX_STKVN_SHARES_DAY_', gsub('-','', LastTrading))
         }
         
  )
  
  
  FilePath       = paste0(CCPRData,   FileName, '.rds')
  CATln_Border(paste("SAVE TO FILEPATH : ", FilePath))
  
  # ................................................................................................
  # DOWNLOAD
  # ................................................................................................
  ExtraFolder = ''
  if (nrow(List_Codes)>0)
  {
    if (file.exists(FilePath))
    {
      Data_old = readRDS(FilePath)
      if (nrow(Data_old)>0) {
        Data_old = Data_old[!is.na(codesource)]
        List_Codes_ToDo = List_Codes[!codesource %in% Data_old$codesource]
        My.Kable.MaxCols(Data_old)
      } else {
        List_Codes_ToDo = List_Codes
      }
    } else { 
      CATln(paste('FILE NO EXIST :', FilePath))
      Data_old = data.table() 
      List_Codes_ToDo = List_Codes
    }
    
    
    
    if (nrow(List_Codes_ToDo)>0)
    {
      List_Codes_ToDo = List_Codes_ToDo[order(codesource)]
      My.Kable(List_Codes_ToDo)
      # x = GET_VST_BOARD_MEMBER_BY_CODE(pCode = 'YEG')
      Data_All = data.table()
      
      # FileName = paste0("LIST_YAH_", pISO3, '.rds') 
      # CATln_Border(FileName)
      Data_List = list()
      
      FileData = List_Codes_ToDo
      FileRow  = nrow(List_Codes_ToDo)
      
      Nb_TODO  = min(Nb_Min, FileRow)
      Nb_Total = FileRow
      
      FileToSave = paste0(FileName, '.rds')
      Total_Time  = 0
      
      for (i in 1:Nb_TODO)
      {
        # i =1
        Start.Time = Sys.time()
        pCode    = FileData[i]$codesource
        CATln("")
        # CATln_Border(paste(i, '/', Nb_TODO, ' = ', pCode))
        CATln_Border(paste('RUN_LOOP_STKVN : ', pOption, AddParam, '=', i, paste0('(', Nb_Group,')'), '/', Nb_TODO, '/', Nb_Total, ' = ', pCode))
        Sys.sleep(1)
        My_Data = data.table()
        switch(pOption,
               
               'DOWNLOAD_STB_STKVN_SECTOR'      = {
                 # pCode = 'X18'
                 LastTrading = CCPR_LAST_TRADING_DAY_VN(pHH=hour(Sys.time()), ToPrompt = F)
                 My_Data = try(DOWNLOAD_STB_STKVN_SECTOR(pCode = pCode))
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='STB', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 }
               },
               
               'NEW_DOWNLOAD_C68_STKVN_PRICES'      = {
                 # pCode = 'X18'
                 LastTrading = CCPR_LAST_TRADING_DAY_VN(pHH=hour(Sys.time())+1, ToPrompt = F)
                 My_Data = try(NEW_DOWNLOAD_C68_STKVN_PRICES(pCode=pCode, MaxPages=2)) # MHM_IFRC
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='C68', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 }
               },
               
               'NEW_DOWNLOAD_CAF_STKVN_INTRADAY'      = {
                 # pCode = 'X18'
                 LastTrading = CCPR_LAST_TRADING_DAY_VN(pHH=hour(Sys.time())+1, ToPrompt = F)
                 My_Data = try(NEW_DOWNLOAD_CAF_STKVN_INTRADAY(pCode = pCode, pDate=LastTrading))
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='CAF', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 }
               },
               'NEW_DOWNLOAD_CAF_STKVN_FOREIGN'      = {
                 # pCode = 'X18'
                 My_Data = try(NEW_DOWNLOAD_CAF_STKVN_FOREIGN(pCode = pCode))
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='CAF', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 }
               },
               
               'NEW_DOWNLOAD_CAF_STKVN_ORDER'      = {
                 # pCode = 'X18'
                 My_Data = try(NEW_DOWNLOAD_CAF_STKVN_ORDER(pCode = pCode))
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='CAF', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 }
               },
               
               'NEW_DOWNLOAD_CAF_STKVN_PRICES'      = {
                 # pCode = 'X18'
                 My_Data = try(NEW_DOWNLOAD_CAF_STKVN_PRICES(pCode = pCode))
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='CAF', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 }
               },
               'DOWNLOAD_SSI_STKVN_PRICES'      = {
                 # pCode = 'X18'
                 # pCode = 'VND'
                 My_Data = try(DOWNLOAD_SSI_STKVN_PRICES(pTicker=pCode, pFrequency='DAY'))
                 # My_Data = try(NEW_DOWNLOAD_CAF_STKVN_PRICES(pCode = pCode))
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='SSI', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 }
               },
               
               'FINAL_DOWNLOAD_C68_STKVN_SHARES'      = {
                 # pCode = 'X18'
                 My_Data = try(FINAL_DOWNLOAD_C68_STKVN_SHARES_BY_CODE(pCode = pCode))
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='C68', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 }
               },
               'DOWNLOAD_STB_STKVN_MAJOR_HOLDERS_PERCENTAGE'      = {
                 # pCode = 'X18'
                 My_Data = try(DOWNLOAD_STB_STKVN_MAJOR_HOLDERS_PERCENTAGE_BY_CODE(pCode = pCode))
                 if (all(class(My_Data)!='try-error') && FillBlank && nrow(My_Data)==0)
                 {
                   LastTrading = CCPR_LAST_TRADING_DAY_VN(max(9, hour(Sys.time())), ToPrompt = F)
                   My_Data = data.table(source='STB', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 }
               },
               
               'FINAL_DOWNLOAD_VST_STKVN_SHARES'      = {
                 # pCode = 'X18'
                 My_Data = try(FINAL_DOWNLOAD_VST_STKVN_SHARES_BY_CODE(pCode = pCode))
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='VST', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 }
               },
               'FINAL_DOWNLOAD_CAF_STKVN_SHARES'      = {
                 # pCode = 'X18'
                 My_Data = try(FINAL_DOWNLOAD_CAF_STKVN_SHARES_BY_CODE(pCode = pCode))
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='CAF', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 }
               },
               
               'FINAL_DOWNLOAD_STB_STKVN_SHARES'      = {
                 # pCode = 'X18'
                 My_Data = try(FINAL_DOWNLOAD_STB_STKVN_SHARES_BY_CODE(pCode = pCode))
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='STB', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 }
               },
               
               'DOWNLOAD_C68_STKVN_INTRADAY'      = {
                 # pCode = 'X18'
                 My_Data = try(DOWNLOAD_C68_STKVN_PRICES_INTRADAY_BY_CODE(pCode = pCode))
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='C68', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 }
               },
               
               'DOWNLOAD_STB_STKVN_PRICES_BY_CODE'      = {
                 # pCode = 'X18'
                 My_Data = try(DOWNLOAD_STB_STKVN_PRICES_BY_CODE(pCode = pCode))
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='STB', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 }
               },
               'DOWNLOAD_STKVN_PRICES_FOR_ALL'      = {
                 # pCode = 'X18'
                 My_Data = try(CCPR_STKVN_PRICES_FOR_SECTORS_DAY (pList = pCode, pFile=paste0(CCPRData,'download_exc_stkvn_ref_history.rds' ), MinNb=1000))
                 
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='VST', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 } else {
                   My_Data [, source := 'VST']
                   My_Data [, code  := paste0('STKVN', pCode)]
                 }
               },
               'DOWNLOAD_STKVN_OWNERSHIP_FOR_ALL'      = {
                 # pCode = 'X18'
                 Data = try(DOWNLOAD_VST_STKVN_OWNERSHIP_CATEGORY_BY_CODE ( pCode))
                 My_Data = Data[[1]]
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='VST', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 } else {
                   My_Data [, source := 'VST']
                   My_Data [, code  := paste0('STKVN', pCode)]
                 }
               },
               'DOWNLOAD_STKVN_PE_FOR_ALL'      = {
                 # pCode = 'X18'
                 My_Data = try(CCPR_STKVN_PE_FOR_SECTORS (pList = pCode, pFile=paste0(CCPRData,'download_exc_stkvn_ref_history.rds' ), MinNb=2000))
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='VST', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 } else {
                   My_Data [, source := 'VST']
                   My_Data [, code  := paste0('STKVN', pCode)]
                 }
               },
               'DOWNLOAD_STKVN_SHAREHOLDER_FOR_ALL'      = {
                 # pCode = 'XDC'
                 My_Data = try(DOWNLOAD_VST_STKVN_MAJOR_SHAREHOLDERS_BY_CODE (codes = pCode))
                 # FillBlank = F
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='VST', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 } else {
                   My_Data [, source := 'VST']
                   My_Data [, code  := paste0('STKVN', pCode)]
                 }
               },
               'DOWNLOAD_CAF_STKVN_ANNUAL_RATIOS_BY_CODE'      = {
                 # pCode = 'XDC'
                 My_Data = try(DOWNLOAD_CAF_STKVN_ANNUAL_RATIOS_BY_CODE (pCode = pCode))
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='CAF', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 } else {
                   My_Data [, source := 'VST']
                   My_Data [, code  := paste0('STKVN', pCode)]
                 }
               },
               'DOWNLOAD_STKVN_PRICES'      = {
                 # pCode = 'SSI'
                 My_Data = try(GET_VST_PRICES(pCode))
                 # FillBlank = T
                 if (FillBlank && nrow(My_Data)==0)
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='VST', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 } else {
                   My_Data [, source := 'VST']
                   My_Data [, code  := paste0('STKVN', pCode)]
                 }
               },
               'DOWNLOAD_STB_STKVN_RATIOS_VALUATION'= {
                 # pCode = 'AC4'
                 My_Data = setDT(try(DOWNLOAD_STB_STKVN_RATIOS_VALUATION(pCode)))
                 # My_Data = data.table()
                 # FillBlank = T
                 if (FillBlank && is.null(nrow(My_Data)))
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='VST', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 } else {
                   My_Data [, source := 'VST']
                   My_Data [, code  := paste0('STKVN', pCode)]
                   My_Data [, codesource  := pCode]
                 }
               },
               'NEW_DOWNLOAD_C68_STKVN_PERFORMANCE'= {
                 # pCode = 'XDC'
                 My_Data =    try(NEW_DOWNLOAD_C68_STKVN_PERFORMANCE(pCode, MaxPages=1))
                 # My_Data = data.table()
                 # FillBlank = T
                 if (FillBlank && is.null(nrow(My_Data)))
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='C68', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 } else {
                   My_Data [, source := 'C68']
                   My_Data [, code  := paste0('STKVN', pCode)]
                   My_Data [, codesource  := pCode]
                 }
               },
               'FINAL_DOWNLOAD_C68_STKVN_INTRADAY_BY_CODE'= {
                 # pCode = 'VNM'
                 My_Data =    try(FINAL_DOWNLOAD_C68_STKVN_INTRADAY_BY_CODE(pCode))
                 # My_Data = data.table()
                 # FillBlank = T
                 if (FillBlank && is.null(nrow(My_Data)))
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='C68', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 } else {
                   My_Data [, source := 'C68']
                   My_Data [, code  := paste0('STKVN', pCode)]
                   My_Data [, codesource  := pCode]
                 }
               },
               'NEW_DOWNLOAD_CAF_STKVN_TRADE_BY_CODE'= {
                 # pCode = 'XDC'
                 My_Data =    try(NEW_DOWNLOAD_CAF_STKVN_TRADE_BY_CODE(pCode))
                 # My_Data = data.table()
                 # FillBlank = T
                 if (FillBlank && is.null(nrow(My_Data)))
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='CAF', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 } else {
                   My_Data [, source := 'CAF']
                   My_Data [, code  := paste0('STKVN', pCode)]
                   My_Data [, codesource  := pCode]
                 }
               },
               'NEW_DOWNLOAD_CAF_STKVN_FOREIGN_BY_CODE'= {
                 # pCode = 'VNM'
                 My_Data =    try(NEW_DOWNLOAD_CAF_STKVN_FOREIGN_BY_CODE(pCode))
                 # My_Data = data.table()
                 # FillBlank = T
                 if (FillBlank && is.null(nrow(My_Data)))
                 {
                   # LastTrading = CCPR_LAST_TRADING_DAY_VN(hour(Sys.time()), ToPrompt = F)
                   My_Data = data.table(source='CAF', codesource=pCode, code=paste0('STKVN', pCode), date=LastTrading)
                   My.Kable.All(My_Data)
                 } else {
                   My_Data [, source := 'CAF']
                   My_Data [, code  := paste0('STKVN', pCode)]
                   My_Data [, codesource  := pCode]
                 }
               },
               
               'DOWNLOAD_HNX_STKVN_SHARES_BY_CODE'= {
                 # pCode = 'AAV'
                 My_Data     =    try(DOWNLOAD_HNX_STKVN_SHARES_BY_CODE(pCode))
                 ExtraFolder = 'DATES/STKVN/'
                 # My_Data = data.table()
                 # FillBlank = T
               }
               
               
        )
        
        if (all(class(My_Data)!='try-error'))
        {
          Data_List[[i]] = My_Data
        }
        Sys.sleep(2)
        
        if (i %% Nb_Group ==0 | i == Nb_TODO)
        {
          # DATE
          Data_All = rbindlist(Data_List, fill=T)
          Data_final = rbind(Data_old, Data_All, fill=T)
          # saveRDS(Data_final, FileToSave)
          SAVERDS_WITH_ONEDRIVE(Data_final, paste0(CCPRData,ExtraFolder), FileToSave, ToSummary = T, SaveOneDrive = T)
          
          # DAY :
          FileAll = paste0(gsub(paste0('_',gsub('-','', LastTrading) ),'',FileName ), '.rds')
          # FullPath_CCPR = paste0(CCPRData,gsub(paste0('_',gsub('-','', LastTrading) ),'',FileName ), '.rds')
          # FullPath_ODDrive = paste0(ODDrive,gsub(':',"",FileAll))
          # FileAll = gsub(paste0('_',gsub('-','', LastTrading) ),'',FileToSave )
          if ( file.exists(paste0(CCPRData, '', FileAll))) {
            # data_old1 = CCPR_READ_RDS_WITH_ALTERNATIVE(FullPath_CCPR,FullPath_ODDrive) [[1]]
            data_old1 = CCPR_READRDS(FileFolder = paste0(CCPRData, ''), FileName = FileAll, ToKable = F, ToPrompt = F, ToRestore = T)
          } else { data_old1 = data.table()}
          data_all_final = unique(rbind( Data_final,data_old1, fill = T), by = c('code', 'date'))
          SAVERDS_WITH_ONEDRIVE(data_all_final, paste0(CCPRData,''), FileAll, ToSummary = T, SaveOneDrive = T)
          
          # HISTORY:
          FileNameHistory = paste0(gsub('DAY','HISTORY' , FileAll) )
          if ( file.exists(paste0(CCPRData,FileNameHistory))) {
            # data_old1 = CCPR_READ_RDS_WITH_ALTERNATIVE(FullPath_CCPR,FullPath_ODDrive) [[1]]
            data_old1 = CCPR_READRDS(FileFolder = CCPRData, FileName = FileNameHistory, ToKable = F, ToPrompt = F, ToRestore = T)
          } else { data_old1 = data.table()}
          data_all_final_history = unique(rbind( Data_final,data_old1, fill = T), by = c('code', 'date'))
          SAVERDS_WITH_ONEDRIVE(data_all_final_history, CCPRData, FileNameHistory, ToSummary = T, SaveOneDrive = T)
          CATln(paste('SAVED : ', FileToSave, '-', Format.Number(nrow(Data_final),0),'records'))
          Sys.sleep(2)
        }
        End.Time = Sys.time()
        Total_Time = as.numeric(difftime(End.Time, Start.Time, units = 'secs')) + Total_Time
        Avg_Time = Total_Time / i
        End_Forecast = Avg_Time * (Nb_TODO - i)
        Time_To_End = as.difftime(End_Forecast, units = "secs") + Sys.time()
        CATln_Border(paste('TIME TO END :', substr(Time_To_End,1,19), '( after',format(End_Forecast, nsmall = 0),'secs )'))
      }
      
      My.Kable.TB(Data_final)
    } else {
      CATln_Border('DATA FULLY UPDATED')
      IFRC_SLEEP(2)
    }
  }
  
  # file = paste0(gsub(paste0('_',gsub('-','', LastTrading) ),'',FileName ), '.rds')
  # Location = CCPRData
  # summary = FILE_SUMMARY_CODESOURCE_DATE(paste0(CCPRData,file))
  # table_all = data.table(
  #   location = Location,files = file, codes = summary[[1]]$codes, start =   summary[[1]]$startdate, end = summary[[1]]$enddate,
  #   codes_updated = Nb_TODO, updated_pct = round(Nb_TODO/nrow(List_Codes)*100,2), records = summary[[1]]$records
  # )
  # FullPath_Local = paste0(CCPRData, 'MONITOR/','batch_files.rds')
  # FullPath_ODDrive = paste0(ODDrive, 'MONITOR/','batch_files.rds')
  # if( file.exists(FullPath_Local))
  # {
  #   data_old = CCPR_READ_RDS_WITH_ALTERNATIVE(FileFrm = FullPath_Local, FileAlt = FullPath_ODDrive) [[1]]
  # } else { data_old = data.table()}
  # data_final = unique(rbind(table_all,data_old), by = c('location', 'files') )
  # SAVERDS_WITH_ONEDRIVE(pData = data_final,FileFolder = paste0(CCPRData, 'MONITOR/'),FileName = 'batch_files.rds',SaveOneDrive = F, ToKable = F,ToSummary = F, ToPrompt = T)
  rm(FN_Name_BATCH_FUNCTION)
  FN_Name_BATCH_FUNCTION    = match.call()[[1]]
  FINAL_BATCH_FUNCTION(FNName=FN_Name_BATCH_FUNCTION)
  return(Data_final)
  
  
}

#=======================================================================================================
DOWNLOAD_C68_STKVN_PRICES_INTRADAY_BY_CODE = function(pCode = 'VND')
{
  try(RELOAD_INSREF())
  Final_Data = data.table()
  pURL       = paste0('https://www.cophieu68.vn/quote/summary.php?id=', tolower(pCode))
  CATln(''); CATln_Border(pURL)
  content    = try(rvest::read_html(pURL))
  
  if (all(class(content)!='try-error'))
  {
    tables     = content %>% html_table(fill = TRUE)
    if (length(tables)>=4)
    {
      LastTrading = CCPR_LAST_TRADING_DAY_VN(max(9,hour(Sys.time())), ToPrompt = T)
      xData       = as.data.table(tables[[4]])
      # My.Kable(xData)
      Final_Data = xData[, .(source='C68', codesource=pCode, code=paste0('STKVN', pCode),
                             date         = LastTrading, timestamp=paste0(X1,':00'), 
                             last         = 1000*as.numeric(gsub(',','', X2)),
                             change       = 1000*as.numeric(gsub(',','', X3)),
                             last_volume  = 1000*as.numeric(gsub(',','', X4)),
                             cumul_volume = 1000*as.numeric(gsub(',','', X5))
      )][!is.na(last)]
      My.Kable.TB(Final_Data)
    }
  }
  return(Final_Data)
}

# ==================================================================================================
CCPR_QUICK_INTEGRATION_DAY_FINAL = function(pFolder=UData, pData=data.table(), 
                                            File_Fr='download_caf_stkvn_shares_history.rds', File_To='download_caf_stkvn_shares_day.rds',
                                            pCondition='', IncludedOnly=F, CheckDateSummary=T, MaxDateOnly=T, SaveOneDrive=F, Merge_STD=F, ToForce=F) {
  # ------------------------------------------------------------------------------------------------
  # Updated = 2022-08-14 10:46
  # pData   = xFINAL
  # File_To = 'download_exc_indasx_history.rds'
  
  # DATA_FROM : ....................................................................................
  DATACENTER_LINE_BORDER(paste('CCPR_QUICK_INTEGRATION_DAY_FINAL : ', File_To))
  TODO = T
  
  if (CheckDateSummary && nchar(File_To)>0 & nchar(File_Fr)>0)
  {
    CheckDateSummary = T
    CATln(paste("CHECK DATE SUMMARY :", File_Fr, '/', File_To), top_border=T, p_border = T)
    TODO             = (CCPR_SUMMARY_DATE_RDS(paste0(pFolder, File_To)) < CCPR_SUMMARY_DATE_RDS(paste0(pFolder, File_Fr)))
  } 
  CATln(TODO)
  
  if (ToForce || TODO)
  {
    if (nrow(pData)>0)
    {
      Data_Fr = pData
      # Data_Fr = CLEAN_FIELDS_DATATABLE(Data_Fr)
      if ('indhome' %in% names(Data_Fr)) { Data_Fr[, indhome:=as.numeric(indhome)]}
      if ('stkhome' %in% names(Data_Fr)) { Data_Fr[, stkhome:=as.numeric(stkhome)]}
    } 
    
    Data_Final = data.table()
    
    if (nrow(pData)==0 && file.exists(paste0(UData, File_Fr)))
    {
      Data_Fr = try(readRDS(paste0(UData, File_Fr)))
      if (all(class(Data_Fr)!='try-error') && nrow(Data_Fr)>0)
      {
        
      }
    }
    My.Kable(Data_Fr)
    # DATA_FROM (END) ..............................................................................
    # str(Data_Fr)
    
    if (nrow(Data_Fr)>0 && file.exists(paste0(UData, File_To)))
    {
      Data_To = try(readRDS(paste0(UData, File_To)))
      if (all(class(Data_To)!='try-error') && nrow(Data_To)>0)
      {
        # str(Data_To)
        # Data_To = CLEAN_FIELDS_DATATABLE(Data_To)
        if ('indhome' %in% names(Data_To)) { Data_To[, indhome:=as.numeric(indhome)]}
        if ('stkhome' %in% names(Data_To)) { Data_To[, stkhome:=as.numeric(stkhome)]}
        # Data_Fr = CONVERT_IFEXIST_FIELDS_WITH_SEPARATOR(pData=Data_Fr, AsType="numeric", 
        #                                                 List_Fields=list("last", "high", "low", "close", "indhome", 'stkhome'), ToConvertSeparator=F)
        # 
        # Data_To = CONVERT_IFEXIST_FIELDS_WITH_SEPARATOR(pData=Data_To, AsType="numeric", 
        #                                                 List_Fields=list("last", "high", "low", "close", "indhome", 'stkhome'), ToConvertSeparator=F)
        if (IncludedOnly) { Data_Fr = Data_Fr[code %in% Data_To$code] }
        if (MaxDateOnly)  { 
          MaxDate = max(Data_Fr[!is.na(date)]$date)
          Data_Fr = Data_Fr[date==MaxDate]
        }
        # str(Data_To)
        Data_Final = unique(rbind(Data_To, Data_Fr, fill=T), by=c('code', 'date'))[order(code, date)]
      } else {
        Data_Final = Data_Fr
      }
    } else {
      Data_Final = Data_Fr
    }
    
    if (nrow(Data_Final)>0)
    {
      # if (Merge_STD) { Data_Final = MERGE_DATASTD_BYCODE(Data_Final)}
      if (nchar(pCondition)>0)
      {
        Data_Final = Data_Final[eval(parse(text = pCondition))] 
      }
      # Data_Final = REMOVE_FIELD_TIMESTAMP(Data_Final)
      Data_Final = unique(MERGE_DATASTD_BYCODE(Data_Final)[order(code, date)], by=c('code', 'date'))
      My.Kable.MaxCols(Data_Final)
      SAVERDS_WITH_ONEDRIVE(Data_Final, UData, File_To, SaveOneDrive = SaveOneDrive, ToSummary=T)
    }
  }
}

# ==================================================================================================
MERGE_DATASTD_BYCODE = function(pData, pOption='STD', ToKable=F) {
  # ------------------------------------------------------------------------------------------------
  switch(pOption,
         "SHORT" = {
           pData = merge(pData[, -c('iso2', 'country', 'continent', 'name', 'type', 'fcat')], 
                         ins_ref[, .(code, iso2, country, continent, name=short_name, type, fcat)], all.x=T, by='code')
         },
         "STD" = {
           pData = merge(pData[, -c('iso2', 'country', 'continent', 'name', 'type', 'fcat', 'indhome', 'stkhome', 'indsample', 'stksample', 'capiusd')], 
                         ins_ref[, .(code, iso2, country, continent, name=short_name, type, fcat, indhome=as.numeric(indhome), 
                                     stkhome, indsample, stksample, capiusd)], all.x=T, by='code')
         },
         "ADVANCED" = {
           pData = merge(pData[, -c('iso2', 'country', 'continent', 'name', 'type', 'fcat', 'indhome', 'stkhome', "isin", 'symbol', 'provider', 'cur', 'capiusd', 
                                    'indsample', 'stksample')], 
                         ins_ref[, .(code, iso2, country, continent, name=short_name, type, fcat, indhome=as.numeric(indhome), 
                                     stkhome, indsample, stksample, isin, symbol, provider, cur, capiusd)], all.x=T, by='code')
         }
  )
  # str(pData)
  pData[, name:=trimws(toupper(name))]
  pData = UPDATE_FIELD_HOME(pData)
  # library(data.table)
  # pData = as.data.table(pData)
  pData = pData %>% dplyr::select('iso2', 'country', 'continent', 'type', 'name', 'code', 'date', everything())
  if (ToKable) {  My.Kable.MaxCols(pData) }
  return(pData)
}


# ==================================================================================================
CCPR_QUICK_INTEGRATION_INTRADAY_FINAL = function(pFolder=UData, pData=data.table(), 
                                                 File_Fr='download_hsx_stkvn_intraday_day.rds', File_To='download_hsx_stkvn_intraday_tmp.rds',
                                                 pCondition='', IncludedOnly=F, CheckDateSummary=F, MaxDateOnly=F) {
  # ------------------------------------------------------------------------------------------------
  # Updated = 2022-08-14 10:46
  # pData   = xFINAL
  # File_To = 'download_exc_indasx_history.rds'
  
  # DATA_FROM : ....................................................................................
  DATACENTER_LINE_BORDER(paste('QUICK_INTEGRATION_INTRADAY_FINAL : ', File_To))
  TODO = T
  if (CheckDateSummary && nchar(File_To)>0 & nchar(File_Fr)>0)
  {
    CheckDateSummary = T
    CATln(paste("CHECK DATE SUMMARY :", File_Fr, '/', File_To), top_border=T, p_border = T)
    TODO             = (CCPR_SUMMARY_DATE_RDS(paste0(pFolder, File_To)) < CCPR_SUMMARY_DATE_RDS(paste0(pFolder, File_Fr)))
  } 
  CATln(TODO)
  
  if (TODO)
  {
    if (nrow(pData)>0)
    {
      Data_Fr = pData
      Data_Fr = CLEAN_FIELDS_DATATABLE(Data_Fr)
      if ('indhome' %in% names(Data_Fr)) { Data_Fr[, indhome:=as.numeric(indhome)]}
      if ('stkhome' %in% names(Data_Fr)) { Data_Fr[, stkhome:=as.numeric(stkhome)]}
    } 
    
    Data_Final = data.table()
    
    if (nrow(pData)==0 && file.exists(paste0(UData, File_Fr)))
    {
      Data_Fr = try(DATACENTER_LOADDTRDS_UDATA(File_Fr))
      if (all(class(Data_Fr)!='try-error') && nrow(Data_Fr)>0)
      {
        
      }
    }
    My.Kable(Data_Fr)
    # DATA_FROM (END) ..............................................................................
    # str(Data_Fr)
    
    if (nrow(Data_Fr)>0 && file.exists(paste0(UData, File_To)))
    {
      Data_To = try(DATACENTER_LOADDTRDS_UDATA(File_To))
      if (all(class(Data_To)!='try-error') && nrow(Data_To)>0)
      {
        # str(Data_To)
        Data_To = CLEAN_FIELDS_DATATABLE(Data_To)
        if ('indhome' %in% names(Data_To)) { Data_To[, indhome:=as.numeric(indhome)]}
        if ('stkhome' %in% names(Data_To)) { Data_To[, stkhome:=as.numeric(stkhome)]}
        # Data_Fr = CONVERT_IFEXIST_FIELDS_WITH_SEPARATOR(pData=Data_Fr, AsType="numeric", 
        #                                                 List_Fields=list("last", "high", "low", "close", "indhome", 'stkhome'), ToConvertSeparator=F)
        # 
        # Data_To = CONVERT_IFEXIST_FIELDS_WITH_SEPARATOR(pData=Data_To, AsType="numeric", 
        #                                                 List_Fields=list("last", "high", "low", "close", "indhome", 'stkhome'), ToConvertSeparator=F)
        if (IncludedOnly) { Data_Fr = Data_Fr[code %in% Data_To$code] }
        if (MaxDateOnly)  { 
          MaxDate = max(Data_Fr[!is.na(date)]$date)
          Data_Fr = Data_Fr[date==MaxDate]
        }
        # str(Data_To)
        Data_Final = unique(rbind(Data_To, Data_Fr, fill=T), by=c('code', 'date', 'timestamp'))[order(code, date, timestamp)]
      } else {
        Data_Final = Data_Fr
      }
    } else {
      Data_Final = Data_Fr
    }
    
    if (nrow(Data_Final)>0)
    {
      if (Merge_STD) { Data_Final = MERGE_DATASTD_BYCODE(Data_Final)}
      if (nchar(pCondition)>0)
      {
        Data_Final = Data_Final[eval(parse(text = pCondition))] 
      }
      # Data_Final = REMOVE_FIELD_TIMESTAMP(Data_Final)
      Data_Final = unique(MERGE_DATASTD_BYCODE(Data_Final)[order(code, date, timestamp)], by=c('code', 'date', 'timestamp'))
      My.Kable.MaxCols(Data_Final)
      DATACENTER_SAVEDTRDS_AND_LAST_WITH_ONEDRIVE(Data_Final, paste0(UData,File_To), ToSummary=T, ToSaveLast = T, ToSaveOneDrive = T)
    }
  }
}


# ==================================================================================================
CCPR_SUMMARY_DATE_RDS = function(pFileName, ToBrowse=T, DateOnly=T) {
  # ------------------------------------------------------------------------------------------------
  # pFileName = paste0(UData, "download_EXC_stk_history.rds")
  # pFileName = paste0(UData, "download_HSX_stkvn_ref.rds")
  # pFileName = paste0(UData, "download_HSX_stkvn_intraday_history.rds")
  # pFileName = paste0(UData, "download_HSX_stkvn_intraday_day.rds")
  File_Summary = gsub(".rds", "_summary.txt", pFileName, ignore.case=T)
  datemax      = as.Date("1900-01-01")
  xDate        = datemax
  
  if (file.exists(File_Summary))
  {
    x = try(setDT(fread(File_Summary, sep='\t', fill=T)))
    if (all(class(x)!='try-error') && nrow(x)>0)
    {
      # str(x)
      x = CONVERT_IFEXIST_FIELDS_WITH_SEPARATOR(pData=x, AsType="numeric", List_Fields=list("last", "nbdays", "nb", 'sample', 'home'), ToConvertSeparator=F)
      x = CONVERT_IFEXIST_FIELDS_WITH_SEPARATOR(pData=x, AsType="character", List_Fields=list("type", "code", "source", "codesource"), ToConvertSeparator=F)
      if ("type" %in% names(x)) { x[, type:=as.character(type)]}
      x = CONVERT_IFEXIST_FIELDS_WITH_SEPARATOR(pData=x, AsType="date", List_Fields=list("start", "date", "datelast"), ToConvertSeparator=F)
      if (DateOnly){ x[nchar(as.character(date))>10, date:=as.character(substr(date,1,10))]}
      x = x[nchar(as.character(date))==10][, date:=as.Date(date)]
      xDate = max(x[!is.na(date)]$date)
    }
    
    # str(x)
  }
  # x = SUMMARY_DATE(gsub(".rds", "_summary.txt", pFileName, ignore.case=T))
  if (ToBrowse)
  {
    CATln(paste(FormatSS(pFileName, 85), FormatSS(xDate, 12)))
    catrep('-',100)
  }
  return(xDate)
}


#===================================================================================================
CONVERT_IFEXIST_FIELDS_WITH_SEPARATOR = function(pData=ins_last, AsType="numeric", List_Fields=list("cf_last", "cf_netchange"), ToConvertSeparator=F, EchoOn=F) {
  # ------------------------------------------------------------------------------------------------
  if (nrow(pData)>0)
  {
    for (k in 1:length(List_Fields))
    {
      # k = 1
      # CATln(k)
      pField = List_Fields[[k]]
      
      if (pField %in% names(pData))
      {
        switch(AsType,
               "numeric"      = { 
                 if (ToConvertSeparator)
                 {
                   cParseText = paste0(pField, ":=", 'gsub(",","",',pField, ')')
                   if (EchoOn) { print(cParseText, quote=F) }
                   pData[, eval(parse(text = cParseText))]
                 }
                 pParseText = paste0(pField,":=as.numeric(", pField, ")") 
                 
               },
               "date"         = { pParseText = paste0(pField,":=as.Date(as.character(", pField, "))") },
               "character"    = { pParseText = paste0(pField,":=as.character(", pField, ")") }
        )
        if (pField %in% names(pData)) { pData[, eval(parse(text = pParseText))] }
        if (AsType == 'character' & pField %in% names(pData))
        {
          pParseTextNA = paste0(pField,":=ifelse(nchar(", pField, ")>0,", pField,",as.character(NA))")
          pData[, eval(parse(text = pParseTextNA))]
        }
        
      }
    }
  }
  # str(pData)
  return(pData)
}

QUICK_DEMO = function(pOption='DOWNLOAD_C68_STKVN_SHARES_DAY', ToAppend=F, IntegrateHistory=F, ToCheckTime=9)
{
  # File_History
  ListofCodes = list('VND', 'FPT', 'YRC', 'XXX', 'VNM', 'VIC', 'SSI')
  History_Name = ''
  RELOAD_INSREF()
  xList =list()
  # File_History
  switch (pOption,
          
          'SIMULATE_SSI_STKVN_SHARES'     = {
            
            RELOAD_INSREF()
            pWeb = try(readr::read_file(paste0(CCPRData, 'SSI/SSI_ACB.htm')))
            LastTrading = CCPR_LAST_TRADING_DAY_VN(15, ToPrompt = T)
            
            pCode      = 'ACB'
            pURL       = paste0(CCPRData, 'SSI/SSI_ACB.htm')
            pWeb       = read_html(pURL)
            # <div class="company-basic-info">
            pSelected  = pWeb %>% html_nodes("div.company-basic-info") %>% html_text(trim = T)
            pList      = as.list(pSelected)
            GC_SILENT()
            
            xListedDate   = gsub('Listed Date','', SEARCH_STRING_IN_LIST(pList=pList, pString='Listed Date'))
            xExchange     = gsub('Exchange','', SEARCH_STRING_IN_LIST(pList=pList, pString='Exchange'))
            switch(xExchange,
                   'HOSE' = { xMarket = 'HSX'},
                   { xMarket = as.character(NA)})
            xListedPrice  = gsub('Listed Price','', SEARCH_STRING_IN_LIST(pList=pList, pString='Listed Price'))
            xListedPrice  = 1000*as.numeric(gsub(',','', word(xListedPrice,2,2,'\\)')))
            
            xListedShares      = as.numeric(gsub(',','', gsub('Listed Shares','', SEARCH_STRING_IN_LIST(pList=pList, pString='Listed Shares'))))
            
            xOutstandingShares = gsub('Outstanding Shares','', SEARCH_STRING_IN_LIST(pList=pList, pString='Outstanding Shares'))
            xOutstandingShares = as.numeric(gsub(',','', word(xOutstandingShares,2,2,'\\)')))
            
            Final_Data = data.table(source='SSI', codesource=pCode, ticker=pCode, code=paste0('STKVN', pCode),
                                    date=LastTrading, market=xMarket, date_listed=as.Date(xListedDate, '%d/%m/%Y'),
                                    sharesout=xOutstandingShares, shareslis=xListedShares, close=xListedPrice, capi=xListedPrice*xOutstandingShares)
            # ...
            My.Kable.All(Final_Data)
            SAVERDS_WITH_ONEDRIVE(Final_Data, UData, 'download_ssi_stkvn_shares_day.rds', ToSummary = T, SaveOneDrive = T)
            try(CCPR_QUICK_INTEGRATION_DAY_FINAL(pFolder = UData, pData=data.table(), 
                                                 File_Fr = 'download_ssi_stkvn_shares_day.rds', 
                                                 File_To = 'download_ssi_stkvn_shares_history.rds',
                                                 pCondition='!is.na(close)', IncludedOnly=F, CheckDateSummary=T, MaxDateOnly=F, SaveOneDrive=T, Merge_STD=F))
            
          },
          'DOWNLOAD_SSI_STKVN_PRICES_DAY' = {
            File_Name  = 'DOWNLOAD_SSI_STKVN_PRICES.rds'
            if (IntegrateHistory)
            {
              IntegrateHistory=T
              History_Name  = 'DOWNLOAD_SSI_STKVN_HISTORY.rds'
            }
            xList =list()
            for (i in 1:length(ListofCodes))
            {
              xList[[i]] = try(DOWNLOAD_SSI_STKVN_PRICES(pTicker=ListofCodes[[i]], pFrequency='DAY'))
            }
            # xList[[1]] = try(FINAL_DOWNLOAD_C68_STKVN_SHARES_BY_CODE(pCode = 'VND'))
            # xList[[2]] = try(FINAL_DOWNLOAD_C68_STKVN_SHARES_BY_CODE(pCode = 'FPT'))
            # xList[[3]] = try(FINAL_DOWNLOAD_C68_STKVN_SHARES_BY_CODE(pCode = 'YRC'))
            # xList[[4]] = try(FINAL_DOWNLOAD_C68_STKVN_SHARES_BY_CODE(pCode = 'VNM'))
            # xList[[5]] = try(FINAL_DOWNLOAD_C68_STKVN_SHARES_BY_CODE(pCode = 'VIC'))
            # z = try(DOWNLOAD_SSI_STKVN_PRICES(pTicker='VNM', pFrequency='DAY'))
          },
          
          'DOWNLOAD_C68_STKVN_PRICES_INTRADAY' = {
            pOption    = 'DOWNLOAD_C68_STKVN_PRICES_INTRADAY'
            File_Name  = 'DOWNLOAD_C68_STKVN_PRICES_INTRADAY_DAY.rds'
            if (IntegrateHistory)
            {
              IntegrateHistory=T
              History_Name  = 'DOWNLOAD_C68_STKVN_PRICES_INTRADAY_HISTORY.rds'
            }
            xList =list()
            for (i in 1:length(ListofCodes))
            {
              xList[[i]] = try(DOWNLOAD_C68_STKVN_PRICES_INTRADAY_BY_CODE(pCode = ListofCodes[[i]]))
            }
            # x = DOWNLOAD_C68_STKVN_PRICES_INTRADAY_BY_CODE (pCode = 'VND')
          },
          
          'DOWNLOAD_STB_STKVN_SHARES_DAY' = {
            pOption    = 'DOWNLOAD_STB_STKVN_SHARES_DAY'
            File_Name  = 'DOWNLOAD_STB_STKVN_SHARES_DAY.rds'
            if (IntegrateHistory)
            {
              IntegrateHistory=T
              History_Name  = 'DOWNLOAD_STB_STKVN_SHARES_HISTORY.rds'
            }
            xList =list()
            for (i in 1:length(ListofCodes))
            {
              xList[[i]] = try(FINAL_DOWNLOAD_STB_STKVN_SHARES_BY_CODE(pCode = ListofCodes[[i]]))
            }
            # x = DOWNLOAD_C68_STKVN_PRICES_INTRADAY_BY_CODE (pCode = 'VND')
          },
          
          'DOWNLOAD_VST_STKVN_OWNERSHIP_CATEGORY' = {
            pOption    = 'DOWNLOAD_VST_STKVN_OWNERSHIP_CATEGORY'
            File_Name  = 'DOWNLOAD_VST_STKVN_OWNERSHIP_CATEGORY.rds'
            if (IntegrateHistory)
            {
              IntegrateHistory=T
              History_Name  = 'DOWNLOAD_VST_STKVN_OWNERSHIP_CATEGORY_HISTORY.rds'
            }
            xList =list()
            for (i in 1:length(ListofCodes))
            {
              # i = 1
              x = try(DOWNLOAD_VST_STKVN_OWNERSHIP_CATEGORY_BY_CODE(pCode = ListofCodes[[i]]))
              xList[[i]] = x[[1]]
            }
            # x = DOWNLOAD_C68_STKVN_PRICES_INTRADAY_BY_CODE (pCode = 'VND')
          },
          
          'DOWNLOAD_STB_STKVN_MAJOR_HOLDERS_PERCENTAGE' = {
            pOption    = 'DOWNLOAD_STB_STKVN_MAJOR_HOLDERS_PERCENTAGE'
            File_Name  = 'DOWNLOAD_STB_STKVN_MAJOR_HOLDERS_PERCENTAGE.rds'
            if (IntegrateHistory)
            {
              IntegrateHistory=T
              History_Name  = 'DOWNLOAD_STB_STKVN_MAJOR_HOLDERS_PERCENTAGE_HISTORY.rds'
            }
            xList =list()
            for (i in 1:length(ListofCodes))
            {
              # i = 1
              x = try(DOWNLOAD_STB_STKVN_MAJOR_HOLDERS_PERCENTAGE_BY_CODE(pCode = ListofCodes[[i]]))
              if (all(class(x)!='try-error')) { xList[[i]] = x }
            }
            # x = DOWNLOAD_C68_STKVN_PRICES_INTRADAY_BY_CODE (pCode = 'VND')
          },
          
          'DOWNLOAD_VST_STKVN_PE_BY_CODE' = {
            pOption    = 'DOWNLOAD_VST_STKVN_PE_BY_CODE'
            File_Name  = 'DOWNLOAD_VST_STKVN_PE.rds'
            if (IntegrateHistory)
            {
              IntegrateHistory=T
              History_Name  = 'DOWNLOAD_VST_STKVN_PE_HISTORY.rds'
            }
            xList =list()
            for (i in 1:length(ListofCodes))
            {
              xList[[i]] = try(DOWNLOAD_VST_STKVN_PE_BY_CODE(pCode = ListofCodes[[i]]))
            }
            # x = DOWNLOAD_C68_STKVN_PRICES_INTRADAY_BY_CODE (pCode = 'VND')
          },
          
          'DOWNLOAD_C68_STKVN_SHARES_DAY' = {
            File_Name  = 'DOWNLOAD_C68_STKVN_SHARES_DAY.rds'
            if (IntegrateHistory)
            {
              IntegrateHistory=T
              History_Name  = 'DOWNLOAD_C68_STKVN_SHARES_HISTORY.rds'
            }
            xList =list()
            for (i in 1:length(ListofCodes))
            {
              xList[[i]] = try(FINAL_DOWNLOAD_C68_STKVN_SHARES_BY_CODE(pCode = ListofCodes[[i]]))
            }
            # xList[[1]] = try(FINAL_DOWNLOAD_C68_STKVN_SHARES_BY_CODE(pCode = 'VND'))
            # xList[[2]] = try(FINAL_DOWNLOAD_C68_STKVN_SHARES_BY_CODE(pCode = 'FPT'))
            # xList[[3]] = try(FINAL_DOWNLOAD_C68_STKVN_SHARES_BY_CODE(pCode = 'YRC'))
            # xList[[4]] = try(FINAL_DOWNLOAD_C68_STKVN_SHARES_BY_CODE(pCode = 'VNM'))
            # xList[[5]] = try(FINAL_DOWNLOAD_C68_STKVN_SHARES_BY_CODE(pCode = 'VIC'))
          },
          
          'DOWNLOAD_VST_STKVN_SHARES_DAY' = {
            File_Name  = 'DOWNLOAD_VST_STKVN_SHARES_DAY.rds'
            if (IntegrateHistory)
            {
              IntegrateHistory=T
              History_Name  = 'DOWNLOAD_VST_STKVN_SHARES_HISTORY.rds'
            }
            xList =list()
            for (i in 1:length(ListofCodes))
            {
              # xL 
              xL = try(FINAL_DOWNLOAD_VST_STKVN_SHARES_BY_CODE(pCode = ListofCodes[[i]]))
              if (all(class(xL)!='try-error'))
              {
                xList[[i]] = xL
              }
              
            }
          },
          'DOWNLOAD_CAF_STKVN_SHARES_DAY' = {
            File_Name  = 'DOWNLOAD_CAF_STKVN_SHARES_DAY.rds'
            if (IntegrateHistory)
            {
              IntegrateHistory=T
              History_Name  = 'DOWNLOAD_CAF_STKVN_SHARES_HISTORY.rds'
            }
            xList =list()
            for (i in 1:length(ListofCodes))
            {
              xList[[i]] = try(FINAL_DOWNLOAD_CAF_STKVN_SHARES_BY_CODE(pCode = ListofCodes[[i]]))
            }
          }
  )
  
  if (length(xList)>0)
  {
    
    if (nchar(History_Name)>0)
    {
      Data_History = paste0(CCPRData, History_Name)
    } else {
      Data_History = data.table()
    }
    
    xALL = rbindlist(xList, fill=T)
    
    if (ToAppend & file.exists(paste0(CCPRData, History_Name)))
    {
      Data_Old = try(readRDS(paste0(CCPRData, File_Name)))
    } else { Data_Old = data.table()}
    
    if (grepl('INTRADAY', pOption)) {
      
      if (nrow(xALL)>0) 
      { 
        xALL = unique(xALL, by=c('code', 'date', 'timestamp'))
        Data_Old = unique(rbind(Data_Old, xALL, fill=T), by=c('code', 'date', 'timestamp'))
        if (nchar(History_Name)>0)
        {
          Data_History = unique(rbind(Data_Old, Data_History, fill=T), by=c('code', 'date', 'timestamp'))
          Data_History$x = NULL
          Data_History = Data_History[!is.na(code)]
          My.Kable(Data_History)
        }
      }
    } else {
      if (nrow(xALL)>0) { 
        xALL = unique(xALL, by=c('code', 'date'))
        Data_Old = unique(rbind(Data_Old, xALL, fill=T), by=c('code', 'date'))[!is.na(source)]
        if (nchar(History_Name)>0)
        {
          Data_Old$x = NULL
          str(Data_Old)
          str(Data_History)
          Data_History = unique(rbind(Data_Old, Data_History, fill=T), by=c('code', 'date'))
          Data_History$x = NULL
          Data_History = Data_History[!is.na(code)]
          My.Kable(Data_History)
        }
      }
    }
    
    My.Kable.TB(Data_Old)
    SAVERDS_WITH_ONEDRIVE(Data_Old, CCPRData, File_Name, ToSummary = T, SaveOneDrive = T)
    # saveRDS(Data_Old, paste0(CCPRData, File_Name))
    # xs   = WRITE_SUMMARY_FULLPATH(paste0(CCPRData, File_Name))
    
    if (nrow(Data_History)>0)
    {
      # saveRDS(Data_History, paste0(CCPRData, History_Name))
      # xs   =WRITE_SUMMARY_FULLPATH(paste0(CCPRData, History_Name))
      SAVERDS_WITH_ONEDRIVE(Data_Old, CCPRData, File_Name, ToSummary = T, SaveOneDrive = T)
    }
    
    if (file.exists(UData))
    {
      SAVERDS_WITH_ONEDRIVE(Data_Old, UData, File_Name, ToSummary = T, SaveOneDrive = T)
      if (nrow(Data_History)>0)
      {
        SAVERDS_WITH_ONEDRIVE(Data_Old, UData, File_Name, ToSummary = T, SaveOneDrive = T)
      }
      
    }
  }
}

MONITOR_FILES_SYNCHRONISE = function()
{
  for (pSource in list('HSX', 'HNX', 'UPC', 'C68', 'SSI'))
  {
    File_Name = paste0('DOWNLOAD_', pSource, '_STKVN_REF.rds')
    try(SYNCHRONISE_FILE_FROM_TO(From_Folder = UData, From_File = File_Name, 
                                 To_Folder   = CCPRData, To_File   = File_Name, Method='OVERWRITE', ToForce=F))
    WRITE_SUMMARY_FULLPATH(paste0(CCPRData, File_Name))
  }
}

SYNCHRONISE_FILE_FROM_TO = function(From_Folder=UData, From_File='efrc_ins_ref.rds', To_Folder=CCPRData, To_File='efrc_ins_ref.rds', Method='OVERWRITE', ToForce=F, CheckCode=F)
{
  # paste()
  CATln_Border(paste('SYNCHRONISE_FILE_FROM_TO : ', From_Folder, From_File))
  From_Fullname = paste0(From_Folder, From_File)
  To_Fullname = paste0(To_Folder, To_File)
  if (file.exists(From_Fullname))
  {
    file.info(From_Fullname)
    From_time = file.info(From_Fullname)$mtime
    if (file.exists(To_Fullname))
    {
      To_time = file.info(To_Fullname)$mtime
      if (ToForce || From_time > To_time)
      {
        CATrp('Loading file ...')
        MyFile = readRDS(From_Fullname)
        CATln('Loaded.')
        
        CATln_Border(paste(From_Fullname, '>', To_Fullname))
        CATrp('Saving file ...')
        saveRDS(MyFile, To_Fullname)
        CATln('Saved')
        if (CheckCode) { xs = WRITE_SUMMARY_FULLPATH(To_Fullname) }
        
        CATln_Border(paste("FILE UPDATED = ", To_Fullname))
        
      } else {
        CATln_Border(paste("FILE ALREADY UPDATED = ", To_Fullname))
      }
    } else {
      CATln_Border(paste("NO FILE = ", To_Fullname))
      CATrp('Loading file ...')
      MyFile = readRDS(From_Fullname)
      CATln('Loaded.')
      
      CATln_Border(paste(From_Fullname, '>', To_Fullname))
      CATrp('Saving file ...')
      SAVERDS_WITH_ONEDRIVE(MyFile, To_Folder, To_File, ToSummary = T, SaveOneDrive = T)
      CATln('Saved')
      # if (CheckCode) { xs = WRITE_SUMMARY_FULLPATH(To_Fullname) }
      CATln_Border(paste("FILE UPDATED = ", To_Fullname))
    }
  }
}

CCPR_LAST_TRADING_DAY_VN = function(pHH=9, ToPrompt=F)
{
  # print(LAST_TRADING_DAY_VN())
  RELOAD_CALENDAR_VN()
  xCalendar = idx_calendar.vn
  TradingDate = max(xCalendar[date<=SYSDATETIME(pHH)]$date)
  if (ToPrompt) { CATln(paste('CCPR_LAST_TRADING_DAY_VN,',pHH, '=', TradingDate)) }
  return(TradingDate)
}


RELOAD_CALENDAR_VN = function()
{
  # RELOAD_CALENDAR_VN()
  if (exists("idx_calendar.vn")) {  } else {
    if (file.exists(paste0(UData, 'idx_calendar.vn.rds')))
    { 
      idx_calendar.vn = readRDS(paste0(UData, 'idx_calendar.vn.rds'))
    } else {
      # UData = CCPRData
      idx_calendar.vn = readRDS(paste0(CCPRData, 'idx_calendar.vn.rds'))
    }
    assign("idx_calendar.vn", idx_calendar.vn, envir = .GlobalEnv)
  }
}

RELOAD_TABLE_GLOBAL = function(pOption='INSREF')
{
  switch(pOption,
         'INSREF'      = { RELOAD_INSREF() },
         'CALENDAR_VN' = { RELOAD_CALENDAR_VN() }
  )
}

OLD_FINAL_DOWNLOAD_C68_STKVN_SHARES_BY_CODE = function(pCode = 'VND')
{
  Final_Data = data.table()
  pURL = paste0('https://www.cophieu68.vn/quote/profile.php?id=', tolower(pCode))
  CATln_Border(pURL)
  content    = try(rvest::read_html(pURL))
  if (all(class(content)!='try-error'))
  {
    tables     = content %>% html_table(fill = TRUE)
    xData      = as.data.table(tables[[1]])
    xData$field = decodeVN(xData$X1, from='Unicode', to='Unicode', diacritics=F)
    SharesOut  = as.numeric(gsub(',','', xData[field=='KL luu hanh']$X2))
    SharesLis  = as.numeric(gsub(',','', xData[field=='KL niem yet']$X2))
    
    Final_Data = data.table(source='C68', codesource=pCode, ticker=pCode, date=SYSDATETIME(10), code=paste0('STKVN', pCode), 
                            sharesout=SharesOut, shareslis=SharesLis, updated=substr(as.character(Sys.time()),1,19)) 
    My.Kable.All(Final_Data)
  }
  return(Final_Data)
}

FINAL_DOWNLOAD_CAF_STKVN_SHARES_BY_CODE = function(pCode  = 'VNM')
{
  Final_Data = data.table()
  RELOAD_INSREF()
  CAF_LINKS = readRDS(paste0(UData, 'download_caf_stkvn_list_links.rds'))
  CAF_ONE   = CAF_LINKS[ticker==pCode][, .(ticker, market, parentid, fullname, link)]
  if (nrow(CAF_ONE)==1)
  {
    # pURL = 'https://s.cafef.vn/hose/vic-tap-doan-vingroup-cong-ty-co-phan.chn'
    pURL      = paste0('https://s.cafef.vn/', CAF_ONE[1]$link)
    CATln_Border(pURL)
    x    = WEB_SAVE_AS(pURL = pURL, SaveFolder = 'c:/python/downloads/')
    grepl('KLCP đang niêm yết', x)
    SharesLis = as.numeric(gsub(',','', trimws(word(trimws(gsub('\n','', str.extract(x, '<div class="l"><b>KLCP đang niêm yết:</b></div>', '</div>'))),2,2, '>'))))
    SharesOut = as.numeric(gsub(',','', trimws(word(trimws(gsub('\n','', str.extract(x, '<div class="l"><b>KLCP đang lưu hành:</b></div>', '</div>'))),2,2, '>'))))
    LastTrading = CCPR_LAST_TRADING_DAY_VN(max(9, hour(Sys.time())), ToPrompt = T)
    Final_Data = data.table(codesource=pCode, source='CAF', code=paste0('STKVN', pCode), date=LastTrading, sharesout=SharesOut, shareslis=SharesLis)
    Final_Data = merge(Final_Data[, -c('name')], ins_ref[, .(code, name=short_name)], all.x=T, by='code')
    My.Kable.All(Final_Data)
  }
  return(Final_Data)
}

FINAL_DOWNLOAD_C68_STKVN_REF = function(ToCheckTime, OldVersion=F)
{
  # x = FINAL_DOWNLOAD_C68_STKVN_REF(ToCheckTime=9)
  try(RELOAD_INSREF())
  LastTrading = CCPR_LAST_TRADING_DAY_VN(ToCheckTime, ToPrompt = T)
  UData
  All_Data = data.table()
  for (i in 1:3)
  {
    # i = 1
    switch (i,
            "1" = {market='HSX'},
            "2" = {market='HNX'},
            "3" = {market='UPC'}
    )
    
    x  = WEB_SAVE_AS(pURL = paste0('https://www.cophieu68.vn/stockonline.php?stcid=',i, "&nd=2000"), SaveFolder = 'c:/python/downloads/')
    
    content    = rvest::read_html(x)
    xDate = content %>% html_elements("h1")
    # length(xDate)
    date  = as.Date(str.extract(as.character(xDate[[1]]), '# ', "<"), '%d/%m/%Y')
    date  = min(LastTrading, date)
    print(date)
    
    tables     = content %>% html_table(fill = TRUE)
    xData      = as.data.table(tables[[2]])
    xHeader    = xData[1:2,]
    xData      = xData[3:nrow(xData)]
    My.Kable.MaxCols(xData)
    # str(xHeader)
    Final_Data = xData[, .(date=date, market=market, ticker=X1, 
                           reference=1000*as.numeric(gsub(',','', as.character(X3))),
                           last=1000*as.numeric(gsub(',','', as.character(X11))),
                           volume=1000*as.numeric(gsub(',','', as.character(X20)))
    )]
    Final_Data[, ':='(change=last-reference, varpc=100*((last/reference)-1), updated=substr(as.character(Sys.time()),1,19))]
    My.Kable(Final_Data)
    All_Data = rbind(All_Data, Final_Data, fill=T)
  }
  All_Data[, ':='(source='C68', codesource=ticker)]
  All_Data[, code:=paste0('STKVN', ticker)]
  All_Data[!is.na(last), close:=last]
  All_Data[is.na(last) & !is.na(reference), close:=reference]
  All_Data[!is.na(close) & !is.na(reference), rt_calculate:=(close/reference)-1]
  All_Data[is.na(last), change:=0]
  All_Data[is.na(last), varpc:=0]
  All_Data = All_Data[nchar(ticker)==3]
  My.Kable.TB(All_Data)
  FileName = paste0(CCPRData, 'download_C68_stkvn_ref.rds')
  
  # saveRDS(All_Data, FileName)
  try(SAVERDS_WITH_ONEDRIVE(All_Data, CCPRData, 'download_C68_stkvn_ref.rds', ToSummary=T, SaveOneDrive = T))
  x  = try(CCPR_INTEGRATION(FileFolder=CCPRData, FileFrom='download_C68_stkvn_ref.rds', 
                            FileTo='download_C68_stkvn_ref_history.rds', GroupBy='code x date', ToSummary=T))
  if (file.exists(UData))
  {
    try(SAVERDS_WITH_ONEDRIVE(All_Data, UData, 'download_C68_stkvn_ref.rds', ToSummary=T, SaveOneDrive = T))
    x  = try(CCPR_INTEGRATION(FileFolder=UData, FileFrom='download_C68_stkvn_ref.rds', 
                              FileTo='download_C68_stkvn_ref_history.rds', GroupBy='code x date', ToSummary=T))
  }
  # SAVERDS_WITH_ONEDRIVE(All_Data, CCPRData, 'download_C68_stkvn_ref.rds', ToSummary=T, SaveOneDrive = T)
  # xs = try(WRITE_SUMMARY_FULLPATH(FileName))
  # x  = try(CCPR_INTEGRATION(FileFolder=CCPRData, FileFrom='download_C68_stkvn_ref.rds', 
  #                           FileTo='download_C68_stkvn_ref_history.rds', GroupBy='code x date', ToSummary=T))
  return(All_Data)
}

# ==================================================================================================
GC_SILENT = function(silent = T)  {
  # ------------------------------------------------------------------------------------------------
  if(silent) invisible(gc(reset = T)) else gc(reset = T)
}

NEW_DOWNLOAD_CAF_STKVN_PRICES = function(pCode = 'VND')
{
  # pCode = 'XYZ'
  # pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=', pCode, '&StartDate=30/12/2022&EndDate=30/12/2023&PageIndex=3&PageSize=20')
  # NbPagesBack = 20
  Data_List = list()
  ToContinu = T
  k = 1
  while (ToContinu)
  {
    pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=', pCode, '&StartDate=&EndDate=&PageIndex=', k, '&PageSize=100')
    CATrp(pURLk)
    x = try(jsonlite::fromJSON(pURLk))
    if (length(x$Data$Data)>0)
    {
      XData = try(as.data.table(x$Data$Data))
      if (all(class(XData)!='try-error'))
      {
        XData = CLEAN_COLNAMES(XData)
        Final_Data = XData[, .(source='CAF', codesource=pCode, ticker=pCode, code=paste0('STKVN', pCode),
                               date      = as.Date(ngay, '%d/%m/%Y'), 
                               open      = 1000*as.numeric(giamocua),
                               high      = 1000*as.numeric(giacaonhat),
                               low       = 1000*as.numeric(giathapnhat),
                               close_adj = 1000*as.numeric(giadieuchinh),
                               close=1000*as.numeric(giadongcua),
                               change=1000*as.numeric(word(thaydoi,1,1,'\\(')),
                               varpc=as.numeric(word(word(thaydoi,2,2,'\\('),1,1,' ')),
                               volume=as.numeric(gsub(',','', khoiluongkhoplenh)),
                               turnover=as.numeric(gsub(',','', giatrikhoplenh))
                               
        )]
        My.Kable.TB(Final_Data)
        Data_List[[k]] = Final_Data
      } else {
        ToContinu = F
      }
    } else {
      ToContinu = F
    }
    k = k+1
  }
  CATln(pURLk); CATln('')
  Data_All = rbindlist(Data_List, fill=T)
  if (nrow(Data_All)>0)
  {
    Data_All = Data_All[order(date)]
    Data_All[, reference:=close-change]
    Data_All[, rt:=change/reference]
    My.Kable.TB(Data_All)
  } else {
    Data_All = data.table()
  }
  return(Data_All)
}

NEW_DOWNLOAD_CAF_STKVN_ORDER = function(pCode = 'VIC')
{
  # pCode = 'XYZ'
  # pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=', pCode, '&StartDate=30/12/2022&EndDate=30/12/2023&PageIndex=3&PageSize=20')
  # NbPagesBack = 20
  Data_List = list()
  ToContinu = T
  k = 1
  while (ToContinu)
  {
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/ThongKeDL.ashx?Symbol=VIC&StartDate=&EndDate=&PageIndex=2&PageSize=100
    pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/ThongKeDL.ashx?Symbol=', pCode, '&StartDate=&EndDate=&PageIndex=', k, '&PageSize=100')
    CATrp(pURLk)
    xf = readr::read_file(pURLk)
    xc = gsub('NaN', '9999999999', xf)
    xc = gsub('Infinity', '9999999999', xc)
    x = try(jsonlite::fromJSON(xc))
    # grepl("NaN", xf)
    # writeLines(xf, 'c:/temp/xf.txt')
    # x = try(jsonlite::fromJSON(pURLk))
    if (length(x$Data$Data)>0)
    {
      XData = try(as.data.table(x$Data$Data))
      if (all(class(XData)!='try-error'))
      {
        XData = CLEAN_COLNAMES(XData)
        XData[solenhmua==9999999999, solenhmua:=as.numeric(NA)]
        XData[kldatmua==9999999999, kldatmua:=as.numeric(NA)]
        XData[kltb1lenhmua==9999999999, kltb1lenhmua:=as.numeric(NA)]
        XData[solenhdatban==9999999999, solenhdatban:=as.numeric(NA)]
        XData[kldatban==9999999999, kldatban:=as.numeric(NA)]
        XData[kltb1lenhban==9999999999, kltb1lenhban:=as.numeric(NA)]
        XData[chenhlechkl==9999999999, chenhlechkl:=as.numeric(NA)]
        
        # str(XData)
        Final_Data = XData[, .(source='CAF', codesource=pCode, ticker=pCode, code=paste0('STKVN', pCode),
                               date      = as.Date(date, '%d/%m/%Y'), 
                               order_b_nb   = as.numeric(solenhmua),
                               order_b_vl   = as.numeric(kldatmua),
                               order_b_avl  = as.numeric(kltb1lenhmua),
                               order_s_nb   = as.numeric(solenhdatban),
                               order_s_vl   = as.numeric(kldatban),
                               order_s_avl  = as.numeric(kltb1lenhban),
                               order_bs_vl  = as.numeric(gsub(',','',chenhlechkl))
                               
        )]
        # My.Kable.TB(Final_Data)
        Data_List[[k]] = Final_Data
      } else {
        ToContinu = F
      }
    } else {
      ToContinu = F
    }
    
    k = k+1
  }
  CATln(pURLk)
  Data_All = rbindlist(Data_List, fill=T)
  if (nrow(Data_All)>0)
  {
    My.Kable.TB(Data_All)
  } else {
    Data_All = data.table()
  }
  return(Data_All)
}


NEW_DOWNLOAD_CAF_STKVN_FOREIGN = function(pCode = 'VIC')
{
  # pCode = 'XYZ'
  # pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=', pCode, '&StartDate=30/12/2022&EndDate=30/12/2023&PageIndex=3&PageSize=20')
  # NbPagesBack = 20
  Data_List = list()
  ToContinu = T
  k = 1
  CATln_Border(paste('NEW_DOWNLOAD_CAF_STKVN_FOREIGN :', pCode))
  while (ToContinu)
  {
    # https://s.cafef.vn/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx?Symbol=VIC&StartDate=&EndDate=&PageIndex=1&PageSize=20
    pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx?Symbol=', pCode, '&StartDate=&EndDate=&PageIndex=', 
                   k, '&PageSize=100')
    CATrp(pURLk)
    xf = readr::read_file(pURLk)
    xc = gsub('NaN', '9999999999', xf)
    xc = gsub('Infinity', '9999999999', xc)
    x = try(jsonlite::fromJSON(xc))
    # grepl("NaN", xf)
    # writeLines(xf, 'c:/temp/xf.txt')
    # x = try(jsonlite::fromJSON(pURLk))
    if (length(x$Data$Data)>0)
    {
      XData = try(as.data.table(x$Data$Data))
      if (all(class(XData)!='try-error'))
      {
        XData = CLEAN_COLNAMES(XData)
        
        # ngay              thaydoi                        
        
        XData[klgdrong==9999999999, klgdrong:=as.numeric(NA)]
        XData[gtdgrong==9999999999, gtdgrong:=as.numeric(NA)]
        XData[klmua==9999999999, klmua:=as.numeric(NA)]
        XData[gtmua==9999999999, gtmua:=as.numeric(NA)]
        XData[klban==9999999999, klban:=as.numeric(NA)]
        XData[gtban==9999999999, gtban:=as.numeric(NA)]
        XData[roomconlai==9999999999, roomconlai:=as.numeric(NA)]
        XData[dangsohuu==9999999999, dangsohuu:=as.numeric(NA)]
        
        # str(XData)
        Final_Data = XData[, .(source='CAF', codesource=pCode, ticker=pCode, code=paste0('STKVN', pCode),
                               date         = as.Date(ngay, '%d/%m/%Y'), 
                               foreign_b_vol     = as.numeric(klmua),
                               foreign_b_val     = as.numeric(gtmua),
                               foreign_s_vol     = as.numeric(klban),
                               foreign_s_val     = as.numeric(gtban),
                               foreign_bs_vol    = as.numeric(klgdrong),
                               foreign_bs_val    = as.numeric(gtdgrong),
                               changevar         = as.character(thaydoi),
                               foreign_room_shr  = as.numeric(roomconlai),
                               foreign_own_pc    = as.numeric(dangsohuu)
                               
        )]
        # My.Kable.TB(Final_Data)
        Data_List[[k]] = Final_Data
      } else {
        ToContinu = F
      }
    } else {
      ToContinu = F
    }
    k = k+1
  }
  CATln(pURLk)
  Data_All = rbindlist(Data_List, fill=T)
  if (nrow(Data_All)>0)
  {
    My.Kable.TB(Data_All)
  } else {
    Data_All = data.table()
  }
  return(Data_All)
}

NEW_DOWNLOAD_CAF_STKVN_INTRADAY = function(pCode = 'VIC', pDate='2023-08-02')
{
  # pCode = 'XYZ'
  # pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=', pCode, '&StartDate=30/12/2022&EndDate=30/12/2023&PageIndex=3&PageSize=20')
  # NbPagesBack = 20
  
  Final_Data = data.table()
  CATln_Border(paste('NEW_DOWNLOAD_CAF_STKVN_INTRADAY :', pCode, '-', substr(Sys.time(),1,19)))
  CAF_Date = paste0(month(pDate),'/', day(pDate), '/', year(pDate))
  
  
  # https://s.cafef.vn/Ajax/PageNew/DataHistory/KhopLenh/DataKhopLenh.ashx?Symbol=VIC&Date=8/2/2023
  pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/KhopLenh/DataKhopLenh.ashx?Symbol=', pCode, '&Date=', CAF_Date)
  CATrp(pURLk)
  xf = readr::read_file(pURLk)
  xc = gsub('NaN', '9999999999', xf)
  xc = gsub('Infinity', '9999999999', xc)
  x = try(jsonlite::fromJSON(xc))
  # grepl("NaN", xf)
  # writeLines(xf, 'c:/temp/xf.txt')
  # x = try(jsonlite::fromJSON(pURLk))
  if (length(x$Data$DlChiTiet)>0)
  {
    XData = try(as.data.table(x$Data$DlChiTiet))
    if (all(class(XData)!='try-error'))
    {
      XData = CLEAN_COLNAMES(XData)
      # str(XData)
      Final_Data = XData[, .(source='CAF', codesource=pCode, ticker=pCode, code=paste0('STKVN', pCode),
                             date         = as.Date(pDate), 
                             time         = as.character(thoigian),
                             datetime     = paste(as.Date(pDate), as.character(thoigian)),
                             timestamp    = paste(as.Date(pDate), as.character(thoigian)),
                             last         = 1000*as.numeric(gia),
                             volume       = as.numeric(kllo),
                             volume_cumul = as.numeric(kltichluy),
                             volume_pc    = as.numeric(titrong),
                             changevar    = as.character(giathaydoi)
                             
                             
      )]
      Final_Data[, change:= 1000*as.numeric(trimws(word(changevar,1,1,'\\(')))]
      Final_Data[, pref_calc:=last-change]
      Final_Data[, rt_calc:=((last/pref_calc)-1)]
      My.Kable.TB(Final_Data)
    } else { Final_Data = data.table()}
  }
  GC_SILENT()
  return(Final_Data)
}

DOWNLOAD_STB_STKVN_SECTOR=function(pCode = 'VND')
{
  LastTrading = CCPR_LAST_TRADING_DAY_VN(max(9, hour(Sys.time())), ToPrompt = T)
  My_HTML = read_html(paste0('https://www.stockbiz.vn/Stocks/', pCode, '/Overview.aspx'))
  a       = My_HTML %>% html_nodes("div.industry") %>% html_text(trim = T)
  b       = gsub('\r\n', '', a)
  left    = word( b,1,1,sep = ':')
  right   = word(b,2,2, sep=':')
  d       = data.table(codesource=pCode, source='STB', code=paste0('STKVN', pCode), date=LastTrading, sector= c(left), industry = c(right))
  My.Kable.All(d)
  return(d)
}

CCPR_READRDS = function(FileFolder=UData, FileName='download_hsx_stkvn_prices.rds', ToKable=F, ToPrompt=F, ToRestore=F)
{
  Data_Local       = data.table()
  ODDrive_Data     = paste0(ODDrive, substr(FileFolder,1,1), word(FileFolder,2,2,":"))
  FullPath_Local   = paste0(FileFolder, FileName)
  FullPath_ODDrive = paste0(ODDrive_Data, FileName)
  if (ToPrompt)
  {
    ToPrompt = T
    CATln(FullPath_Local)
    CATln(FullPath_ODDrive)
  }
  if (file.exists(FullPath_Local))
  {
    Success = F
    NbTry   = 1
    while (!Success & NbTry <= 3)
    {
      Data_Local = try(readRDS(FullPath_Local))
      if (all(class(Data_Local)!='try-error'))
      {
        Success = T
        if (ToKable)
        {
          ToKable = T
          My.Kable.MaxCols(Data_Local)
        }
      }
      NbTry = NbTry+1
    }
    
    if (!Success)
    {
      if (file.exists(FullPath_ODDrive))
      {
        Success = F
        NbTry   = 1
        while (!Success & NbTry <= 3)
        {
          Data_ODDrive = try(readRDS(FullPath_ODDrive))
          if (all(class(Data_ODDrive)!='try-error'))
          {
            Success = T
            if (ToKable)
            {
              ToKable = T
              My.Kable.MaxCols(Data_ODDrive)
            }
          }
          NbTry = NbTry+1
        }
        if (Success)
        {
          Data_Local = copy(Data_ODDrive)
          if (ToRestore)
          {
            CATln(paste('Restoring ... ', FullPath_Local))
            try(saveRDS(Data_Local, FullPath_Local))
            CATln('Done.')
          }
        }
      }
    }
  }
  if (nrow(Data_Local)>0 & "x" %in% names(Data_Local)) { Data_Local$x = NULL}
  return(Data_Local)
}
DAUTHAU_LIST = function(List_Level3, sectorname)
{
  All_Companies = data.table()
  for (Level3 in List_Level3)
  {
    IndustryID = Level3
    pURL       =  paste0('https://dauthau.asia/en/businesslistings/stocks/', Level3, '/')
    CATln_Border(pURL)
    content    = try(rvest::read_html(pURL))
    if (all(class(content)!='try-error'))
    {
      tables  = try(content %>% html_table(fill = TRUE))
      s       = read_html(pURL)
      a       = s %>% html_nodes("div.bidding-list-body") %>% html_text(trim = T)
      
      Lines = unlist(strsplit(a, '\n\n\n'))
      List_Comp = list()
      if (length(Lines)>=1 & all(!grepl('not found', Lines)))
      {
        List_Comp = list()
        for (k in 1:length(Lines))
        {
          # k = 1
          Linek = Lines[[k]]
          Linek = gsub('\n\n','\t', Linek)
          Linek = gsub('\n','\t', Linek)
          Colk  = unlist(strsplit(Linek, '\t'))
          xk       = data.table(
            icb_id  = IndustryID,
            code    = paste0("STKVN", trimws(word(Colk[[2]], 2,2,':'))),
            nr      = trimws(word(Colk[[1]], 2,2,':')),
            ticker  = trimws(word(Colk[[2]], 2,2,':')),
            address = trimws(Colk[[6]]),
            city    = trimws(word(Colk[[8]], 2,2,':')),
            name_vn = trimws(Colk[[5]]),
            date_listed  = as.Date(trimws(word(Colk[[7]], 2,2,':')), '%d/%m/%Y'),
            date    = SYSDATETIME(8)
          )
          List_Comp[[k]] = xk
        }
      }
      
      Data_All = rbindlist(List_Comp, fill=T)
      if (nrow(Data_All)>0)
      {
        All_Companies = rbind(All_Companies, Data_All, fill=T)
        All_Companies[, sector_name:=sectorname]
      }
    }
  }
  if (nrow(All_Companies)>0) { All_Companies[, sector_name:=sectorname] }
  return(All_Companies)
}

DOWNLOAD_SSI_STKVN_PRICES = function(pTicker='VNM', pFrequency='DAY')
{
  # pTicker='XXX'; pFrequency='DAY'
  Data_STD = data.table()
  switch(pFrequency,
         'DAY' = {
           Epoch_Start = floor(EPOCH_NOW()/1000) - (20*365*24*60*60)
           Epoch_End   = floor(EPOCH_NOW()/1000)
           # pURL    ='https://iboard.ssi.com.vn/dchart/api/history?resolution=D&symbol=ABC&from=1621746341&to=1655874401'
           # pURL    = paste0('https://iboard.ssi.com.vn/dchart/api/history?resolution=D&symbol=', pTicker, '&from=1621746341&to=',Epoch_End)
           pURL    = paste0('https://iboard.ssi.com.vn/dchart/api/history?resolution=D&symbol=', pTicker, '&from=', Epoch_Start, '&to=',Epoch_End)
           # 
           #            pURL    = paste0('https://iboard.ssi.com.vn/dchart/api/history?resolution=1&symbol=', pTicker, '&from=', Epoch_StartM, '&to=',Epoch_EndM)
           CATln_Border(pURL)
           x = try(as.data.table(jsonlite::fromJSON(pURL)))
           if (nrow(x)>1)
           {
             x[, timestamp:=as.POSIXct(t, origin="1970-01-01")]
             My.Kable.TB(x)
             Data_STD = x[, .(source="SSI", codesource=pTicker, ticker=pTicker, code=paste0('STKVN', pTicker), date=as.Date(substr(as.character(timestamp),1,10)), 
                              timestamp=as.character(timestamp),
                              open=1000*as.numeric(o), high=1000*as.numeric(h), low=1000*as.numeric(l), close=1000*as.numeric(c), volume=as.numeric(v), updated=as.character(Sys.time()))]
             Data_STD = MERGE_DATASTD_BYCODE(Data_STD, pOption='SHORT')
             My.Kable.TB(Data_STD)
             rm(x); try(GC_SILENT())
           }
         },
         'INTRADAY' = {
           Epoch_StartM = floor(EPOCH_NOW()/1000) - (31*24*60*60)
           Epoch_EndM   = floor(EPOCH_NOW()/1000)
           pURL    = paste0('https://iboard.ssi.com.vn/dchart/api/history?resolution=1&symbol=', pTicker, '&from=', Epoch_StartM, '&to=',Epoch_EndM)
           CATln_Border(pURL)
           x = try(as.data.table(jsonlite::fromJSON(pURL)))
           if (nrow(x)>1)
           {
             x[, timestamp:=as.POSIXct(t, origin="1970-01-01")]
             My.Kable.TB(x)
             Data_STD = x[, .(source="SSI", codesource=pTicker, ticker=pTicker, code=paste0('STKVN', pTicker), date=as.Date(substr(as.character(timestamp),1,10)), 
                              timestamp=as.character(timestamp),
                              open=1000*as.numeric(o), high=1000*as.numeric(h), low=1000*as.numeric(l), close=1000*as.numeric(c), volume=as.numeric(v), updated=as.character(Sys.time()))]
             Data_STD = MERGE_DATASTD_BYCODE(Data_STD, pOption='SHORT')
             My.Kable.TB(Data_STD)
             rm(x); try(GC_SILENT())
           }
         }
  )
  return(Data_STD)
}

FINAL_DOWNLOAD_CAF_STKVN_PRICES_BY_DATE = function(l.date=SYSDATETIME(10), ToSavePrices=F) {
  try(RELOAD_INSREF(pFolder=UData))
  # l.date='2022-12-16'
  my_date = as.Date(as.character(l.date))
  d_mm    = str_pad(month(my_date), 2, pad = "0")
  d_dd    = str_pad(mday(my_date), 2, pad = "0")
  d_yyyy  = year(my_date)
  data_all = data.table()
  dt_ref   = data.table()
  my_date = as.Date(l.date)
  d_mm    = str_pad(month(my_date), 2, pad = "0")
  d_dd    = str_pad(mday(my_date), 2, pad = "0")
  d_yyyy  = year(my_date)
  
  data_all = data.table()
  list_mkid = list("HOSE", "HASTC", "UPCOM")
  
  for (imkid in list_mkid)
  {
    # imkid = "HOSE"
    LcURLi = paste0("http://s.cafef.vn/TraCuuLichSu2/1/", imkid, "/", d_dd, "/", d_mm, "/", d_yyyy, ".chn")
    CATln_Border(LcURLi)
    
    WEB_str  = WEB_SAVE_AS(pURL = LcURLi, SaveFolder = 'c:/python/downloads/')
    
    # WEB_str = getURL(LcURLi)
    Sys.sleep(2)
    # WEB_str = str_replace_all(WEB_str, "\r\r\n", "\r\n")
    # WEB_str = str_replace_all(WEB_str, "\t", "")  
    # file.remove("c:/temp/web_str.txt")
    # writeLines(WEB_str, "c:/temp/web_str.txt")
    
    # sep_bloc = '<tr id="ctl00_ContentPlaceHolder1_ctl01_rptData_'
    sep_bloc = '<tr id="ContentPlaceHolder1_ctl01_rptData'
    sep_col  = '<td '
    list_bloc = unlist(strsplit(WEB_str, sep_bloc))
    length(list_bloc)
    
    if (length(list_bloc)>1)
    {
      data_list = list()
      for (ibloc in 2:length(list_bloc))
      {
        # ibloc    = 2
        one_bloc = list_bloc[[ibloc]]
        list_col = unlist(strsplit(one_bloc, sep_col))
        # list_col
        l_code     = str.extract(list_col[[2]], 'class=\"symbol\" rel=\"', '\"')
        l_name     = ""
        l_pcls     = 1000*CONVERT_STR_TO_NUMBER(gsub("&nbsp;", "", str.extract(list_col[[3]], '>', '</td>')))
        l_pref     = 1000*CONVERT_STR_TO_NUMBER(gsub("&nbsp;", "", str.extract(list_col[[7]], 'class=\"Item_Price1\">', '<')))
        l_popn     = 1000*CONVERT_STR_TO_NUMBER(gsub("&nbsp;", "", str.extract(list_col[[8]], 'class=\"Item_Price1\">', '<')))
        l_phgh     = 1000*CONVERT_STR_TO_NUMBER(gsub("&nbsp;", "", str.extract(list_col[[9]], 'class=\"Item_Price1\">', '<')))
        l_plow     = 1000*CONVERT_STR_TO_NUMBER(gsub("&nbsp;", "", str.extract(list_col[[10]], 'class=\"Item_Price1\">', '<')))
        l_vlm      = CONVERT_STR_TO_NUMBER(gsub("&nbsp;", "", str.extract(list_col[[11]], 'class=\"Item_Price1\">', '<')))
        l_trn      = CONVERT_STR_TO_NUMBER(gsub("&nbsp;", "", str.extract(list_col[[12]], 'class=\"Item_Price1\">', '<')))
        d_vlm      = CONVERT_STR_TO_NUMBER(gsub("&nbsp;", "", str.extract(list_col[[13]], 'class=\"Item_Price1\">', '<')))
        d_trn      = CONVERT_STR_TO_NUMBER(gsub("&nbsp;", "", str.extract(list_col[[14]], 'class=\"Item_Price1\">', '<')))
        
        l_pchgvar  = str.extract(list_col[[5]], '<div', '</div')
        l_pchgvar  = str.extract(l_pchgvar, '>', '%)')
        l_pchgvarl = trimws(unlist(strsplit(l_pchgvar, "\\(")))
        
        l_pchg     = 1000*CONVERT_STR_TO_NUMBER(l_pchgvarl[[1]])
        l_pvar     = CONVERT_STR_TO_NUMBER(l_pchgvarl[[2]])
        
        data_one   = data.table(mkid = imkid, ticker=l_code, 
                                close=l_pcls, pcls=l_pcls, pchg=l_pchg, pvar=l_pvar, date=my_date, 
                                code=paste0("STKVN", l_code), 
                                source="CAF", codesource=l_code, pref=l_pref, popn=l_popn, phgh=l_phgh, plow=l_plow, 
                                volume=l_vlm, turnover=l_trn, deal_volume=d_vlm, deal_turnover=d_trn, rt_calculated=(l_pcls/l_pref)-1)
        data_list[[ibloc-1]] = data_one
      }
      data_all = rbind(data_all, rbindlist(data_list, fill=T), fill=T)
    } 
    Sys.sleep(2)
  }
  
  if (nrow(data_all)>0)
  {
    # My.Kable(data_all)
    # str(data_all)
    data_all[mkid=="HOSE", market:="HSX"]
    data_all[mkid=="HASTC", market:="HNX"]
    data_all[mkid=="UPCOM", market:="UPC"]
    
    My.Kable(data_all)
    if (ToSavePrices)
    {
      File_output = 'download_CAF_STKVN_prices.rds'
      DATACENTER_SAVEDTRDS_AND_LAST_WITH_ONEDRIVE(data_all, paste0(UData, File_output), ToSummary=T)
    }
    # File_output = paste0(U, "YAH/YYYYMMDD/", "download_caf_stkvn_", gsub("-","", l.date), ".rds")
    # CATln(File_output)
    # saveRDS(data_all, File_output)
    # WRITE_SUMMARY_FULLPATH(File_output)
    
  } else {
    CATln_Border(paste('NO DATA =', l.date))
  }
  return(data_all)
}

SEARCH_STRING_IN_LIST = function(pList, pString)
{
  Found        = F
  Found_String = as.character(NA)
  for (k in 1:length(pList))
  {
    if (!Found)
    {
      if (grepl(pString, pList[[k]]))
      {
        Found = T
        Found_String = pList[[k]]
      }
    }
  }
  return(Found_String)
}
# REPLACE_LOOP_ALL = function(pStr='MAI   Huu  Minh', Str_From='  ', Str_By=' ')
# {
#   pString = pStr
#   while (str_count(pString, Str_From)>0)
#   {
#     pString = gsub(Str_From, Str_By, pString)
#   }
#   return(pString)
# }