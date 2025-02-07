
# DBL_DOAWLOAD_YAH_MARKETS = function(pCodesource = '^GSPC', Hour_adjust = -4)
# {
#   # pCodesource = '^GSPC'; Hour_adjust = -4
#   index = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_OHLC_INTRADAY( pCodesource = pCodesource, pInterval = '5m', Hour_adjust = Hour_adjust)))
#   index_day = unique(index[order(codesource, date, -datetime)], by='date')[order(date)][, -c('sample', 'home', 'capiusd', 'provider', 'isin', 'fcat')]
#   # str(index)
#   index_day = CALCULATE_CHANGE_RT_VARPC(index_day)[!is.na(change)]
#   index_oneday = index_day[.N]
#   
#   My.Kable.TB(index)
#   My.Kable.TB(index_day)
#   My.Kable.All(index_oneday)
#   Final_Data = list(intraday=index, day=index_day, last=index_oneday)
#   return(Final_Data)
# }

# 
# # ====================================================================================================
# DBL_DOWNLOAD_YAH_INTRADAY_BY_FILE = function(Folder_List = 'LIST_INTRADAY_VOLATILITY.txt',
#                                              File_List   = 'S:/CCPR/DATA/LIST/',
#                                              pType       = 'index', pCoverage = 'INTERNATIONAL',
#                                              Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
#                                              Save_Prefix = 'DBL_YAH_IND<PREFIX>_<FREQUENCY>_<HISTORY>.rds',
#                                              ToSave      = T, ToHistory = T, Nb_Min = 500)
#   
#   # Folder_List = 'LIST_INTRADAY_VOLATILITY.txt'; File_List   = 'S:/CCPR/DATA/LIST/'; Nb_Min = 5
#   # pType       = 'index'; pCoverage = 'INTERNATIONAL';Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/';Save_Prefix = 'DBL_YAH_IND<PREFIX>_<FREQUENCY>_<HISTORY>.rds';ToSave = T
# {
#   xALL_INTRADAY = data.table()
#   xALL_DAY      = data.table()
#   xALL_LAST     = data.table()
#   LIST_CODES = try(setDT(fread('S:/CCPR/DATA/LIST/LIST_INTRADAY_VOLATILITY.txt')))
#   
#   if (all(class(LIST_CODES)!='try-error'))
#   {
#     LIST_CODES = LIST_CODES[SOURCE=='YAH' & !is.na(HOUR_ADJUST) & nchar(HOUR_ADJUST)>0]
#     if (nchar(pType)>0)   { LIST_CODES = LIST_CODES[TYPE==pType]   }
#     if (nchar(pCoverage)>0)   { LIST_CODES = LIST_CODES[COVERAGE==pCoverage]   }
#     My.Kable.All(LIST_CODES)
#     
#     xLIST_INTRADAY = list()
#     xLIST_DAY      = list()
#     xLIST_LAST     = list()
#     
#     NB_TODO        = min(Nb_Min, nrow(LIST_CODES))
#     for (k in 1:NB_TODO)
#     {
#       # k = 1
#       pCode   = LIST_CODES[k]$SYMBOL 
#       CATln('')
#       CATln_Border(paste(k, '/', NB_TODO, '>>>', pCode))
#       pOffset = as.numeric(LIST_CODES[k]$HOUR_ADJUST)
#       x = try(DBL_DOAWLOAD_YAH_MARKETS(pCodesource = pCode, Hour_adjust = pOffset))
#       # sort(names(x[[1]]))
#       # str(x[[1]])
#       if (all(class(x)!='try-error'))
#       {
#         xLIST_INTRADAY[[k]] = x$intraday
#         xLIST_DAY[[k]]      = x$day
#         xLIST_LAST[[k]]     = x$last
#       }
#     }
#     
#     xALL_INTRADAY       = rbindlist(xLIST_INTRADAY, fill=T)
#     xALL_DAY            = rbindlist(xLIST_DAY, fill=T)
#     xALL_LAST           = rbindlist(xLIST_LAST, fill=T)
#     
#     My.Kable.TB(xALL_INTRADAY[order(date, timestamp_vn)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent')][order(date, timestamp, code)])
#     My.Kable.TB(xALL_DAY[order(date, code)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent')][order(date, timestamp, code)])
#     My.Kable.All(xALL_LAST[order(date, timestamp_vn)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent')])
#     
#     if(ToSave)
#     {
#       FileName = gsub('[<]HISTORY>', 'TODAY', gsub('[<]FREQUENCY>', 'INTRADAY', gsub('[<]PREFIX>', '', Save_Prefix)))
#       try(CCPR_SAVERDS(xALL_INTRADAY, Save_Folder, FileName, ToSummary = T, SaveOneDrive = F))
#       
#       FileName = gsub('[<]HISTORY>', 'TODAY', gsub('[<]FREQUENCY>', 'DAY', gsub('[<]PREFIX>', '', Save_Prefix)))
#       try(CCPR_SAVERDS(xALL_DAY, Save_Folder, FileName, ToSummary = T, SaveOneDrive = T))
#       
#       FileName = gsub('[<]HISTORY>', 'TODAY', gsub('[<]FREQUENCY>', 'LAST', gsub('[<]PREFIX>', '', Save_Prefix)))
#       try(CCPR_SAVERDS(xALL_LAST, Save_Folder, FileName, ToSummary = T, SaveOneDrive = T))
#     }
#     
#     if(ToHistory)
#     {
#       ToHistory = T
#       FileName = gsub('[<]HISTORY>', 'DAY', gsub('[<]FREQUENCY>', 'INTRADAY', gsub('[<]PREFIX>', '', Save_Prefix)))
#       CATln_Border(FileName)
#       Data     = CHECK_CLASS(try(CCPR_READRDS(Save_Folder, FileName, ToKable = T, ToRestore = T)))
#       
#       Data     = rbind(Data, xALL_INTRADAY, fill = T)
#       if(nrow(Data) > 0 )
#       {
#         Data = unique(Data, by = c('code', 'timestamp'), FromLast = T)
#         Data$capiusd = NULL
#         My.Kable.TB(Data[order(date, timestamp_vn)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent', 'isin')][order(date, timestamp, code)])
#         try(CCPR_SAVERDS(Data, Save_Folder, FileName, ToSummary = T, SaveOneDrive = F))
#       }
#       
#       FileName = gsub('[<]HISTORY>', 'DAY', gsub('[<]FREQUENCY>', 'DAY', gsub('[<]PREFIX>', '', Save_Prefix)))
#       CATln_Border(FileName)
#       Data     = CHECK_CLASS(try(CCPR_READRDS(Save_Folder, FileName, ToKable = T, ToRestore = T)))
#       
#       Data     = rbind(Data, xALL_DAY, fill = T)
#       if (nrow(Data) > 0 )
#       {
#         Data = unique(Data, by = c('code', 'date'), FromLast = T)
#         Data$capiusd = NULL
#         My.Kable.TB(Data[order(date, code)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent', 'isin')][order(date, code)])
#         try(CCPR_SAVERDS(Data, Save_Folder, FileName, ToSummary = T, SaveOneDrive = F))
#       }
#       
#       FileName = gsub('[<]HISTORY>', 'DAY', gsub('[<]FREQUENCY>', 'LAST', gsub('[<]PREFIX>', '', Save_Prefix)))
#       CATln_Border(FileName)
#       Data     = CHECK_CLASS(try(CCPR_READRDS(Save_Folder, FileName, ToKable = T, ToRestore = T)))
#       
#       Data     = rbind(Data, xALL_LAST, fill = T)
#       if (nrow(Data) > 0 )
#       {
#         Data = unique(Data[order(code, -date, -timestamp)], by = c('code'), FromLast = T)
#         Data$capiusd = NULL
#         My.Kable.TB(Data[order(date, code)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent', 'isin')][order(date, code)])
#         try(CCPR_SAVERDS(Data, Save_Folder, FileName, ToSummary = T, SaveOneDrive = F))
#       }
#     }
#   }
#   return(list(xALL_INTRADAY, xALL_DAY, xALL_LAST))
# }

# ==================================================================================================
DBL_UDATA_LOOP = function() {
  # ------------------------------------------------------------------------------------------------
  
  SOURCES_LIST = list('INV', 'YAH', 'BLG')
  TYPES_LIST   = list('IND', 'STK')
  
  
  TYPES_LIST   = list('IND')
  pPREFIX      = 'AFRICA'
  pPREFIX      = 'OVN'
  
  for (pTYPE in sample(TYPES_LIST))
  {
    for (pSOURCE in sample(SOURCES_LIST))
    {
      MY_DATA = try(DBL_DOWNLOAD_SOURCE_INS(pSOURCE = pSOURCE, pPREFIX = pPREFIX,  pType = pTYPE, Nb_Min = 25, pSleep = 5, Integration_PROBA = 50))
    }
  }
  
  DBL_RELOAD_INSREF()
  LIST_FCAT = sample(as.list(unique(ins_ref[type=='IND' & !is.na(fcat)]$fcat)))
  
  for (k in 1:length(LIST_FCAT))
  {
    # k            = 1
    pPREFIX      = gsub('^IND','', LIST_FCAT[[k]])
    CATln_Border(pPREFIX)
    
    for (pTYPE in sample(TYPES_LIST))
    {
      for (pSOURCE in sample(SOURCES_LIST))
      {
        MY_DATA = try(DBL_DOWNLOAD_SOURCE_INS(pSOURCE = pSOURCE, pPREFIX = pPREFIX,  pType = pTYPE, Nb_Min = 25, pSleep = 5, Integration_PROBA = 50))
      }
    }
  }
}

# ==================================================================================================
DBL_UDATA_MISC = function() {
  # ------------------------------------------------------------------------------------------------
  NbMinutes = 15
  TO_DO     = CCPR_MONITOR_EXECUTION_SUMMARY(pOption='LIST_INTRADAY_VOLATILITY', pAction="COMPARE", NbSeconds=NbMinutes*60, ToPrint=T)
  if (TO_DO)
  {
    x3 = DBL_DOWNLOAD_YAH_INTRADAY_BY_FILE(Folder_List = 'LIST_INTRADAY_VOLATILITY.txt',
                                           File_List   = 'S:/CCPR/DATA/LIST/',
                                           pType       = 'index', pCoverage = 'INTERNATIONAL',
                                           Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                           Save_Prefix = 'DBL_YAH_IND<PREFIX>_<FREQUENCY>_<HISTORY>.rds',
                                           ToSave      = T, ToHistory = T, Nb_Min = 500)
    
    x3 = DBL_DOWNLOAD_YAH_INTRADAY_BY_FILE(Folder_List = 'LIST_INTRADAY_VOLATILITY.txt',
                                           File_List   = 'S:/CCPR/DATA/LIST/',
                                           pType       = 'stock', pCoverage = 'INTERNATIONAL',
                                           Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                           Save_Prefix = 'DBL_YAH_STK<PREFIX>_<FREQUENCY>_<HISTORY>.rds',
                                           ToSave      = T, ToHistory = T, Nb_Min = 500)
    
    x3 = DBL_DOWNLOAD_YAH_INTRADAY_BY_FILE(Folder_List = 'LIST_INTRADAY_VOLATILITY.txt',
                                           File_List   = 'S:/CCPR/DATA/LIST/',
                                           pType       = 'currency', pCoverage = 'INTERNATIONAL',
                                           Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                           Save_Prefix = 'DBL_YAH_CUR<PREFIX>_<FREQUENCY>_<HISTORY>.rds',
                                           ToSave      = T, ToHistory = T, Nb_Min = 500)
    
    x3 = DBL_DOWNLOAD_YAH_INTRADAY_BY_FILE(Folder_List = 'LIST_INTRADAY_VOLATILITY.txt',
                                           File_List   = 'S:/CCPR/DATA/LIST/',
                                           pType       = 'commodity', pCoverage = 'INTERNATIONAL',
                                           Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                           Save_Prefix = 'DBL_YAH_CMD<PREFIX>_<FREQUENCY>_<HISTORY>.rds',
                                           ToSave      = T, ToHistory = T, Nb_Min = 500)
    TO_DO = CCPR_MONITOR_EXECUTION_SUMMARY(pOption='LIST_INTRADAY_VOLATILITY', pAction="COMPARE", NbSeconds=NbMinutes*60, ToPrint=T)
  }
}

# ==================================================================================================
DBB_READRDS_SUMMARY = function(pFolder = UData, pFile = 'DOWNLOAD_BLG_INDHOME_HISTORY.rds', ToCheckDate = T, pHH = 23, pDelay = 0) {
  # ------------------------------------------------------------------------------------------------
  SUMMARY_SELECTED = data.table()
  FullPath_RDS = paste0(pFolder, pFile)
  FullPath_TXT = gsub('.rds$', '_summary.txt', FullPath_RDS)
  CATln_Border(FullPath_TXT)
  if (file.exists(FullPath_TXT))
  {
    SUMMARY = CHECK_CLASS(try(fread(FullPath_TXT)))
    if (nrow(SUMMARY)>0)
    {
      if (ToCheckDate)
      {
        SUMMARY_SELECTED = SUMMARY[datelast < SYSDATETIME_DELAY(pHH, pDelay)][order(-datelast)]
      } else {
        SUMMARY_SELECTED = SUMMARY[order(-datelast)]
      }
      My.Kable.TB(SUMMARY_SELECTED)
    }
  }
  return(SUMMARY_SELECTED)
}
# ==================================================================================================
DBL_DOWNLOAD_SOURCE_INS = function(pSOURCE = 'BLG', pPREFIX = 'HOME', pType = 'IND', Nb_Min = 10, pSleep = 5, Integration_PROBA = 100) {
  # ------------------------------------------------------------------------------------------------
  MY_SUMMARY = DBB_READRDS_SUMMARY(pFolder = UData, pFile = paste0('DOWNLOAD_', pSOURCE, '_', pType, pPREFIX, '_HISTORY.rds'), ToCheckDate = T, pHH = 23, pDelay = 0)
  xALL       = data.table()
  NB_TODO    = min(Nb_Min, nrow(MY_SUMMARY))
  if (NB_TODO>0)
  {
    xLIST      = list()
    for (k in 1:NB_TODO)
    {
      # k        = 1 
      pCode    =  MY_SUMMARY[k]$codesource
      CATln_Border(paste(k, '/', NB_TODO, ' = ', pCode))
      switch(pSOURCE,
             'BLG' = { x = CHECK_CLASS(try(DBL_DOWNLOAD_BLG_BY_CODE_LONGHISTORY(pCodesource = pCode, ToKable=T))) },
             'INV' = { x = CHECK_CLASS(try(DBL_DOWNLOAD_INV_BY_CODE(l_code = pCode, delai=0, nbmax=365, ToPrompt=F))) },
             # 'YAH' = { x = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_BY_CODE(p_nbdays_back=10000, tickers=pCode, freq.data = "daily"))) }, 
             
             'YAH' = { x =  CHECK_CLASS(try(DOWNLOAD_YAH_INS_PRICES_UNADJ(p_nbdays_back= 250000, tickers=pCode, freq.data='daily', p_saveto="", NbTry=15))) }, 
             {
               x = data.table()
             }
      )
      if (nrow(x)>0)
      {
        x = CALCULATE_CHANGE_RT_VARPC(x)
        xLIST[[k]] = x
      }
      IFRC_SLEEP(runif(1,1, pSleep))
    }
    
    xALL = rbindlist(xLIST, fill=T)
    if (nrow(xALL)>0)
    {
      xALL = unique(xALL, by=c('code', 'date'))
      My.Kable.Min(xALL)
    }
    
    CCPR_SAVERDS(xALL, UData, paste0('DOWNLOAD_', pSOURCE, '_', pType, pPREFIX, '_day.rds'), ToSummary=T, SaveOneDrive = T)
    
    try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO(Folder_Fr = UData, File_Fr = paste0('DOWNLOAD_', pSOURCE, '_', pType, pPREFIX, '_day.rds'), 
                                                             Folder_To = UData, File_To = paste0('DOWNLOAD_', pSOURCE, '_', pType, pPREFIX, '_history.rds'), 
                                                             RemoveNA  = T, NbDays = 0, pCondition = '', MinPercent = 0, ToForce = F, IncludedOnly = F))
    
    if (runif(1,1,100)>=Integration_PROBA)
    {
      try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO(Folder_Fr = UData, File_Fr = paste0('DOWNLOAD_', pSOURCE, '_', pType, pPREFIX, '_history.rds'), 
                                                               Folder_To = UData, File_To = paste0('DOWNLOAD_', pSOURCE, '_', pType, '_history.rds'), 
                                                               RemoveNA  = T, NbDays = 0, pCondition = '', MinPercent = 0, ToForce = F, IncludedOnly = F))
      
    }
    GC_SILENT()
    rm(xLIST)
  }
  return(xALL)
}

# ==============================================================================
DBL_DOWNLOAD_YAH_BY_CODE = function(p_nbdays_back, tickers, freq.data)  {
  # ----------------------------------------------------------------------------
  # p_nbdays_back=100; tickers = ('^TYX'); freq.data="daily"; p_saveto='')
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
    my_data = setDT(l.out)
    head(my_data)
    # str(my_data)
    if (nrow(my_data)>0)
    {
      colnames(my_data) = gsub("price.","", colnames(my_data))
      setnames(my_data, "ref_date", "date")
      my_data[, ":="(source="YAH", codesource=ticker, code=as.character(NA))]
      list_first = c( "date", "close", "source", "codesource")
      setcolorder(my_data, c(list_first, setdiff(colnames(my_data), list_first)))
      
    } else { my_data=data.table()}
  } else { my_data=data.table()}
  return(my_data)
}

#===================================================================================================
DBL_DOWNLOAD_INV_BY_CODE = function(l_code = "indices/rwanda-all-share", delai=0, nbmax=365, ToPrompt=F) {
  # ------------------------------------------------------------------------------------------------
  # l_code = "indices/us-30"; delai=0; nbmax=365; ToPrompt=F
  # l_code = "indices/rwanda-all-share"; delai=0; nbmax=365; ToPrompt=F
  # NEW_DOWNLOAD_INV_BY_CODE(l_code = "rates-bonds/argentina-1-year", delai=0, nbmax=365, ToPrompt=T)
  # l_code = list_codesource[[ilist]]
  if (ToPrompt) { CATln_Border(paste0("DOWNLOAD_INV_INS_BY_ONE :", l_code)) }
  URL = paste0("https://www.investing.com/", l_code, "-historical-data")
  req <- curl::curl_fetch_memory(URL)
  x.data = as.data.table(req$content)
  x = rawToChar(req$content)
  # writeLines(x, "c:/temp/x.r")
  # str_block = unlist(str_split(x, '"list-unstyled">'))
  str_block = str.extract(x, '<table class="genTbl closedTbl historicalTbl" id="curr_table" tablesorter>', "</table")
  # x == x_body
  # writeLines(str_block, "c:/temp/x.r")
  
  data.list = list()
  my.line = unlist(str_split(str_block, "<tr"))
  
  if (length(my.line)>3)
  {
    for (j in 3: min(nbmax, length(my.line)))
    {
      #j=3
      one.line = my.line[[j]]
      my.col = unlist(str_split(one.line, "<td"))
      if (length(my.col)> 3) {
        my.price    = as.numeric(trimws(gsub(",","",str.extract(my.col[[3]], '>', '<'))))
        my.open     = as.numeric(trimws(gsub(",","",str.extract(my.col[[4]], '>', '<'))))
        my.high     = as.numeric(trimws(gsub(",","",str.extract(my.col[[4]], '>', '<'))))
        my.low      = as.numeric(trimws(gsub(",","",str.extract(my.col[[4]], '>', '<'))))
        my.date     = as.Date(gsub('"','', str.extract(my.col[[2]], "dateTime=", ">")), format = "%B %d, %Y")
        data.list[[j-2]] = data.table(date=my.date,close=my.price, open=my.open, high=my.high, low=my.low, updated=substr(as.character(Sys.time()),1,19))
        data.ok = T
      } else {data.ok = F}
    }
    data_one = rbindlist(data.list, fill=T)
    if (nrow(data_one)>0)      {
      data_one[, ':='(source='INV', codesource=l_code)]
      # str(data_one)
      # V_DBL_RELOAD_INSREF()
      # My.Kable.INSREF(ins_ref)
      try(DBL_RELOAD_INSREF())
      data_one = merge(data_one[, -c('code', 'name', 'iso2', 'iso3', 'country', 'continent')], ins_ref[, .(codesource=inv, code, name=short_name, iso2, iso3, country, continent)],
                       all.x = T, by='codesource')
      data_one = unique(data_one, by=c('codesource', 'date'))
      # data_one[, ":="(codesource=l_code, source="INV")]
      # data_one = MERGE_DATASRC_CODE_BYCODESOURCE('INV', data_one)
      My.Kable.TB(data_one)
    }
  } else { data_one = data.table()}
  # GC_SILENT()
  return(data_one)
}
