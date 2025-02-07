# ==================================================================================================
# DBL_PREMIUM_LIBRARY.R 2024-09-30 15:00
# ==================================================================================================
# ==================================================================================================
DBL_FINAL_INDHOME = function(LIST_SOURCE = list('YAH', 'RTS', 'CNBC', 'BLG'),
                             SaveFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                             SaveFile = 'DOWNLOAD_SOURCE_INDHOME_HISTORY.rds')
{
  
  xLIST = list()
  DATA = data.table()
  for(k in 1:length(LIST_SOURCE))
  {
    # k=1
    pSource = LIST_SOURCE[[k]]
    xData_OLD = CHECK_CLASS(try(CCPR_READRDS(UData, paste0('DOWNLOAD_', pSource, '_INDHOME_HISTORY.rds'))))
    xData_NEW = CHECK_CLASS(try(CCPR_READRDS(SData, paste0('DBL_', pSource, '_INDHOME_HISTORY.rds'))))
    xData = rbind(xData_NEW, xData_OLD, fill = T)
    if(nrow(xData) > 0 )
    {
      xData = unique(xData[order(-date)], by = c('code','date'), fromLast = T)
      xLIST [[k]] = xData
    }
  }
  DATA = rbindlist(xLIST, fill = T)
  cols = as.character(append(STRUCTURE_DAY_HISTORY, list('close_adj', 'source')))
  DATA = DATA[,..cols]
  list_dataset = list('open','high','low','close', 'close_adj')
  OLD = data.table()
  for(k in 1:length(list_dataset))
  {
    # k = 2
    pDATASET = list_dataset[[k]]
    xDATA = CHECK_CLASS(try(FINAL_VALUE(DATA, DATASET = toupper(pDATASET), order_list = list('YAH', 'RTS', 'CNBC', 'BLG'))))
    xDATA = xDATA[,. (code, date, final )]
    setnames(xDATA, old = 'final', new = pDATASET)
    if(nrow(xDATA) > 0 )
    {
      if(nrow(OLD) > 0)
      {
        OLD = merge(OLD, xDATA, by = c('code', 'date'))
      }else{
        OLD = copy(xDATA)
      }
    }
  }
  try(DBL_CCPR_SAVERDS(OLD, SaveFolder, SaveFile, ToSummary = T, SaveOneDrive = T))
  return(OLD)
}

# ==================================================================================================



UPDATE_LIST_FEARGREED_TXT = function()
{
  LIST_CODES = setDT(read.xlsx(paste0(ODDrive,"/DBL/RD_BATCH_MANAGEMENT.xlsx"), sheet = 'DUO'))
  LIST_CODES = LIST_CODES[FEARGREED == 1]
  LIST_PROD  = LIST_CODES[,. (PRODUCT, DUO, PRODUCT_NAME,  YAH_PRODUCT,INV_PRODUCT, GROUP_BY)]
  LIST_PROD  = LIST_PROD[,.(code = PRODUCT, name = PRODUCT_NAME, yah = YAH_PRODUCT,inv = INV_PRODUCT, group_by = GROUP_BY)]
  LIST_PROD [, active := 1]
  LIST_PROD = unique(LIST_PROD, by = 'code')
  try(fwrite(LIST_PROD, 'S:/CCPR/DATA/LIST/LIST_UNDERLYING_FEARGREED.txt', sep = '\t'))
  return(LIST_PROD)
}

# ==================================================================================================
DBL_DOWNLOAD_BINANCE_VOLAT = function(FREQUENCY = 'last')
{
  xData = data.table()
  DATA = data.table()
  switch (FREQUENCY,
          'day' = {
            pURL = 'https://www.binance.com/fapi/v1/marketKlines?interval=1d&symbol=iBTCBVOLUSDT'
            data = jsonlite::fromJSON(pURL)
            xData      = as.data.table(data)
            xData = xData [,1:5]
            setnames (xData, colnames(xData), c ('timestamp','open','high','low','close' ))
            
            xData [, timestamp_loc := EPOCH_TO_DATE (xEpoch=as.numeric (timestamp)/1000, ConvertTo='DATETIME') ]
            xData [, timestamp_vn := timestamp_loc ]
            xData [,':=' (source = 'BFN', codesource = 'iBTCBVOLUSDT')]
            xData = DBL_CLEAN_OHLC(xData)
            xData = CLEAN_TIMESTAMP(xData)
            xData = UPDATE_UPDATED(xData)
            xData [, date := as.Date (timestamp_loc)]
            xData [, code := 'INDVLIBVOL']
            # xDay = xData
          },
          'intraday' = {
            INTRADAY =  'https://www.binance.com/fapi/v1/marketKlines?interval=1m&symbol=iBTCBVOLUSDT'
            data = jsonlite::fromJSON(INTRADAY)
            xData      = as.data.table(data)
            xData = xData [,1:5]
            setnames (xData, colnames(xData), c ('timestamp','open','high','low','close' ))
            
            xData [, timestamp_loc := EPOCH_TO_DATE (xEpoch=as.numeric (timestamp)/1000, ConvertTo='DATETIME') ]
            xData [, timestamp_vn := timestamp_loc ]
            xData [,':=' (source = 'BFN', codesource = 'iBTCBVOLUSDT')]
            xData = CLEAN_TIMESTAMP(xData)
            xData [, date := as.Date (timestamp_loc)]
            xData [, code := 'INDVLIBVOL']
            list_pdate = unique(xData, by = 'date')
            list_pdate [, pdate:= shift(date)]
            # xData = merge(xData, list_pdate[,.(date, pdate)], by = 'date' )
            
            xDay = DBL_DOWNLOAD_BINANCE_VOLAT (FREQUENCY = 'day')
            xDay = xDay[order(date)]
            xDay [, pclose := shift(close)]
            xDay [, pdate:= shift(date)]
            xDay = xDay[!is.na(date) & date == unique(xData$date)]
            xData = merge(xData[,-c('pclose')], xDay[,.(pdate,date,pclose)], by = 'date', all.x = T)
            # xData$pdate = $pdate
            xData = xData[date == max(date)]
            xData = DBL_CLEAN_OHLC(xData)
            xData[, varpc  := 100*((close/pclose)-1)]
            xData[, change := (close-pclose)]
            xData = UPDATE_UPDATED(xData)
            xData = CLEAN_TIMESTAMP(xData)
          },
          'last' = {
            xData = DBL_DOWNLOAD_BINANCE_VOLAT (FREQUENCY = 'intraday')
            xData = CLEAN_COLNAMES(xData)  
            xData = DBL_CLEAN_OHLC(xData)
            xData = xData[, .(code, date, timestamp, timestamp_vn, open = as.numeric(open), high = as.numeric(high),
                              low = as.numeric(low), close = as.numeric(close), pclose = as.numeric(pclose))]
            xData[, varpc  := 100*((close/pclose)-1)]
            xData[, change := (close-pclose)]
            xData = UPDATE_UPDATED(xData)
            xData = unique(xData[order(date, -timestamp)], by='code')
            My.Kable.Index(xData)
          }
  )
  if (nrow(xData) > 0)
  {
    xData[, ':='(source='BFN')]
    My.Kable(xData)
    DBL_CCPR_SAVERDS(xData, "S:/CCPR/DATA/DASHBOARD_LIVE/",paste0("DBL_BFN_INS_", toupper(FREQUENCY),"_HISTORY.rds"))
  }
  return(xData)
  
}
# ==================================================================================================
DBL_DOWNLOAD_MCT_INDIA = function(FREQUENCY = 'last')
{
  
  xData = data.table()
  DATA = data.table()
  switch (FREQUENCY,
          'day' = {
            DAY      = paste0( 'https://priceapi.moneycontrol.com/techCharts/indianMarket/index/history?symbol=in%3BIDXN&resolution=1D&from=167987&to=', as.integer(Sys.time()),'&countback=10000&currencyCode=INR')
            data = jsonlite::fromJSON(DAY)
            xData      = as.data.table(data)
            xData [, t := as.numeric(t)]
            xData = xData [,.(timestamp = t, open = o, high =h, low = l, close = c)]
            xData [, timestamp_vn := EPOCH_TO_DATE (xEpoch=timestamp, ConvertTo='CHARACTER') ]
            xData [, timestamp_loc := as.POSIXct(timestamp) - (1.5*3600)]
            xData [, timestamp := as.POSIXct(timestamp) - (1.5*3600)]
            xData [,':=' (source = 'MCT', codesource = 'INDIAVIX')]
            xData = CLEAN_TIMESTAMP(xData)
            xData [, date := as.Date (timestamp_loc)]
            xData [, code := 'INDNIFTYVIX']
          },
          'intraday' = {
            INTRADAY = paste0( 'https://priceapi.moneycontrol.com/techCharts/indianMarket/index/history?symbol=in%3BIDXN&resolution=1&from=167987&to=', as.integer(Sys.time()),'&countback=10000&currencyCode=INR')
            data = jsonlite::fromJSON(INTRADAY)
            xData      = as.data.table(data)
            xData [, t := as.numeric(t)]
            xData = xData [,.(timestamp = t, open = o, high =h, low = l, close = c)]
            xData [, timestamp_vn := EPOCH_TO_DATE (xEpoch=timestamp, ConvertTo='CHARACTER') ]
            xData [, timestamp_loc := as.POSIXct(timestamp) - (1.5*3600)]
            xData [, timestamp := as.POSIXct(timestamp) - (1.5*3600)]
            xData [,':=' (source = 'MCT', codesource = 'INDIAVIX')]
            xData = CLEAN_TIMESTAMP(xData)
            xData [, date := as.Date (timestamp_loc)]
            xData [, code := 'INDNIFTYVIX']
            list_pdate = unique(xData, by = 'date')
            list_pdate [, pdate:= shift(date)]
            xData = merge(xData, list_pdate[,.(date, pdate)], by = 'date' )
            
            xDay = DBL_DOWNLOAD_MCT_INDIA (FREQUENCY = 'day')
            xDay = xDay[order(date)]
            xDay [, pclose := shift(close)]
            xData = merge(xData[,-c('pclose')], xDay[,.(pdate = date,pclose)], by = 'pdate', all.x = T)
            xData = xData[date == max(date)]
            xData[, varpc  := 100*((close/pclose)-1)]
            xData[, change := (close-pclose)]
            xData = DBL_CLEAN_OHLC(xData)
            xData = UPDATE_UPDATED(xData)
            xData = CLEAN_TIMESTAMP(xData)
          },
          'last' = {
            xData = DBL_DOWNLOAD_MCT_INDIA (FREQUENCY = 'intraday')
            xData = CLEAN_COLNAMES(xData)  
            xData = DBL_CLEAN_OHLC(xData)
            xData = xData[, .(code, date, timestamp, timestamp_vn, open = as.numeric(open), high = as.numeric(high),
                              low = as.numeric(low), close = as.numeric(close), pclose = as.numeric(pclose))]
            xData[, varpc  := 100*((close/pclose)-1)]
            xData[, change := (close-pclose)]
            xData = UPDATE_UPDATED(xData)
            xData = unique(xData[order(date, -timestamp)], by='code')
            My.Kable.Index(xData)
          }
  )
  if (nrow(xData) > 0)
  {
    xData[, ':='(source='MCT')]
    xData = MERGE_DATASTD_BYCODE(xData)
    My.Kable(xData)
    try(DBL_CCPR_SAVERDS(xData, "S:/CCPR/DATA/DASHBOARD_LIVE/",paste0("DBL_MCT_INS_", toupper(FREQUENCY),"_TODAY.rds")))
  }
  return(xData)
  
}

# ==================================================================================================
DBL_INTEGRATE_FILE_FGD = function (SaveFolder = 'S:/STKVN/INDEX_2024/',
                                   SaveFile = 'dbl_source_fgd_day_history.rds')
{
  LIST_PROD  = try(setDT(fread("S:/CCPR/DATA/LIST/LIST_UNDERLYING_FEARGREED.txt")))
  XLIST      = list()
  DATA_ALL   = data.table()
  DATA       = data.table()
  OLD  = CHECK_CLASS(try(DBL_CCPR_READRDS(SaveFolder, SaveFile)))
  LAST = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_source_ins_last_today.rds' )))
  DATA_LAST = LAST[code %in% LIST_PROD$code]
  
  #try(DBL_CCPR_SAVERDS(DATA_LAST, 'S:/STKVN/INDEX_2024/dbl_source_fgd_last_today.rds', SaveOneDrive = T, ToSummary = T))
  My.Kable(LAST[order(code)][,.(code,source, date)])
  # DATA = rbind(OLD, DATA_LAST, fill = T)
  DATA = rbind(OLD, DATA_LAST, fill = T)
  DATA = DATA[code %in% LIST_PROD$code]
  DATA = unique(DATA, by = c('code', 'date'), fromLast = T)
  #DATA = DATA[code %in% LIST_PROD$PRODUCT ]
  DATA = MERGE_DATASTD_BYCODE(DATA)
  asean40 = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'ASEAN_40.rds', ToKable = T, ToRestore = T)))
  cols = intersect(names(DATA), names(asean40))
  DATA = rbind(DATA, asean40[,..cols], fill = T)
  DATA = unique(DATA, by= c('code', 'date'), fromLast = T)
  
  unique(DATA[order(-date)], by = c('code'))
  My.Kable.All(unique(DATA[order(code, -date)], by = 'code')[,.(code, date)])
  My.Kable(DATA[,.(start = min(date), end = max(date)), by = code])
  try(DBL_CCPR_SAVERDS(DATA, SaveFolder, SaveFile, ToSummary = T, SaveOneDrive = T))
  return(DATA)
}


# ==================================================================================================
DBL_UPDATE_INDFGD_BFNCNN = function()
{
  try(UPDATE_IND_FEARGREED_BY_SOURCE (pSource='CNN'))
  try(UPDATE_IND_FEARGREED_BY_SOURCE (pSource='BFN'))
  
  BFN = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/STKVN/INDEX_2024/', 'DOWNLOAD_BFN_IND_FEARGREED_DAY.rds')))
  CNN = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/STKVN/INDEX_2024/', 'DOWNLOAD_CNN_IND_FEARGREED_DAY.rds')))
  BFN[, code := 'INDBFNCURCRYFGD']
  CNN[, code := 'INDCNNSPXFGD']
  
  BFN = unique(BFN[order(-date)], by = 'code')
  CNN = unique(CNN[order(-date)], by = 'code')
  CNN = CLEAN_TIMESTAMP(CNN)
  BFN = CLEAN_TIMESTAMP(BFN)
  
  x = rbind(BFN, CNN, fill = T)
  
  dbl_ind_feargreed_current = try(setDT(TRAINEE_LOAD_SQL_HOST (pTableSQL = 'dbl_ind_feargreed_current', pHost = 'dashboard_live', ToDisconnect=T, ToKable=T)))
  updated_data = UPDATE_FEARGREED_DATA(x, dbl_ind_feargreed_current)
  updated_data[, datetime := date]
  updated_data = updated_data[, -c('id', 'updated')]
  updated_data = UPDATE_UPDATED(updated_data)
  try(CCPR_SAVERDS(updated_data, 'S:/STKVN/INDEX_2024/',  'dbl_ind_feargreed_current.rds', ToSummary = T, SaveOneDrive = T))
  try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_ind_feargreed_current',
                                   l.filepath= paste0('S:/STKVN/INDEX_2024/','dbl_ind_feargreed_current.rds'),
                                   CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
  
}
# ==================================================================================================

REPAIR_INSREF_CODE = function(){
  # ------------------------------------------------------------------------------------------------
  DBL_RELOAD_INSREF(ToForce = T)
  LIST_SOURCE = list('CBOE', 'CNBC','YAH', 'INV')
  file_duo = setDT(read.xlsx(paste0(ODDrive,"/DBL/RD_BATCH_MANAGEMENT.xlsx"), sheet = 'DUO'))
  
  code_in_1 = ins_ref[code %in% file_duo$PRODUCT]
  code_in_2 = ins_ref[code %in% file_duo$DUO]
  code_in   = unique(rbind(code_in_1, code_in_2), by = 'code')
  for (i in 1:length(LIST_SOURCE)){
    # i = 2
    pSource = LIST_SOURCE[[i]]
    LIST_PROD  = file_duo[,. (code = PRODUCT, name = PRODUCT_NAME, CODESOURCE = get(paste0(pSource, "_PRODUCT")))][!is.na(CODESOURCE)]
    LIST_DUO   = file_duo[,. (code =  DUO, name = DUO_NAME, CODESOURCE = get(paste0(pSource, "_DUO")))][!is.na(CODESOURCE)]
    LIST.CODES = unique(rbind(LIST_PROD, LIST_DUO, fill = T)[!is.na(CODESOURCE)], by = c('CODESOURCE'))
    code_in = merge(code_in,LIST.CODES[,.(code, CODESOURCE)], all.x=T, by = 'code')
    code_in[is.na(get(tolower(pSource))), tolower(pSource) := CODESOURCE]
    code_in[, CODESOURCE := NULL]
  }
  final_insref = rbind(ins_ref[!code %in% code_in$code], code_in)
  try(DBL_SAVE_INSREF(ins_ref=final_insref, ToReload=T))
}
# ==================================================================================================
READ_INDFEARGREED = function()
{
  x = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/STKVN/INDEX_2024/', 'dbl_indfeargreed_history.rds', ToKable = T)))
  file_list = as.data.table(list.files('S:/STKVN/INDEX_2024/', '*.rds$'))
  fie_select = file_list[grepl('^dbl_indfeargreed_history_20', V1)][order(-V1)]
  NB_TODO = min(2, nrow(fie_select))
  
  xLIST = list()
  for (k in 1:NB_TODO)
  {
    # k = 1
    x = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/STKVN/INDEX_2024/', fie_select[k]$V1, ToKable = T)))
    if (nrow(x)>0)
    {
      xLIST[[k]]=x
    }
  }
  xALL = unique(rbindlist(xList, fill=T), by=c('code', 'date'), fromLast = T)
  xALL = unique(rbind(x, xALL, fill=T), by=c('code', 'date'), fromLast = T)
  xALL = xALL[!is.na(close) & !is.na(code)]
  My.Kable.Index(xALL[order(date, code)])
  DBL_CCPR_SAVERDS(xALL, 'S:/STKVN/INDEX_2024/', 'dbl_indfeargreed_history.rds', ToSummary=T, SaveOneDrive = T)
  return(xALL)
}
# ==================================================================================================
# DBL_DOWNLOAD_MERGE_LAST_2 = function (Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
#                                       LIST_SOURCE = list('CBOE', 'CNBC', 'YAH'),
#                                       TOTOP = T, pFREQUENCY = 'LAST')
# {
#   XLIST = list()
#   DATA_ALL = data.table()
#   if (nchar(TOTOP) == 0)
#   {
#     FILE_NAME = paste0('DBL_SOURCE_INS_', pFREQUENCY, '_TODAY.rds' )
#   }
#   if(nchar(as.character(TOTOP)) > 0)
#   {
#     if (TOTOP)
#     {
#       FILE_NAME = paste0('DBL_SOURCE_INS_', pFREQUENCY, '_TODAY.rds' )
#     }else{
#       FILE_NAME = paste0('DBL_SOURCE_INS_', pFREQUENCY, '_TODAY.rds' )
#     }
#   }
#   for (k in 1:length(LIST_SOURCE))
#   {
#     psource = LIST_SOURCE[[k]]
#     if (nchar(TOTOP) == 0)
#     {
#       FILE_NAME_SOURCE = paste0('DBL_', psource, '_INS_', pFREQUENCY, '_TODAY.rds' )
#     }
#     if(nchar(as.character(TOTOP)) > 0)
#     {
#       if (TOTOP)
#       {
#         FILE_NAME_SOURCE = paste0('DBL_', psource, '_TOP_', pFREQUENCY, '_TODAY.rds' )
#       }else{
#         FILE_NAME_SOURCE = paste0('DBL_', psource, '_NOTOP_', pFREQUENCY, '_TODAY.rds' )
#       }
#     }
#     DATA   = CHECK_CLASS(try(DBL_CCPR_READRDS(Folder, FILE_NAME_SOURCE)))
#     DATA   = CLEAN_TIMESTAMP(DATA)
#     if (nrow(DATA) > 0)
#     {
#       XLIST [[k]] = DATA
#     }
#   }
#   DATA_ALL = rbindlist(XLIST, fill = T)
#   DATA_ALL = unique(DATA_ALL[!is.na(code)][order(-timestamp)], by = c('code'))
#   
#   #DBL_CCPR_SAVERDS(DATA_ALL, Folder, FILE_NAME, ToSummary = T)
#   return (DATA_ALL)
# }

# ==================================================================================================
DOWNLOAD_YAH_INTRADAY = function(pCode = '^VIX', frequency = 'intraday'){
  # ------------------------------------------------------------------------------------------------
  Start.time = Sys.time()
  final_data_last = data.table()
  final_data      = data.table()
  switch(frequency,
         'intraday' = {
           xData = DBL_DOWNLOAD_YAH_OHLC_INTRADAY(pCodesource=pCode, pInterval="1m", ToKable=T)
           xData = xData[date == max(date)]
           xData = DBL_CLEAN_OHLC(xData)
           xData = UPDATE_UPDATED(xData)
           xData = CLEAN_TIMESTAMP(xData)
         },
         'day' = {
           xData = DBL_DOWNLOAD_YAH_OHLC_INTRADAY(pCodesource=pCode, pInterval="1d", ToKable=T)
           xData = DBL_CLEAN_OHLC(xData)
           xData = UPDATE_UPDATED(xData)
           xData = CLEAN_TIMESTAMP(xData)
         }, 
         'intraday_with_pclose' = {
           #pCode = "^GSPC"
           xData = DBL_DOWNLOAD_YAH_OHLC_INTRADAY(pCodesource=pCode, pInterval="1m", ToKable=T)
           xDay = DBL_DOWNLOAD_YAH_OHLC_INTRADAY(pCodesource=pCode, pInterval="1d", ToKable=T)
           xDay = xDay[order(date)]
           xDay [, pclose := shift(close)]
           xDay [, pdate := shift(date)]
           # xDate = unique(xData[order(timestamp)][,.(code,date,close)], by = 'date')[, ':=' (pdate = shift(date), pclose = shift(close))]
           xData = merge(xData[,-c('pclose','pdate')], xDay[,.(date,pdate,pclose)], by = 'date', all.x = T)
           xData = xData[date == max(date)]
           xData = DBL_CLEAN_OHLC(xData)
           xData = UPDATE_UPDATED(xData)
           xData = CLEAN_TIMESTAMP(xData)
           xData[, varpc  := 100*((close/pclose)-1)]
           xData[, change := (close-pclose)]
         },
         'last' = {
           xData = DOWNLOAD_YAH_INTRADAY (pCode, frequency = 'intraday_with_pclose')
           xData = CLEAN_COLNAMES(xData)  
           xData = DBL_CLEAN_OHLC(xData)
           xData = xData[, .(code, date, timestamp, timestamp_vn, open = as.numeric(open), high = as.numeric(high),
                             low = as.numeric(low), close = as.numeric(close), pclose = as.numeric(pclose))]
           xData[, varpc  := 100*((close/pclose)-1)]
           xData[, change := (close-pclose)]
           xData = UPDATE_UPDATED(xData)
           xData = unique(xData[order(date, -timestamp)], by='code')
           My.Kable.Index(xData)
         })
  if (nrow(xData) > 0)
  {
    xData[, ':='(source='YAH', codesource=pCode)]
    xData = DBL_MERGE_DATASRC_CODE_BYCODESOURCE('YAH', xData)
    My.Kable(xData)
  }
  DBL_DURATION(Start.time)
  return (xData)
  
}

# ==================================================================================================
DOWNLOAD_CBOE_VIX_BY_CODE_DAY = function(pCode = 'SPX')
{
  # pCode = 'VIX'
  data = try(setDT(fread(paste0('https://cdn.cboe.com/api/global/us_indices/daily_prices/', pCode, '_History.csv'))))
  if (!'CLOSE' %in% names(data)){
    data[, CLOSE := get(pCode)]
  }
  if(nrow(data) > 0)
  {
    setnames(data, new = tolower(names(data)))
    data[, date :=  as.Date(date, format = "%m/%d/%Y")]
    data[, codesource := pCode]
    data  = DBL_MERGE_DATASRC_CODE_BYCODESOURCE('CBOE', data)
    data  = MERGE_DATASTD_BYCODE(data)
    data  = DBL_CLEAN_OHLC(data)
    data  = DBL_IND_CALCULATE_RT_VAR_CHANGE(data)
  }
  return(data)
}
# ==================================================================================================
# pCode = "VIX"; yCode = '^VIX'; frequency = 'intraday'
DOWNLOAD_CBOE_YAH_INTRADAY_NEW = function(pCode = "VIX", yCode = '^VIX', frequency = 'day', houradj = 'NA')
{
  # pCode = "VIX"; yCode = '^VXN'; frequency = 'intraday_with_pclose'
  # pCode = "SPX"; yCode = '^GSPC'; frequency = 'intraday_with_pclose'
  # houradj = -4
  Start.time      = Sys.time()
  final_data_last = data.table()
  final_data      = data.table()
  xData           = data.table()
  # nchar(houradj)
  
  pURL      = paste0('https://cdn.cboe.com/api/global/delayed_quotes/quotes/_',pCode,'.json' )
  data      = jsonlite::fromJSON(pURL)
  timestamp =  data$timestamp 
  timeloc   =  data$data$last_trade_time
  
  if (houradj == 'NA' | nchar(houradj) == 0)
  {
    tz_offset = 0
  }else{
    tz_offset = houradj
  }
  if(nchar(timeloc) > 19)
  {
    match = regmatches(timeloc, regexec("([+-]\\d{2}:\\d{2})$", timeloc))
    if (length(match[[1]]) > 1) {
      tz_offset = gsub("0", "", gsub(":", "", match[[1]][2]))
    }
  }
  
  # hour      = format(as.POSIXct(str_sub(timestamp,12,19),format="%H:%M:%S"),"%H")
  # phour     = format(as.POSIXct(str_sub(time_loc,12,19),format="%H:%M:%S"),"%H")
  # tz_offset = as.numeric(phour) - as.numeric(hour) ; tz_offset
  
  tz_offset = as.numeric(tz_offset)
  pURL     = paste0('https://www.cboe.com/indices/data/?symbol=', pCode, '&timeline=intraday')
  datetime = gsub('-| |:|\\.', '',as.character(Sys.time()))
  data     = jsonlite::fromJSON(pURL)
  xData    = try(as.data.table(data))
  My.Kable(xData)
  if(nchar(tz_offset) != 0 )
  {
    if (all(class(xData) !='try-error') & nrow(xData) > 0)
    {
      switch(frequency, 
             'intraday' = {
               xData = xData %>%
                 separate(data.V1, into = c("date", "timestamp"), sep = "T")
               xData = as.data.table(xData)
               xData [, date := as.Date(date)]
               # xData [, codesource := pCode]
               xData [, timestamp := as.POSIXct(paste0(date, ' ', timestamp))]
               xData [, timestamp := as.POSIXct(timestamp + (tz_offset*3600)) ]
               xData [, timestamp_vn := as.POSIXct(timestamp) - ((tz_offset - 7)*3600)]
               xData [order(-timestamp)]
               #min(xData$data.V4)
               setnames(xData, old = c('data.V2','data.V3', 'data.V4', 'data.V5'), new = c('open', 'high', 'low', 'close'))
               xData[, datetime := timestamp]
               xData = xData[order(timestamp_vn)]
               xData = xData[-.N,]
               xData = xData[, id:=seq.int(1,.N)]
               xData = xData[!(open==high & high==low & low==close) & id>.N*0.8]
               xData = UPDATE_UPDATED(xData)
               xData = CLEAN_TIMESTAMP(xData)
               My.Kable(xData)
             },
             'intraday_with_pclose' = {
               xData = xData %>%
                 separate(data.V1, into = c("date", "timestamp"), sep = "T")
               xData = as.data.table(xData)
               xData [, date := as.Date(date)]
               xData [, timestamp := as.POSIXct(paste0(date, ' ', timestamp))]
               xData [, timestamp := as.POSIXct(timestamp + (tz_offset*3600)) ]
               xData [, timestamp_vn := as.POSIXct(timestamp) - ((tz_offset - 7)*3600)]
               xData [order(-timestamp)]
               min(xData$data.V4)
               setnames(xData, old = c('data.V2','data.V3', 'data.V4', 'data.V5'), new = c('open', 'high', 'low', 'close'))
               xData = xData[order(timestamp_vn)]
               xData = UPDATE_UPDATED(xData)
               xData = CLEAN_TIMESTAMP(xData)
               My.Kable(xData)
               
               index_cboe_day = CHECK_CLASS(try(DOWNLOAD_CBOE_VIX_BY_CODE_DAY(pCode)))
               
               index_yah_data = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_OHLC_INTRADAY( pCodesource = yCode, pInterval = '1d', Hour_adjust = -5)))
               if (nrow(index_yah_data) > 0){
                 index_yah_date = index_yah_data[,.(date)]
                 index_yah_date = rbind(index_yah_date, index_cboe_day[!date %in% index_yah_date$date][, .(date)], fill = T)
                 index_yah_date = index_yah_date[order(date)]
                 index_yah_date[, pdate := shift(date)]
                 index_yah_date = index_yah_date[!is.na(pdate)]
                 xData = merge(xData[, -c('pdate')], index_yah_date, by = 'date', all.x = T)
                 index_cboe_day = index_cboe_day[order(date)]
                 index_cboe_day[, pclose := shift(close)]
                 index_cboe_day[, varpc := 100*((close/shift(close))-1)]
                 My.Kable(index_cboe_day[, .(code, date, close,pclose, varpc)])
                 xData = merge(xData[, -c('pclose')], index_cboe_day[, .(pdate = date, pclose = close)], all.x = T, by = "pdate")
                 xData = DBL_CLEAN_OHLC(xData)
                 xData = xData[!is.na(pclose)]
                 xData[, change:=close-pclose]
                 xData[, varpc:=100*((close/pclose)-1)]
                 xData = copy(xData)
               } else {
                 index_cboe_day = index_cboe_day[order(date)]
                 index_cboe_day[, pclose := shift(close)]
                 index_cboe_day[, pdate := shift(date)]
                 index_cboe_day[, varpc := 100*((close/shift(close))-1)]
                 My.Kable(index_cboe_day[, .(code, date, close,pclose, varpc)])
                 xData = merge(xData[, -c('pclose')], index_cboe_day[, .(date,pdate, pclose)], all.x = T, by = "date")
                 xData = CLEAN_COLNAMES(xData)  
                 xData = DBL_CLEAN_OHLC(xData)
                 xData = xData[!is.na(pclose)]
                 xData[, change:=close-pclose]
                 xData[, varpc:=100*((close/pclose)-1)]
               }
               # index_cboe_day = CHECK_CLASS(try(DOWNLOAD_CBOE_VIX_DAY_BY_CODE(pCode , CODE)))
               My.Kable(xData)
             }, 
             'last' = {
               xData = DOWNLOAD_CBOE_YAH_INTRADAY_NEW (pCode = pCode, yCode = yCode, frequency = 'intraday_with_pclose', houradj = tz_offset)
               xData = CLEAN_COLNAMES(xData)  
               xData = DBL_CLEAN_OHLC(xData)
               xData = xData[, .(code, pdate, date, timestamp, timestamp_vn, open = as.numeric(open), high = as.numeric(high),
                                 low = as.numeric(low), close = as.numeric(close), pclose = as.numeric(pclose))]
               xData[, varpc  := 100*((close/pclose)-1)]
               xData[, change := (close-pclose)]
               xData = UPDATE_UPDATED(xData)
               xData = unique(xData[order(date, -timestamp)], by='code')
               My.Kable.Index(xData)
             },
             'day' = {
               # pCode = 'OVX'
               xData = CHECK_CLASS(try(DOWNLOAD_CBOE_VIX_BY_CODE_DAY(pCode )))
               xData = CLEAN_COLNAMES(xData)  
               xData = DBL_CLEAN_OHLC(xData)
               My.Kable.Index(xData)
             })
    }
  }
  if (nrow(xData) > 0)
  {
    xData[, ':='(source='CBOE', codesource=pCode)]
    xData = DBL_MERGE_DATASRC_CODE_BYCODESOURCE('CBOE', xData)
    My.Kable(xData)
  }
  DBL_DURATION(Start.time)
  return (xData)
}

# DATA = DOWNLOAD_CBOE_YAH_INTRADAY_NEW (pCode = "VIX", yCode = '^VIX', frequency = 'day')
# ==================================================================================================
# pSource   = 'CBOE'
# ToTOP     = ''
# FREQUENCY = 'LAST'
DBL_DOWNLOAD_INS_FREQUENCY = function (pSource   = 'CBOE',
                                       ToTOP     = T,
                                       FREQUENCY = 'LAST')
{
  DBL_RELOAD_INSREF()
  Start.time = Sys.time()
  DATA = data.table()
  FILE_NAME = ''
  
  LIST_CODES = setDT(read.xlsx(paste0(ODDrive,"/DBL/RD_BATCH_MANAGEMENT.xlsx"), sheet = 'DUO'))
  LIST_PROD  = LIST_CODES[,. (PRODUCT, DUO, PRODUCT_NAME, CODESOURCE = get(paste0(pSource, "_PRODUCT")), YAH_PRODUCT, TOP)][!is.na(CODESOURCE)]
  LIST_DUO   = LIST_CODES[,. (PRODUCT, DUO, PRODUCT_NAME, CODESOURCE = get(paste0(pSource, "_DUO")),  YAH_DUO, TOP)][!is.na(CODESOURCE)]
  LIST.CODES = unique(rbind(LIST_PROD, LIST_DUO, fill = T)[!is.na(CODESOURCE)], by = c('CODESOURCE'))
  if (nchar(ToTOP) == 0)
  {
    FILE_NAME = paste0('DBL_', pSource, '_INS_', FREQUENCY, '_TODAY.rds' )
  }
  if(nchar(as.character(ToTOP)) > 0)
  {
    if (ToTOP)
    {
      LIST.CODES = LIST.CODES [TOP == 1]
      FILE_NAME = paste0('DBL_', pSource, '_TOP_', FREQUENCY, '_TODAY.rds' )
    }else{    
      LIST.CODES = LIST.CODES [TOP == 0]
      FILE_NAME = paste0('DBL_', pSource, '_NOTOP_', FREQUENCY, '_TODAY.rds' )
    }
  }
  FREQUENCY  = ifelse(FREQUENCY == 'INTRADAY','INTRADAY_WITH_PCLOSE' ,FREQUENCY)
  LIST.CODES = as.list(LIST.CODES$CODESOURCE)
  #LIST.CODES= list('.VIX')
  # DAY ......................,......................................................................
  
  switch (pSource,
          'CNBC' = {
            DATA = DBL_RUN_LIST_FUNCTIONS(EXC_STR = paste0("CHECK_CLASS(try(DBL_CNBC_INTRADAY_DAY (pCode = <CODE>, frequency=", "'", tolower(FREQUENCY), "'", ")))"),
                                          XLIST_CODES = LIST.CODES,
                                          GroupBy = 'codesource x date x timestamp', NB = 100)
          },
          'YAH'  = {
            DATA = DBL_RUN_LIST_FUNCTIONS(EXC_STR = paste0("CHECK_CLASS(try(DOWNLOAD_YAH_INTRADAY (pCode = <CODE>, frequency=", "'", tolower(FREQUENCY), "'", ")))"),
                                          XLIST_CODES = LIST.CODES,
                                          GroupBy = 'codesource x date x timestamp', NB = 100)
            
            
          },
          'CBOE' = {
            DATA = DBL_RUN_LIST_FUNCTIONS(EXC_STR = paste0("CHECK_CLASS(try(DOWNLOAD_CBOE_YAH_INTRADAY_NEW (pCode = <CODE>, yCode = <yCODE>, frequency=", "'", tolower(FREQUENCY), "'", ",houradj = <TIME>)))"),
                                          XLIST_CODES = LIST.CODES,
                                          GroupBy = 'codesource x date x timestamp', NB = 100)
          }
  )
  if(nrow(DATA) > 0)
  {
    if(FREQUENCY == 'LAST')
    {
      OLD  = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', FILE_NAME)))
      DATA = rbind(DATA, OLD, fill = T)
      DATA = unique(DATA[order(-timestamp_vn)],by = c('code', 'date'))
    }
    My.Kable(DATA[, .(source, codesource, code, date,  open, high, low, close)])
    DBL_CCPR_SAVERDS(DATA, 'S:/CCPR/DATA/DASHBOARD_LIVE/', FILE_NAME, ToSummary = T)
  }
  
  
  
  DBL_DURATION(Start.time)
  return (DATA)
}
# ==================================================================================================
DBL_DOWNLOAD_SOURCE_BY_LIST = function (Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                        LIST_SOURCE = list('CBOE', 'CNBC','YAH'),
                                        TOTOP = T, pFREQUENCY = 'LAST')
{
  XLIST = list()
  DATA_ALL = data.table()
  
  for (k in 1:length(LIST_SOURCE))
  {
    # k =1
    psource = LIST_SOURCE[[k]]
    DATA   = DBL_DOWNLOAD_INS_FREQUENCY (pSource   = psource,
                                         ToTOP     = TOTOP,
                                         FREQUENCY = pFREQUENCY)
    
    if (nrow(DATA) > 0)
    {
      DATA = CLEAN_TIMESTAMP(DATA)
      XLIST [[psource]] = DATA
    }
  }
  DATA_ALL = rbindlist(XLIST, fill = T)
  DATA_ALL = unique(DATA_ALL[order(-timestamp)], by = c('code', 'timestamp'))
  
  return (DATA_ALL)
}
# ==================================================================================================
DBL_DOWNLOAD_SOURCE_BY_LIST_FREQUENCY = function (Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                  LIST_FREQUENCY = list('INTRADAY', 'LAST'),
                                                  pTOTOP = T,  LIST_PSOURCE = list('CBOE'))
{
  XLIST = list()
  DATA_ALL = data.table()
  
  for (k in 1:length(LIST_FREQUENCY))
  {
    # k = 1
    PFREQUENCY = LIST_FREQUENCY[[k]]
    DATA = try(DBL_DOWNLOAD_SOURCE_BY_LIST(Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                           LIST_SOURCE = LIST_PSOURCE,
                                           TOTOP = pTOTOP, pFREQUENCY = PFREQUENCY))
    
    if (nrow(DATA) > 0)
    {
      DATA = CLEAN_TIMESTAMP(DATA)
      DATA [, FREQUENCY := PFREQUENCY ]
      XLIST [[PFREQUENCY]] = DATA
    }
  }
  return (XLIST)
}

# ==================================================================================================
FEARGREED_HISTORY_REPAIR = function(pFolder = 'S:/SHINY/REPORT/FEARGREED/',
                                    pFile = 'report_day_feargreed.rds')
{
  
  REPORT  = CHECK_CLASS(try(DBL_CCPR_READRDS(pFolder, pFile)))
  My.Kable.All(REPORT)
  
  X_ERROR     = REPORT[!is.na(status_und)]
  X_ERROR     = merge(X_ERROR [, -c('yah')], ins_ref[,. (code, yah)], by = 'code')
  X_ERROR_YAH = X_ERROR[!is.na(yah)]
  My.Kable.All(X_ERROR_YAH)
  
  XLIST    = list()
  DATA_ALL = data.table()
  if (nrow(X_ERROR_YAH) > 0)
  {
    for(k in 1:nrow(X_ERROR_YAH))
    {
      # k = 1
      pCode = X_ERROR_YAH[k]$yah
      DATA  = CHECK_CLASS(try(DOWNLOAD_YAH_INS_PRICES_UNADJ(p_nbdays_back= 1000, tickers = pCode, freq.data='daily', p_saveto="", NbTry=15)))
      if(nrow(DATA) > 0 )
      {
        XLIST [[k]] = DATA
      }
    }
    DATA_ALL = rbindlist(XLIST, fill = T)
    DATA_ALL = DBL_IND_CALCULATE_RT_VAR_CHANGE(DATA_ALL)
    My.Kable(DATA_ALL)
    
    
    DATA_OLD = CHECK_CLASS (try (DBL_CCPR_READRDS ('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_source_ins_day_history.rds', ToKable = T, ToRestore = T) ) )
    DATA_OLD = rbind(DATA_OLD, DATA_ALL, fill = T)
    DATA_OLD = unique(DATA_OLD, by = c('code', 'date'))
    
    DBL_CCPR_SAVERDS(DATA_OLD,'S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_source_ins_day_history.rds', ToSummary = T, SaveOneDrive = T )
    
    REPORT = try(DBL_REPORT_FEARGREED (Folder_und = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                       File_und =  'dbl_source_ins_day_history.rds',
                                       file_codes = paste0('S:/STKVN/INDEX_2024/', 'dbl_indfgd_history.rds')))
    
    
    x =  TRAINEE_LOADRDS_SUMMARY('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_source_ins_day_history.rds')
    My.Kable(x[code %in% DATA_ALL$code])
    
  }
  return(REPORT)
  
}
# ==================================================================================================
# DBL_REPAIR_IMPLIED_VOLAT = function (Folder_File = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
#                                      List_codes = list("INDVIX", "INDSPX", "INDVXN", "INDNDX", "INDOVX", "FUTCMDCLC1", "INDGVZ", "CMDGOLD", "INDVXD", "INDDJI", "INDRVX", "INDRTY"))
# {
#   DATA_OLD = CHECK_CLASS(try(DBL_CCPR_READRDS(Folder_File, 'dbl_source_ins_all_day_history.rds', ToKable = T)))
#   IND_VOLATILITY = DATA_OLD[code %in% List_codes]
#   IND_VOLATILITY = UPDATE_UPDATED(IND_VOLATILITY)
#   IND_VOLATILITY = unique(IND_VOLATILITY[order(code, -date)], by = 'code')[,. (code, date)]
#   IND_VOLATILITY = merge(IND_VOLATILITY, ins_ref[,. (code, yah)], by = 'code')
#   
#   XLIST    = list()
#   DATA_ALL = data.table()
#   
#   for(k in 1: nrow(IND_VOLATILITY))
#   {
#     # k = 1
#     pCode = IND_VOLATILITY[k]$yah
#     if(LastTrading > IND_VOLATILITY[k]$date)
#     {
#       
#       DATA =  CHECK_CLASS(try(DOWNLOAD_YAH_INS_PRICES_UNADJ(p_nbdays_back= 1000, tickers = pCode, freq.data='daily', p_saveto="", NbTry=15)))
#       if(nrow(DATA) > 0 )
#       {
#         DATA = DBL_IND_TO_CLEAN(DATA)
#         DATA = DBL_IND_CALCULATE_RT_VAR_CHANGE(DATA)
#         XLIST [[k]] = DATA
#       }
#     }
#   }
#   DATA_ALL = rbindlist(XLIST, fill = T)
#   DATA_OLD = rbind(DATA_OLD, DATA_ALL, fill = T)
#   DATA_OLD = unique(DATA_OLD, by = c('code', 'date'))
#   DBL_CCPR_SAVERDS(DATA_OLD, Folder_File, 'dbl_source_ins_all_day_history.rds', ToSummary = T, SaveOneDrive = T )
#   try(DBL_UPDATE_IMPLIED_VOLATILITY (Folder_File = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Frequency = 'DAY',
#                                      List_codes = list('INDVXD', 'INDVXN', 'INDVIX', 'INDGVZ', 'INDOVX', 'INDRVX'),
#                                      ToSave = T, ToUpload = T))
#   
#   return (DATA_OLD)
#   
# }



# ==================================================================================================

DBL_INTEGRATE_FILE_DAY_BY_LIST = function (LIST_FILES = 'S:/CCPR/DATA/LIST/LIST_FILES_INTEGRATE_DAY.txt',
                                           SaveFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', 
                                           SaveFile   = 'dbl_source_ins_day_history.rds' ) 
{
  LIST.FILES = setDT(CHECK_CLASS(try(fread(LIST_FILES, sep = '\t'))))
  My.Kable.All(LIST.FILES)
  
  DATA_ALL = data.table()
  
  for (k in 1:nrow(LIST.FILES) )
  {
    # k = 1
    if(LIST.FILES[k]$active == 1)
    {
      Folder = LIST.FILES[k]$folder
      File   = LIST.FILES[k]$filename
      DATA = try(DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
                                    Fr_Folder = paste0(Folder), Fr_File = paste0(File),
                                    To_Folder = SaveFolder, To_File = SaveFile ) )
      
    }
    
  }
  return(DATA)
  
}

# ==================================================================================================

# DBL_CALCULATE_4INDEX = function (Folder_Prices = 'S:/CCPR/DATA/',
#                                  File_Prices   = 'download_yah_curcrypto_history.rds', ToSave = T){
# 
# 
#   data = CHECK_CLASS(try(CCPR_READRDS(Folder_Prices, File_Prices)))
#   data = data[order(date)][!is.na(capiusd)]
#   x = unique(data, by = 'ticker')[order(-capi)][,. (code, name, codesource, market_cap)]
#   data[, pClose := shift(close_adj), by = 'code']
#   data[, nbDay := as.numeric(date - shift(date)), by = 'code']
#   data[, pDate := as.Date(shift(date)), by = 'code']
#   My.Kable(data[order(rt)][,.(codesource, date, pDate,close_adj, pClose, nbDay, rt)])
#   data = data [date >= '2018-12-31']
#   #data = data[order(date)]
#   # data[rt == 0 ]
#   data[!is.finite(rt)] 
#   
#   INDEX = data[,.(
#     n                   = .N, 
#     sum_capi            = sum(capiusd, na.rm=T),
#     sum_rt              = sum(rt, na.rm=T), 
#     sum_prod_capixrt    = sum(capiusd*rt, na.rm=T)
#   ), by = 'date']
#   
#   My.Kable(INDEX[order(date)])
#   
#   INDEX[sum_rt == 0 | n == 0]
#   # INDEX = INDEX [sum_rt == 0]
#   INDEX[!is.na(n) & n>0,  rt_ew:=  sum_rt/n]
#   # INDEX =  INDEX[is.finite(rt_ew)] 
#   INDEX[!is.na(sum_capi), rt_cw:=  sum_prod_capixrt/sum_capi]
#   INDEX[, index_ew:=1000]
#   INDEX[, index_cw:=1000]
#   INDEX[, index_fw:=1000]
#   
#   INDEX[!is.finite(rt_ew)]
#   
#   starter = 1000 
#   INDEX[, nr:=seq.int(.N)]
#   
#   INDEX[nr==1, rt_ew:=0]
#   INDEX[nr==1, rt_cw:=0]
#   # View(STKVN_4INDEX_SUMS_ONE)
#   INDEX %>%
#     mutate(wgtrt.unit = order_by(date,cumprod(1+rt_ew)),
#            index_ew = wgtrt.unit*starter) -> INDEX
#   INDEX %>%
#     mutate(wgtrt.unit = order_by(date,cumprod(1+rt_cw)),
#            index_cw = wgtrt.unit*starter) -> INDEX
#   INDEX = setDT(INDEX)[order( date)]
#   
#   INDEX$index_temp = NULL
#   INDEX$nr = NULL
#   INDEX$wgtrt.unit = NULL
#   is.infinite(x)
#   My.Kable(INDEX)
# 
# 
# }
# ==================================================================================================
DOWNLOAD_LIST_YAH_ETFCURCRY = function (list_codes = list(),
                                        SaveFolder = 'S:/CCPR/DATA/', type = 'CRYPTO', ToUpload = T,
                                        SaveFile   = 'download_yah_curcrypto_history.rds',
                                        ToSave     = T, Nb = 10000){
  LIST_CODES    = list_codes
  # LIST_CODES    = list('BTC-USD')
  XLIST_DATA    = list()
  combined_data = data.table()
  DATA_OLD      = data.table()
  DATA_NEW      = data.table()
  Nb = 10
  
  if ( length(LIST_CODES) > 0)
  {
    IFRC_SLEEP(3)
    for (PCode in sample(LIST_CODES))
    {
      DATA = CHECK_CLASS(try(TRAINEE_DOWNLOAD_YAH_PRICES_ALL_BY_CODE (pCode = PCode, NB_Day = Nb, Dividend = T, Capi = T)))
      if(nrow(DATA) > 0 )
      {
        XLIST_DATA [[PCode]] = DATA
      }
    }
    DATA_NEW = rbindlist(XLIST_DATA, use.names = TRUE, fill = TRUE)
    DATA_NEW = unique(DATA_NEW, by = c('code', 'date'))
    if(nchar(SaveFolder) * nchar(SaveFile) > 0)
    {
      DATA_OLD      = CHECK_CLASS(try(DBL_CCPR_READRDS(SaveFolder, SaveFile, ToKable = T)))
      
      if (nrow(DATA_OLD) > 0 )
      {
        DATA_NEW      = rbind(DATA_OLD, DATA_NEW, fill = T)
        DATA_NEW      = unique(DATA_NEW[order(-updated)], by = c('code', 'date'))
      }
      DATA_NEW = DATA_NEW[!is.na(code)]
      DATA_NEW [!is.na(capi) & !is.na(rt), capixrt := (capi * rt)]
      if (ToSave)
      {
        try(CCPR_SAVERDS(DATA_NEW, SaveFolder, SaveFile, ToSummary = T, SaveOneDrive = T))
      }
      
      if(ToUpload)
      {
        try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= paste0('dbl_', tolower(type), 'backtesting_history_chart'),
                                         l.filepath= paste0(SaveFolder ,SaveFile),
                                         CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
      }
    }
    return(DATA_NEW)
  }
}

# ==================================================================================================
DBL_REPORT_INTRADAY_VOLATILITY = function(Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                          File = 'dbl_indbeq_intraday_volatility_day.rds')
{
  data = data.table()
  data = CHECK_CLASS(try(DBL_CCPR_READRDS(Folder, File)))
  if(nrow(data) > 0)
  {
    day = unique(data[order(code, date, - timestamp)], by = c('code', 'date'))[order(code, date)]
    day [, pClose := shift(close), by = 'code']
    day [, pClose_und := shift(close_und), by = 'code']
    data = merge(data, day[,.(code, date, pClose, pClose_und)], by = c('code', 'date'))
    
    data [, change := (close-pClose)]
    data [, varpc := ((close/pClose)- 1)*100]
    
    data [, change_und := (close_und-pClose_und)]
    data [, varpc_und := ((close_und/pClose_und)- 1)*100]
    data = UPDATE_UPDATED(data)
    
    data_last = unique(data[order(code, -timestamp)], by ='code')
    data_last = data_last[,. (name, date, timestamp, timestamp_vn, close, change, varpc, code_und,close_und, change_und, varpc_und)][order(timestamp_vn)]
    My.Kable.Min(data_last[grepl('USD', name)])
    My.Kable.All(data_last[,. (name, date, timestamp, timestamp_vn, close, change, varpc, code_und,close_und, change_und, varpc_und)][order(timestamp_vn)])
    try(CCPR_SAVERDS(data_last, 'S:/CCPR/DATA/DASHBOARD_LIVE/REPORT/', 'REPORT_INTRADAY_VOLATILITY.rds', ToSummary = T, SaveOneDrive = T))
    try(CCPR_SAVERDS(data_last, 'S:/SHINY/REPORT/VOLATILITY/', 'REPORT_INTRADAY_VOLATILITY.rds', ToSummary = T, SaveOneDrive = T))
    try(TRAINEE_CONVERT_FILE_RDS_TO_FST(pData = data.table(), File_Folder = paste0("S:/SHINY/REPORT/VOLATILITY/"), File_Name = 'REPORT_INTRADAY_VOLATILITY.rds', FST_Folder = ''))
  }
  return(data_last)
}
# ==================================================================================================
DBL_REPORT_FEARGREED = function(Folder_und = 'S:/STKVN/INDEX_2024/', File_und   =  'dbl_source_fgd_day_history.rds')
{
  
  data = CHECK_CLASS(try(DBL_CCPR_READRDS(Folder_und, File_und , ToKable = T, ToRestore = T))) 
  stat_raw = data[,. (start_und = min(date), end_und = max(date)), by = 'code']
  stat_raw = merge(stat_raw, unique(data[order(-timestamp)], by = 'code')[,.(code, timestamp)], by = 'code')
  setnames(stat_raw, old = 'timestamp', new = 'UND_TIMESTAMP')
  data_fgd = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/STKVN/INDEX_2024/', 'dbl_indfeargreed_history.rds')))
  data_fgd[is.na(code_und)]
  data_fgd[code_und == 'INDSPX']
  stat_fgd = data_fgd[,. (start_fgd = min(date), end_fgd = max(date)), by = 'code_und']
  
  dbl_ind_feargreed_current = try(setDT(TRAINEE_LOAD_SQL_HOST (pTableSQL = 'dbl_ind_feargreed_current', pHost = 'dashboard_live', ToDisconnect=T, ToKable=T)))
  REPORT = dbl_ind_feargreed_current[,. (name = subtitle, code, code_und, group_by, date_fgd = date, timestamp, updated)]
  REPORT = merge(REPORT, stat_raw[,.(code_und = code, start_und, end_und, UND_TIMESTAMP)], by = 'code_und', all.x = T)
  
  
  REPORT[, date_fgd := as.Date(date_fgd, format = "%Y-%m-%d")]
  REPORT[, start_und := as.Date(start_und, format = "%Y-%m-%d")]
  REPORT[, end_und := as.Date(end_und, format = "%Y-%m-%d")]
  
  REPORT[group_by != 'VIETNAM' & (SYSDATETIME_DELAY(l.hour = 23, delay = 1) > date_fgd), 
         FGD_DELAY := as.numeric(SYSDATETIME_DELAY(l.hour = 23, delay = 1) - date_fgd)]
  
  REPORT[group_by == 'VIETNAM' & (SYSDATETIME_DELAY(l.hour = 18, delay = 0) > date_fgd), 
         FGD_DELAY := as.numeric(SYSDATETIME_DELAY(l.hour = 18, delay = 0) - date_fgd)]
  
  REPORT[group_by != 'VIETNAM' & (SYSDATETIME_DELAY(l.hour = 23, delay = 1) > end_und), UND_DELAY := as.numeric(SYSDATETIME_DELAY(l.hour = 23, delay = 1) - end_und)]
  REPORT[group_by == 'VIETNAM' & (SYSDATETIME_DELAY(l.hour = 18, delay = 0) > end_und), UND_DELAY := as.numeric(SYSDATETIME_DELAY(l.hour = 23, delay = 0) - end_und)]
  
  current_time = as.POSIXct(Sys.time(), tz = "Asia/Ho_Chi_Minh")
  
  REPORT[, updated := as.POSIXct(updated, format = "%Y-%m-%d %H:%M:%S")]
  REPORT[, upload := round(difftime(current_time, updated, units = "mins"), 2)]
  REPORT = UPDATE_UPDATED(REPORT[, -c('updated')])
  try(saveRDS(REPORT, 'S:/SHINY/FEARGREED/FEARGREED/REPORT_DAY_FEARGREED.rds'))
  try(TRAINEE_CONVERT_FILE_RDS_TO_FST(pData = data.table(), File_Folder = paste0("S:/SHINY/FEARGREED/FEARGREED/"), File_Name = 'REPORT_DAY_FEARGREED.rds', FST_Folder = ''))
  
  return(REPORT)
}

# ==================================================================================================
# DBL_FEARGREED_LOOP = function (pOption = 'DOWNLOAD',  NbMinutes = 15 ) {
#   # ------------------------------------------------------------------------------------------------
#   # try (DBL_INTRADAY_VOLATILITY_LOOP( pOption = 'YAH', pMinutes = 30) )
#   # try (DBL_INTRADAY_VOLATILITY_LOOP( pOption = 'DNSE', pMinutes = 30) )
#   
#   switch (pOption,
#           'DOWNLOAD' = {
#             TO_DO     = TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption= paste0('DBL_FEARGREED_LOOP>', pOption), pAction="COMPARE", NbSeconds=NbMinutes*60, ToPrint=T)
#             if (TO_DO)
#             {
#               
#               # try(DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
#               #                        Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_yah_ins_day_day.rds',
#               #                        To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_source_ins_day_day.rds') )
#               
#               # 
#               # if (runif(1,1,100)>50)
#               # {
#               #   
#               #   
#               #   try(DBL_INTEGRATE_INTRADAY(data = data.table(), 
#               #                              Fr_Folder = 'S:/STKVN/INDEX_2024/', Fr_File = paste0('DOWNLOAD_ENT_', 'INDVN', '_INTRADAY_HISTORY.RDS'),
#               #                              To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_source_ins_intraday_history.rds'))
#               #   
#               #   try(DBL_INTEGRATE_INTRADAY (data = data.table(), 
#               #                               Fr_Folder = 'S:/STKVN/INDEX_2024/', Fr_File = 'DOWNLOAD_ENT_STKVN_VN30_INTRADAY_HISTORY.RDS',
#               #                               To_Folder = 'S:/STKVN/INDEX_2024/', To_File = paste0('DOWNLOAD_ENT_', 'STKVN', '_INTRADAY_HISTORY.RDS')))
#               #   
#               #   
#               #   try(DBL_INTEGRATE_INTRADAY (data = data.table(), 
#               #                               Fr_Folder = 'S:/STKVN/INDEX_2024/', Fr_File = paste0('DOWNLOAD_ENT_', 'STKVN', '_INTRADAY_HISTORY.RDS'),
#               #                               To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_source_ins_intraday_history.rds'))
#               # }
#               # 
#               # if (runif(1,1,100)>50)
#               # {
#               #   for (pType in list('CMD', 'CUR', 'STK', 'IND'))
#               #   {
#               #     try(DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
#               #                            Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = paste0('dbl_yah_', pType, '_day_day.rds'),
#               #                            To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_source_ins_day_day.rds') )
#               #   }
#               #   DATA = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_source_ins_day_day.rds')))
#               #   #My.Kable.Index(DATA [code == 'INDFSSTI'][order(varpc)])
#               #   
#               #   try(DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
#               #                          Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_source_ins_day_day.rds',
#               #                          To_Folder = 'S:/STKVN/INDEX_2024/', To_File = 'dbl_source_fgd_day_history.rds'))
#               #   
#                 
#                 
#                 
#                 try(DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
#                                        Fr_Folder = 'S:/STKVN/INDEX_2024/', Fr_File = paste0('DOWNLOAD_ENT_', 'INDVN', '_DAY_HISTORY.RDS'),
#                                        To_Folder = 'S:/STKVN/INDEX_2024/', To_File = 'dbl_source_fgd_day_history.rds'))
#                 
#                 
#                 try(DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
#                                        Fr_Folder = 'S:/STKVN/INDEX_2024/', Fr_File = paste0('DOWNLOAD_ENT_STKVN_VN30_DAY_HISTORY.RDS'),
#                                        To_Folder = 'S:/STKVN/INDEX_2024/', To_File = 'dbl_source_fgd_day_history.rds'))
#                 
#                 # try(DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
#                 #                        Fr_Folder = 'S:/STKVN/INDEX_2024/', Fr_File = paste0('DOWNLOAD_SOURCES_INDVN_HISTORY.rds'),
#                 #                        To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_source_ins_all_day_history.rds'))
#                 # 
#                 try(DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
#                                        Fr_Folder = UData, Fr_File = paste0('DOWNLOAD_CAF_INDVN_PRICES_HISTORY.rds'),
#                                        To_Folder = 'S:/STKVN/INDEX_2024/', To_File = 'dbl_source_fgd_day_history.rds'))
#                 
#                 try(DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
#                                        Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = paste0('MSCI_WORLD.rds'),
#                                        To_Folder = 'S:/STKVN/INDEX_2024/', To_File = 'dbl_source_fgd_day_history.rds'))
#         
#           
#                 # try(DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
#                 #                        Fr_Folder = UData, Fr_File = 'download_ifrc_insworld_history.rds',
#                 #                        To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_source_ins_all_day_history.rds') )
#                 
#                 
#                 # try(DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
#                 #                        Fr_Folder = UData, Fr_File = 'download_yah_ind_history.rds',
#                 #                        To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_source_ins_all_day_history.rds') )
#                 # 
#                 # try(DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
#                 #                        Fr_Folder = 'S:/STKVN/INDEX_2024/', Fr_File = paste0('DOWNLOAD_SOURCES_INDVN_HISTORY.rds'),
#                 #                        To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_source_ins_all_day_history.rds'))
#                 
#               }
#  
#               TO_DO = TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0('DBL_FEARGREED_LOOP>', pOption), pAction="SAVE", NbSeconds=0*60, ToPrint=T)
#           },
#           'CALCULATE' = {
#             TO_DO     = TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption= paste0('DBL_FEARGREED_LOOP>', pOption), pAction="COMPARE", NbSeconds=NbMinutes*60, ToPrint=T)
#             if (TO_DO)
#             {
#               
# 
#               TO_DO = TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0('DBL_FEARGREED_LOOP>', pOption), pAction="SAVE", NbSeconds=0*60, ToPrint=T)
#             }
#           },
#           'UPLOAD' = {
#             TO_DO     = TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption= paste0('DBL_FEARGREED_LOOP>', pOption), pAction="COMPARE", NbSeconds=NbMinutes*60, ToPrint=T)
#             if (TO_DO)
#             {
#               
#  
#               TO_DO = TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0('DBL_FEARGREED_LOOP>', pOption), pAction="SAVE", NbSeconds=0*60, ToPrint=T)
#             }
#           },
#           'REPORT' = {
#             
#             Proba = 40
#             if (runif(1,1,100) > Proba )
#             {
#               try(UPDATE_IND_FEARGREED_BY_SOURCE (pSource='CNN'))
#             }
#             if (runif(1,1,100) > Proba )
#             {
#               try(UPDATE_IND_FEARGREED_BY_SOURCE (pSource='BFN'))
#             }
#             
#             try(TRAINEE_DBL_STATS_FGD (Folder_File = 'S:/STKVN/INDEX_2024/', File_All = 'dbl_indfeargreed_history.rds',
#                                        ToUpload = T,  ToSave = T, ToUpdateCurrent = T,  Proba = 50))
#           }
#   )
#   
# }
# ==================================================================================================
DBL_DOWNLOAD_SPECIAL = function ()
{
  data = CHECK_CLASS(try(DBL_CCPR_READRDS(UData,  'DOWNLOAD_YAH_IND_HISTORY.rds', ToKable = T, ToRestore = T)))
  rvx = data.table()
  rvx = try(setDT(fread('https://cdn.cboe.com/api/global/us_indices/daily_prices/RVX_History.csv')))
  if(nrow(rvx) > 0)
  {
    setnames(rvx, new = tolower(names(rvx)))
    rvx[, date :=  as.Date(date, format = "%m/%d/%Y")]
    rvx[, code :=  "INDRVX"]
    rvx = MERGE_DATASTD_BYCODE(rvx)
    rvx = DBL_IND_CALCULATE_RT_VAR_CHANGE(rvx)
    rvx_day = unique(rvx[order(-date)], by = 'code')
    data = rbind(data, rvx_day, fill = T)
  }
  
  FTSE    = FINAL_DOWNLOAD_INV_BY_CODESOURCE (pCodesource = 'indices/ftse-asean-40')
  asean40 = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'ASEAN_40.rds', ToKable = T, ToRestore = T)))
  FTSE    = rbind (FTSE, asean40, fill = T)
  FTSE    = unique(FTSE[order(-date)], by = 'date' )
  FTSE [, code := "INDFTSEASEAN40"]
  #ins_ref[code == 'INDFTSEASEAN40']
  #FTSE = MERGE_DATASRC_CODE_BYCODESOURCE('INV', FTSE)
  FTSE = MERGE_DATASTD_BYCODE(FTSE, ToKable = F)
  (try(CCPR_SAVERDS (FTSE, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'ASEAN_40.rds', ToSummary = T, SaveOneDrive = T)))
  
  data = rbind(data, FTSE, fill = T)
  data = unique(data, by = c("code", "date"))
  
  # DOWNLOAD INTRADAY RVX FROM CBOE
  pURL = paste0('https://www.cboe.com/indices/data/?symbol=RVX&timeline=intraday')
  x = TRAINEE_CCPR_WEB_SAVE_AS_NOBROWSER(pURL=pURL, File_Temp = paste0('c:/temp/','trainee_rvx_index.txt') , ToDeleteTemp = F)
  if (grepl('<pre>', x[1])) { x = str.extract(x[1], '<pre>', '</pre>') }
  data = jsonlite::fromJSON(x)
  xData      = as.data.table(data)
  My.Kable(xData)
  xData = xData %>% 
    separate(data.V1, into = c("date", "timestamp"), sep = "T")
  xData = as.data.table(xData)
  xData [, date := as.Date(date)]
  xData [, timestamp := as.POSIXct(paste0(date, ' ', timestamp))]
  xData [, timestamp := as.POSIXct(timestamp) - 4*3600 ]
  xData [, timestamp_vn := as.POSIXct(timestamp) + (11*3600)]
  xData [order(-timestamp)]
  min(xData$data.V4)
  setnames(xData, old = c('data.V2','data.V3', 'data.V4', 'data.V5'), new = c('open', 'high', 'low', 'close'))
  xData[, code := 'INDRVX']
  xData[, source := 'CBOE']
  xData[, datetime := timestamp] 
  xData = xData[order(timestamp_vn)]
  xData = MERGE_DATASTD_BYCODE(xData)
  xData = UPDATE_UPDATED(xData)
  xData = CLEAN_TIMESTAMP(xData)
  data_old = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_source_ins_intraday_day.rds')))
  integrate_col = intersect(names(data_old), names(xData))
  data_new = rbind(data_old, xData[,..integrate_col], fill = T)
  data_new = unique(data_new, by = c('code', 'timestamp'))
  try(CCPR_SAVERDS(data_new, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_source_ins_intraday_day.rds'))
}


# ==================================================================================================
DBL_UPDATE_IMPLIED_VOLATILITY = function (Folder_File = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Frequency = 'DAY',
                                          List_codes = list('INDVXD', 'INDVXN', 'INDVIX', 'INDGVZ', 'INDOVX', 'INDRVX'),
                                          ToSave = T, ToUpload = T) {
  # ------------------------------------------------------------------------------------------------
  ALL = data.table()
  switch (Frequency,
          'DAY' = {
            File_name = 'dbl_source_ins_day_history.rds'
            DATA = CHECK_CLASS(try(DBL_CCPR_READRDS(Folder_File, File_name, ToKable = T)))
            #DATA = DBL_IND_TO_CORRECT(DATA)
            DATA = MERGE_DATASTD_BYCODE(DATA)
            IND_VOLATILITY = DATA[code %in% List_codes]
            IND_VOLATILITY = UPDATE_UPDATED(IND_VOLATILITY)
            unique(IND_VOLATILITY[order(code, -date)], by = 'code')[,. (code, date)]
            
            My.Kable(unique(IND_VOLATILITY, by = 'code'))
            My.Kable.Min(IND_VOLATILITY[order(code, -date)])
            My.Kable.Min(unique(IND_VOLATILITY[order(code, -date)], by='code'))
            IND_VOLATILITY = DBL_IND_CALCULATE_RT_VAR_CHANGE(IND_VOLATILITY)
            IND_VOLATILITY = MERGE_DATASTD_BYCODE(IND_VOLATILITY)
            unique(IND_VOLATILITY[order(code, -date)], by = 'code')[,. (code, date, name)]
            
            
            LIST_DUO = list( c("INDVIX", "INDSPX"), c("INDVXN", "INDNDX"), c("INDOVX", "FUTCMDCLC1"), c("INDGVZ", "CMDGOLD"), c("INDVXD", "INDDJI"), c("INDRVX", "INDRTY")  )
            VOL_UND_ALL = list()
            for ( k in 1:length(LIST_DUO))
            {
              # k = 1
              VOL = DATA[code == LIST_DUO[[k]][1]]
              My.Kable(VOL)
              UND = DATA[code == LIST_DUO[[k]][2]]
              My.Kable(UND)
              VOL_UND = merge(VOL[,. (code, name, date, close)], UND[,.(code_und = code, date, name_und = name, close_und = close)], all.x = T, by = c("date") )
              VOL_UND_ALL[[k]] = VOL_UND
              
            }
            
            ALL = rbindlist(VOL_UND_ALL, fill = T)
            ALL = ALL[!is.na(close_und) & !is.na(code_und) & !is.na(name_und) & close != 0]
            ALL = ALL[order(code, -date)]
            unique(ALL[order(-date)], by = c("code"))
            ALL = MERGE_DATASTD_BYCODE(ALL)
            unique(ALL[order(code, -date)], by = 'code')[,. (code, date, name)]
            
            
            if(ToSave)
            {
              try(CCPR_SAVERDS(ALL, 'S:/STKVN/INDEX_2024/', 'dbl_ind_volatility_day_chart.rds', ToSummary = T, SaveOneDrive = T))
              
              try(CCPR_SAVERDS(IND_VOLATILITY, 'S:/STKVN/INDEX_2024/', 'dbl_ind_volatility_history.rds', ToSummary = T, SaveOneDrive = T))
              
              IND_VOLATILITY_PERFORMANCE =  try(TRAINEE_CALCULATE_FILE_PERFORMANCE  (pData = data.table(), pFolder = 'S:/STKVN/INDEX_2024/', pFile   = 'dbl_ind_volatility_history.rds', EndDate  = SYSDATETIME(1),
                                                                                     ToAddRef = F, Remove_MAXABSRT = F) )
              try(CCPR_SAVERDS(IND_VOLATILITY_PERFORMANCE, 'S:/STKVN/INDEX_2024/', 'dbl_ind_volatility_performance.rds', ToSummary = T, SaveOneDrive = T))
            }
            
            if(ToUpload)
            {
              
              try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_ind_volatility_history',
                                               l.filepath= paste0('S:/STKVN/INDEX_2024/','dbl_ind_volatility_history.rds'),
                                               CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
              
              try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live_dev", l.tablename= 'dbl_ind_volatility_history',
                                               l.filepath= paste0('S:/STKVN/INDEX_2024/','dbl_ind_volatility_history.rds'),
                                               CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
              
              try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_ind_volatility_performance',
                                               l.filepath= paste0('S:/STKVN/INDEX_2024/','dbl_ind_volatility_performance.rds'),
                                               CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
              
              try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live_dev", l.tablename= 'dbl_ind_volatility_performance',
                                               l.filepath= paste0('S:/STKVN/INDEX_2024/','dbl_ind_volatility_performance.rds'),
                                               CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = F))
              
              try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_ind_volatility_chart',
                                               l.filepath= paste0('S:/STKVN/INDEX_2024/','dbl_ind_volatility_day_chart.rds'),
                                               CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
              
              try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live_dev", l.tablename= 'dbl_ind_volatility_chart',
                                               l.filepath= paste0('S:/STKVN/INDEX_2024/','dbl_ind_volatility_day_chart.rds'),
                                               CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
            }
          },
          'INTRADAY' = {
            File_name = 'dbl_source_ins_intraday_day.rds'
            DATA = CHECK_CLASS(try(DBL_CCPR_READRDS(Folder_File, File_name, ToKable = T, ToRestore = T)))
            DATA = MERGE_DATASTD_BYCODE(DATA)
            
            LIST_DUO = list( c("INDVIX", "INDSPX"), c("INDVXN", "INDNDX"), c("INDOVX", "FUTCMDCLC1"), c("INDGVZ", "CMDGOLD"), c("INDVXD", "INDDJI"), c("INDRVX", "INDRTY")  )
            VOL_UND_ALL = list()
            for ( k in 1:length(LIST_DUO))
            {
              # k = 1
              VOL = DATA[code == LIST_DUO[[k]][1]]
              My.Kable(VOL)
              UND = DATA[code == LIST_DUO[[k]][2]]
              My.Kable(UND)
              VOL_UND = merge(VOL[,. (code, name, datetime, timestamp_vn, timestamp, close)], UND[,.(code_und = code, datetime, timestamp_vn, timestamp, name_und = name, close_und = close)], all.x = T, by = c("timestamp", "datetime", "timestamp_vn") )
              VOL_UND_ALL[[k]] = VOL_UND
              
            }
            
            ALL = rbindlist(VOL_UND_ALL, fill = T)
            ALL = ALL[!is.na(close_und) & !is.na(code_und) & !is.na(name_und) & close != 0]
            ALL = ALL[order(code, -datetime)]
            ALL = MERGE_DATASTD_BYCODE(ALL)
            
            
            if (ToSave)
            {
              try(CCPR_SAVERDS(ALL, 'S:/STKVN/INDEX_2024/', 'dbl_ind_volatility_intraday_chart.rds', ToSummary = T, SaveOneDrive = T))
            }
            if (ToUpload)
            {
              try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_ind_volatility_intraday_chart',
                                               l.filepath= paste0('S:/STKVN/INDEX_2024/','dbl_ind_volatility_intraday_chart.rds'),
                                               CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
              
              try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live_dev", l.tablename= 'dbl_ind_volatility_intraday_chart',
                                               l.filepath= paste0('S:/STKVN/INDEX_2024/','dbl_ind_volatility_intraday_chart.rds'),
                                               CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
            }
            
          }
  )
  return(ALL)
}

# ==================================================================================================
# DBL_DOWNLOAD_YAH_INTRADAY_BY_FILE = function(Folder_List = 'S:/CCPR/DATA/LIST/',
#                                              File_List   = 'LIST_INTRADAY_VOLATILITY.txt',
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
#   LIST_CODES = try(setDT(fread(paste0(Folder_List, File_List))))
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
DBL_DOAWLOAD_YAH_MARKETS = function(pCodesource = '^GSPC', Hour_adjust = -4)
{
  # pCodesource = '^GSPC'; Hour_adjust = -4
  index = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_OHLC_INTRADAY( pCodesource = pCodesource, pInterval = '5m', Hour_adjust = Hour_adjust)))
  index_day = unique(index[order(codesource, date, -datetime)], by='date')[order(date)][, -c('sample', 'home', 'capiusd', 'provider', 'isin', 'fcat')]
  # str(index)
  index_day = CALCULATE_CHANGE_RT_VARPC(index_day)[!is.na(change)]
  index_oneday = index_day[.N]
  
  My.Kable.TB(index)
  My.Kable.TB(index_day)
  My.Kable.All(index_oneday)
  Final_Data = list(intraday=index, day=index_day, last=index_oneday)
  return(Final_Data)
}

# ==================================================================================================
DOWNLOAD_INTRADAY_FGDVOLAT = function(Folder_List = 'LIST_INTRADAY_VOLATILITY.txt',
                                      File_List   = 'S:/CCPR/DATA/LIST/',
                                      Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                      ToSave = T, SaveMonth = F)
{
  LIST_CODES = setDT(fread('S:/CCPR/DATA/LIST/LIST_INTRADAY_VOLATILITY.txt'))
  LIST_CODES = LIST_CODES[SOURCE=='YAH' & !is.na(HOUR_ADJUST) & nchar(HOUR_ADJUST)>0]
  My.Kable.All(LIST_CODES)
  
  xLIST_INTRADAY = list()
  xLIST_DAY      = list()
  xLIST_LAST     = list()
  
  for (k in 1:nrow(LIST_CODES))
  {
    # k = 1
    pCode   = LIST_CODES[k]$SYMBOL 
    pOffset = as.numeric(LIST_CODES[k]$HOUR_ADJUST)
    x = try(DBL_DOAWLOAD_YAH_MARKETS(pCodesource = pCode, Hour_adjust = pOffset))
    sort(names(x[[1]]))
    if (all(class(x)!='try-error'))
    {
      xLIST_INTRADAY[[k]] = x$intraday
      xLIST_DAY[[k]]      = x$day
      xLIST_LAST[[k]]     = x$last
    }
  }
  
  xALL_INTRADAY       = rbindlist(xLIST_INTRADAY, fill=T)
  xALL_DAY            = rbindlist(xLIST_DAY, fill=T)
  xALL_LAST           = rbindlist(xLIST_LAST, fill=T)
  
  My.Kable.All(xALL_LAST[order(date, timestamp_vn)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent')])
  My.Kable.All(xALL_INTRADAY[order(date, timestamp_vn)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent')])
  
  if(ToSave)
  {
    try(CCPR_SAVERDS(xALL_INTRADAY, Save_Folder, 'DBL_YAH_SAMPLE_INTRADAY_5DAY.rds', ToSummary = T, SaveOneDrive = T))
    try(CCPR_SAVERDS(xALL_DAY, Save_Folder, 'DBL_YAH_SAMPLE_DAY_HISTORY.rds', ToSummary = T, SaveOneDrive = T))
    try(CCPR_SAVERDS(xALL_DAY, Save_Folder, 'DBL_YAH_SAMPLE_MARKET_LAST.rds', ToSummary = T, SaveOneDrive = T))
  }
  if(ToHistory)
  {
    intraday_data = CHECK_CLASS(try(DBL_CCPR_READRDS(Save_Folder, 'dbl_download_fgdvolat_intraday_history.rds')))
    if(nrow(history_data) > 0 )
    {
      intraday = rbind(intraday, xALL_INTRADAY, fill = T)
      intraday = unique(intraday, by = 'datetime')
    }
    if(SaveMonth)
    {
      history_data = CHECK_CLASS(try(DBL_CCPR_READRDS(Save_Folder, 'dbl_download_fgdvolat_month_history.rds')))
      if(nrow(history_data) > 0 )
      {
        history_data = rbind(history_data, xALL_INTRADAY, fill = T)
        history_data = unique(history_data, by = )
      }
    }
  }
  Final_list = data.table()
  Final_list = list(intraday=xALL_INTRADAY, day=xALL_DAY, last=xALL_LAST)
  return(Final_list)
}

# # ==================================================================================================
# DOWNLOAD_VOLATILITY_YAH = function ()
# {
#   LIST_TODO = CCPR_READRDS('F:/NHATRANG/', 'LIST_CODES_YAH.rds')
#   XLIST = list()
#   for (k in 1:nrow(LIST_TODO))
#   {
#     # k = 1
#     pCode = LIST_TODO[k]$codesource
#     CATln_Border(paste(k, pCode))
#     x = CHECK_CLASS(try(DOWNLOAD_YAH_INS_PRICES_UNADJ (p_nbdays_back = 10, tickers = pCode, freq.data = 'daily', p_saveto="", NbTry=10)))
#     if (nrow(x)>0)
#     {
#       XLIST[[k]] = x
#     }
#   }
#   xALL = data.table()
#   xALL = rbindlist(XLIST, fill=T)
#   unique(xALL, by = 'code')
#   
#   data = CHECK_CLASS(try(CCPR_READRDS(UData,  'DOWNLOAD_YAH_IND_HISTORY.rds', ToKable = T, ToRestore = T)))
#   
#   rvx = data.table()
#   rvx = try(setDT(fread('https://cdn.cboe.com/api/global/us_indices/daily_prices/RVX_History.csv')))
#   if(nrow(rvx) > 0)
#   {
#     setnames(rvx, new = tolower(names(rvx)))
#     rvx[, date :=  as.Date(date, format = "%m/%d/%Y")]
#     rvx[, code :=  "INDRVX"]
#     rvx = MERGE_DATASTD_BYCODE(rvx)
#     rvx = DBL_IND_CALCULATE_RT_VAR_CHANGE(rvx)
#     rvx_day = unique(rvx[order(-date)], by = 'code')
#     data = rbind(data, rvx_day, fill = T)
#   }
#   
#   data = rbind(data, xALL , fill = T)
#   data = unique(data, by = c("code", "date"))
#   try(CCPR_SAVERDS(data, UData,  'DOWNLOAD_YAH_IND_HISTORY.rds', ToSummary = T, SaveOneDrive = T))
#   try(CCPR_SAVERDS(data, 'F:/NHATRANG/', 'DOWNLOAD_YAH_INDHOME_PRICES_DAY.rds', ToSummary = T, SaveOneDrive = T))
# }



# ==================================================================================================
DOWNLOAD_INTRADAY = function()
{
  vix  = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_OHLC_INTRADAY( pCodesource = '^VIX', pInterval = '5m', Hour_adjust = -5)))
  sp5  = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_OHLC_INTRADAY( pCodesource = '^GSPC', pInterval = '5m', Hour_adjust = -4)))
  
  dt = rbind(vix, sp5, fill = T)
  
  dt[code == 'INDVIX', timestamp_cdt := as.POSIXct(datetime) + 0*3600]
  dt[code == 'INDSPX', timestamp_cdt := as.POSIXct(datetime) + (-1*3600)]
  y = setDT(spread(dt[,.(timestamp_cdt, date, code, close)], key = 'code', value = 'close'))
  
  DATA = y [!is.na(INDVIX) & !is.na(INDSPX)]
  
  DATA[, code := 'INDINTRADAYSPXVIX']
  DATA[, timestampvn := as.POSIXct(timestamp_cdt) + 12*3600]
  DATA[, HHMM := format(as.POSIXct(timestampvn), "%H:%M")]
  DATA = UPDATE_UPDATED(DATA)
  setnames(DATA, old = c('INDSPX', 'INDVIX'), new = c('SPX', 'VIX'))
  DATA[!is.na(SPX), SPX_rtpc := ((SPX / shift(SPX)) - 1) * 100]
  DATA[!is.na(VIX), VIX_rtpc := ((VIX / shift(VIX)) - 1) * 100]
  
  (try(CCPR_SAVERDS (DATA, 'S:/STKVN/INDEX_2024/', 'dbl_vix_chart_intraday.rds', ToSummary = T, SaveOneDrive = T)))
  
  
  try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_vix_chart_intraday',
                                   l.filepath= paste0('S:/STKVN/INDEX_2024/','dbl_vix_chart_intraday.rds'),
                                   CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
  
  
}

# ==================================================================================================
DOWNLOAD_MSCI_FTSE = function () {
  
  # DBL_INS_REF_ADD_MODIFY_NEW (pOption="MODIFY")
  # DBL_INS_REF_ADD_MODIFY_NEW (pOption="ADD")
  try(INSREF_ADD_MSCI_FRONTIER_PRICE_USD_NEW())
  #try(INSREF_ADD_MSCI_WORLD_PRICE_USD())
  
  data_all = CHECK_CLASS(try(DBL_CCPR_READRDS(UData,  'DOWNLOAD_YAH_IND_HISTORY.rds', ToKable = T, ToRestore = T)))
  
  MSCI_YAH = DOWNLOAD_YAH_INS_PRICES_UNADJ (p_nbdays_back = 100, tickers = '^990100-USD-STRD', freq.data = 'daily', p_saveto="", NbTry=10)
  MSCI_YAH = MERGE_DATASTD_BYCODE (MSCI_YAH)
  MSCI_OLD = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'MSCI_WORLD.rds', ToKable = T)))
  MSCI_OLD    = rbind (MSCI_OLD, MSCI_YAH, fill = T)
  MSCI_NEW    = unique(MSCI_OLD[order(-date)], by = 'date' )
  #MSCI_NEW [, code:= 'INDMSCIWORLD']
  #MSCI_NEW = MERGE_DATASTD_BYCODE (MSCI_NEW)
  #ins_ref[yah == '^990100-USD-STRD']
  (try(CCPR_SAVERDS (MSCI_NEW, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'MSCI_WORLD.rds', ToSummary = T, SaveOneDrive = T)))
  
  FTSE    = FINAL_DOWNLOAD_INV_BY_CODESOURCE (pCodesource = 'indices/ftse-asean-40')
  asean40 = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'ASEAN_40.rds', ToKable = T, ToRestore = T)))
  FTSE    = rbind (FTSE, asean40, fill = T)
  FTSE    = unique(FTSE[order(-date)], by = 'date' )
  FTSE [, code := "INDFTSEASEAN40"]
  #ins_ref[code == 'INDFTSEASEAN40']
  #FTSE = MERGE_DATASRC_CODE_BYCODESOURCE('INV', FTSE)
  FTSE = MERGE_DATASTD_BYCODE(FTSE, ToKable = F)
  (try(CCPR_SAVERDS (FTSE, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'ASEAN_40.rds', ToSummary = T, SaveOneDrive = T)))
  
  data_all = rbind(data_all, MSCI_NEW, FTSE, fill = T) 
  data_all = unique(data_all[order(-date)], by = c('code', 'date') )
  (try(CCPR_SAVERDS(data_all, UData,  'DOWNLOAD_YAH_IND_HISTORY.rds', ToSummary = T, SaveOneDrive = T)))
  
  #FTSE_MSCI = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DASHBOARD_LIVE > FTSE_MSCI', pAction = "SAVE", NbSeconds = 1, ToPrint = F))
}

# ==================================================================================================
FINAL_DOWNLOAD_INV_BY_CODESOURCE = function(pCodesource = 'indices/ftse-asean-40'){
  # ------------------------------------------------------------------------------------------------
  #https://www.investing.com/indices/ftse-asean-40-historical-data
  #pCodesource = 'indices/ftse-asean-40'
  Final_Data  = data.table()
  pURL        = paste0("http://www.investing.com/", pCodesource, "-historical-data")
  CATln(''); CATln_Border(pURL)
  
  content    = try(rvest::read_html(pURL))
  if (all(class(content)!='try-error'))
  {
    tables     = content %>% html_table(fill = TRUE)
    xData      = TRAINEE_SELECT_TABLE_WITH_FIELD(tables=tables, pField='price', pContent = '', like = F)
    if (nrow(xData)>0)
    {
      # xData      = as.data.table(tables[[2]])
      # xData      = CLEAN_COLNAMES(xData)
      # My.Kable.TB(xData)
      Final_Data = xData[, .(source='INV', codesource = pCodesource, date=as.Date(date, "%B %d, %Y"), 
                             open  = as.numeric(gsub(',','',open)), 
                             high  = as.numeric(gsub(',','',high)), 
                             low   = as.numeric(gsub(',','',low)), 
                             close  = as.numeric(gsub(',','',price)), 
                             varpc=as.numeric(gsub('%','',changepercent_)))]
      Final_Data = MERGE_DATASRC_CODE_BYCODESOURCE('INV', Final_Data)
      Final_Data = MERGE_DATASTD_BYCODE(Final_Data, ToKable = F)
      Final_Data = Final_Data[order(date)]
      Final_Data[, rt:=(close/shift(close))-1]
      Final_Data$capiusd = NULL
      My.Kable.TB(Final_Data)
    }
  }
  return(Final_Data)
}
# ==================================================================================================

DBL_INS_REF_ADD_MODIFY = function(pOption="MODIFY") {
  # ------------------------------------------------------------------------------------------------
  # DBL_INS_REF_ADD_MODIFY(pOption="ADD")
  switch(pOption,
         "MODIFY" = {
           # MODIFY
           ins_ref  = CHECK_CLASS(try(DBL_CCPR_READRDS(UData, "efrc_ins_ref.rds")))
           ins_ref_short = ins_ref[, .(w=world, type, scat, sub_menu, code, fcat, eik, blg, inv, qdl, 
                                       enx, yah, inv, short_name, iso3, country, continent, provider, enddate, last)]
           
           ins_ref_mod = setDT(openxlsx::read.xlsx(paste0(UData, "efrc_ins_ref_add.xlsx"), sheet=2))
           colnames(ins_ref_mod) = tolower(colnames(ins_ref_mod))
           My.Kable(ins_ref_mod[, 6:15], HeadOnly = T, Nb=100)
           ins_ref_mod = ins_ref_mod[selection==1]
           ins_ref_mod$selection=NULL
           # ins_ref_todo = ins_ref_mod
           My.Kable(ins_ref_mod[, 1:15])
           
           for (irow in 1:nrow(ins_ref_mod))
           {
             # irow = 1
             one_row     = ins_ref_mod[irow, ]
             DATACENTER_LINE_BORDER(paste("INSREF_MODIFY :", irow, "/", nrow(ins_ref_mod), "-", one_row[1]$code))
             
             one_row
             oth_field   = setdiff(colnames(ins_ref_mod), "code")
             
             ins_ref_tmp = copy(ins_ref)
             if (nrow(ins_ref_tmp[code==one_row$code])==1) 
             {
               ins_ref[code==one_row$code, .(type, scat, menu, sub_menu, fcat, code, eik, blg, cur, 
                                             inv, qdl, enx, name, iso2, iso3, country, continent, provider, enddate, last)]
             }
             nr.row = which(ins_ref$code==one_row$code)
             nr.row
             for (i.field in 1:ncol(one_row))
             {
               # i.field = 13
               # print(colnames(one_row)[i.field])
               # if (is.na(one_row[1, ..i.field])) {print(paste(colnames(one_row)[i.field], "SKIP"))} else {print(paste(colnames(one_row)[i.field], ">"))}
               if (is.na(one_row[1, ..i.field])) {} else {
                 print(paste(colnames(one_row)[i.field], ">", one_row[1, ..i.field]))
                 set(ins_ref, nr.row, colnames(one_row)[i.field], value=one_row[1, ..i.field])
               }
             }
             ins_ref[code==one_row$code, .(world, type, scat, sub_menu, code, eik, blg, inv, qdl, enx, 
                                           name, iso2, iso3, country, continent, provider, enddate, last)]
           }
           CATln("INSREF_MODIFY : END.")
           ins_ref[is.na(short_name), short_name:=trimws(toupper(name))]
           ins_ref[, code:=toupper(trimws(code))]
           # My.Kable(ins_ref[grepl("UPCOM", name), .(type, scat, menu, sub_menu, fcat, code, eik, blg, inv, qdl, enx, name, iso2, iso3, cur, price, country, continent, provider)], 
           #          HeadOnly = T, Nb=10)
           str(ins_ref)
           ind_ref = ins_ref[type=="IND"]
           My.Kable(ins_ref[type=='STK' & grepl('BNP', code), .(code, type, fcat, blg, eik, yah, qdl, 
                                                                inv, yah, enddate, last)][order(-enddate)])
           
           # SAVE .............................................................................................
           DATACENTER_SAVEDTRDS(ins_ref, paste0(UData, "efrc_ins_ref.rds"))
           DATACENTER_SAVEDTRDS(ins_ref[type=="IND"], paste0(UData, "efrc_ind_ref.rds"))
           # SAVE .............................................................................................
           
           MAINTENANCE_DAILY_INSREF()
           rm(ins_ref); LOAD_INSREF_INDREF(l_option="DAY", ToReload=T)
           
           My.Kable(ins_ref[type=='STK' & grepl('APPLE', name), 
                            .(code, type, fcat, blg, eik, yah, qdl, inv, yah, enddate, last)][order(-enddate)])
           My.Kable(ins_ref[type=='STK' & grepl('SAMSUNG', name), 
                            .(name, code, type, fcat, blg, eik, yah, qdl, inv, yah, enddate, last)][order(-enddate)])
           My.Kable(ins_ref[type=='STK' & grepl('ACCOR', name), 
                            .(name, code, type, fcat, blg, eik, yah, qdl, inv, yah, enddate, last)][order(-enddate)])
           My.Kable(ins_ref[type=='STK' & grepl('SONY', name), 
                            .(name, code, type, fcat, blg, eik, yah, qdl, inv, yah, enddate, last)][order(-enddate)])
           My.Kable(ins_ref[type=='STK' & grepl('VINHOME', name), 
                            .(name, code, type, fcat, blg, eik, yah, qdl, inv, enddate, last)][order(-enddate)])
         },
         "ADD" = {
           # ADD INS REF
           
           ins_ref     = CHECK_CLASS(try(DBL_CCPR_READRDS(UData, "efrc_ins_ref.rds")))
           ins_ref     = ins_ref[!is.na(code)]
           country_ref = CHECK_CLASS(try(DBL_CCPR_READRDS(UData, "efrc_country_ref.rds")))
           # ind_ref     = ins_ref[type=="IND"]
           
           # ins_ref     = ins_ref[!(code %in% list("BNDJGBUS40Y",  "BNDUSTRLT", "BNDUSHQMC"))]
           ins_ref_add = setDT(openxlsx::read.xlsx(paste0(UData, "efrc_ins_ref_add.xlsx"), sheet=1))
           colnames(ins_ref_add) = tolower(colnames(ins_ref_add))
           ins_ref_add = ins_ref_add[selection==1]
           str(ins_ref_add)
           
           My.Kable(ins_ref_add[, 1:18], HeadOnly = T, Nb=20)
           
           ins_ref_add$selection=NULL
           
           ins_ref_add = transform(ins_ref_add, country = country_ref$name[match(toupper(iso2), toupper(country_ref$iso2))])
           ins_ref_add[, name:=toupper(name)]
           ins_ref_add[, qdl:=toupper(qdl)]
           ins_ref_add = ins_ref_add[!(code %in% ins_ref$code)]  # New codes only
           
           ncol1 = ncol(ins_ref); ncol1
           nrow1 = nrow(ins_ref_add); nrow1
           
           My.Kable(ins_ref_add[, 1:15])
           
           if (nrow1>0)
           {
             ins_ref = rbind(ins_ref, ins_ref_add, fill=T)
             ins_ref = ins_ref[!is.na(code)]
             ncol2   = ncol(ins_ref)
             if (ncol1==ncol2)   
             {
               print(" EQUAL COLUMNS")
               CCPR_SAVERDS(ins_ref, UData, "efrc_ins_ref.rds", ToSummary=T, SaveOneDrive = T)
               CCPR_SAVERDS(ins_ref, 'S:/R/DATA/', "efrc_ins_ref.rds", ToSummary=T, SaveOneDrive = T)
               DBL_RELOAD_INSREF(ToForce = T)
             }
           }
           # My.Kable(ins_ref[code=="INDHSXVNX50", .(code, type, scat, sub_menu, name, enddate, last)])
           # N0_IND = nrow(ind_ref)
           # ind_ref = ins_ref[type=="IND"]
           # # N0_IND = nrow(ind_ref)
           # # DATACENTER_LINE_BORDER(paste("IND REF, NEW RECORDS = ", Format.Number(N1_IND-N0_IND, 0)))
           # 
           # DATACENTER_SAVEDTRDS_AND_LAST(ind_ref, paste0(UData, "efrc_ind_ref.rds"), ToSummary=T)
           # 
           # rm(ins_ref); LOAD_INSREF_INDREF(l_option="", ToReload=T)       # RELOAD in GLOBAL VARIABLES
           # MAINTENANCE_DAILY_INSREF()                                     # CREATE INS_REF_DAY and WITH DATE
         }
  )
}
# ==================================================================================================
UPDATE_IND_FEARGREED_BY_SOURCE = function(pSource='CNN') {
  # x = UPDATE_IND_FEARGREED_BY_SOURCE(pSource='BFN')
  # My.Kable(x[order(date)], Nb=10)
  Final_Data =  data.table()
  switch(pSource,
         'BFN' = {
           #get fear/greed data - CRYPTO - BFN 
           get.url   = "https://api.alternative.me/fng/?limit=0&format=json"
           test.get  = httr::GET(get.url)
           test.char = rawToChar(test.get$content)
           test.json = jsonlite::fromJSON(test.char)
           
           df = as.data.table(test.json$data)
           df[, ":=" ( value = as.numeric(value), timestamp = as.numeric(timestamp))]
           df[, date := as.Date(as.POSIXct(as.numeric(timestamp)-as.numeric(4*60*60), origin = "1970-01-01", tz = "UTC"))]
           df[, c("value_classification", "timestamp", "time_until_update") := NULL]
           colnames(df)[colnames(df) == "value"] = "close"
           str(df)
           
           df[, `:=`(
             source = "BFN",
             code   = "INDBFNCURCRYFGD",
             name   = "BINANCE CRYPTO FEAR GREED",
             change = close - shift(close, type = "lead"),
             rt     = close / shift(close) - 1
           )]
           df[, varpc:= 100*rt]
           
           Final_Data = df[, .(code, source, name, date, close, change, rt, varpc)]
           Final_Data = Final_Data[order(-date)]
           
           Final_Data = Final_Data[change != 0]
           Final_Data = unique(Final_Data, by='date')
           Final_Data = MERGE_DATASTD_BYCODE(Final_Data, pOption='FINAL')
           Final_Data = Final_Data[order(date)]
           Final_Data = DBL_IND_CALCULATE_RT_VAR_CHANGE(Final_Data)
           My.Kable(Final_Data)
           My.Kable(Final_Data[order(-date)])
           maxdate     = max(Final_Data$date)
           pre_maxdate = max(Final_Data[date != as.Date(maxdate)]$date)
           if (Final_Data[date == as.Date(maxdate)]$close == Final_Data[date == pre_maxdate]$close)
           {
             Final_Data = Final_Data[date != maxdate]
           }
           
           Final_Data = UPDATE_UPDATED(Final_Data)
           Final_Data[, timestamp := date]
           Final_Data= CLEAN_TIMESTAMP(Final_Data)
           Final_Data = unique(Final_Data[order(-date,-timestamp)], by = 'code')
           
           
           BFN_OLD = CHECK_CLASS(try(DBL_CCPR_READRDS( 'S:/STKVN/INDEX_2024/', 'DOWNLOAD_BFN_IND_FEARGREED_DAY.rds', ToKable = T, ToRestore = T)))
           BFN_OLD = CLEAN_TIMESTAMP(BFN_OLD)
           BFN_OLD = rbind(BFN_OLD, Final_Data, fill = T)
           BFN_OLD = unique(BFN_OLD, by = c('code', 'date'))
           BFN_OLD[order(-date)]
           try(DBL_CCPR_SAVERDS(BFN_OLD, 'S:/STKVN/INDEX_2024/', 'DOWNLOAD_BFN_IND_FEARGREED_DAY.rds', SaveOneDrive = T, ToSummary = T ))
           
           DATA_OLD = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/STKVN/INDEX_2024/', 'dbl_indfeargreed_history.rds', pSleep = 8)))
           DATA_OLD = rbind(DATA_OLD, BFN_OLD, fill = T)
           DATA_OLD = unique(DATA_OLD[order(-timestamp)], by = c('code', 'date'))
           try(DBL_CCPR_SAVERDS(DATA_OLD, 'S:/STKVN/INDEX_2024/', 'dbl_indfeargreed_history.rds', ToSummary = T, SaveOneDrive = T))
           
           # try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = 'S:/STKVN/INDEX_2024/', File_Fr     = 'DOWNLOAD_BFN_IND_FEARGREED_DAY.rds', 
           #                                                           Folder_To   = 'S:/STKVN/INDEX_2024/', File_To     = 'dbl_indfeargreed_history.rds', 
           #                                                           pCondition  = '', 
           #                                                           RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>0)))
           
           # View(df)
         },
         'CNN' = {
           get.url   = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/2021-01-01"
           test.get  = httr::GET(get.url)
           test.char = rawToChar(test.get$content)
           # test.char
           test.json = jsonlite::fromJSON(test.char)
           
           df = test.json$fear_and_greed_historical$data
           df = setDT(df)
           #1723454026000
           df[, datetime := EPOCH_TO_DATE(xEpoch= x/1000 , ConvertTo='CHARACTER') ]
           df[, date := as.Date(as.POSIXct(x/1000 , origin = "1970-01-01", tz ='GMT'))]
           df[, close := y]
           df[, ":=" (source = "CNN",code = "INDCNNSPXFGD")]
           df = df[, .(code, source, date, datetime , close)]
           df = df[order(-date)]
           Final_Data = df[close!=50]
           Final_Data = unique(MERGE_DATASTD_BYCODE(Final_Data, pOption='FINAL'), by='date')
           Final_Data = Final_Data[order(date)]
           Final_Data = DBL_IND_CALCULATE_RT_VAR_CHANGE(Final_Data)
           My.Kable(Final_Data[order(date)])
           maxdate       = max(Final_Data$date)
           pre_maxdate   = max(Final_Data[date != as.Date(maxdate)]$date)
           
           close_maxdate    = Final_Data[date == as.Date(maxdate)]$close
           close_premaxdate = Final_Data[date == pre_maxdate]$close
           if (close_maxdate == close_premaxdate)
           {
             Final_Data = Final_Data[date != maxdate]
           }
           Final_Data = UPDATE_UPDATED(Final_Data)
           Final_Data[, timestamp := date]
           Final_Data= CLEAN_TIMESTAMP(Final_Data)
           
           
           DATA_UND = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_source_ins_day_day.rds')))
           #DATA_UND = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_source_ins_day_history.rds')))
           DATA_UND = DATA_UND[code == 'INDSPX']
           DATA_UND = unique(DATA_UND[order(-date)], by = 'code')
           Final_Data = merge(Final_Data[, -c('code_und', 'close_und')], DATA_UND[order(-date)][,. (code_und = code, date, close_und = close)], by = c('date'), all.x = T)
           Final_Data = CLEAN_TIMESTAMP(Final_Data)
           Final_Data = unique(Final_Data[order(-date,-timestamp)], by = 'code')
           
           
           CNN_OLD = CHECK_CLASS(try(DBL_CCPR_READRDS( 'S:/STKVN/INDEX_2024/', 'DOWNLOAD_CNN_IND_FEARGREED_DAY.rds', ToKable = T, ToRestore = T)))
           CNN_OLD = CLEAN_TIMESTAMP(CNN_OLD)
           CNN_OLD = DBL_IND_CALCULATE_RT_VAR_CHANGE(CNN_OLD)
           
           maxdate       = max(CNN_OLD$date)
           pre_maxdate   = max(CNN_OLD[date != as.Date(maxdate)]$date)
           
           close_maxdate    = CNN_OLD[date == as.Date(maxdate)]$close
           close_premaxdate = CNN_OLD[date == pre_maxdate]$close
           if (close_maxdate == close_premaxdate)
           {
             CNN_OLD = CNN_OLD[date != maxdate]
           }
           CNN_OLD = rbind(CNN_OLD, Final_Data, fill = T)
           CNN_OLD = unique(CNN_OLD, by = c('code', 'date'), fromLast = T)
           try(DBL_CCPR_SAVERDS(CNN_OLD, 'S:/STKVN/INDEX_2024/', 'DOWNLOAD_CNN_IND_FEARGREED_DAY.rds', SaveOneDrive = T, ToSummary = T))
           
           DATA_OLD = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/STKVN/INDEX_2024/', 'dbl_indfeargreed_history.rds', ToKable = T, ToRestore = T, pSleep = 15)))
           DATA_OLD = rbind(DATA_OLD, CNN_OLD, fill = T)
           DATA_OLD = unique(DATA_OLD, by = c('code', 'date'), fromLast = T)
           DATA_OLD[code_und=='INDSPX']
           try(DBL_CCPR_SAVERDS(DATA_OLD, 'S:/STKVN/INDEX_2024/', 'dbl_indfeargreed_history.rds', ToSummary = T, SaveOneDrive = T,  pSleep = 15))
           # try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = 'S:/STKVN/INDEX_2024/', File_Fr     = 'DOWNLOAD_CNN_IND_FEARGREED_DAY.rds', 
           #                                                           Folder_To   = 'S:/STKVN/INDEX_2024/', File_To     = 'dbl_indfeargreed_history.rds', 
           #                                                           pCondition  = '', 
           #                                                           RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>0)))
         })
  return(Final_Data)
}

# ==================================================================================================
UPDATE_FEARGREED_DATA = function(current_data, dbl_data) 
{
  
  # current_data= BIT_CNN_CURRENT
  # BIT_CNN_CURRENT[1]$code = 'INDBFNCURCRYFGD'
  # current_data= BIT_CNN_CURRENT
  # dbl_data = dbl_ind_feargreed_current
  # current_data = CURRENT_DATA;dbl_data = dbl_ind_feargreed_current
  # current_data = x;dbl_data = dbl_ind_feargreed_current
  max_id = max(dbl_data$id, na.rm = TRUE) 
  
  for (i in 1:nrow(current_data)) 
  {
    # i = 1
    pcode       = as.character(current_data[i]$code)
    new_date    = current_data[i]$timestamp
    #match_index = which(dbl_data$code == code)
    nrow_data =  dbl_data[code == pcode]
    if (nrow(nrow_data) > 0) 
    {
      old_date     = dbl_data[code == pcode]$timestamp
      timestamp_vn = dbl_data[code == pcode]$timestamp_vn
      xcode_und =  dbl_data[code == pcode]$code_und
      if( is.na(xcode_und))
      {
        dbl_data[code == pcode, code_und  := current_data[i]$code_und]
      }
      
      if (is.na(old_date) | (new_date > old_date & !is.na(new_date)) | is.na(timestamp_vn)) 
      {
        date = as.character(new_date)
        dbl_data[code == pcode, timestamp := date]
        dbl_data[code == pcode, timestamp  := current_data[i]$timestamp]
        dbl_data[code == pcode, timestamp_vn  := current_data[i]$timestamp_vn]
        dbl_data[code == pcode, code_und  := current_data[i]$code_und]
        dbl_data[code == pcode, date  := current_data[i]$date]
        dbl_data[code == pcode, close := current_data[i]$close]
        dbl_data[code == pcode, change  := current_data[i]$change]
        dbl_data[code == pcode, varpc   := current_data[i]$varpc]
        dbl_data[code == pcode, updated := as.character(format(Sys.time(), "%Y-%m-%d %H:%M:%S"))]
        xgroup = dbl_data[code == pcode]$group_by 
        if('group_by' %in% names(current_data))
        {
          ygroup = current_data[i]$group_by
        }else{
          
          current_data[, group_by := as.character(NA)]
          current_data[i]$group_by = as.character(NA)
          ygroup = current_data[i]$group_by
          
        }

        if (is.na(xgroup) & !is.na(ygroup)) {
          dbl_data[code == pcode, group_by := current_data[i]$group_by]
        }

        xtimevn = dbl_data[code == pcode]$timestamp_vn
        if('timestamp_vn' %in% names(current_data))
        {
          ytimevn = current_data[i]$timestamp_vn
        }else{

          current_data[, timestamp_vn := as.character(NA)]
          current_data[i]$timestamp_vn = as.character(NA)
          ytimevn = current_data[i]$timestamp_vn

        }

        if (is.na(xtimevn) & !is.na(ytimevn)) {
          dbl_data[code == pcode, timestamp_vn := current_data[i]$timestamp_vn]
        }

        
      }else{
        if (all( dbl_data[code == pcode]$close   != current_data[i]$close))
        {
          # dbl_data[code == pcode]$close
          dbl_data[code == pcode, close := current_data[i]$close]
        }
      }
    } else {
      new_row    = data.table(
        name     = as.character('IFRC/BEQ FEAR/GREED'),
        subtitle = as.character(paste0(gsub('IFRC/BEQ FEAR/GREED', '', current_data[i]$name))),
        code     = current_data[i]$code,
        code_und = current_data[i]$code_und,
        group_by = current_data[i]$group_by,
        date     = as.character(current_data[i]$date),
        timestamp= as.character(current_data[i]$timestamp),
        timestamp_vn= as.character(current_data[i]$timestamp_vn),
        datetime = as.character(current_data[i]$date),
        close    = current_data[i]$close,
        change   = current_data[i]$change,
        varpc    = current_data[i]$varpc,
        updated  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        active   = 1,
        nr       = NA
      )
      dbl_data = rbind(dbl_data, new_row, fill = T)
    }
  }
  
  return(dbl_data)
}

# ==================================================================================================
TRAINEE_DBL_STATS_FGD = function (Folder_File = 'S:/STKVN/INDEX_2024/', File_All = 'dbl_indfeargreed_history.rds',
                                  ToUpload = F,  ToSave = T, ToUpdateCurrent = T, Proba = 50)
{
  
  FGD_ALL_1 = CHECK_CLASS(try(DBL_CCPR_READRDS(Folder_File,File_All, ToKable = T, ToRestore = T, pSleep = 8)))
  
  FGD_ALL_1[!is.na(code) & is.na(code_und), code_und := gsub("INDFGD", "", code)]
  FGD_ALL_1[!is.na(name) & is.na(short_name), short_name := name]
  FGD_ALL_1[!is.na(short_name) & is.na(name), name := short_name]
  FGD_ALL_1 = FGD_ALL_1[!is.na(name)]
  FGD_ALL_1 =  DBL_IND_CALCULATE_RT_VAR_CHANGE(FGD_ALL_1)
  unique(FGD_ALL_1[order(-date)], by = 'code_und')
  BFN_CNN = FGD_ALL_1[code  %in% list('INDBFNCURCRYFGD' ,'INDCNNSPXFGD')]
  BFN_CNN = BFN_CNN [order(-date, -timestamp)]
  BFN_CNN[code_und=='INDSPX']
  BFN_CNN[code_und=='CNNSPX']
  setnames(BFN_CNN, old = c("close_und", "close", "rt", "change", "varpc"),
           new = paste0(c("close_und", "close", "rt", "change", "varpc"), "_", '1M'))
  
  My.Kable(FGD_ALL_1[order(-date)])  
  patterns  = data.table(pat = c("1M", "3M", "6M",  "1Y" ))
  prefixs   = (unique(FGD_ALL_1, by = 'code_und'))[,.(code_und, code)]
  
  FGD_ALL = FGD_ALL_1[grepl("COMBINED", name) & !grepl(paste0("NA$"), name)]
  FGD_ALL[order(-date)]
  
  
  FGD_ALL [grepl("FEAR & GREED COMBINED", name) & !is.na(name), industry := trim(gsub("FEAR & GREED COMBINED", '', name)) ]
  FGD_ALL [!grepl("FEAR & GREED COMBINED", name) & !is.na(name), industry := trim(gsub('FEAR/GREED', '', name)) ]
  
  FGD_ALL = FGD_ALL[is.na(timestamp) & !is.na(date), timestamp := date]
  My.Kable.Min(unique(FGD_ALL, by = 'industry'))
  for (k in 1:nrow(patterns) )
  {
    # k = 1
    FGD_ALL = FGD_ALL[, industry := trim(gsub(patterns[k]$pat , "", industry)) ]
  }
  My.Kable.Min(unique(FGD_ALL, by = 'industry'))
  industries = unique(FGD_ALL, by = 'industry')[!is.na(industry),. (industry)]
  
  lookup = c("INTERNATIONAL BUSINESS MACHINES" = "IBM",
             "DOW JONES INDUSTRIAL AVERAGE"    = "DJIA",
             "FUTURES OIL (WTI CRUDE) C1"      = "CRUDE OIL",
             "MSCI WORLD PR (USD)"             = "MSCI WORLD")
  
  new_name   = unique(FGD_ALL, by = 'code_und')[,.(code_und, industry)]
  new_name[, short := ifelse(industry %in% names(lookup),
                             lookup[industry],
                             industry)]
  FGD_ALL[order(-timestamp)]
  list_data     = list ()
  DATA_ALL      = data.table()
  list_data_all = list()
  data          = data.table()
  DATA_ALL_2    = data.table()
  
  for (j in 1:nrow(patterns) )
  {
    # j = 2
    pattern = patterns[j]$pat
    list_data     = list ()
    data          = data.table()
    for (k in 1:nrow(industries) )
    {
      # k = 2
      indus = industries[k]$industry
      if ( grepl("\\(", indus))
      {
        indus = sub("\\s*\\(.*$", "", indus)
        char = paste0(pattern,' ',indus)
        
      }else{
        char = paste0(pattern,' ',indus, "$")
      }
      data = FGD_ALL[grepl(char, name)]
      data = data[,. (code_und, date, timestamp, timestamp_vn, close_und,  close, rt ,change, varpc)]
      data = unique(data, by = c("date", "timestamp",  "code_und"))
      setnames(data, old = c("close_und", "close", "rt", "change", "varpc"),
               new = paste0(c("close_und", "close", "rt", "change", "varpc"), "_", pattern))
      if (nrow(data) > 0 )
      {
        list_data [[indus]] = data
      }
    }
    
    DATA_ALL = rbindlist(list_data, fill = T)
    DATA_ALL = DATA_ALL[order(-date)]
    if( nrow(DATA_ALL_2) > 0 )
    {
      DATA_ALL_2 = merge(DATA_ALL_2[order(-date, -timestamp)], DATA_ALL,  by = c("date", "code_und", "timestamp", "timestamp_vn"), all.x = T)
    }else{
      DATA_ALL_2 = DATA_ALL
    }
    DATA_ALL_2 = na.omit(DATA_ALL_2)
    # list_data_all [[pattern]] = DATA_ALL
  }
  
  DATA_ALL_2[, ind_pref := paste0('IND', code_und, 'FGD')]
  DATA_ALL_2 = merge(DATA_ALL_2, new_name, by = "code_und", all.x = T)
  DATA_ALL_2 = DATA_ALL_2[!is.na(industry)] 
  DATA_ALL_2 [, short_name := paste0('IFRC/BEQ FEAR/GREED ', short) ]
  DATA_ALL_2 [, name := paste0('IFRC/BEQ FEAR/GREED ', industry) ]
  unique(DATA_ALL_2[order(-date, -timestamp)], by = 'name')
  
  DATA_ALL_2 = DATA_ALL_2 [, code := ind_pref]
  
  setcolorder(DATA_ALL_2, c("code_und", "ind_pref", "code", "name", "industry", "date","timestamp", "timestamp_vn", "close_und_1M", "close_1M", "rt_1M", "change_1M", "varpc_1M", "close_und_3M", "close_3M","rt_3M", "change_3M","varpc_3M", "close_und_6M", "close_6M","rt_6M", "change_6M", "varpc_6M", "close_und_1Y", "close_1Y","rt_1Y", "change_1Y", "varpc_1Y"))
  DATA_ALL_2 = DATA_ALL_2[order(-date)]
  DATA_ALL_2[code_und == 'INDFTSEASEAN40']
  BFN_CNN[, code := gsub('FGD', '', code)]
  BFN_CNN[, code := gsub('IND', '', code)]
  BFN_CNN[, code := paste0('IND', code, 'FGD')]
  
  DATA_ALL_2 = rbind(DATA_ALL_2, BFN_CNN, fill=T )
  DATA_ALL_2 = UPDATE_UPDATED(DATA_ALL_2)
  
  unique(DATA_ALL_2[order(-date, -timestamp)][,.(code,name, close_1M, timestamp)], by =  'code')
  unique(BFN_CNN[order(-date, -timestamp)][,.(code,name, date, close_1M, timestamp)], by =  'code')
  
  My.Kable.Min(unique(DATA_ALL_2[order(-date)], by = 'name'))
  
  dbl_ind_feargreed_stats_day = unique(DATA_ALL_2[order(-date, -timestamp)], by = c('code'))
  dbl_ind_feargreed_stats_day = dbl_ind_feargreed_stats_day [, -c('close_und_1M', 'close_und_3M', 'close_und_6M', 'close_und_1Y')]
  My.Kable(dbl_ind_feargreed_stats_day)
  dbl_ind_feargreed_stats_day[,. (code, name, close_1M, date, timestamp)]
  
  
  dt = dbl_ind_feargreed_stats_day
  dbl_ind_feargreed_stats_day[,.(code_und, name)]
  long_dt = melt(dt, id.vars = c( "code_und", "code", "name", "industry", "date", 'timestamp',"timestamp_vn"), 
                 measure.vars = patterns("^close", "^change", "^varpc"), 
                 variable.name = "period", 
                 value.name = c("close", "change", "varpc"))
  long_dt[, period := gsub("^(close|change|varpc)_", "", period)]
  LIST_CODES = setDT(try(fread('S:/CCPR/DATA/LIST/LIST_UNDERLYING_FEARGREED.txt')))
  long_dt = merge(long_dt, LIST_CODES[,.(code_und = code, group_by)], by = 'code_und', all.x = T )
  result_dt = long_dt[period == 1][,.(name, code, code_und , date, timestamp, timestamp_vn, close, change, varpc, group_by)]
  dbl_ind_feargreed_stats_day[is.na(short_name) & !is.na(name), short_name := name]
  dbl_ind_feargreed_stats_day[!is.na(short_name) & is.na(name), name := short_name]
  dbl_ind_feargreed_stats_day = dbl_ind_feargreed_stats_day[!is.na(name)]
  active = try(setDT(TRAINEE_LOAD_SQL_HOST (pTableSQL = 'dbl_ind_feargreed_current',  pHost = 'dashboard_live', ToDisconnect=T, ToKable=T)))
  active = active [,. (code, active)]
  dbl_ind_feargreed_stats_day = merge(dbl_ind_feargreed_stats_day, active, by = 'code')
  My.Kable.Min(dbl_ind_feargreed_stats_day[order(-date)])
  CURRENT_DATA = unique(result_dt[order(-timestamp)], by = 'code')
  
  
  if (ToSave) 
  {
    try(CCPR_SAVERDS(dbl_ind_feargreed_stats_day, Folder_File,  'dbl_indfgd_stats_day.rds', ToSummary = T, SaveOneDrive = T))
    try(CCPR_SAVERDS(DATA_ALL_2, Folder_File,  'dbl_indfgd_stats_history.rds', ToSummary = T, SaveOneDrive = T))
    try(DBL_CCPR_SAVERDS(FGD_ALL_1, Folder_File,  'dbl_ind_fearandgreed_history_l2.rds', ToSummary = T, SaveOneDrive = T))
    
    
    # try(CCPR_SAVERDS(BIT_CNN_CURRENT, Folder_File,  'dbl_ind_bfn_cnn_fgd_day.rds', ToSummary = T, SaveOneDrive = T))
  }
  
  if (ToUpdateCurrent)
  {
    dbl_ind_feargreed_current = try(setDT(TRAINEE_LOAD_SQL_HOST (pTableSQL = 'dbl_ind_feargreed_current', pHost = 'dashboard_live', ToDisconnect=T, ToKable=T)))
    updated_data = UPDATE_FEARGREED_DATA(CURRENT_DATA, dbl_ind_feargreed_current)
    updated_data[, datetime := date]
    updated_data = updated_data[, -c('id', 'updated')]
    updated_data = UPDATE_UPDATED(updated_data)
    updated_data[code == 'INDBFNCURCRYFGD', timestamp := date]
    updated_data[code == 'INDCNNSPXFGD', timestamp := date]
    try(CCPR_SAVERDS(updated_data, Folder_File,  'dbl_ind_feargreed_current.rds', ToSummary = T, SaveOneDrive = T))
    
  }
  if (ToUpload)
  {
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_ind_feargreed_history',
                                     l.filepath= paste0('S:/STKVN/INDEX_2024/','dbl_ind_fearandgreed_history_l2.rds'),
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
    
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_ind_feargreed_current',
                                     l.filepath= paste0('S:/STKVN/INDEX_2024/','dbl_ind_feargreed_current.rds'),
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
    
    
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_ind_feargreed_stats_day',
                                     l.filepath= paste0('S:/STKVN/INDEX_2024/','dbl_indfgd_stats_day.rds'),
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F,  AutoUpload = T))
    
    
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_ind_feargreed_stats_history',
                                     l.filepath= paste0('S:/STKVN/INDEX_2024/','dbl_indfgd_stats_history.rds'),
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
    
    
  }
  My.Kable(DATA_ALL_2)
  return(DATA_ALL_2)
  
}
 