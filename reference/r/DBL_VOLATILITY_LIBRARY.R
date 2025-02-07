# ==================================================================================================
# DBL_VOLATILITY_LIBRARY 2024-10-02 09:03
# ==================================================================================================


DBL_DOWNLOAD_NIKKEI_AVERAGE_VOLATILITY = function (ToIntegrate=F)
{
  url = "https://indexes.nikkei.co.jp/nkave/historical/nikkei_stock_average_vi_daily_en.csv"
  
  response = GET(url)
  response$status_code
  
  
  csv_content = content(response, "raw")
  data = setDT(fread(text = rawToChar(csv_content)))
  # My.Kable(data)
  
  names(data) = c("date", "close", "open", "high", "low")
  # My.Kable(data)
  # str(data)
  
  # data[, date := gsub("/", "-", date)]
  data[, date := as.Date(gsub("/", "-", date), format = "%Y-%m-%d")]
  # My.Kable(data)
  
  # ins.ref = CHECK_CLASS(try(readRDS(paste0('V:/CCPR/DATA/','DBL_INS_REF.rds'))))
  # My.Kable.INSREF(ins.ref[grepl("NIKKEI VOLATILITY", name)])
  # My.Kable.INSREF(ins.ref[inv == "indices/nikkei-volatility"])
  
  data = data[order(date)]
  data[, pclose := shift(close)]
  data = data[, ':='(
    code = "INDVNKY",
    name = "NIKKEI VOLATILITY",
    source = "NIKKEI",
    # pclose = shift(close),
    change = close - pclose,
    rt = (close/shift(close)) - 1,
    varpc = ((close/shift(close)) - 1)*100
  )]
  
  
  data = data[, .(code, name, date, open, high, low, close, pclose, change, rt, varpc, source)]
  # My.Kable(data)
  
  if (ToIntegrate)
  {
    data[,nbdate:=seq(1,.N),by = "code"]
    data = data[nbdate<=5][order(code,date)]
    DBL_CCPR_SAVERDS(data, "S:/CCPR/DATA/DASHBOARD_LIVE/","DBL_NIKKEI_INS_DAY_DAY.rds", ToSummary = T)
    
    try(DBL_INTEGRATE_DAY (data, LIST_CODES = list(),
                           Fr_Folder = '', Fr_File = '',
                           To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'DBL_NIKKEI_INS_DAY_HISTORY.rds'))
  }
  return (data)
}

DBL_DOWNLOAD_NIKKEI_AVERAGE_VOLATILITY_INTRADAY = function (ToSave = T)
{
  checkdate_url = 'https://indexes.nikkei.co.jp/en/nkave/index/profile?idx=nk225vi'
  url_content = content(GET(checkdate_url),"text",encoding = "UTF-8")
  data_date = gsub('.*<span class="date" id="datedtime">',"",url_content)
  data_date = gsub('\\(.*',"",data_date)
  data_date = gsub('.*<!--daily_changing-->',"",data_date)
  data_date = mdy(data_date)
  
  if (Sys.Date()==data_date)
  {
    URL = "https://indexes.nikkei.co.jp/nkave/get_real_daily_chart?idx=nk225vi&_=1725616742666"
    # EPOCH_NOW()
    x = jsonlite::fromJSON(URL)
    xDATA = as.data.table(x$price)
    names(xDATA) = c("timestamp", "close")
    # EPOCH_TO_DATE(1725616742666/1000)
    # xDATA[, timestamp := substr(EPOCH_TO_DATE(1725616742666/1000),1,19)]
    # xDATA[, date := as.Date(substr(EPOCH_TO_DATE(1725616742666/1000),1,19))]
    xDATA[, date := Sys.Date()]
    xDATA[, timestamp := paste(date, timestamp)]
    xDATA[, timestamp_vn := as.character(as.POSIXct(timestamp) - 2*60*60)]
    xDATA[, ':='(
      code = "INDVNKY",
      name = "NIKKEI VOLATILITY",
      source = "NIKKEI"
    )]
    xDATA = CLEAN_TIMESTAMP(xDATA)
    xDATA = DBL_CLEAN_OHLC(xDATA)
    
    DAY = DBL_DOWNLOAD_NIKKEI_AVERAGE_VOLATILITY ()
    DAY = DAY[date!=max(xDATA$date)][order(-date)][1]
    
    xDATA = merge(xDATA[, -c('pclose')], DAY[, .(code, pclose)], all.x = T, by = "code")
    xDATA = xDATA[, ':='(
      change = close - pclose,
      rt = (close/pclose) - 1,
      varpc = ((close/pclose) - 1)*100
    )]
    My.Kable(xDATA)
    
    if (ToSave)
    {
      DBL_CCPR_SAVERDS(xDATA[.N], "S:/CCPR/DATA/DASHBOARD_LIVE/","DBL_NIKKEI_INS_LAST_TODAY.rds", ToSummary = T)
      DBL_CCPR_SAVERDS(xDATA, "S:/CCPR/DATA/DASHBOARD_LIVE/","DBL_NIKKEI_INS_INTRADAY_TODAY.rds", ToSummary = T)
      x = try(DBL_INTEGRATE_DAY (data = xDATA[.N], LIST_CODES = list(),
                                 Fr_Folder = '', Fr_File = '',
                                 To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'DBL_NIKKEI_INS_LAST_DAY.rds'))
      
      
      x = try(DBL_INTEGRATE_INTRADAY(data = xDATA, 
                                     Fr_Folder = '', Fr_File = '',
                                     To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'DBL_NIKKEI_INS_INTRADAY_DAY.rds'))
      
      x = try(DBL_INTEGRATE_INTRADAY(data = xDATA, 
                                     Fr_Folder = '', Fr_File = '',
                                     To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'DBL_NIKKEI_INS_INTRADAY_HISTORY.rds'))
    }
  }else{ xDATA = data.table() }
  return (xDATA)
}

DOWNLOAD_STOXX_DATA = function(ListCodesource = list("v2tx", "v1x"), Frequency = "intraday"){
  dt_stoxx = list()
  for (icode in 1:length(ListCodesource)) {
    dt_one = DOWNLOAD_STOXX_BY_CODE(pCode = ListCodesource[[icode]],
                                    pFrequency = ifelse(Frequency=="last", "intraday", Frequency))
    switch (ListCodesource[[icode]],
            "v2tx" = {
              dt_one[,":="(code = "INDV2TX", name = "EURO STOXX50 VOLATILITY")]
            },
            "v1x" = {
              dt_one[,":="(code = "INDV1XI", name = "DAX NEW VOLATILITY")]
            }
    )
    dt_stoxx[[icode]] = dt_one
  }
  dt_stoxx = rbindlist(dt_stoxx)
  dt_stoxx = unique(dt_stoxx,by=c("code","timestamp"))
  
  if (Frequency!="intraday") { dt_stoxx = unique(dt_stoxx[order(-timestamp)],by=c("code")) }
  
  DBL_CCPR_SAVERDS(dt_stoxx, 'S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_stoxx_ins_',Frequency,'_TODAY.rds'))
  return(dt_stoxx)
}

DOWNLOAD_STOXX_BY_CODE = function(pCode = "v2tx", pFrequency = "day"){
  pURL = paste0("https://stoxx.com/index/",pCode,"/")
  response   = GET(pURL, add_headers(
    `User-Agent`       = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"
    ,`Accept-Language` = "en-US,en;q=0.5"))
  xweb       = content(response, as="text")
  content    = try(rvest::read_html(xweb))
  ndata = try(html_node(content, xpath = '//*[@id="site-main"]/script[1]'))
  ndata= html_text(ndata)
  matches = regexpr("window\\.index_id\\s*=\\s*'\\d+'", ndata)
  match_text = regmatches(ndata, matches)
  index_id = sub(".*window\\.index_id\\s*=\\s*'(\\d+)'.*", "\\1", match_text)
  post_id = as.numeric(index_id)
  
  
  ndata = try(html_node(content, xpath = paste0('//*[@id="post-',post_id,'"]/script/text()')))
  
  ndata= html_text(ndata)
  xData=sub("window\\.chart_name.*", "", ndata, perl = TRUE)
  
  switch (pFrequency,
          "intraday" = {
            pattern = "(?s)window\\.chart_data\\s*=\\s*(\\[\\[.*?\\]\\]);"
            DAY = str_match(xData, pattern)[,2]
            DAY = str_remove(DAY, ",\\s*\\]$")  
            
            DAY = jsonlite::fromJSON(DAY)
            # Final_data = rbindlist(lapply(xData, function(x) as.list(x)), use.names = FALSE)
            FINAL_DAY = setDT(as.data.frame(DAY))
            
            setnames(FINAL_DAY, c("timestamp", "close"))
            FINAL_DAY[, timestamp := as.POSIXct(as.numeric(timestamp) / 1000, format = "1970-01-01")]
            FINAL_DAY[, timestamp:=as.character(as.Date(timestamp))]
            FINAL_DAY[, ":="(date = as.Date(timestamp), timestamp_vn = timestamp, timestamp_loc = timestamp)]
            FINAL_DAY[, ":="(codesource = pCode, pclose = shift(close))]
            FINAL_DAY[, ":="(change = close - pclose, varpc = ((close/pclose)-1)*100)]
            
            #DATA INTRADAY
            pattern = "(?s)window\\.chart_data_one_day\\s*=\\s*(\\[\\[.*?\\]\\]);"
            INTRADAY = str_match(xData, pattern)[,2]
            INTRADAY = str_remove(INTRADAY, ",\\s*\\]$")  
            
            INTRADAY = jsonlite::fromJSON(INTRADAY)
            # Final_data = rbindlist(lapply(xData, function(x) as.list(x)), use.names = FALSE)
            FINAL_INTRADAY = setDT(as.data.frame(INTRADAY))
            
            setnames(FINAL_INTRADAY, c("timestamp", "close"))
            FINAL_INTRADAY[, timestamp := as.POSIXct(as.numeric(timestamp) / 1000, format = "1970-01-01")]
            FINAL_INTRADAY[, timestamp := timestamp - 7*60*60]
            FINAL_INTRADAY[, timestamp_vn := timestamp + 5*60*60]
            FINAL_INTRADAY[, timestamp_loc := timestamp]
            FINAL_INTRADAY[, date := as.Date(timestamp_loc)]
            FINAL_INTRADAY = merge(FINAL_INTRADAY[, -c("pclose")], FINAL_DAY[, .(date, pclose)], by = "date", all.x = T)
            FINAL_INTRADAY[, ":="( codesource = pCode)]
            FINAL_INTRADAY[, ":="(change = close - pclose, varpc = ((close/pclose)-1)*100, open = as.numeric(NA),
                                  high = as.numeric(NA), low = as.numeric(NA))]
            FINAL = FINAL_INTRADAY
          }, 
          "day" = {
            #DATA DAY
            
            
            pattern = "(?s)window\\.chart_data\\s*=\\s*(\\[\\[.*?\\]\\]);"
            DAY = str_match(xData, pattern)[,2]
            DAY = str_remove(DAY, ",\\s*\\]$")  
            
            DAY = jsonlite::fromJSON(DAY)
            # Final_data = rbindlist(lapply(xData, function(x) as.list(x)), use.names = FALSE)
            FINAL_DAY = setDT(as.data.frame(DAY))
            
            setnames(FINAL_DAY, c("timestamp", "close"))
            FINAL_DAY[, timestamp := as.POSIXct(as.numeric(timestamp) / 1000, format = "1970-01-01")]
            FINAL_DAY[, timestamp:=as.character(as.Date(timestamp))]
            FINAL_DAY[, ":="(date = as.Date(timestamp), timestamp_vn = timestamp, timestamp_loc = timestamp)]
            FINAL_DAY[, ":="(codesource = pCode, pclose = shift(close))]
            FINAL_DAY[, ":="(change = close - pclose, varpc = ((close/pclose)-1)*100, open = as.numeric(NA),
                             high = as.numeric(NA), low = as.numeric(NA))]
            FINAL = FINAL_DAY
          }
  )
  
  My.Kable(FINAL)
  return(FINAL)
  
}

DBL_DOWNLOAD_YAH_TOP_INTRADAY_BY_FILE = function(Folder_List = ODDrive,
                                                 File_List   = "DBL/RD_BATCH_MANAGEMENT.xlsx",
                                                 pType       = '', pCoverage = '',
                                                 Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                 Save_Prefix = 'DBL_YAH_IND<PREFIX>_<FREQUENCY>_<HISTORY>.rds',
                                                 ToSave      = T, ToHistory = T, Nb_Min = 500, pInterval = "1m") {
  
  # Folder_List = 'LIST_INTRADAY_VOLATILITY.txt'; File_List   = 'S:/CCPR/DATA/LIST/'; Nb_Min = 5
  # pType       = 'stock'; pCoverage = 'INTERNATIONAL';Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/';Save_Prefix = 'DBL_YAH_IND<PREFIX>_<FREQUENCY>_<HISTORY>.rds';ToSave = T
  
  xALL_INTRADAY = data.table()
  xALL_DAY      = data.table()
  xALL_LAST     = data.table()
  LIST_CODES = try(setDT(read_xlsx(paste0(Folder_List,File_List), sheet = 'DUO')))
  
  if (all(class(LIST_CODES)!='try-error'))
  {
    LIST_CODES = LIST_CODES[PRIORITY == 'top' & ACTIVE == 1]
    My.Kable.All(LIST_CODES)
    
    xLIST_INTRADAY = list()
    xLIST_DAY      = list()
    xLIST_LAST     = list()
    
    NB_TODO        = min(Nb_Min, nrow(LIST_CODES))
    for (k in 1:NB_TODO)
    {
      # k = 1
      pCode   = LIST_CODES[k]$YAH_PRODUCT 
      CATln('')
      CATln_Border(paste(k, '/', NB_TODO, '>>>', pCode))
      x = try(DBL_DOWNLOAD_YAH_MARKETS(pCodesource = pCode, Hour_adjust = '', pInterval = "5m" ))
      # sort(names(x[[1]]))
      # str(x[[1]])
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
    
    My.Kable.TB(xALL_INTRADAY[order(date, timestamp_vn)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent')][order(date, timestamp, code)])
    My.Kable.TB(xALL_DAY[order(date, code)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent')][order(date, timestamp, code)])
    My.Kable.All(xALL_LAST[order(date, timestamp_vn)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent')])
    
    if(ToSave)
    {
      FileName = gsub('[<]HISTORY>', 'TODAY', gsub('[<]FREQUENCY>', 'INTRADAY', gsub('[<]PREFIX>', '', Save_Prefix)))
      try(CCPR_SAVERDS(xALL_INTRADAY, Save_Folder, FileName, ToSummary = T, SaveOneDrive = F))
      
      FileName = gsub('[<]HISTORY>', 'TODAY', gsub('[<]FREQUENCY>', 'DAY', gsub('[<]PREFIX>', '', Save_Prefix)))
      try(CCPR_SAVERDS(xALL_DAY, Save_Folder, FileName, ToSummary = T, SaveOneDrive = T))
      
      FileName = gsub('[<]HISTORY>', 'TODAY', gsub('[<]FREQUENCY>', 'LAST', gsub('[<]PREFIX>', '', Save_Prefix)))
      try(CCPR_SAVERDS(xALL_LAST, Save_Folder, FileName, ToSummary = T, SaveOneDrive = T))
    }
    
    if(ToHistory)
    {
      ToHistory = T
      FileName = gsub('[<]HISTORY>', 'DAY', gsub('[<]FREQUENCY>', 'INTRADAY', gsub('[<]PREFIX>', '', Save_Prefix)))
      CATln_Border(FileName)
      Data     = CHECK_CLASS(try(CCPR_READRDS(Save_Folder, FileName, ToKable = T, ToRestore = T)))
      
      Data     = rbind(Data, xALL_INTRADAY, fill = T)
      if(nrow(Data) > 0 )
      {
        Data = unique(Data, by = c('code', 'timestamp'), FromLast = T)
        Data$capiusd = NULL
        My.Kable.TB(Data[order(date, timestamp_vn)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent', 'isin')][order(date, timestamp, code)])
        try(CCPR_SAVERDS(Data, Save_Folder, FileName, ToSummary = T, SaveOneDrive = F))
      }
      
      FileName = gsub('[<]HISTORY>', 'DAY', gsub('[<]FREQUENCY>', 'DAY', gsub('[<]PREFIX>', '', Save_Prefix)))
      CATln_Border(FileName)
      Data     = CHECK_CLASS(try(CCPR_READRDS(Save_Folder, FileName, ToKable = T, ToRestore = T)))
      
      Data     = rbind(Data, xALL_DAY, fill = T)
      if (nrow(Data) > 0 )
      {
        Data = unique(Data, by = c('code', 'date'), FromLast = T)
        Data$capiusd = NULL
        My.Kable.TB(Data[order(date, code)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent', 'isin')][order(date, code)])
        try(CCPR_SAVERDS(Data, Save_Folder, FileName, ToSummary = T, SaveOneDrive = F))
      }
      
      FileName = gsub('[<]HISTORY>', 'DAY', gsub('[<]FREQUENCY>', 'LAST', gsub('[<]PREFIX>', '', Save_Prefix)))
      CATln_Border(FileName)
      Data     = CHECK_CLASS(try(CCPR_READRDS(Save_Folder, FileName, ToKable = T, ToRestore = T)))
      
      Data     = rbind(Data, xALL_LAST, fill = T)
      if (nrow(Data) > 0 )
      {
        Data = unique(Data[order(code, -date, -timestamp)], by = c('code'), FromLast = T)
        Data$capiusd = NULL
        My.Kable.TB(Data[order(date, code)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent', 'isin')][order(date, code)])
        try(CCPR_SAVERDS(Data, Save_Folder, FileName, ToSummary = T, SaveOneDrive = F))
      }
    }
  }
  return(list(xALL_INTRADAY, xALL_DAY, xALL_LAST))
}

# ==================================================================================================
DBL_CONVERT_TIMEZONE_BY_ABBREVIATIONS = function (time = '2024-09-03 12:35:55', tz = 'JST')  {
  # time = as.POSIXct(paste (time, timezone))
  LIST_TZ = setDT (fread ('S:/CCPR/DATA/LIST/LIST_TZ_OFFSETS.txt'))
  LIST_TZ [, ':=' (name = trimws(name), offset = as.numeric(offset), abbr = trimws(as.character(abbr) ))]
  LIST_TZ[abbr == 'EDT']
  LIST_TZ$abbr = as.character( LIST_TZ$abbr )
  offsets =  LIST_TZ [abbr == tz][1]$offset
  timestamp_utc = as.POSIXct(time)-(offsets)*60*60
  timestamp_loc = as.POSIXct(timestamp_utc)+(offsets)*60*60
  timestamp_vn = as.POSIXct(timestamp_utc)+(7)*60*60
  My.Kable.All (data.table (timestamp_loc = timestamp_loc,timestamp_vn = timestamp_vn ))
  return (list(timestamp_loc,timestamp_vn ))
}
# ==================================================================================================
DBL_RUN_LIST_FUNCTIONS = function(EXC_STR = "CHECK_CLASS(try(DEV_DOWNLOAD_SOURCE_STKVN_PRICESINTRADAY_BY_CODE(pSource = 'VND', pCode = <CODE>, minute = 1, NbDaysBack = 1)))",
                                  XLIST_CODES = list('VNINDEX', 'VN30', 'HNX', 'HXN30', 'UPCOM'),
                                  GroupBy = 'code x date x timestamp',
                                  NB = 100) 
{
  
  XLIST = list()
  X_ALL = data.table()
  
  DBL_RELOAD_INSREF(ToForce = FALSE)
  if (all(grepl(c('<yCODE>'), EXC_STR) & grepl(c('<TIME>'), EXC_STR) ))
  {
    pSource = 'CBOE'
    LIST_CODES = setDT(read.xlsx(paste0(ODDrive, "/DBL/RD_BATCH_MANAGEMENT.xlsx"), sheet = 'DUO'))
    
    LIST_PROD = LIST_CODES[, .(PRODUCT, DUO, PRODUCT_NAME, CODESOURCE = get(paste0(pSource, "_PRODUCT")), YAH_PRODUCT, TIME_OFFSET_PRODUCT, TOP)][!is.na(CODESOURCE)]
    LIST_DUO  = LIST_CODES[, .(PRODUCT, DUO, PRODUCT_NAME, CODESOURCE = get(paste0(pSource, "_DUO")), YAH_DUO, TIME_OFFSET_DUO, TOP)][!is.na(CODESOURCE)]
    
    LIST.CODES = unique(rbind(LIST_PROD, LIST_DUO, fill = TRUE)[!is.na(CODESOURCE)], by = 'CODESOURCE')
    LIST.CODES[, TIME_OFFSET := ifelse(!is.na(TIME_OFFSET_PRODUCT), TIME_OFFSET_PRODUCT, TIME_OFFSET_DUO)]
    LIST.CODES[, YAH := ifelse(!is.na(YAH_PRODUCT), YAH_PRODUCT, YAH_DUO)]
  }
  i = 0
  # NB = 100
  for (k in 1:min(NB, length(XLIST_CODES))) {
    # k = 1
    i = i +1
    MY_STR = EXC_STR
    pCode = XLIST_CODES[[k]]
    CATln_Border(i)
    
    if (grepl('<yCODE>', MY_STR)) {
      yCode = as.character(LIST.CODES[CODESOURCE == pCode]$YAH)
      yCode = ifelse(length(yCode) > 0, paste0("'", yCode, "'"), "''")
      MY_STR = gsub('<yCODE>', yCode, MY_STR)
    }
    
    if (grepl('<TIME>', MY_STR)) {
      offset = as.numeric(LIST.CODES[CODESOURCE == pCode]$TIME_OFFSET)
      offset = ifelse(length(offset) > 0, paste0("'", offset, "'"), "''")
      MY_STR = gsub('<TIME>', offset, MY_STR)
    }
    
    MY_STR = gsub('<CODE>', paste0("'", pCode, "'"), MY_STR)
    
    CATln_Border(MY_STR)
    TODO_STR = eval(parse(text = MY_STR))
    My.Kable.Min(TODO_STR)
    
    if (nrow(TODO_STR) > 0) {
      XLIST[[k]] = TODO_STR
    }
  }
  
  X_ALL = rbindlist(XLIST, fill = TRUE)
  
  if (nrow(X_ALL) > 0) 
  {
    # if ( all(grepl("code", names(X_ALL) ) & grepl("date", names(X_ALL) ) & grepl("timestamp", names(X_ALL) )) )
    if (all("code" %in% names(X_ALL) & "date" %in% names(X_ALL) & "timestamp" %in% names(X_ALL)))
    {
      print(GroupBy)
      xlist = unlist(strsplit(GroupBy, ' x '))
    }else{
      GroupBy = gsub('timestamp', '', GroupBy)
      print(GroupBy)
      xlist = unlist(strsplit(GroupBy, ' x '))
    }
    X_ALL = unique(X_ALL, by = xlist)
    X_ALL = UPDATE_UPDATED(X_ALL[, -c('updated')])
  }
  return(X_ALL)
}

# ==================================================================================================
DBL_CNBC_INTRADAY_DAY = function(pCode = '@VX.1', frequency='intraday') {
  # ------------------------------------------------------------------------------------------------
  start_time = Sys.time()
  # pCode = '.SPX'; frequency='last'
  # pCode = '.AEX'; frequency='last'
  
  LIST_CODES = setDT(read.xlsx(paste0(ODDrive, "/DBL/RD_BATCH_MANAGEMENT.xlsx"), sheet = 'DUO'))
  LIST_PROD = LIST_CODES[, .(PRODUCT, DUO, PRODUCT_NAME, CODESOURCE = get(paste0("CNBC_PRODUCT")), YAH_PRODUCT, TIME_OFFSET_PRODUCT, TOP)][!is.na(CODESOURCE)]
  LIST_DUO  = LIST_CODES[, .(PRODUCT, DUO, PRODUCT_NAME, CODESOURCE = get(paste0("CNBC_DUO")), YAH_DUO, TIME_OFFSET_DUO, TOP)][!is.na(CODESOURCE)]
  
  LIST.CODES = unique(rbind(LIST_PROD, LIST_DUO, fill = TRUE)[!is.na(CODESOURCE)], by = 'CODESOURCE')
  LIST.CODES[, TIME_OFFSET := ifelse(!is.na(TIME_OFFSET_PRODUCT), TIME_OFFSET_PRODUCT, TIME_OFFSET_DUO)]
  offset = LIST.CODES[CODESOURCE == pCode]$TIME_OFFSET
  
  if (is.na(offset) || (is.character(offset) && (offset == 'NA' || nchar(offset) == 0))) 
  {
    HOURS = paste0( 'https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol?symbols=', pCode)
    hours = jsonlite::fromJSON(HOURS)
    TZONE = hours$FormattedQuoteResult$FormattedQuote$timeZone
    last.time = hours$FormattedQuoteResult$FormattedQuote$last_time
    if (nchar(last.time) > 10)
    {
      offset    = as.numeric(substr(last.time,nchar("2024-09-04T01:00:00.000")+1, nchar(last.time)))/100
      
    }else{
      LIST_TZ = setDT (fread ('S:/CCPR/DATA/LIST/LIST_TZ_OFFSETS.txt'))
      LIST_TZ = LIST_TZ[abbr == TZONE]
      if(nrow(LIST_TZ) >= 1)
      {
        offset    = (as.numeric(LIST_TZ[1]$offset))
      }
    }
  }
  
  offset = as.numeric(offset)
  # intraday
  # pURL = 'https://webql-redesign.cnbcfm.com/graphql?operationName=getQuoteChartData&variables=%7B%22symbol%22%3A%22%40VX.1%22%2C%22timeRange%22%3A%221D%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%2261b6376df0a948ce77f977c69531a4a8ed6788c5ebcdd5edd29dd878ce879c8d%22%7D%7D'
  switch(frequency,
         'intraday_with_pclose'     = {
           My_intraday = DBL_CNBC_INTRADAY_DAY(pCode = pCode, frequency='intraday')
           
           My_day      = DBL_CNBC_INTRADAY_DAY(pCode = pCode, frequency='day')
           
           # My_day[, date:=as.Date(substr(tradetime ,1,8), '%Y%m%d')]
           My_day      = My_day[order(date)]
           
           # My_day[, pclose:=shift(close)]
           My.Kable(My_day[, .(source,codesource,code,date,timestamp, timestamp_vn,close, change, varpc)])
           My_dates = unique(rbind(My_day[, .(date)], My_intraday[!stri_trans_general(weekdays(date), "ASCII") %in% c("Sunday","Chu Nhat"), .(date)]), by='date')[order(date)]
           My_dates[, pdate:=as.Date(shift(date))]
           My_intraday = merge(My_intraday[, -c('pdate')], My_dates[, .(date, pdate)], all.x = T, by='date')
           My_day = merge(My_day[, -c('pdate')], My_dates[, .(date, pdate)], all.x = T, by='date')
           
           My_intraday = merge(My_intraday[, -c('pclose')], My_day[, .(pdate=date, pclose=close)], all.x = T, by='pdate')
           My_intraday = My_intraday[!is.na(pclose)]
           My_intraday[, change:=close-pclose]
           My_intraday[, varpc:=100*((close/pclose)-1)]
           xData = copy(My_intraday)
           My.Kable(xData[, .(source,codesource,code,date,timestamp, timestamp_vn,close, pclose, change, varpc)])
         },
         'last' = {
           # pCode = '.DJI'
           My_intraday = DBL_CNBC_INTRADAY_DAY(pCode = pCode, frequency='intraday_with_pclose')
           My.Kable(My_intraday[, .(source,codesource,code,date,timestamp, timestamp_vn,close,pclose,change,varpc)])
           
           # My.Kable(My_intraday[varpc != 0])
           # My_intraday = My_intraday[varpc != 0]
           My_Last     = unique(My_intraday[order(-timestamp)], by = c('codesource'))
           My.Kable(My_Last[, .(source,codesource,code,date,timestamp, timestamp_vn,close, pclose, change, varpc)])
           xData = copy(My_Last)
           My.Kable.Index(xData)
         },
         
         'intraday' = {
           # pCode = '.AEX'
           pURL = paste0('https://webql-redesign.cnbcfm.com/graphql?operationName=getQuoteChartData&variables=%7B%22symbol%22%3A%22', pCode,
                         '%22%2C%22timeRange%22%3A%221D%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%2261b6376df0a948ce77f977c69531a4a8ed6788c5ebcdd5edd29dd878ce879c8d%22%7D%7D')
           data = jsonlite::fromJSON(pURL)
           x = data$data$chartData$priceBars
           # View(x)
           xData = try(as.data.table(data$data$chartData$priceBars))
           xData = CLEAN_COLNAMES(xData)  
           xData = DBL_CLEAN_OHLC(xData)
           
           # str(xData)
           xData[, timestamp_vn:= as.POSIXct(as.numeric(tradetimeinmills)/1000, origin="1970-01-01")]
           # if (pCode=="@VX.1" & Sys.time())
           # {
           xData = xData[-.N,]
           # xData = xData[, id:=seq.int(1,.N)]
           # xData = xData[!(open==high & high==low & low==close) & id>.N*0.8]
           xData = xData[!(open==high & high==low & low==close)]
           # }
           
           # View(xData)
           # xData_error = xData[open==high & high==low & low==close]
           # My.Kable(xData_error[, .(date,timestamp, timestamp_vn,open, high, low, close, close)])
           # xData = xData[!(open==high & high==low & low==close)]
           # View(xData)
           # xData[, timestamp_loc:=paste0(
           #   substr(tradetime,1,4),'-',
           #   substr(tradetime,5,6),'-',
           #   substr(tradetime,7,8),' ',
           #   substr(tradetime,9,10),':',
           #   substr(tradetime,11,12),':',
           #   substr(tradetime,13,14)
           #   )]
           
           xData[, timestamp_utc:=lubridate::ymd_hms(timestamp_vn)-7*60*60]
           xData[, timestamp_loc:=lubridate::ymd_hms(timestamp_utc)+offset*60*60]
           xData[, timestamp := timestamp_loc]
           
           xData[, date:=as.Date(timestamp_loc)]
           # xData[, ':='(source='CNBC', codesource=pCode)]
           # xData = MERGE_DATASRC_CODE_BYCODESOURCE('CNBC', xData)
           xData = CLEAN_TIMESTAMP(xData)
           xData = xData[, -c('__typename')]
           xData[, ':='(source='CNBC', codesource=pCode)]
           xData = MERGE_DATASRC_CODE_BYCODESOURCE('CNBC', xData)
           xData[, offset := offset]
           My.Kable(xData[, .(source,codesource,code,date,timestamp, timestamp_utc, timestamp_vn,open, high, low, close, offset)], Nb=10)
           
         },
         'day' = {
           pURL = paste0('https://webql-redesign.cnbcfm.com/graphql?operationName=getQuoteChartData&variables=%7B%22symbol%22%3A%22', pCode, '%22%2C%22timeRange%22%3A%221Y%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%2261b6376df0a948ce77f977c69531a4a8ed6788c5ebcdd5edd29dd878ce879c8d%22%7D%7D')
           data = jsonlite::fromJSON(pURL)
           xData = try(as.data.table(data$data$chartData$priceBars))
           xData = CLEAN_COLNAMES(xData)  
           xData = DBL_CLEAN_OHLC(xData)
           
           # str(xData)
           xData[, timestamp_vn:= as.POSIXct(as.numeric(tradetimeinmills)/1000, origin="1970-01-01")]
           xData[,timestamp_vn:=as.Date(timestamp_vn)]
           # xData[, timestamp_loc:=paste0(
           #   substr(tradetime,1,4),'-',
           #   substr(tradetime,5,6),'-',
           #   substr(tradetime,7,8),' ',
           #   substr(tradetime,9,10),':',
           #   substr(tradetime,11,12),':',
           #   substr(tradetime,13,14)
           #   )]
           
           xData[, timestamp_utc:=timestamp_vn]
           xData[, timestamp_loc:=timestamp_vn]
           xData[, timestamp := timestamp_vn]
           xData[, date:=as.Date(timestamp_loc)]
           xData[, ':='(source='CNBC', codesource=pCode)]
           xData = DBL_MERGE_DATASRC_CODE_BYCODESOURCE('CNBC', xData)
           xData[, offset := offset]
           xData = xData[!is.na(tradetimeinmills)]
           xData[, pclose:=shift(close)]
           xData = CALCULATE_CHANGE_RT_VARPC(xData)
           # xData[, time]
           My.Kable(xData[, .(source,codesource,code,date,timestamp, timestamp_vn,close, change, varpc)])
           # str(xData)
           # 
           # 
           # pURL = paste0('https://webql-redesign.cnbcfm.com/graphql?operationName=getQuoteChartData&variables=%7B%22symbol%22%3A%22','%22%2C%22timeRange%22%3A%221D%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%2261b6376df0a948ce77f977c69531a4a8ed6788c5ebcdd5edd29dd878ce879c8d%22%7D%7D')
           
           
           
           # pURL = 'https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol?symbols=%40VX.1&requestMethod=itv&noform=1&partnerId=2&fund=1&exthrs=1&output=json&events=1'
           # x = jsonlite::fromJSON(pURL)
           # xData = as.data.table(x$FormattedQuoteResult$FormattedQuote)
           # xData = CLEAN_COLNAMES(xData)  
         })
  
  if (nrow(xData) > 0)
  {
    xData[, ':='(source='CNBC', codesource=pCode)]
    xData = MERGE_DATASRC_CODE_BYCODESOURCE('CNBC', xData)
    xData[, offset := offset]
  }
  DBL_DURATION(start_time)
  return(xData)
}

DBL_CNBC_CURRENT = function(XALL)
{
  DATA_LAST = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'DOWNLOAD_CNBC_INS_CURRENT.rds')))
  DATA_LAST = rbind(DATA_LAST, XALL, fill=T)
  if (nrow(DATA_LAST)>0)
  {
    DATA_LAST = unique(DATA_LAST[order(code, -timestamp_loc)], by='code', fromLast = T)
    My.Kable.Min(DATA_LAST)
    try(DBL_CCPR_SAVERDS(DATA_LAST, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'DOWNLOAD_CNBC_INS_CURRENT.rds', ToSummary = T, SaveOneDrive = T))
  }
}


DBL_DEMO_CNBC = function(pType = 'BND', ToReset = F)
{
  DBL_RELOAD_INSREF()
  switch(pType,
         'INDHOME' = {
           
           MY_LIST = ins_ref[!is.na(rts) & home ==1 & !rts %in% list('XXZY') & type=='IND']
           My.Kable.INSREF(MY_LIST)
           XLIST = list()
           
           for (k in 1:nrow(MY_LIST))
           {
             # k = 1
             pCode = MY_LIST[k]$rts
             pCODE = MY_LIST[k]$code
             CATln_Border(pCode)
             x = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode=pCode,CODE=pCODE)))
             if (nrow(x)>0)
             {
               XLIST[[k]] = x[, code:=pCODE]
             }
           }
           XALL = rbindlist(XLIST, fill=T)
           My.Kable.Index(XALL)
           if (nrow(XALL)>0)
           {
             try(DBL_CCPR_SAVERDS(XALL, UData, 'DOWNLOAD_CNBC_INDHOME_TODAY.rds', ToSummary = T, SaveOneDrive = T))
             DATA_DAY = DBL_INTEGRATE_DAY (data = XALL, LIST_CODES = list(),
                                           Fr_Folder = UData, Fr_File = '',
                                           To_Folder = UData, To_File = 'DOWNLOAD_CNBC_INDHOME_PRICES_DAY.rds')
             
             try(DBL_CNBC_CURRENT(XALL = XALL))
             # DATA_LAST = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'DOWNLOAD_CNBC_INS_TODAY.rds')))
             # DATA_LAST = rbind(DATA_LAST, XALL, fill=T)
             # if (nrow(DATA_LAST)>0)
             # {
             #   DATA_LAST = unique(DATA_LAST[order(code, -date, -timestamp_loc)], by='code')
             #   My.Kable.Min(DATA_LAST)
             #   try(DBL_CCPR_SAVERDS(DATA_LAST, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'DOWNLOAD_CNBC_INDHOME_CURRENT.rds', ToSummary = T, SaveOneDrive = T))
             # }
             
             DATA_DAY = DBL_INTEGRATE_DAY (data = XALL, LIST_CODES = list(),
                                           Fr_Folder = UData, Fr_File = '',
                                           To_Folder = UData, To_File = 'DOWNLOAD_CNBC_INDHOME_HISTORY.rds')
             DATA_DAY = DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
                                           Fr_Folder = UData, Fr_File = 'DOWNLOAD_CNBC_INDHOME_HISTORY.rds',
                                           To_Folder = UData, To_File = 'DOWNLOAD_CNBC_INDHOME_HISTORY.rds')
           }
           
           
         },
         
         'BND' = {
           XLIST = list()
           XLIST[[1]] = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode='US1M',     CODE='BNDGUS1M')))
           XLIST[[2]] = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode='US1Y',     CODE='BNDGUS1Y')))
           XLIST[[3]] = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode='US10Y',    CODE='BNDGUS10Y')))
           XALL = rbindlist(XLIST, fill=T)
           My.Kable.Index(XALL)
           
           if (nrow(XALL)>0)
           {
             if (ToReset)
             {
               file.remove(paste0(UData, 'DOWNLOAD_CNBC_BND_TODAY.rds'))
               file.remove(paste0(UData, 'DOWNLOAD_CNBC_BND_PRICES_DAY.rds'))
               file.remove(paste0(UData, 'DOWNLOAD_CNBC_BND_HISTORY.rds'))
             }
             try(DBL_CCPR_SAVERDS(XALL, UData, 'DOWNLOAD_CNBC_BND_TODAY.rds', ToSummary = T, SaveOneDrive = T))
             try(DBL_CNBC_CURRENT(XALL = XALL))
             
             DATA_DAY = DBL_INTEGRATE_DAY (data = XALL, LIST_CODES = list(),
                                           Fr_Folder = UData, Fr_File = '',
                                           To_Folder = UData, To_File = 'DOWNLOAD_CNBC_BND_PRICES_DAY.rds')
             DATA_DAY = DBL_INTEGRATE_DAY (data = XALL, LIST_CODES = list(),
                                           Fr_Folder = UData, Fr_File = '',
                                           To_Folder = UData, To_File = 'DOWNLOAD_CNBC_BND_HISTORY.rds')
           }
         },
         
         'FUT' = {
           XLIST = list()
           XLIST[[1]] = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode='@SP.1',     CODE='FUTSPXC1')))
           XLIST[[2]] = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode='@VX.1',     CODE='FUTVIXC1')))
           # XLIST[[3]] = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode='US10Y',    CODE='BNDGUS10Y')))
           XALL = rbindlist(XLIST, fill=T)
           My.Kable.Index(XALL)
           
           if (nrow(XALL)>0)
           {
             if (ToReset)
             {
               file.remove(paste0(UData, 'DOWNLOAD_CNBC_FUT_TODAY.rds'))
               file.remove(paste0(UData, 'DOWNLOAD_CNBC_FUT_PRICES_DAY.rds'))
               file.remove(paste0(UData, 'DOWNLOAD_CNBC_FUT_HISTORY.rds'))
             }
             XALL[is.na(name), name:=source_name]
             My.Kable.Index(XALL)
             try(DBL_CCPR_SAVERDS(XALL, UData, 'DOWNLOAD_CNBC_FUT_TODAY.rds', ToSummary = T, SaveOneDrive = T))
             try(DBL_CNBC_CURRENT(XALL = XALL))
             
             DATA_DAY = DBL_INTEGRATE_DAY (data = XALL, LIST_CODES = list(),
                                           Fr_Folder = UData, Fr_File = '',
                                           To_Folder = UData, To_File = 'DOWNLOAD_CNBC_FUT_PRICES_DAY.rds')
             DATA_DAY = DBL_INTEGRATE_DAY (data = XALL, LIST_CODES = list(),
                                           Fr_Folder = UData, Fr_File = '',
                                           To_Folder = UData, To_File = 'DOWNLOAD_CNBC_FUT_HISTORY.rds')
           }
         },
         'TOP' = {
           XLIST = list()
           XLIST[[1]] = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode='@SP.1',     CODE='FUTSPXC1')))
           XLIST[[2]] = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode='@VX.1',     CODE='FUTVIXC1')))
           XLIST[[3]] = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode='.SPX',      CODE='INDSPX')))
           XLIST[[4]] = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode='.VIX',      CODE='INDVIX')))
           # XLIST[[3]] = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode='US10Y',    CODE='BNDGUS10Y')))
           XALL = rbindlist(XLIST, fill=T)
           My.Kable.Index(XALL)
           
           if (nrow(XALL)>0)
           {
             XALL[is.na(name), name:=source_name]
             My.Kable.Index(XALL)
             try(DBL_CCPR_SAVERDS(XALL, UData, 'DOWNLOAD_CNBC_TOP_TODAY.rds', ToSummary = T, SaveOneDrive = T))
             try(DBL_CNBC_CURRENT(XALL = XALL))
             
             DATA_DAY = DBL_INTEGRATE_DAY (data = XALL, LIST_CODES = list(),
                                           Fr_Folder = UData, Fr_File = '',
                                           To_Folder = UData, To_File = 'DOWNLOAD_CNBC_TOP_PRICES_DAY.rds')
             DATA_DAY = DBL_INTEGRATE_DAY (data = XALL, LIST_CODES = list(),
                                           Fr_Folder = UData, Fr_File = '',
                                           To_Folder = UData, To_File = 'DOWNLOAD_CNBC_TOP_HISTORY.rds')
           }
         },
         
         'CUR' = {
           XLIST[[1]] = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode='EUR=',CODE='CUREURUSD')))
           XLIST[[2]] = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode='JPY=',CODE='CURUSDJPY')))
           XLIST[[3]] = CHECK_CLASS(try(DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE(pCode='GBP=',CODE='CURUSDGBP')))
           XALL = rbindlist(XLIST, fill=T)
           My.Kable.Index(XALL)
           if (nrow(XALL)>0)
           {
             if (ToReset)
             {
               file.remove(paste0(UData, 'DOWNLOAD_CNBC_CUR_TODAY.rds'))
               file.remove(paste0(UData, 'DOWNLOAD_CNBC_CUR_PRICES_DAY.rds'))
               file.remove(paste0(UData, 'DOWNLOAD_CNBC_CUR_HISTORY.rds'))
             }
             try(DBL_CCPR_SAVERDS(XALL, UData, 'DOWNLOAD_CNBC_CUR_TODAY.rds', ToSummary = T, SaveOneDrive = T))
             try(DBL_CNBC_CURRENT(XALL = XALL))
             
             DATA_DAY = DBL_INTEGRATE_DAY (data = XALL, LIST_CODES = list(),
                                           Fr_Folder = UData, Fr_File = '',
                                           To_Folder = UData, To_File = 'DOWNLOAD_CNBC_CUR_PRICES_DAY.rds')
             DATA_DAY = DBL_INTEGRATE_DAY (data = XALL, LIST_CODES = list(),
                                           Fr_Folder = UData, Fr_File = '',
                                           To_Folder = UData, To_File = 'DOWNLOAD_CNBC_CUR_HISTORY.rds')
           }
         },
         
         'BAS' = {
           DATA_DAY = DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
                                         Fr_Folder = UData, Fr_File = 'DOWNLOAD_CNBC_CUR_HISTORY.rds',
                                         To_Folder = UData, To_File = 'DOWNLOAD_CNBC_BAS_HISTORY.rds')
           DATA_DAY = DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
                                         Fr_Folder = UData, Fr_File = 'DOWNLOAD_CNBC_BND_HISTORY.rds',
                                         To_Folder = UData, To_File = 'DOWNLOAD_CNBC_BAS_HISTORY.rds')
           DATA_DAY = DBL_INTEGRATE_DAY (data = data.table(), LIST_CODES = list(),
                                         Fr_Folder = UData, Fr_File = 'DOWNLOAD_CNBC_FUT_HISTORY.rds',
                                         To_Folder = UData, To_File = 'DOWNLOAD_CNBC_BAS_HISTORY.rds')
         },
         
         'ALL' = {
           try(DBL_DEMO_CNBC(pType = 'BND', ToReset = F))
           try(DBL_DEMO_CNBC(pType = 'CUR', ToReset = F))
           try(DBL_DEMO_CNBC(pType = 'FUT', ToReset = F))
           try(DBL_DEMO_CNBC(pType = 'BAS', ToReset = F))
           try(DBL_DEMO_CNBC(pType = 'INDHOME', ToReset = F))
         })
}

# ==================================================================================================
DBL_DOWNLOAD_CNBC_SNAPSHOT_BY_CODE = function(pCode = '.VIX', CODE  = 'INDVIX') {
  # ------------------------------------------------------------------------------------------------
  Start.time = Sys.time()
  DBL_RELOAD_INSREF()
  pURL = paste0( 'https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol?symbols=', pCode)
  
  x = jsonlite::fromJSON(pURL)
  xData = as.data.table(x$FormattedQuoteResult$FormattedQuote)
  xData = CLEAN_COLNAMES(xData)
  My.Kable.All(xData)
  my_ref = ins_ref [ cnbc == pCode,.(code, cnbc)]
  if ('currencycode' %in% names(xData)) { x.currencycode = xData$currencycode} else { x.currencycode = as.character(NA)}
  if (nrow (my_ref ) >0 )
  {
    Final_Data = xData[, .(source = 'CNBC', symbol, source_name=name, codesource = pCode, code = my_ref$code,
                           
                           open   = as.numeric(gsub(',|[%]','', open)), 
                           high   = as.numeric(gsub(',|[%]','', high)), 
                           low    = as.numeric(gsub(',|[%]','', low)), 
                           last   = as.numeric(gsub(',|[%]','', last)), 
                           pclose = as.numeric(gsub(',|[%]','', previous_day_closing )), 
                           change = as.numeric(gsub(',|[%]','', change )), 
                           varpc  = as.numeric(gsub(',|[%]','', change_pct )), 
                           timestamp_loc  = substr(gsub('T', ' ', last_time),1,19) ,
                           tz     = timezone,             
                           cur    = x.currencycode,
                           tz_offset = as.numeric(str_sub(last_time,-5,-3)) )]
  } else {
    Final_Data = xData[, .(source = 'CNBC', symbol, source_name=name, codesource = pCode, code = CODE,
                           
                           open   = as.numeric(gsub(',|[%]','', open)), 
                           high   = as.numeric(gsub(',|[%]','', high)), 
                           low    = as.numeric(gsub(',|[%]','', low)), 
                           last   = as.numeric(gsub(',|[%]','', last)), 
                           pclose = as.numeric(gsub(',|[%]','', previous_day_closing )), 
                           change = as.numeric(gsub(',|[%]','', change )), 
                           varpc  = as.numeric(gsub(',|[%]','', change_pct )), 
                           timestamp_loc  = substr(gsub('T', ' ', last_time),1,19) ,
                           tz     = timezone,             
                           cur    = x.currencycode,
                           tz_offset = as.numeric(str_sub(last_time,-5,-3)) )]
  }
  # My.Kable.FieldVertical(xData)
  
  
  Final_Data[, date:=as.Date(substr(timestamp_loc,1,10))]
  Final_Data[, updated:= SYS.TIME()]
  Final_Data[, close:= last]
  Final_Data[, timestamp_utc := as.character ( as.POSIXct(timestamp_loc) - (tz_offset)*60*60 )]
  Final_Data[, timestamp_vn := as.character (as.POSIXct(timestamp_utc)+(7)*60*60 )]
  DBL_RELOAD_INSREF()
  Final_Data = MERGE_DATASTD_BYCODE(Final_Data, pOption='FINAL')
  Final_Data$caiusd = NULL
  My.Kable.Index(Final_Data[, -c('home', 'sample', 'isin', 'fcat', 'scat', 'timestamp_utc', 'source_name', 'type', 'continent')])
  DBL_DURATION(Start.time)
  return(Final_Data)
}

# ==================================================================================================
DBL_CLEAN_OHLC = function (pData)  {
  # ------------------------------------------------------------------------------------------------
  if(nrow(pData) > 0)
  {
    if('high' %in% names(pData)){pData[, high := as.numeric(high)]}
    if('open' %in% names(pData)){pData[, open := as.numeric(open)]}
    if('low' %in% names(pData)){pData[, low := as.numeric(low)]}
    if('close' %in% names(pData)){pData[, close := as.numeric(close)]}
    if('volume' %in% names(pData)){pData[, volume := as.numeric(volume)]}
  }
  return(pData)
}

# ==================================================================================================
DBL_INTEGRATE_DAY = function (data = data.table(), LIST_CODES = list('INDVIX', 'INDSPX'),
                              Fr_Folder = UData, Fr_File = 'download_yah_ind_history.rds',
                              To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_source_ins_day_day.rds')  {
  # ------------------------------------------------------------------------------------------------
  
  # data = data.table(); LIST_CODES = list()
  # Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/'; Fr_File = 'dbl_yah_ind_day_day.rds'
  # To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/'; To_File = 'dbl_source_ins_day_history.rds'
  DATA_OLD = data.table()
  if (nchar (Fr_Folder) * nchar (Fr_File) > 0 | nrow(data) == 0)
  {
    data = CHECK_CLASS(try( CCPR_READRDS (Fr_Folder, Fr_File, ToRestore = T, ToKable = T) ))
    data = DBL_CLEAN_OHLC(data)
  }
  data = CLEAN_TIMESTAMP(data)
  data = DBL_IND_TO_CORRECT(data)
  str(data)
  if (nchar (To_Folder) * nchar (To_File) > 0 & nrow (data) >0)
  {
    DATA_OLD = CHECK_CLASS (try (CCPR_READRDS (To_Folder, To_File, ToKable = T, ToRestore = T) ) )
    DATA_OLD = DBL_CLEAN_OHLC(DATA_OLD)
    if (length(LIST_CODES) > 0 )
    {
      data = data[code %in% LIST_CODES]
    }
    if (nrow (DATA_OLD) >0)
    {
      DATA_OLD = rbind (DATA_OLD, data, fill = T)
      DATA_OLD = unique (DATA_OLD[order(code, -date)], by = c('code', 'date'), fromLast = T) [!is.na(code) & !is.na(close) & !is.na(date)]
      My.Kable.Min(DATA_OLD)
      if (nrow(DATA_OLD)>0)
      {
        DATA_OLD = DBL_CLEAN_OHLC(DATA_OLD)
        #DATA_OLD = DBL_IND_TO_CLEAN(DATA_OLD)
        DBL_CCPR_SAVERDS ( DATA_OLD, To_Folder, To_File, ToSummary = T, SaveOneDrive = F)
      }
      
    } else {
      My.Kable.Min(data)
      if (nrow(data)>0)
      {
        DATA_OLD = copy(data)
        #DATA_OLD = DBL_IND_TO_CLEAN(DATA_OLD)
        DBL_CCPR_SAVERDS ( DATA_OLD, To_Folder, To_File, ToSummary = T, SaveOneDrive = F)
      }
    }
  }
  My.Kable.Min(DATA_OLD)
  return (DATA_OLD)
  
}

# ==================================================================================================
DBL_VOLATILITY_LOOP = function (pOption = 'DOWNLOAD',  NbMinutes = 15 ) {
  # ------------------------------------------------------------------------------------------------
  # try (DBL_INTRADAY_VOLATILITY_LOOP( pOption = 'YAH', pMinutes = 30) )
  # try (DBL_INTRADAY_VOLATILITY_LOOP( pOption = 'DNSE', pMinutes = 30) )
  
  switch (pOption,
          'DOWNLOAD' = {
            TO_DO     = TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption= paste0('DBL_VOLATILITY_LOOP>', pOption), pAction="COMPARE", NbSeconds=NbMinutes*60, ToPrint=T)
            if (TO_DO)
            {
              LIST_RANDOM = sample (list(1,2,3,4,5))
              for (k in 1: length (LIST_RANDOM))
              {
                RANDOM = as.character (LIST_RANDOM [[k]])
                
                switch (RANDOM,
                        '1'= {
                          x3 = try(DBL_DOWNLOAD_YAH_INTRADAY_BY_FILE(Folder_List = 'LIST_INTRADAY_VOLATILITY.txt',
                                                                     File_List   = 'S:/CCPR/DATA/LIST/',
                                                                     pType       = 'index', pCoverage = 'INTERNATIONAL',
                                                                     Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                                     Save_Prefix = 'DBL_YAH_IND<PREFIX>_<FREQUENCY>_5m_<HISTORY>.rds',
                                                                     ToSave      = T, ToHistory = T, Nb_Min = 500,pInterval = '5m') )
                          try(DBL_INTEGRATE_INTRADAY  (data = data.table(),
                                                       Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_yah_ind_intraday_5m_day.rds',
                                                       To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_yah_ins_intraday_5m_day.rds') )
                        },
                        '2' = {
                          x3 = try(DBL_DOWNLOAD_YAH_INTRADAY_BY_FILE(Folder_List = 'LIST_INTRADAY_VOLATILITY.txt',
                                                                     File_List   = 'S:/CCPR/DATA/LIST/',
                                                                     pType       = 'stock', pCoverage = 'INTERNATIONAL',
                                                                     Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                                     Save_Prefix = 'DBL_YAH_STK<PREFIX>_<FREQUENCY>_5m_<HISTORY>.rds',
                                                                     ToSave      = T, ToHistory = T, Nb_Min = 500,pInterval = '5m'))
                          try(DBL_INTEGRATE_INTRADAY  (data = data.table(),
                                                       Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_yah_stk_intraday_5m_day.rds',
                                                       To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_yah_ins_intraday_5m_day.rds') )
                          
                          x3 = try(DBL_DOWNLOAD_YAH_INTRADAY_BY_FILE(Folder_List = 'LIST_INTRADAY_VOLATILITY.txt',
                                                                     File_List   = 'S:/CCPR/DATA/LIST/',
                                                                     pType       = 'stock', pCoverage = 'VIETNAM',
                                                                     Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                                     Save_Prefix = 'DBL_YAH_STK<PREFIX>_<FREQUENCY>_5m_<HISTORY>.rds',
                                                                     ToSave      = T, ToHistory = T, Nb_Min = 500,pInterval = '5m'))
                          try(DBL_INTEGRATE_INTRADAY  (data = data.table(),
                                                       Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_yah_stk_intraday_5m_day.rds',
                                                       To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_yah_ins_intraday_5m_day.rds') )
                        },
                        '3' = {
                          x3 = try (DBL_DOWNLOAD_YAH_INTRADAY_BY_FILE(Folder_List = 'LIST_INTRADAY_VOLATILITY.txt',
                                                                      File_List   = 'S:/CCPR/DATA/LIST/',
                                                                      pType       = 'currency', pCoverage = 'INTERNATIONAL',
                                                                      Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                                      Save_Prefix = 'DBL_YAH_CUR<PREFIX>_<FREQUENCY>_5m_<HISTORY>.rds',
                                                                      ToSave      = T, ToHistory = T, Nb_Min = 500,pInterval = '5m'))
                          try(DBL_INTEGRATE_INTRADAY  (data = data.table(),
                                                       Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_yah_cur_intraday_5m_day.rds',
                                                       To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_yah_ins_intraday_5m_day.rds') )
                        },
                        '4' = {
                          x3 = try (DBL_DOWNLOAD_YAH_INTRADAY_BY_FILE(Folder_List = 'LIST_INTRADAY_VOLATILITY.txt',
                                                                      File_List   = 'S:/CCPR/DATA/LIST/',
                                                                      pType       = 'commodity', pCoverage = 'INTERNATIONAL',
                                                                      Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                                      Save_Prefix = 'DBL_YAH_CMD<PREFIX>_<FREQUENCY>_5m_<HISTORY>.rds',
                                                                      ToSave      = T, ToHistory = T, Nb_Min = 500,pInterval = '5m'))
                          try(DBL_INTEGRATE_INTRADAY  (data = data.table(),
                                                       Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_yah_cmd_intraday_5m_day.rds',
                                                       To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_yah_ins_intraday_5m_day.rds'))
                        },
                        '5' = {
                          x3 = try (DBL_DOWNLOAD_YAH_TOP_INTRADAY_BY_FILE (Folder_List = ODDrive,
                                                                           File_List   = "DBL/RD_BATCH_MANAGEMENT.xlsx",
                                                                           pType       = '', pCoverage = '',
                                                                           Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                                           Save_Prefix = 'DBL_YAH_TOP_<FREQUENCY>_5m_<HISTORY>.rds',
                                                                           ToSave      = T, ToHistory = T, Nb_Min = 500,pInterval = '5m') )
                          try(DBL_INTEGRATE_INTRADAY  (data = data.table(),
                                                       Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_yah_top_intraday_5m_day.rds',
                                                       To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_yah_ins_intraday_5m_day.rds'))
                        }
                )
              }
              
              try(DBL_INTEGRATE_INTRADAY  (data = data.table(),
                                           Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_yah_ins_intraday_5m_day.rds',
                                           To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_source_ins_intraday_5m_day.rds') )
              
              if (runif(1,1,100)>75)
              {
                try(DBL_INTEGRATE_INTRADAY  (data = data.table(),
                                             Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_source_ins_intraday_5m_day.rds',
                                             To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_source_ins_intraday_5m_history.rds') )
              }
              
              # if (runif(1,1,100)>75)
              # {
              #   try(DBL_INTEGRATE_INTRADAY  (data = data.table(),
              #                                Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_yah_ins_intraday_day.rds',
              #                                To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_yah_ins_intraday_history.rds'))
              # }
              
              TO_DO = TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0('DBL_VOLATILITY_LOOP>', pOption), pAction="SAVE", NbSeconds=0*60, ToPrint=T)
            }
          },
          'CALCULATE' = {
            TO_DO     = TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption= paste0('DBL_VOLATILITY_LOOP>', pOption), pAction="COMPARE", NbSeconds=NbMinutes*60, ToPrint=T)
            if (TO_DO)
            {
              
              index = try(DBL_CALCULATE_INTRADAY_VOLATILITY (index = data.table(), LIST_CODES = list(),
                                                             Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_source_ins_intraday_5m_day.rds',
                                                             To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_indbeq_intraday_volatility_day.rds'))
              # CCPR_SAVERDS (index, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_indbeq_intraday_volatility_day.rds')
              
              TO_DO = TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0('DBL_VOLATILITY_LOOP>', pOption), pAction="SAVE", NbSeconds=0*60, ToPrint=T)
            }
          },
          'UPLOAD' = {
            x = DBL_SUMMARY_UPDATED(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_indbeq_intraday_volatility_day.rds'))
            difference = difftime(Sys.time(), x, units='mins')
            if ( difference > 1)
            {
              try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_volatility_intraday_chart',
                                               l.filepath= paste0('S:/CCPR/DATA/DASHBOARD_LIVE/','dbl_indbeq_intraday_volatility_day.rds'),
                                               CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
            }
            
            
            x = DBL_SUMMARY_UPDATED(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_volatility_intraday_performance.rds'))
            difference = difftime(Sys.time(), x, units='mins')
            if ( difference > 1)
            {
              x= CHECK_CLASS(try(CCPR_READRDS ('S:/CCPR/DATA/DASHBOARD_LIVE/','dbl_indbeq_intraday_volatility_day.rds', ToKable = T, ToRestore = T)))
              x = x [code != 'INDVLINA']
              x = x [!grepl('VIX',name)]
              x = x [code != 'INDVLIINDTA125']
              # unique(x, by = 'code')
              if (nrow (x)>0)
              {
                # x [code_und == 'INDSPX']
                y = setDT(x %>% group_by(code_und) %>% filter(date < (max(date)) ))
                # y = unique (y [order (-timestamp)] , by = 'code')
                y = y [!is.na (close) & !is.na (close_und)]
                y = unique (y [order (-timestamp)] , by = 'code')
                
                y [, reference := close]
                y [, last := close_und]
                xx = unique (x [order (-timestamp)], by = 'code_und')
                my_data =merge ( xx , y [,.(code_und, reference, last)], all.x = T, by = 'code_und')
                my_data[grepl('STK',code), type := 'STK']
                my_data [is.na(type), type := 'IND']
                my_data [, volat_pc := (close/reference - 1)*100]
                my_data [, und_pc := (close_und/last - 1)*100]
                my_data [, ':=' (duo = 'VOLATILITY', change = close_und - last, close_change = close - reference,
                                 time_loc = gsub (Sys.Date(),'', substr (timestamp,1,nchar (timestamp) -3)), time_vn = gsub (Sys.Date(),'', substr (timestamp_vn,1,nchar (timestamp_vn) -3)),
                                 type_und = substr (code_und,1,3), short_name = gsub ('INTRADAY ','', name))]
                
                my_data[, short_name := gsub('IFRC/BEQ','BEQ',short_name)]
                my_data[code_und == 'INDVLIINDXDB', short_name := 'BEQ VOLATILITY GBP INDEX']
                my_data[code_und == 'INDVLIINDXDA', short_name := 'BEQ VOLATILITY AUS INDEX']
                my_data[code_und == 'INDVLIINDXDN', short_name := 'BEQ VOLATILITY JPN INDEX']
                my_data[code_und == 'STKVNHDB', short_name := 'BEQ VOLATILITY HDBANK']
                unique (my_data[!is.na (close) & !is.na (close_und)], by = 'code') 
                my_data [, group_by := continent]
                my_data [!is.na(close) & close <= 100, active := 1]
                my_data [iso2 == 'VN', group_by := 'VN']
                my_data [iso2 == 'US', group_by := 'US']
                my_data [code =='INDVLIFUTINDYMC1', group_by := 'US']
                my_data [active == 1, nr := seq(.N), by = 'group_by']
                my_data [active == 1, subtitle := gsub ('BEQ VOLATILITY ','', short_name)]
                my_data [active == 1, title := 'BEQ VOLATILITY']
                # my_data [code == 'INDVLIINDTA125TA']
                my_data [code %in% c('INDVLIINDFTAW01', 'INDVLICURBTCUSD','INDVLICMDGOLD', 'INDVLIINDSPX',
                                     'INDVLIINDNDX', 'INDVLIINDFTSE', 'INDVLIINDCAC','INDVLIINDDAX',
                                     'INDVLIINDVN30'), nr_lite := seq(.N)]
                CCPR_SAVERDS(my_data[!is.na(country) & active == 1 & !is.na (close_change)], 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_volatility_intraday_performance.rds', ToSummary = T)
                
                try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_volatility_intraday_performance',
                                                 l.filepath= paste0('S:/CCPR/DATA/DASHBOARD_LIVE/','dbl_volatility_intraday_performance.rds'),
                                                 CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
              }
              
              # 
              # x = DBL_SUMMARY_UPDATED(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_volatility_intraday_performance.rds'))
              # difference = difftime(Sys.time(), x, units='mins')
              # if ( difference < 15)
              # {
              #   
              #   try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_volatility_intraday_performance',
              #                                    l.filepath= paste0('S:/CCPR/DATA/DASHBOARD_LIVE/','dbl_volatility_intraday_performance.rds'),
              #                                    CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
              # }
            }
            
            x = DBL_SUMMARY_UPDATED(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_volatility_intraday_history.rds'))
            difference = difftime(Sys.time(), x, units='mins')
            if ( difference > 10)
            {
              y = CCPR_READRDS ('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_indbeq_intraday_volatility_day.rds', ToKable = T)
              y = y [code != 'INDVLINA']
              my_data = setDT(y  %>%
                                group_by(code, date) %>% filter(timestamp == (max(timestamp)) ) )
              my_data [order (date), ':=' ( volat_pc = close/shift (close) - 1, und_pc = close/shift (close) - 1), by = 'code']
              CCPR_SAVERDS(my_data, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_volatility_intraday_history.rds', ToSummary = T)
              # My.Kable.Min (unique (y [], by = 'code') )
              # My.Kable.Min (unique (my_data [], by = 'code') )
              try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_volatility_intraday_history',
                                               l.filepath= paste0('S:/CCPR/DATA/DASHBOARD_LIVE/','dbl_volatility_intraday_history.rds'),
                                               CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = T))
              
            }
          },
          'REPORT' = {
            x = DBL_CCPR_READRDS ('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_indbeq_intraday_volatility_day.rds', ToKable = T, ToRestore = T)
            summary = x [,. (name = name [1],code_und = code_und[1], name_und = name_und [1],start = timestamp [1], end = timestamp[.N], end_vn = timestamp_vn[.N], 
                             records = .N, last = close[.N], last_und = close_und [.N], updated = updated [.N]),by = 'code']
            yah = setDT (fread('V:/CCPR/DATA/LIST/LIST_INTRADAY.txt')) [SOURCE == 'YAH']
            yah = merge (yah, ins_ref [,.(SYMBOL = yah, code, name)], all.x = T, by = 'SYMBOL')
            
            my_data = merge (summary, yah [!is.na(code),.(code_und = code, DELAY)], all.x = T, by = 'code_und')
            str(my_data)
            
            my_data [DELAY ==1 & difftime (Sys.time(),as.POSIXct(my_data$updated , format="%Y-%m-%d %H:%M:%S"),  units = 'secs') > 1*60*60*24, STATUS := 'LATE']
            my_data [is.na(DELAY)&difftime (Sys.time(),as.POSIXct(my_data$updated , format="%Y-%m-%d %H:%M:%S"),  units = 'secs') > 5*60, STATUS := 'LATE']
            My.Kable.All (my_data [,-c('name')])
            my_data =merge (my_data [, -c ('country')], ins_ref [,.(code_und = code, country)], all.x= T, by = 'code_und')
            my_data = my_data [order (end_vn)]
            my_data [, date:= Sys.Date()]
            my_data = UPDATE_UPDATED(my_data)
            # CCPR_SAVERDS (my_data, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_indbeq_intraday_volatility_report.rds', ToSummary = T)
            # my_data = my_data[,-c('name','fcat','iso3','timestamp')]
            # my_data = my_data %>% select ('iso2','country','continent','code', 'code_und', 'name_und', 'date','hhmm', everything())
            my_data = my_data[,-c('codesource','source','close','name')]
            DBL_CCPR_SAVERDS (my_data, 'S:/SHINY/VOLATILITY/', 'dbl_indbeq_intraday_volatility_report.rds', ToSummary = T)
            library(fst)
            my_data = my_data[,-c('codesource','source','close','name')]
            write_fst(my_data, 'S:/SHINY/VOLATILITY/dbl_indbeq_intraday_volatility_report.fst')
            
            x = DBL_CCPR_READRDS ('S:/SHINY/VOLATILITY/', 'dbl_indbeq_intraday_volatility_report.rds', ToKable = T)
            yah = setDT (fread('V:/CCPR/DATA/LIST/LIST_INTRADAY.txt')) [SOURCE == 'YAH']
            yah = merge (yah, ins_ref [,.(SYMBOL = yah, code)], all.x = T, by = 'SYMBOL')
            NOT_DOWNLOAD = yah [!code %in% x$code_und]
            write_fst(NOT_DOWNLOAD, 'S:/SHINY/VOLATILITY/dbl_indbeq_intraday_novolatility_report.fst')
            DBL_CCPR_SAVERDS (NOT_DOWNLOAD, 'S:/SHINY/VOLATILITY/', 'dbl_indbeq_intraday_novolatility_report.fst')
            
            # report last
            x = DBL_CCPR_READRDS ( 'S:/CCPR/DATA/DASHBOARD_LIVE/',  'dbl_yah_ins_intraday_history.rds')
            report = try(CCPR_WRITE_SUMMARY_UPDATE_ALL(pData=x, pFileName='', ToExclude=T, ToPrompt=F, ToRemoveCodeNA=T)) 
            y = setDT(x %>% group_by(code) %>% filter(date < (max(date)) ))
            y = y [!is.na (close)]
            y = unique (y [order (-timestamp)] , by = 'code')
            
            y [, pclose := close]
            my_data =merge ( report , y [,.(code, pclose)], all.x = T, by = 'code')
            my_data[grepl('STK',code), type := 'STK']
            my_data [is.na(type), type := 'IND']
            str (my_data)
            my_data [, varpc := (last/pclose - 1)*100]
            my_data [, change := last - pclose]
            my_data = my_data [,.(code, name, country, continent, date, starttime, endtime, close  =last, pclose, varpc, change)]
            my_data [, updated := SYS.TIME()]
            
            DBL_CCPR_SAVERDS(my_data, 'S:/SHINY/VOLATILITY/', 'dbl_intraday_last.rds')
            library(fst)
            write_fst(my_data, 'S:/SHINY/VOLATILITY/dbl_intraday_last.fst')
          }
  )
  
}

# ==================================================================================================
DBL_DOWNLOAD_YAH_MARKETS = function(pCodesource = '^GSPC', Hour_adjust = -4 , pInterval = '5m')
{
  # pCodesource = '^GSPC'; Hour_adjust = -4
  index = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_OHLC_INTRADAY( pCodesource = pCodesource, pInterval = pInterval, Hour_adjust = Hour_adjust)))
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
DBL_DOWNLOAD_YAH_INTRADAY_BY_FILE = function(Folder_List = 'LIST_INTRADAY_VOLATILITY.txt',
                                             File_List   = 'S:/CCPR/DATA/LIST/',
                                             pType       = 'index', pCoverage = 'INTERNATIONAL',
                                             Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                             Save_Prefix = 'DBL_YAH_IND<PREFIX>_<FREQUENCY>_<HISTORY>.rds',
                                             ToSave      = T, ToHistory = T, Nb_Min = 500,   pInterval = '1m') {
  
  # Folder_List = 'LIST_INTRADAY_VOLATILITY.txt'; File_List   = 'S:/CCPR/DATA/LIST/'; Nb_Min = 5
  # pType       = 'stock'; pCoverage = 'INTERNATIONAL';Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/';Save_Prefix = 'DBL_YAH_IND<PREFIX>_<FREQUENCY>_<HISTORY>.rds';ToSave = T
  
  xALL_INTRADAY = data.table()
  xALL_DAY      = data.table()
  xALL_LAST     = data.table()
  LIST_CODES = try(setDT(fread('S:/CCPR/DATA/LIST/LIST_INTRADAY_VOLATILITY.txt')))
  
  if (all(class(LIST_CODES)!='try-error'))
  {
    LIST_CODES = LIST_CODES[SOURCE=='YAH'  & ACTIVE == 1]
    if (nchar(pType)>0)   { LIST_CODES = LIST_CODES[TYPE==pType]   }
    if (nchar(pCoverage)>0)   { LIST_CODES = LIST_CODES[COVERAGE==pCoverage]   }
    My.Kable.All(LIST_CODES)
    
    xLIST_INTRADAY = list()
    xLIST_DAY      = list()
    xLIST_LAST     = list()
    
    NB_TODO        = min(Nb_Min, nrow(LIST_CODES))
    for (k in 1:NB_TODO)
    {
      # k = 1
      pCode   = LIST_CODES[k]$SYMBOL 
      CATln('')
      CATln_Border(paste(k, '/', NB_TODO, '>>>', pCode))
      pOffset = as.numeric(LIST_CODES[k]$HOUR_ADJUST)
      x = try(DBL_DOWNLOAD_YAH_MARKETS(pCodesource = pCode, Hour_adjust = pOffset, pInterval = pInterval))
      # sort(names(x[[1]]))
      # str(x[[1]])
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
    
    My.Kable.TB(xALL_INTRADAY[order(date, timestamp_vn)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent')][order(date, timestamp, code)])
    My.Kable.TB(xALL_DAY[order(date, code)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent')][order(date, timestamp, code)])
    My.Kable.All(xALL_LAST[order(date, timestamp_vn)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent')])
    
    if(ToSave)
    {
      FileName = gsub('[<]HISTORY>', 'TODAY', gsub('[<]FREQUENCY>', 'INTRADAY', gsub('[<]PREFIX>', '', Save_Prefix)))
      try(CCPR_SAVERDS(xALL_INTRADAY, Save_Folder, FileName, ToSummary = T, SaveOneDrive = F))
      
      FileName = gsub('[<]HISTORY>', 'TODAY', gsub('[<]FREQUENCY>', 'DAY', gsub('[<]PREFIX>', '', Save_Prefix)))
      try(CCPR_SAVERDS(xALL_DAY, Save_Folder, FileName, ToSummary = T, SaveOneDrive = T))
      
      FileName = gsub('[<]HISTORY>', 'TODAY', gsub('[<]FREQUENCY>', 'LAST', gsub('[<]PREFIX>', '', Save_Prefix)))
      try(CCPR_SAVERDS(xALL_LAST, Save_Folder, FileName, ToSummary = T, SaveOneDrive = T))
    }
    
    if(ToHistory)
    {
      ToHistory = T
      FileName = gsub('[<]HISTORY>', 'DAY', gsub('[<]FREQUENCY>', 'INTRADAY', gsub('[<]PREFIX>', '', Save_Prefix)))
      CATln_Border(FileName)
      Data     = CHECK_CLASS(try(CCPR_READRDS(Save_Folder, FileName, ToKable = T, ToRestore = T)))
      
      Data     = rbind(Data, xALL_INTRADAY, fill = T)
      if(nrow(Data) > 0 )
      {
        Data = unique(Data, by = c('code', 'timestamp'), FromLast = T)
        Data$capiusd = NULL
        My.Kable.TB(Data[order(date, timestamp_vn)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent', 'isin')][order(date, timestamp, code)])
        try(CCPR_SAVERDS(Data, Save_Folder, FileName, ToSummary = T, SaveOneDrive = F))
      }
      
      FileName = gsub('[<]HISTORY>', 'DAY', gsub('[<]FREQUENCY>', 'DAY', gsub('[<]PREFIX>', '', Save_Prefix)))
      CATln_Border(FileName)
      Data     = CHECK_CLASS(try(CCPR_READRDS(Save_Folder, FileName, ToKable = T, ToRestore = T)))
      
      Data     = rbind(Data, xALL_DAY, fill = T)
      if (nrow(Data) > 0 )
      {
        Data = unique(Data, by = c('code', 'date'), FromLast = T)
        Data$capiusd = NULL
        My.Kable.TB(Data[order(date, code)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent', 'isin')][order(date, code)])
        try(CCPR_SAVERDS(Data, Save_Folder, FileName, ToSummary = T, SaveOneDrive = F))
      }
      
      FileName = gsub('[<]HISTORY>', 'DAY', gsub('[<]FREQUENCY>', 'LAST', gsub('[<]PREFIX>', '', Save_Prefix)))
      CATln_Border(FileName)
      Data     = CHECK_CLASS(try(CCPR_READRDS(Save_Folder, FileName, ToKable = T, ToRestore = T)))
      
      Data     = rbind(Data, xALL_LAST, fill = T)
      if (nrow(Data) > 0 )
      {
        Data = unique(Data[order(code, -date, -timestamp)], by = c('code'), FromLast = T)
        Data$capiusd = NULL
        My.Kable.TB(Data[order(date, code)][, -c('open', 'high', 'low', 'symbol', 'cur', 'source', 'iso2', 'continent', 'isin')][order(date, code)])
        try(CCPR_SAVERDS(Data, Save_Folder, FileName, ToSummary = T, SaveOneDrive = F))
      }
    }
  }
  return(list(xALL_INTRADAY, xALL_DAY, xALL_LAST))
}

# ==================================================================================================
CLEAN_TIMESTAMP = function (data) {
  # ------------------------------------------------------------------------------------------------
  if (nrow (data) >0)
  {
    if ('timestamp' %in% names (data)) { data[, timestamp := substr(as.character (timestamp), 1, 19)]}
    if ('datetime' %in% names (data)) { data[, datetime := substr(as.character (datetime), 1, 19)]}
    if ('timestamp_vn' %in% names (data)) { data[, timestamp_vn := substr(as.character (timestamp_vn), 1, 19)]}
    if ('timestamp_loc' %in% names (data)) { data[, timestamp_loc := substr(as.character (timestamp_loc), 1, 19)]}
    if ('timestamp_utc' %in% names (data)) { data[, timestamp_utc := substr(as.character (timestamp_utc), 1, 19)]}
  }
  return (data)
}


# ==================================================================================================
DBL_INTEGRATE_INTRADAY = function (data = data.table(),
                                   Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_yah_ind_intraday_day.rds',
                                   To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_yah_intraday_history.rds')  {
  # ------------------------------------------------------------------------------------------------
  if (nchar (Fr_Folder) * nchar (Fr_File) >0)
  {
    data =CHECK_CLASS(try( CCPR_READRDS (Fr_Folder, Fr_File, ToRestore = T, ToKable = T) ))
    data = CLEAN_TIMESTAMP(data)
    data = DBL_CLEAN_OHLC (data)
  }
  data = CLEAN_TIMESTAMP(data)
  if (nchar (To_Folder) * nchar (To_File) >0 & nrow (data) >0)
  {
    DATA_OLD = CHECK_CLASS (try (CCPR_READRDS (To_Folder, To_File, ToKable = T, ToRestore = T) ) )
    DATA_OLD = DBL_CLEAN_OHLC(DATA_OLD)
    DATA_OLD = CLEAN_TIMESTAMP (DATA_OLD)
    DATA_OLD = rbind (DATA_OLD, data, fill = T)
    if (nrow (DATA_OLD) >0)
    {
      DATA_OLD = unique (DATA_OLD, by = c('code', 'date', 'timestamp')) [!is.na(code) & !is.na(close) & !is.na(date)& !is.na(timestamp)]
      DATA_OLD = CLEAN_TIMESTAMP (DATA_OLD)
      DATA_OLD [, name := toupper (name)]
      DATA_OLD = DBL_CLEAN_OHLC (DATA_OLD)
      CCPR_SAVERDS ( DATA_OLD, To_Folder, To_File, ToSummary = T, SaveOneDrive = F)
    }
  }
  
}

# ==================================================================================================
# DBL_INTEGRATE_DAY = function (data = data.table(), LIST_CODES = list('INDVIX', 'INDSPX'),
#                               Fr_Folder = UData, Fr_File = 'download_yah_ind_history.rds',
#                               To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_source_ins_day_day.rds', NbDays = 5)  {
#   # ------------------------------------------------------------------------------------------------
#   if (nchar (Fr_Folder) * nchar (Fr_File) >0)
#   {
#     data =CHECK_CLASS(try( CCPR_READRDS (Fr_Folder, Fr_File, ToRestore = T, ToKable = T) ))
#     # data = CLEAN_TIMESTAMP(data)
#   }
#   # data = CLEAN_TIMESTAMP(data)
#   if (nchar (To_Folder) * nchar (To_File) >0 & nrow (data) >0)
#   {
#     DATA_OLD = CHECK_CLASS (try (CCPR_READRDS (To_Folder, To_File, ToKable = T, ToRestore = T) ) )
#     # DATA_OLD = CLEAN_TIMESTAMP (DATA_OLD)
#     # DATA_OLD = rbind (DATA_OLD, data, fill = T)
#     if (nrow (DATA_OLD) >0)
#     {
#       DATA_OLD = rbind (DATA_OLD, data, fill = T)
#       DATA_OLD = unique (DATA_OLD, by = c('code', 'date'), FromLast = T) [!is.na(code) & !is.na(close) & !is.na(date)]
#       My.Kable.Min(DATA_OLD)
#       if (nrow(DATA_OLD)>0)
#       {
#         DATA_OLD [, name := toupper (name)]
#         CCPR_SAVERDS ( DATA_OLD, To_Folder, To_File, ToSummary = T, SaveOneDrive = F)
#       }
#       # DATA_OLD = CLEAN_TIMESTAMP (DATA_OLD)
#       
#     } else {
#       data = data[code %in% LIST_CODES]
#       My.Kable.Min(data)
#       if (nrow(data)>0)
#       {
#         data [, name := toupper (name)]
#         CCPR_SAVERDS ( data, To_Folder, To_File, ToSummary = T, SaveOneDrive = F)
#       }
#     }
#   }
#   
# }

# ==================================================================================================
DBL_INTRADAY_VOLATILITY_LOOP = function ( pOption = 'YAH', pMinutes = 120) {
  # ------------------------------------------------------------------------------------------------
  rm(LIST_CODES)
  switch (pOption,
          'YAH' = {
            LIST_CODES = setDT(fread ('S:/CCPR/DATA/LIST/LIST_INTRADAY_VOLATILITY.txt'))
            LIST_CODES = LIST_CODES [ACTIVE == 1 & SOURCE =='YAH']
            
            LIST_DOWNLOADED = setDT(fread ('S:/CCPR/DATA/DASHBOARD_LIVE/dbl_indbeq_volatility_intraday_history_summary.txt'))
            NOT_DOWNLOAD = setdiff (LIST_CODES$SYMBOL, LIST_DOWNLOADED$codesource)
            
            x = SUMMARY_UPDATED(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_indbeq_volatility_intraday_history.rds'))
            difference = difftime(Sys.time(), x, units='mins')
            ToDownload = F
            if (difference > pMinutes)
            {            LIST_CODES = setDT(fread ('S:/CCPR/DATA/LIST/LIST_INTRADAY_VOLATILITY.txt'))
            LIST_CODES = LIST_CODES [ACTIVE == 1 & SOURCE =='YAH']
            ToDownload = T
            }
            if (difference < pMinutes & length (NOT_DOWNLOAD) >0)
            {
              LIST_CODES = LIST_CODES [SYMBOL %in% NOT_DOWNLOAD]
              ToDownload = T
            }
            if (nrow (LIST_CODES) >0 & ToDownload) 
            {
              MY_DATA = list()
              for (i in 1:nrow(LIST_CODES))
              {
                # i = 1
                my_code = LIST_CODES[i]$SYMBOL
                pHour   = LIST_CODES[i]$HOUR_ADJUST
                
                index = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_OHLC_INTRADAY( pCodesource = my_code, pInterval = '5m', Hour_adjust = pHour)))
                if (nrow (index) >0)
                {
                  index[, timestamp_IND:= datetime]
                  index = DBL_CALCULATE_INTRADAY_VOLATILITY ( pSource = 'YAH', index)
                  index [, codesource := my_code]
                  MY_DATA [[i]] = index
                } 
                
                Sys.sleep(10)
              }
              data = rbindlist (MY_DATA, fill =T)
              data [, datetime := as.character (datetime)]
              ToIntegrate = T
              Folder_To = 'S:/CCPR/DATA/DASHBOARD_LIVE/' 
              File_Day  = 'dbl_indbeq_volatility_intraday_day.rds' 
              File_History  = 'dbl_indbeq_volatility_intraday_history.rds'
              
              if (ToIntegrate & (nchar(Folder_To)* nchar( File_Day)) >0 & nrow (data) >0) 
              {
                DATA_OLD = CHECK_CLASS (try(CCPR_READRDS(Folder_To, File_Day)))
                DATA_OLD [, datetime := as.character (datetime)]
                DATA_NEW = rbind (DATA_OLD, data, fill = T)
                DATA_NEW = unique (DATA_NEW, fromLast = T, by = c('code','date','timestamp'))
                DATA_NEW = DATA_NEW [!is.na(code_und)]
                # DATA_NEW [code_und == 'STKVNTCB']
                CCPR_SAVERDS (DATA_NEW, Folder_To, File_Day, ToSummary = T, SaveOneDrive = T)
                if (nchar (File_History) > 0) { 
                  DATA_HISTORY = CHECK_CLASS (try(CCPR_READRDS(Folder_To, File_History)))
                  DATA_HISTORY [, datetime := as.character (datetime)]
                  DATA_NEW = rbind (DATA_HISTORY,  data, fill = T)
                  DATA_NEW = unique (DATA_NEW, fromLast = T, by = c('code','date','timestamp'))
                  CCPR_SAVERDS (DATA_NEW, Folder_To, File_History, ToSummary = T, SaveOneDrive = T)
                }
              }
            }else { CATln_Border('ALL INDEXES ARE UPDATED...')}
          },
          'DNSE' = {
            
            LIST_CODES = setDT(fread ('S:/CCPR/DATA/LIST/LIST_INTRADAY_VOLATILITY.txt'))
            LIST_CODES = LIST_CODES [ACTIVE == 1 & SOURCE == 'DNSE']
            
            LIST_DOWNLOADED = setDT(fread ('S:/CCPR/DATA/DASHBOARD_LIVE/dbl_indbeq_volatility_intraday_history_summary.txt'))
            NOT_DOWNLOAD = setdiff (LIST_CODES$SYMBOL, LIST_DOWNLOADED$codesource)
            
            x = SUMMARY_UPDATED(  paste0('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_indbeq_volatility_intraday_history.rds'))
            difference = difftime(Sys.time(), x, units='mins')
            ToIntegrate = T
            Folder_To = 'S:/CCPR/DATA/DASHBOARD_LIVE/' 
            File_Day  = 'dbl_indbeq_volatility_intraday_day.rds' 
            File_History  = 'dbl_indbeq_volatility_intraday_history.rds'
            ToDownload = F
            if (difference > pMinutes)
            {
              LIST_CODES = setDT(fread ('S:/CCPR/DATA/LIST/LIST_INTRADAY_VOLATILITY.txt'))
              LIST_CODES = LIST_CODES [ACTIVE == 1 & SOURCE == 'DNSE']
              ToDownload = T
            }
            if (difference < pMinutes & length (NOT_DOWNLOAD) >0)
            {
              LIST_CODES = LIST_CODES [SYMBOL %in% NOT_DOWNLOAD]
              ToDownload = T
            }
            if (nrow (LIST_CODES) >0 & ToDownload ) 
            {
              # file.remove(paste0(Folder_To,File_Day))
              # file.remove(paste0(Folder_To,File_History))
              
              MY_DATA = list()
              if (nrow (LIST_CODES) > 0) 
              {
                for (i in 1:nrow(LIST_CODES))
                {
                  # i = 2
                  my_code = LIST_CODES[i]$SYMBOL
                  pType  = LIST_CODES[i]$TYPE
                  pCode  = LIST_CODES[i]$CODE
                  
                  index = DOWNLOAD_ENTRADE_INDEX_INTRADAY (type = pType,codesource = my_code, code = pCode, NbMinutes   = 11*365*24*60, OffsetDays  = 0,
                                                           Save_folder = '', Save_file = '',
                                                           Save_history = '',
                                                           pInterval = '5')
                  if (nrow (index) >0)
                  {
                    index [, timestamp_loc := timestamp_vn]
                  }
                  if (nchar (pCode) >0) {
                    index [, code := pCode]
                  }
                  index [, timestamp_IND := timestamp_loc]
                  index [, datetime := timestamp_loc]
                  index [,.(code, date, timestamp_IND, close,datetime)]
                  index = DBL_CALCULATE_INTRADAY_VOLATILITY  ( pSource = 'DNSE', index)
                  index [, codesource := my_code]
                  MY_DATA [[i]] = index
                  
                  Sys.sleep(10)
                }
                data = rbindlist (MY_DATA, fill =T)
                
                # file.remove(paste0(Folder_To,File_Day))
                # file.remove(paste0(Folder_To,File_History))
                
                if (ToIntegrate & (nchar(Folder_To)* nchar( File_Day)) >0 & nrow (data) >0) 
                {
                  DATA_OLD = CHECK_CLASS (try(CCPR_READRDS(Folder_To, File_Day)))
                  DATA_OLD [, datetime := as.character (datetime)]
                  DATA_NEW = rbind (DATA_OLD, data, fill = T)
                  DATA_NEW = unique (DATA_NEW, fromLast = T, by = c('code','date','timestamp'))
                  DATA_NEW = DATA_NEW [!is.na(timestamp)]
                  # DATA_NEW [code_und == 'STKVNTCB']
                  CCPR_SAVERDS (DATA_NEW, Folder_To, File_Day, ToSummary = T, SaveOneDrive = T)
                  if (nchar (File_History) > 0) { 
                    DATA_HISTORY = CHECK_CLASS (try(CCPR_READRDS(Folder_To, File_History)))
                    DATA_HISTORY [, datetime := as.character (datetime)]
                    DATA_NEW = rbind (DATA_HISTORY, data, data, fill = T)
                    DATA_NEW = unique (DATA_NEW, fromLast = T, by = c('code','date','timestamp'))
                    CCPR_SAVERDS (DATA_NEW, Folder_To, File_History, ToSummary = T, SaveOneDrive = T)
                  }
                }
                
              }else { CATln_Border('ALL INDEXES ARE UPDATED...')}
            }
          },
          ''
  )
  
  x = DBL_SUMMARY_UPDATED(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_volatility_intraday_performance.rds'))
  difference = difftime(Sys.time(), x, units='mins')
  if ( difference > 10)
  {
    x= CHECK_CLASS(try(CCPR_READRDS ('S:/CCPR/DATA/DASHBOARD_LIVE/','dbl_indbeq_volatility_intraday_history.rds')))
    x = x [code != 'INDVLINA']
    if (nrow (x)>0)
    {
      
      y = setDT(x %>% group_by(code) %>% filter(date < (max(date)) ))
      # y = unique (y [order (-timestamp)] , by = 'code')
      y = y [!is.na (close) & !is.na (close_und)]
      y = unique (y [order (-timestamp)] , by = 'code')
      
      y [, reference := close]
      y [, last := close_und]
      xx = unique (x [order (-timestamp)], by = 'code')
      my_data =merge ( xx , y [,.(code, reference, last)], all.x = T, by = 'code')
      my_data[grepl('STK',code), type := 'STK']
      my_data [is.na(type), type := 'IND']
      my_data [, volat_pc := (close/reference - 1)*100]
      my_data [, und_pc := (close_und/last - 1)*100]
      unique (my_data[!is.na (close) & !is.na (close_und)], by = 'code') 
      CCPR_SAVERDS(my_data[!is.na(country)], 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_volatility_intraday_performance.rds', ToSummary = T)
    }
  }
  try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0('DBL_YAH_INTRADAY_LOOP>',pOption), pAction="SAVE", NbSeconds=1, ToPrint=F))
}

# ==================================================================================================
DBL_CALCULATE_INTRADAY_VOLATILITY = function (index = data.table(), LIST_CODES = list(),
                                              Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_source_ins_intraday_5m_day.rds',
                                              To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_indbeq_intraday_volatility_day.rds')  {
  # ------------------------------------------------------------------------------------------------
  # Fr_File = 'dbl_yah_ins_intraday_day.rds'
  # pSource = 'DNSE'
  start = Sys.time()
  
  if (nrow (index) ==0)
  { 
    index = try(CCPR_READRDS (Fr_Folder, Fr_File, ToKable = T))
    index = CLEAN_TIMESTAMP(index)
    # My.Kable (index [order (-timestamp)])
  }
  
  data = data.table ()
  if (nrow (index) >0)
  {
    index = index [!is.na(code) & !is.na(close)& close != 0]
    if (length (LIST_CODES) >0)
    {
      index = index [code %in% LIST_CODES]
      index = CLEAN_TIMESTAMP(index)
    }
    # My.Kable.Min (index [order (-timestamp)])
  }
  
  index [, check_time := str_sub (timestamp_vn, -5, -4)]
  index [!as.integer(check_time) %% 5]
  index [as.integer(check_time) %% 5 == 0, to_select := 1]
  index [check_time == '00', to_select := 1]
  index = index [!is.na (to_select)]
  index = DBL_CLEAN_OHLC(index)
  index = index [ !grepl ('VIX', name)]
  index = index [ !grepl ('VOLATILITY', name)]
  index [code == 'INDVXN']
  # index [code == 'CURCRYUSDCUSD']

  # unique (index , by = 'type')
  if (all((nrow (index) >0 & c('code', 'date', 'timestamp', 'close') %in% names (index) ) ) )
  {
    index [,timestamp_IND := timestamp ]
    data = index[, .(code, date, timestamp_IND, close,timestamp, timestamp_vn)][order(code, date,timestamp_IND)]
    # My.Kable.Min (data [order (-timestamp)])
    # My.Kable (data [order (close)])
    # data = data [order (code, date, -timestamp)]
    # data = data [close != 0]
    # # str(data)
    # data[ , ":="(rt = close/shift(close) - 1, lnrt = log(close/shift(close))), by = c("code", "date")]
    # 
    # My.Kable.All (data [date >= '2024-09-03'])
    # data[ , ":="(rt = close/shift(close) - 1), by = c("code", "date")]
    data = data %>%
      group_by(code, date) %>%
      mutate (rt = (close/shift(close) - 1) )
    
    data = data %>%
      group_by(code, date) %>%
      mutate (lnrt = log(close/shift(close))) 
    data = setDT (data)
    data = data[!is.na(lnrt)]
    max_tick = max(data[, .(n = .N), by = c('date', 'code')]$n)
    
    # My.Kable(data [code == 'INDVLICURCRYETH'])
    
    tick_by_day = round(24*60/5,0)
    adjust = 250*tick_by_day
    data = data[order(timestamp_IND)]
    data = data [!is.na(lnrt)][order(date, timestamp_IND)]
    # data [order (lnrt)]
    # data[, volatility := 100*sqrt(adjust)*roll_sd(lnrt, width = max_tick, min_obs = max_tick - 2 ) ]
    data = data[!is.na(lnrt)] %>%
      group_by(code) %>%
      mutate (volatility = 100*sqrt(adjust)*roll_sd(lnrt, width = max_tick, min_obs = max_tick - 2 ) ) 
    data = setDT (data)
    data [, close_und := close]
    data [, code_und := code]
    data [, close  := volatility]
    data [, code := paste0('INDVLI',code_und)]
    data [, timestamp := NULL]
    setnames (data , 'timestamp_IND', 'timestamp')
    data [, volatility := NULL]
    data [, rt := NULL]
    data [, lnrt := NULL]
    data = data [!is.na (close) & !is.na (timestamp)]
    # My.Kable(data [order (-timestamp)], Nb = 10)
    
    DBL_RELOAD_INSREF()
    data = merge (data [, -c('iso2','iso3','country','continent','name_und')],ins_ref [,.(code_und = code, iso2, iso3, country, continent, name_und = name)], all.x = T, by = 'code_und' ) 
    data [, ':=' (type = 'IND', fcat = 'INDVLI')]
    data [, name:=  paste ('IFRC/BEQ INTRADAY VOLATILITY', name_und)]
    data [, hhmm := substr(timestamp, 12, 16)]
    data = CLEAN_TIMESTAMP (data)
    
    data[, updated := SYS.TIME()]
    data = CLEAN_TIMESTAMP(data)
    data = data %>% select ('code', 'name', 'code_und', 'name_und', 'iso2','iso3', 'country', 'continent', 'date','timestamp', 'hhmm','close','close_und', everything())
    # My.Kable(data [order (timestamp)] [,-c('iso2','iso3','continent', 'type','fcat')], Nb = 10)
    My.Kable(unique(data [order (code,-timestamp)] [,-c('iso2','iso3','continent', 'type','fcat','name_und')], by = 'code') [order(-timestamp_vn)], Nb = 10)
    data [, name:= toupper (name)]
    data [, name_und := toupper (name_und)]
    data [, timestamp_vn := timestamp_vn  ]
    
    # if (nchar (To_Folder) * nchar (To_File) >0)
    # {
    #   DATA_OLD = CHECK_CLASS (try (CCPR_READRDS (To_Folder, To_File, ToKable = T) ) )
    #   DATA_OLD = CLEAN_TIMESTAMP (DATA_OLD)
    #   DATA_OLD = rbind (DATA_OLD, data, fill = T)
    #   if (nrow (DATA_OLD) >0)
    #   {
    #     DATA_OLD = unique (DATA_OLD, by = c('code', 'date', 'timestamp')) [!is.na(code) & !is.na(close) & !is.na(date)& !is.na(timestamp)]
    #     DATA_OLD = CLEAN_TIMESTAMP (DATA_OLD)
    #     CCPR_SAVERDS ( data, To_Folder, To_File, ToSummary = T, SaveOneDrive = T)
    #   }
    # }
    # unique (data [code == 'INDVLICURCRYETH'], by = 'date')
    # CCPR_SAVERDS (data,To_Folder,To_File )
    DBL_INTEGRATE_INTRADAY  (data = data,
                             Fr_Folder = '', Fr_File = '',
                             To_Folder = To_Folder, To_File = To_File)
    
    if (runif (1,1,100) > 75)
    {
      DBL_INTEGRATE_INTRADAY  (data = data,
                               Fr_Folder = '', Fr_File = '',
                               To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = gsub ('_day.rds','_history.rds', To_File) )
    }
  }
  
  end = Sys.time ()
  CATln_Border( paste('DURATION = ', Format.Number (difftime (end, start, units = 'secs'), 0) ) )
  return(data)
}