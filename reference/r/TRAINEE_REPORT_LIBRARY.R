# ==================================================================================================
# TRAINEE_REPORT_LIBRARY.R - 2024-07-25 10:50
# ==================================================================================================
# ..................................................................................................
# ..................................................................................................
# source("c:/R/TRAINEE_CONFIG.r", echo=T)
# print(SDrive)
# print(ODDrive)
# print(ODLibrary)
# print(SDLibrary)
# print(ODData)

# source(paste0(SDLibrary, "TRAINEE_INTRO.R"),                                                 echo=F)
# source(paste0(SDLibrary, "TRAINEE_LIBRARY.R"),                                               echo=F)
# ..................................................................................................

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# LOAD LIBRARY
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# ==================================================================================================

LastTrading     = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))

# ==================================================================================================
DBL_DUO_REPORT_FILES = function(Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', List_Src = c("CNBC", "CBOE", "YAH", "MULTI", "SOURCE")){
  # ------------------------------------------------------------------------------------------------
  # List_Src = c("CNBC", "CBOE", "YAH", "DNSE", "RTS", "MULTI", "SOURCE")
  # List_Prefix = c('INS', 'TOP', 'NOTOP')
  # List_fre_1 = c('LAST', 'INTRADAY', 'DAY')
  # List_fre_2 = c('TODAY', 'DAY', 'HISTORY')
  # 
  # xList = list()
  # result_table = data.table(folder = character(), filename = character(), source = character(),prefix = character(), type = character(), 
  #                           day = character(), had = character(), date = character(), updated = character(), delay = numeric())
  # 
  # for (isrc in 1:length(List_Src)) {
  #   pSource = List_Src[[isrc]]
  #   prefixes_to_use = if (pSource %in% c("SOURCE", "MULTI")) 'INS' else List_Prefix
  #   
  #   for (prefix in prefixes_to_use) {
  #     for (i in 1:length(List_fre_1)) {
  #       lv1 = List_fre_1[[i]]
  #       for (k in 1:length(List_fre_2)) {
  #         lv2 = List_fre_2[[k]]
  #         file_path = paste0(Folder, 'DBL_', pSource, '_', prefix, '_', lv1, '_', lv2, '.rds')
  #         
  #         if (file.exists(file_path)) {
  #           yes_val = 1
  #           file_data = DBL_CCPR_READRDS(Folder, paste0('DBL_', pSource, '_', prefix, '_', lv1, '_', lv2, '.rds'), ToRestore = T)
  #           file_date = max(file_data$date, na.rm = TRUE)
  #           updated = format(file.info(file_path)$mtime, "%Y-%m-%d %H:%M:%S")
  #           
  #           if (lv2 == 'DAY' | lv2 == 'TODAY') {
  #             sys_time = Sys.time()
  #             delay = round(as.numeric(difftime(sys_time, as.POSIXct(updated, format="%Y-%m-%d %H:%M:%S"), units = "min")),2)
  #           } else {
  #             delay = NA
  #           }
  #           
  #         } else {
  #           yes_val = 0
  #           file_date = NA
  #           updated = NA
  #           delay = NA
  #         }
  #         
  #         result_table = rbind(result_table, data.table(folder = Folder, 
  #                                                       filename = paste0('DBL_', pSource, '_', prefix, '_', lv1, '_', lv2, '.rds'),
  #                                                       source = pSource, prefix = prefix, type = lv1, day = lv2, had = yes_val, 
  #                                                       date = as.character(file_date), 
  #                                                       updated = as.character(updated),
  #                                                       delay = delay))
  #       }
  #     }
  #   }
  # }
  # 
  # result_table[, type := factor(type, levels = List_fre_1)]
  # result_table[, day := factor(day, levels = List_fre_2)]
  # result_table[, prefix := factor(prefix, levels = List_Prefix)]
  # 
  # result_table = result_table[order(-delay, prefix, type, day)]
  # 
  # # x = readRDS('S:/CCPR/DATA/DASHBOARD_LIVE/report_files_duo.rds')
  # # result_table = x
  # library(fst)
  # write_fst(result_table, "S:/SHINY/REPORT/DUO_FILES/report_file_duo.fst")
  # 
  # CCPR_SAVERDS(result_table, 'S:/SHINY/REPORT/DUO_FILES/','report_file_duo.rds')
  # 
  # library(data.table)
  # library(DBI)
  # library(RMySQL)
  # library(RMariaDB)
  # library(DT)
  # connection <- dbConnect(
  #   dbDriver("MySQL"), 
  #   host = "192.168.1.105", 
  #   port = 3306,
  #   user = "intranet_admin", 
  #   password = "admin@123456", 
  #   db = "intranet_dev"
  # )
  # #data = readRDS("S:/SHINY/REPORT/REPORT_WCEO.rds")
  # # data = readRDS("S:/STKVN/PRICES/REPORTS/process_pricesday.rds")
  # #  data = readRDS('S:/WCEO_INDEXES/REPORT_WCEO.rds')
  # # current_date <- Sys.Date()
  # # cols_to_update <- setdiff(names(data), c("dataset", "date", "updated", "id"))
  # # data[data$id == 3, cols_to_update] <- current_date
  # 
  # My.Kable.All(result_table)
  # 
  # result = dbWriteTable(connection, 'report_file_duo', result_table, row.names = FALSE, overwrite = TRUE)
  # print(result)
  # dbDisconnect(connection)   
  Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/'
  file_str = setDT(read.xlsx(paste0(ODDrive,"/DBL/RD_BATCH_MANAGEMENT.xlsx"), sheet = 'FILES_STR'))
  header_row = file_str[1, ]
  new_colnames = paste(names(file_str), header_row, sep = "_")
  colnames(file_str) = new_colnames
  setnames(file_str,'X1_SOURCE','SOURCE')
  setnames(file_str,'X2_PREFIX','PREFIX')
  file_str = file_str[-1, ]
  
  for (i in 1:nrow(file_str))
  {
    for (j in 3:ncol(file_str))
    {
      # i = 1
      # j = 6
      file_name = file_str[i,..j]
      if (file.exists(paste0(Folder, paste0(file_name,'.rds')))){
        data = DBL_CCPR_READRDS(Folder, paste0(file_name,'.rds'))
        data = unique(data[order(-date)], by = 'code')
        if (grepl('_TODAY',file_name)){
          if ('timestamp_vn' %in% names(data) & (any(!is.na(data$timestamp_vn)))){
            last_timestamp = max(data$timestamp_vn, na.rm = T)
          } else {
            last_timestamp = NA
          }
          
          if ('updated' %in% names(data) & (any(!is.na(data$updated)))){
            last_update = max(data$updated, na.rm = T)
          } else {
            last_update = NA
          }
          
          if (!is.na(last_timestamp) & !is.na(last_update)){
            diff = as.numeric(difftime(Sys.time(), as.POSIXct(last_update, format="%Y-%m-%d %H:%M:%S"), units = "min"))
            if (!is.na(diff)){
              if (diff >= 10) {
                file_str[i, (j) := paste0(last_timestamp,' ***')]
              } else {
                file_str[i, (j) := last_timestamp]
              }
            }
          } else {
            if (any(!is.na(data$date))){
              file_str[i, (j) :=  max(data$date, na.rm = T)]
            } else {
              if ('updated' %in% names(data)){
                file_str[i, (j) :=  max(substr(data$updated,1,10), na.rm = T)]
              }
            }
          }
        }
        
        if (grepl('_DAY',file_name) | grepl('_HISTORY',file_name)){
          max_date = max(data$date, na.rm = T)
          
          diff = as.numeric(difftime(Sys.Date(), as.POSIXct(max_date, format="%Y-%m-%d"), units = "day"))
          if (!is.na(diff)){
            if (diff > 1) {
              file_str[i, (j) := paste0(max_date,' ***')]
            } else {
              file_str[i, (j) := max_date]
            }
          }
        }
      } else {
        file_str[i, (j) := "."] 
      }
    }
  }
  CCPR_SAVERDS(file_str, 'S:/SHINY/REPORT_FILES/DUO_FILES/','report_file_duo.rds')
  
  library(fst)
  write_fst(file_str, "S:/SHINY/REPORT_FILES/DUO_FILES/report_file_duo.fst")
  
  library(data.table)
  library(DBI)
  library(RMySQL)
  library(RMariaDB)
  library(DT)
  connection <- dbConnect(
    dbDriver("MySQL"), 
    host = "192.168.1.105", 
    port = 3306,
    user = "intranet_admin", 
    password = "admin@123456", 
    db = "intranet_dev"
  )
  #data = readRDS("S:/SHINY/REPORT/REPORT_WCEO.rds")
  # data = readRDS("S:/STKVN/PRICES/REPORTS/process_pricesday.rds")
  #  data = readRDS('S:/WCEO_INDEXES/REPORT_WCEO.rds')
  # current_date <- Sys.Date()
  # cols_to_update <- setdiff(names(data), c("dataset", "date", "updated", "id"))
  # data[data$id == 3, cols_to_update] <- current_date
  
  My.Kable.All(file_str)
  
  result = dbWriteTable(connection, 'report_file_duo', file_str, row.names = FALSE, overwrite = TRUE)
  print(result)
  dbDisconnect(connection)   
  return(file_str)
}


# ==================================================================================================
FINAL_VALUE = function (DATA_ALL, DATASET = 'SHARESOUT', order_list = list('CAF', 'VND', 'C68', 'DNSE', 'STB')) {
  # ------------------------------------------------------------------------------------------------
  
  fields = c('code', 'date', 'source', tolower(DATASET))
  DATA_ALL = DATA_ALL[, ..fields]
  
  list_source = unique(DATA_ALL, by = 'source')$source
  additional_sources = setdiff(list_source, order_list)
  order_list = c(order_list, additional_sources)
  
  col = setdiff(names(DATA_ALL), c('code', 'date', 'source'))
  DATA_ALL = melt(DATA_ALL, id.vars = c('code', 'date', 'source'), measure.vars = col, 
                  variable.name = 'dataset', value.name = 'final')
  DATA_ALL = unique(DATA_ALL, by = c('code', 'date', 'source'))
  
  DATA_SOURCE = dcast(DATA_ALL, code + date ~ source, value.var = 'final')
  
  ALL_NOT_NA = DATA_ALL[!is.na(final)]
  ALL_NOT_NA = ALL_NOT_NA[, .(code, date, final, n_have = .N), by = .(code, date)]
  ALL_NOT_NA = ALL_NOT_NA[, .(n_have, n = .N), by = .(code, date, final)]
  ALL_NOT_NA[, per := floor((as.numeric(n) / as.numeric(n_have)) * 100)]
  
  ALL_FINAL = ALL_NOT_NA[per > 50]
  ALL_FINAL = unique(ALL_FINAL, by = c('code', 'date'))
  
  DATA = merge(DATA_SOURCE, ALL_FINAL[, .(code, date, final)], by = c('code', 'date'), all.x = T)
  X = DATA[is.na(final)]
  col_source = setdiff(names(DATA_SOURCE), c('code', 'date', 'final'))
  for (col in order_list) {
    if (col %in% col_source) {
      X[is.na(final), final := get(col)]
    }
  }
  DATA_ALL = rbind(DATA[!is.na(final)], X, fill = T)
  
  final_count = DATA_ALL[, lapply(.SD, function(x) sum(x == final, na.rm = TRUE)), 
                         by = .(code, date, final), .SDcols = col_source]
  
  final_count[, nb := rowSums(.SD), .SDcols = col_source]
  final_data = merge(DATA_ALL, final_count[, .(code, date, final, nb)], by = c("code", "date", "final"), all.x = TRUE)
  final_data[, close := final]
  
  return(final_data)
}

# ==================================================================================================
DBL_EXTRACT_INTRADAY_NB_DAYS = function(File_Folder = 'S:/STKVN/INDEX_2024/', File_Name = 'DOWNLOAD_ENT_VNI_INTRADAY_PRICES.RDS', Nb_Days = 5){
  # ------------------------------------------------------------------------------------------------
  xnDays   = data.table()
  FullPath = paste0(File_Folder, File_Name)
  if (file.exists(FullPath))
  {
    x = CHECK_CLASS(try(CCPR_READRDS(File_Folder, File_Name, ToKable = T, ToRestore = T)))
    if (nrow(x)>0)
    {
      if ('volume' %in% names(x)) { x[, volume:=as.numeric(volume)]}
      x = CLEAN_TIMESTAMP(x)
      x$hhmmss = NULL
      
      xDay = unique(x, by=c('code', 'date'))[, .(code, date)][order(code, -date)]
      xDay[, seq:=seq.int(1, .N), by='code']
      x = merge(x[, -c('seq')], xDay[, .(code, date, seq)], all.x = T, by=c('code', 'date'))
      str(x)
      My.Kable(x)
      xnDays = x[seq<=Nb_Days]
      if ('timestamp_vn' %in% names(xDay)){
        xnDays[, hhmm:=substr(timestamp_vn, 12,16)]
      }
      My.Kable(xnDays)
    }
  }
  return(xnDays)
}


# ==================================================================================================
LOOP_DOWNLOAD_INDVN_INTRADAY = function(){
  # ------------------------------------------------------------------------------------------------
  # x = readRDS("S:/STKVN/PRICES/data_source_by_market_day.rds")
  # x[, ticker := gsub('STKVN','',code)]
  # list_codes = unique(x$ticker)
  list_codes = setDT(fread('S:/STKVN/vn30.txt'))[index == 'INDVN30']$ticker
  for (SOURCE in list('VND', 'C68', 'CAF')){
    # SOURCE = 'C68'
    TODO_SOURCE =  try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0('INTRADAY_STKVN > ',SOURCE), pAction="COMPARE", NbSeconds=3*60*60, ToPrint=F))
    if (TODO_SOURCE){
      TODO_SOURCE =  try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0('INTRADAY_STKVN > ',SOURCE), pAction="SAVE", NbSeconds=30*60, ToPrint=F))
      xList = list()
      for (i in 1:length(list_codes)){
        # i = 1
        code = list_codes[[i]]
        My_Data = CHECK_CLASS(try(TRAINEE_DOWNLOAD_SOURCE_STKVN_PRICESINTRADAY_BY_CODE (pSource = SOURCE, pCode=code, NbDaysBack = 10000)))
        if (nrow(My_Data) > 0){
          xList[[i]] = My_Data
        }
      }
      Final_data = rbindlist(xList, fill = T)
      if (nrow(Final_data) > 0){
        CCPR_SAVERDS(Final_data,'S:/STKVN/PRICES/DAY/',paste0('DOWNLOAD_',SOURCE,'_STKVN_PRICES_INTRADAY.rds'), ToSummary = T, SaveOneDrive = T)
      }
    }
  }
  
  # TODO_DN =  try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DOWNLOAD_TELEGRAM', pAction="COMPARE", NbSeconds=15*60, ToPrint=F))
  # if (TODO_DN){
  #   for (code in list('^VXN', '^VIX', '^GSPC', '^NDX', '^DJI', '^VXD')){
  #     try(TELEGRAM_DOWNLOAD_BY_CODE(pCode = code, RemoveNA = T))
  #   }
  #   TODO = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DOWNLOAD_TELEGRAM', pAction="SAVE", NbSeconds=1, ToPrint=F))
  # }  
  
  
  TODO_ENT =  try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DOWNLOAD_ENTRADE_INTRDAY', pAction="COMPARE", NbSeconds=30*60, ToPrint=F))
  
  if (TODO_ENT){
    mydata = try(DOWNLOAD_ENTRADE_INDEX_INTRADAY(codesource = 'VN30', code = 'INDVN30', NbMinutes   = 5*24*60, OffsetDays  = 0,
                                                 Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = 'DOWNLOAD_ENT_VN30_INTRADAY_PRICES.RDS',
                                                 Save_history = ''))
    
    mydata = try(DOWNLOAD_ENTRADE_INDEX_INTRADAY(codesource = 'VNINDEX', code = 'INDVNINDEX', NbMinutes   = 5*24*60, OffsetDays  = 0,
                                                 Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = 'DOWNLOAD_ENT_VNI_INTRADAY_PRICES.RDS',
                                                 Save_history = ''))
    
    mydata = try(DOWNLOAD_ENTRADE_INDEX_INTRADAY(codesource = 'HNX30', code = 'INDHNX30', NbMinutes   = 5*24*60, OffsetDays  = 0,
                                                 Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = 'DOWNLOAD_ENT_HNX30_INTRADAY_PRICES.RDS',
                                                 Save_history = ''))
    
    mydata = try(DOWNLOAD_ENTRADE_INDEX_INTRADAY(codesource = 'HNX', code = 'INDVHINDEX', NbMinutes   = 5*24*60, OffsetDays  = 0,
                                                 Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = 'DOWNLOAD_ENT_HNX_INTRADAY_PRICES.RDS',
                                                 Save_history = ''))
    
    for (PREFIX in list('VNI', 'VN30', 'HNX', 'HNX30'))
    {
      DBL_INTEGRATE_INTRADAY (data = data.table(),
                              Fr_Folder = 'S:/STKVN/INDEX_2024/', Fr_File = paste0('DOWNLOAD_ENT_', PREFIX, '_INTRADAY_PRICES.RDS'),
                              To_Folder = 'S:/STKVN/INDEX_2024/', To_File = paste0('DOWNLOAD_ENT_', 'INDVN', '_INTRADAY_PRICES.RDS')) 
    }
    
    for (PREFIX in list('VNI', 'VN30', 'HNX', 'HNX30'))
    {
      DBL_INTEGRATE_INTRADAY (data = data.table(),
                              Fr_Folder = 'S:/STKVN/INDEX_2024/', Fr_File = paste0('DOWNLOAD_ENT_', PREFIX, '_INTRADAY_PRICES.RDS'),
                              To_Folder = 'S:/STKVN/INDEX_2024/', To_File = paste0('DOWNLOAD_ENT_', PREFIX, '_INTRADAY_HISTORY.RDS')) 
    }
    
    DBL_INTEGRATE_INTRADAY (data = data.table(),
                            Fr_Folder = 'S:/STKVN/INDEX_2024/', Fr_File = paste0('DOWNLOAD_ENT_', 'INDVN', '_INTRADAY_PRICES.RDS'),
                            To_Folder = 'S:/STKVN/INDEX_2024/', To_File = paste0('DOWNLOAD_ENT_', 'INDVN', '_INTRADAY_HISTORY.RDS')) 
    
    FIVE_DAYS = DBL_EXTRACT_INTRADAY_NB_DAYS(File_Folder = 'S:/STKVN/INDEX_2024/', File_Name = 'DOWNLOAD_ENT_INDVN_INTRADAY_PRICES.rds', Nb_Days = 5)

    DBL_INTEGRATE_INTRADAY (data = FIVE_DAYS,
                            Fr_Folder = '', Fr_File = '',
                            To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_source_ins_intraday_day.rds')

    index = try(DBL_CALCULATE_INTRADAY_VOLATILITY (index = data.table(), LIST_CODES = list(),
                                                   Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', Fr_File = 'dbl_source_ins_intraday_day.rds',
                                                   To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_indbeq_intraday_volatility_day.rds'))
    # 
    # 
    INTRADAY_TODAY = DBL_EXTRACT_INTRADAY_NB_DAYS(File_Folder = 'S:/STKVN/INDEX_2024/', File_Name = 'DOWNLOAD_ENT_INDVN_INTRADAY_PRICES.rds', Nb_Days = 1)
    
    DAY = DBL_CCPR_READRDS('S:/STKVN/INDEX_2024/','DOWNLOAD_ENT_INDVN_DAY_HISTORY.RDS', ToRestore = T)[order(code,date)]
    # DAY[, pdate := shift(date), by = 'code']
    # DAY[, pclose := shift(close), by = 'code']
    
    RELOAD_CALENDAR_VN()
    
    INTRADAY_TODAY = merge(INTRADAY_TODAY, idx_calendar.vn[, .(date,pdate = prv_date)], all.x = T, by = 'date')
    
    INTRADAY_TODAY = merge(INTRADAY_TODAY[,-c('pclose')], DAY[,. (code,pdate = date,pclose = close)], by = c('code', 'pdate'), all.x = T)
    
    INTRADAY_TODAY = INTRADAY_TODAY[order(code, date)]
    INTRADAY_TODAY[, ':=' (change = close - pclose,
                           varpc  = (close/pclose - 1)*100
    ), by = 'code']
    
    DAY_TODAY = DAY[date == max(date)]
    LAST_TODAY = unique(INTRADAY_TODAY[order(code, -timestamp)], by = 'code')
    INTRADAY_TODAY = UPDATE_UPDATED(INTRADAY_TODAY) 
    DAY_TODAY      = UPDATE_UPDATED(DAY_TODAY)
    LAST_TODAY     = UPDATE_UPDATED(LAST_TODAY)

    
    try(DBL_CCPR_SAVERDS(INTRADAY_TODAY, 'S:/CCPR/DATA/DASHBOARD_LIVE/','DBL_DNSE_TOP_INTRADAY_TODAY.rds', ToSummary = T, SaveOneDrive = T))
    try(DBL_CCPR_SAVERDS(LAST_TODAY, 'S:/CCPR/DATA/DASHBOARD_LIVE/','DBL_DNSE_TOP_LAST_TODAY.rds', ToSummary = T, SaveOneDrive = T))
    try(DBL_CCPR_SAVERDS(DAY_TODAY, 'S:/CCPR/DATA/DASHBOARD_LIVE/','DBL_DNSE_TOP_DAY_TODAY.rds', ToSummary = T, SaveOneDrive = T))
    
    try(DBL_CCPR_SAVERDS(INTRADAY_TODAY, 'S:/CCPR/DATA/DASHBOARD_LIVE/','DBL_DNSE_INS_INTRADAY_TODAY.rds', ToSummary = T, SaveOneDrive = T))
    try(DBL_CCPR_SAVERDS(LAST_TODAY, 'S:/CCPR/DATA/DASHBOARD_LIVE/','DBL_DNSE_INS_LAST_TODAY.rds', ToSummary = T, SaveOneDrive = T))
    try(DBL_CCPR_SAVERDS(DAY_TODAY, 'S:/CCPR/DATA/DASHBOARD_LIVE/','DBL_DNSE_INS_DAY_TODAY.rds', ToSummary = T, SaveOneDrive = T))
    
    # day
    if (CHECK_TIMEBETWEEN('15:00','23:00')){
      mydata = try(DOWNLOAD_ENTRADE_INDEX_DAY(codesource = 'VN30', code = 'INDVN30', NbMinutes   = 5*24*60, OffsetDays  = 0,
                                              Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = 'DOWNLOAD_ENT_VN30_DAY_PRICES.RDS',
                                              Save_history = 'DOWNLOAD_ENT_INDVN_DAY_HISTORY.RDS'))
      
      mydata = try(DOWNLOAD_ENTRADE_INDEX_DAY(codesource = 'VNINDEX', code = 'INDVNINDEX', NbMinutes   = 5*24*60, OffsetDays  = 0,
                                              Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = 'DOWNLOAD_ENT_VNI_DAY_PRICES.RDS',
                                              Save_history = 'DOWNLOAD_ENT_INDVN_DAY_HISTORY.RDS'))
      
      mydata = try(DOWNLOAD_ENTRADE_INDEX_DAY(codesource = 'HNX30', code = 'INDHNX30', NbMinutes   = 5*24*60, OffsetDays  = 0,
                                              Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = 'DOWNLOAD_ENT_HNX30_DAY_PRICES.RDS',
                                              Save_history = 'DOWNLOAD_ENT_INDVN_DAY_HISTORY.RDS'))
      
      mydata = try(DOWNLOAD_ENTRADE_INDEX_DAY(codesource = 'HNX', code = 'INDVHINDEX', NbMinutes   = 5*24*60, OffsetDays  = 0,
                                              Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = 'DOWNLOAD_ENT_HNX_DAY_PRICES.RDS',
                                              Save_history = 'DOWNLOAD_ENT_INDVN_DAY_HISTORY.RDS'))
    }

    
    TODO_ENT =  try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DOWNLOAD_ENTRADE_INTRDAY', pAction="SAVE", NbSeconds=15*60, ToPrint=F))
    
  }
  
  TODO_ENT_STK =  try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DOWNLOAD_ENTRADE_STKVN_INTRDAY', pAction="COMPARE", NbSeconds=1*60, ToPrint=F))
  if (TODO_ENT_STK){
    list_codes = setDT(fread('S:/STKVN/vn30.txt'))[index == 'INDVN30']
    # xList = list
    for (i in 1:nrow(list_codes)){
      day = try(DOWNLOAD_ENTRADE_INDEX_INTRADAY(type = "stock", codesource = list_codes$ticker[i], code = paste0('STKVN', list_codes$ticker[i]), NbMinutes   = 11*365*24*60, OffsetDays  = 0,
                                                Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = 'DOWNLOAD_ENT_STKVN_INTRADAY_PRICES.RDS',
                                                Save_history = 'DOWNLOAD_ENT_STKVN_VN30_INTRADAY_HISTORY.RDS'))
      # 
      # FIVE_DAYS = DBL_EXTRACT_INTRADAY_NB_DAYS(File_Folder = 'S:/STKVN/INDEX_2024/', File_Name = 'DOWNLOAD_ENT_STKVN_VN30_INTRADAY_HISTORY.RDS', Nb_Days = 5)
      # 
      # DBL_INTEGRATE_INTRADAY (data = FIVE_DAYS,
      #                         Fr_Folder = '', Fr_File = '',
      #                         To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_source_ins_intraday_day.rds') 
      
      
      if (CHECK_TIMEBETWEEN('17:00','23:00')){
        intraday = try(DOWNLOAD_ENTRADE_INDEX_DAY(type = "stock", codesource = list_codes$ticker[i], code = paste0('STKVN', list_codes$ticker[i]), NbMinutes   = 11*365*24*60, OffsetDays  = 0,
                                                  Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = 'DOWNLOAD_ENT_STKVN_DAY_PRICES.RDS',
                                                  Save_history = 'DOWNLOAD_ENT_STKVN_VN30_DAY_HISTORY.RDS'))
      }
      
    }
    TODO_ENT_STK =  try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DOWNLOAD_ENTRADE_STKVN_INTRDAY', pAction="SAVE", NbSeconds=15*60, ToPrint=F))
  }
  
  MERGE_FINAL = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='MERGE_FINAL', pAction="COMPARE", NbSeconds=6*60*60, ToPrint=F))
  if (MERGE_FINAL){
    
    try(TRAINEE_DOWNLOAD_INDVN_BY_SOURCE(source = 'CAF', Save_Folder = 'S:/STKVN/INDEX_2024/', ToIntergrate = T))
    try(TRAINEE_DOWNLOAD_INDVN_BY_SOURCE(source = 'FANT', Save_Folder = 'S:/STKVN/INDEX_2024/', ToIntergrate = T))
    try(TRAINEE_DOWNLOAD_INDVN_BY_SOURCE(source = 'VND', Save_Folder = 'S:/STKVN/INDEX_2024/', ToIntergrate = T))
    try(TRAINEE_DOWNLOAD_INDVN_BY_SOURCE(source = 'STB', Save_Folder = 'S:/STKVN/INDEX_2024/', ToIntergrate = T))
    
    DATA1 = CCPR_READRDS('S:/STKVN/INDEX_2024/', 'DOWNLOAD_VND_INDVN_PRICES.rds', ToKable=T, ToRestore = T)
    DATA2 = CCPR_READRDS('S:/STKVN/INDEX_2024/', 'DOWNLOAD_CAF_INDVN_PRICES.rds', ToKable=T, ToRestore = T)
    DATA3 = CCPR_READRDS('S:/STKVN/INDEX_2024/', 'DOWNLOAD_FANT_INDVN_PRICES.rds', ToKable=T, ToRestore = T)
    DATA4 = CCPR_READRDS('S:/STKVN/INDEX_2024/', 'DOWNLOAD_STB_INDVN_PRICES.rds', ToKable=T, ToRestore = T)
    ENT = CCPR_READRDS('S:/STKVN/INDEX_2024/', 'DOWNLOAD_ENT_INDVN_DAY_HISTORY.rds', ToKable=T, ToRestore = T)
    
    DATA = rbind(DATA1,DATA2, DATA3, fill = T)
    DATA = rbind(DATA, DATA4, fill = T)
    DATA = rbind(DATA, ENT, fill = T)
    close = FINAL_VALUE ( DATA, DATASET = 'CLOSE' , order_list = list('CAF', 'VND', 'FANT', 'STB', 'ENT'))
    
    max_updated_by_code = setDT(DATA %>%
                                  group_by(code) %>%
                                  summarize(updated = max(updated, na.rm = TRUE)))
    
    close = merge(close, max_updated_by_code, all.x = T, by = 'code')
    saveRDS(close, 'S:/STKVN/INDEX_2024/DOWNLOAD_SOURCES_INDVN_HISTORY.rds')
    My.Kable(close[order(date, code)], Nb = 10)
    MERGE_FINAL = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='MERGE_FINAL', pAction="SAVE", NbSeconds=15*60, ToPrint=F))
  }
}

# ==================================================================================================
FINAL_CLOSE_BY_FILES_SOURCES = function (Folder      = UData, type = 'IND',
                                         List_source = list('WEXC','CAF','YAH', 'RTS', 'BIN', 'ENX', 'INV'),
                                         File        = 'DOWNLOAD_XXX_INDHOME_HISTORY.rds',
                                         Save_Folder = '',
                                         Save_File   = '',
                                         MAX_VAL     = 0.2, MAX_RT = 0.5, Rounding = 16,
                                         ToMerge     = T,
                                         ToForce     = T) {
  # ------------------------------------------------------------------------------------------------
  ##merge
  List_data = list()
  DATA_ALL = data.table()
  DATA_FLUCTUATE = data.table()
  FINAL_DATA = data.table()
  #DATA_OUT = data.table()
  if(ToMerge)
  {
    try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = UData, File_Fr     = paste0('DOWNLOAD_YAH_', type, '_HISTORY.rds'), 
                                                              Folder_To   = UData, File_To     = paste0('DOWNLOAD_YAH_', type, 'HOME_HISTORY.rds'), 
                                                              pCondition  = 'home == 1', 
                                                              RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>0)))
    x = CCPR_READRDS(UData,'DOWNLOAD_YAH_INDHOME_HISTORY.rds')
    #My.Kable.Min(x[code == pCode & year(date) == year(pDate)][order(date)] )
    IFRC_SLEEP(10)
  }
  
  data_source = setDT(as.data.frame(t(setDT(List_source))))
  names(data_source) = 'source'
  data_source[, nr := seq.int(1, .N)]
  My.Kable.All(data_source)
  for(k in 1:length(List_source))
  {
    # k = 3
    File_name = gsub('XXX', List_source[[k]], File)
    Data      = CHECK_CLASS(try(CCPR_READRDS(Folder, File_name, ToKable = T, ToRestore = T)))
    #names(Data)
    if (nrow(Data) > 0 )
    {
      Data = Data[!is.na(code) & !is.na(date) & !is.na(close) & close > 0]
      Data = unique(Data, by = c('code', 'date'))
      # Data = Data [source == List_source[[k]]]
      Data[, pSource := List_source[[k]]]
      List_data[[k]] = Data
    }
  }
  DATA_ALL = rbindlist(List_data, fill = T)
  DATA_ALL = DATA_ALL[,.(source, codesource, code, date, close)]
  My.Kable(DATA_ALL)
  #My.Kable(DATA_ALL[code == 'INDDJI' & year(date) == 2024][order(date)])
  DATA_ALL = merge(DATA_ALL[, -c('nr_source')], data_source[,. (source, nr_source = nr)], all.x = T, by = 'source')
  DATA_ALL = DATA_ALL[order(code, date, nr_source)]
  DATA_ALL = DBL_IND_EXCLUDE(DATA_ALL)
  ##CORRECT
  My.Kable(DATA_ALL)
  dbl_correct = CHECK_CLASS(try(setDT(fread('S:/CCPR/DATA/DASHBOARD_LIVE/dbl_list_ind_to_correct.txt'))))
  DATA_ALL = merge(DATA_ALL[, -c('close_correct')], dbl_correct[,.(code, date, close_correct = close)], all.x = T, by =c('code', 'date'))
  My.Kable(DATA_ALL[!is.na(close_correct)])
  DATA_ALL = rbind(DATA_ALL[!is.na(close_correct)][close_correct > 0 ], DATA_ALL[is.na(close_correct)])
  DATA_ALL[, close_temp := close]
  DATA_ALL[!is.na(close_correct), close_temp := close_correct]
  DATA_ALL[, close_median := median(close_temp), by = c('code', 'date')]
  ##Remove outliers
  My.Kable(DATA_ALL[is.na(close)])
  TEST = DATA_ALL[!is.na(close_median) & close_temp > close_median * (1 - MAX_VAL) &  close_temp < close_median * (1 + MAX_VAL)]
  #TEST = DATA_ALL[!is.na(close_median)]
  My.Kable(TEST[is.na(close_median)])
  DATA_ALL = copy(TEST)
  
  DATA_ALL[, close_round := as.character(round(close_temp, Rounding))]
  DATA_NB = DATA_ALL [,.(nb_source = .N ), by = c('code', 'date', 'close_round')]
  DATA_DIFF = DATA_NB [,.(nb_diff = .N ), by = c('code', 'date')]
  My.Kable(DATA_DIFF[order(-nb_diff, code, code)])
  My.Kable(DATA_NB[order(code, code)])
  
  DATA_ALL = merge(DATA_ALL[, -c('nb_source')], DATA_NB[,.(code, date, close_round,  nb_source)], by = c('code', 'date', 'close_round'), all.x = T)
  DATA_ALL = merge(DATA_ALL[, -c('nb_diff')], DATA_DIFF[,.(code, date, nb_diff)], by = c('code', 'date'), all.x = T)
  
  
  TEST_1 = DATA_ALL[nb_diff == 1 & nb_source > 1]
  TEST_1[, final := close_temp]
  TEST_1 = unique(TEST_1[order(code, date, nr_source)], by = c('code', 'date'))
  My.Kable(TEST_1)
  
  
  
  TEST_2 = DATA_ALL[nb_diff >= 2 & nb_source > 1]
  TEST_2B = unique(TEST_2[order(code, date, nr_source)], by = c('code', 'date'))
  TEST_2B[, final := close_temp]
  My.Kable(TEST_2)
  My.Kable(TEST_2B)
  
  
  
  TEST_3 = DATA_ALL[source %in% list('WEXC', 'ENX', 'CAF', 'YAH') & nb_source == 1]
  TEST_3 = unique(TEST_3[order(code, date, nr_source)], by = c('code', 'date'))
  TEST_3[, final := close_temp]
  My.Kable(TEST_3)
  
  
  
  TEST_4 = DATA_ALL[(!code %in% TEST_3$code) & nb_diff == 1 & nb_source == 1]
  TEST_4 = unique(TEST_4[order(code, date)], by = c('code', 'date'))
  TEST_4[, final := close_temp]
  
  
  # My.Kable(DATA_OUT[code == 'INDAEX' & date == '1992-11-02'])
  # My.Kable(DATA_OUT[code == 'INDWIG20' & date == '2024-07-10'])
  TEST_ALL = rbind(TEST_1, TEST_2B, TEST_3,TEST_4, fill = T)
  TEST_ALL [, key := paste(code,date)]
  
  
  DATA_ALL [, key := paste(code,date)]
  
  DATA_OUT = DATA_ALL [!key %in% TEST_ALL$key ]
  My.Kable(DATA_OUT)
  DATA_OUT = unique(DATA_OUT, by = c('code', 'date'))
  
  TEST_ALL = rbind(TEST_1, TEST_2B, TEST_3,TEST_4, DATA_OUT, fill = T)
  DATA_OUT = unique(DATA_OUT, by = c('code', 'date'))
  
  DATA_OUT = DATA_ALL [!key %in% TEST_ALL$key ]
  My.Kable(DATA_OUT)
  
  TEST_RT = TEST_ALL[,. (code, source, date, final_close = final)]
  
  
  TEST_RT = TEST_RT[order(code, date)]
  
  TEST_RT[, rt := (final_close / shift(final_close) ) - 1,  by = 'code']
  TEST_RT[, pdate := as.Date(shift(date)), by = 'code']
  TEST_RT[, pclose := shift(final_close), by = 'code']
  TEST_RT[, nclose := shift(final_close, type = 'lead'), by = 'code']
  TEST_RT[, ndate := as.Date(shift(date, type = 'lead')), by = 'code']
  #TEST_RT[code == 'INDDJI' & year(date) < 2024]
  TEST_RT = MERGE_DATASTD_BYCODE(TEST_RT, pOption = 'FINAL')
  
  My.Kable.All(TEST_RT[abs(rt) > MAX_RT][order(rt)][,. (code,source, name, rt, pdate, date, ndate, pclose, final_close, nclose)])
  My.Kable(TEST_RT[order(rt)][,. (code,source, name, rt, pdate, date, ndate, pclose, final_close, nclose)])
  My.Kable(TEST_RT[])
  
  
  DATA_FLUCTUATE = TEST_RT[abs(rt) > MAX_RT][order(rt)][,. (code,source, name, rt, pdate, date, ndate, pclose, final_close, nclose)]
  nr_rt = nrow(DATA_FLUCTUATE)
  if (nr_rt == 0 )
  {
    FINAL_DATA = TEST_RT[,. (code,source, date, rt, close = final_close)]
    FINAL_DATA = MERGE_DATASTD_BYCODE(FINAL_DATA, pOption = 'FINAL')
    My.Kable.Min(FINAL_DATA)
    if (nchar(Save_Folder) > 0 & nchar(Save_File) > 0 )
    {
      try(CCPR_SAVERDS(FINAL_DATA, Save_Folder, Save_File, ToSummary = T, SaveOneDrive = T))
    }
  }else{
    
    CATln_Border("DATA_FLUCTUATE")
    My.Kable.All(DATA_FLUCTUATE)
    FINAL_DATA = TEST_RT[,. (code,source, date, rt, close = final_close)]
    FINAL_DATA = MERGE_DATASTD_BYCODE(FINAL_DATA, pOption = 'FINAL')
    FINAL_DATA = FINAL_DATA[!code %in% DATA_FLUCTUATE$code]
    
    CCPR_SAVERDS(DATA_FLUCTUATE,"S:/DATA_FLUCTUATE/",paste0("DATA_FLUCTUATE_", type,".rds"))
    
    UPLOAD_RDS_TABLE(pName = "intranet_dev",
                     pHost = "192.168.1.105",
                     pUser = "intranet_admin",
                     pPass = "admin@123456",
                     folder_name = "S:/DATA_FLUCTUATE/",
                     file_name = paste0("DATA_FLUCTUATE_", type,".rds"),
                     table_name = paste0("data_fluctuate_", type),
                     key_column = "id")
    
    # str(DATA_FLUCTUATE)
    # str(TEST_RT)
    # pCode = DATA_FLUCTUATE[k]$code
    # pDate = DATA_FLUCTUATE[k]$date
    # My.Kable(TEST_RT[code == pCode & year(date) == year(pDate)][order(date)] )
    # My.Kable(DATA_ALL[code == pCode & year(date) == year(pDate)][order(date)] )
    # My.Kable(List_data[[3]][code == pCode & year(date) == year(pDate)][order(date)] )
    if(ToForce)
    {
      if (nchar(Save_Folder) > 0 & nchar(Save_File) > 0 )
      {
        try(CCPR_SAVERDS(FINAL_DATA, Save_Folder, Save_File, ToSummary = T, SaveOneDrive = T))
      }
    }
    
  }
  return(FINAL_DATA)
}

# ==================================================================================================
TRAINEE_MERGE_STKALL = function ( ToUpload = F)  {
  # ------------------------------------------------------------------------------------------------
  
  STKVN = CCPR_READRDS ('S:/CCPR/DATA/DASHBOARD_LIVE/', 'ccpi_dashboard_stkvn.rds')  [, ':=' (coverage = 'VIETNAM')]
  STKVN [ size == 'LARGE', category := 'LARGE']
  STKVN [ size == 'MID', category := 'MID']
  STKVN [ size == 'SMALL', category := 'SMALL']
  My.Kable(STKVN)
  # YAH_STK = CCPR_READRDS (UData, 'download_yah_stkhome_history.rds')
  YAH_STK = CCPR_READRDS (UData, 'download_yah_stkhome_history.rds')
  
  OVERVIEW = try( TRAINEE_DASHBOARD_CALCULATE_STATS_BLOCK  (pOption = 'CCPI_INTERNATIONAL_STOCKS', Fr_Data = YAH_STK,  Fr_Folder = '', 
                                                            Fr_File = '',
                                                            To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = paste0('ccpi_dashboard_stkin.rds'), 
                                                            DateLimit = Sys.Date()  ,ToAddBenchmark = T, REF_LIST = list ('INDSPX', 'INDVNINDEX','CMDGOLD'),
                                                            ToSave = F, ToUpload = F , pHost = 'dashboard_live_bat') )
  
  # x = setDT(fread(paste0(UData, gsub('.rds', '_summary.txt', 'DOWNLOAD_YAH_STK_HISTORY.rds'))))
  x = CCPR_READRDS('S:/STKVN/', 'download_yah_stk_sector_history.rds')
  x[, sum_capi:=sum(capiusd), by='date']
  x[, cum_capi:=cumsum(capiusd), by='date']
  x[, pc_capi:=100*cum_capi/sum_capi, by='date']
  x[!is.na(sum_capi), size := ifelse(pc_capi<=85, 'LARGE', ifelse(pc_capi>85 & pc_capi<=95, 'MID', 'SMALL'))]
  x[is.na(sum_capi), size := NA]  # Set SIZE to NA when sum_capi is NA
  My.Kable(x)
  
  STKIN = OVERVIEW  [, ':=' (coverage = 'INTERNATIONAL')]
  
  STKIN = merge(STKIN[, -c('size', 'sector', 'industry')], x[, .(codesource, size, sector, industry)], by = 'codesource', all.x = T)
  STKIN [ size == 'LARGE', category := 'LARGE']
  STKIN [ size == 'MID', category := 'MID']
  STKIN [ size == 'SMALL', category := 'SMALL']
  # STKIN[!is.na(size)]
  STKIN [, market := sub(".*\\.", "", codesource)]
  STKIN [,':='(sector = toupper(sector), industry = toupper (industry))]
  MY_DATA = unique ( rbind (STKVN, STKIN, fill = T) , by = c('code', 'date') )
  
  pData = MY_DATA %>% dplyr::select(names (STKVN))
  str (pData)
  
  CCPR_SAVERDS (pData, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'ccpi_dashboard_stkall.rds')
  
  if (ToUpload)  {
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = 'dashboard_live_dev', l.tablename= tolower( 'ccpi_dashboard_stkall'),
                                     l.filepath= tolower(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/' , 'ccpi_dashboard_stkall.rds')),
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
  }
}

# # ==================================================================================================
# TRAINEE_STKVN_FOR_DASHBOARD = function()  {
#   # ------------------------------------------------------------------------------------------------
#   STKVN_SHARES = CCPR_READRDS('S:/STKVN/PRICES/FINAL/', 'FINAL_STKVN_SHARESOUT_4INDEX_HISTORY.rds', ToKable = T, ToRestore = T)
#   STKVN_SHARES[, sharesout:=as.numeric(sharesout)]
#   My.Kable(STKVN_SHARES[order(date, code)])
#   
#   # PRICES ............................................................................................
#   STKVN = CCPR_READRDS("S:/STKVN/PRICES/DAY/", paste0("download_caf_stkvn_pricesday_history.rds"))
#   STKVN_CAF = CCPR_READRDS(UData, 'DOWNLOAD_CAF_STKVN_HISTORY.rds', ToKable = T, ToRestore = T)
#   STKVN = unique(rbind(STKVN_CAF, STKVN, fill=T),by=c("code","date"))
#   GC_SILENT()
#   My.Kable(STKVN[order(date, code)][, .(market, code, date, reference, close, rt, change, varpc)])
#   
#   STKVN = STKVN[order(code, date)]
#   STKVN[is.na(rt), rt:=close/reference_open -1,by="code"]
#   
#   STKVN[, close_unadj:=close]
#   STKVN[, varpc:=100*rt]
#   My.Kable(STKVN[order(date, code)][, .(market, code, date, reference, close, close_unadj, rt, change, varpc)][code=='STKVNVIC'])
#   
#   SHARES = CCPR_READRDS(UData, 'IFRC_STKVN_4INDEX.rds', ToKable = T, ToRestore = T)
#   SHARES[, shareslis := sharesout]
#   SHARES = SHARES[, .(market, code, date, shares, shareslis, sharesout)]
#   
#   Compo = merge(STKVN[, -c('market')], SHARES, all.x=T, by=c('code', 'date'))
#   GC_SILENT()
#   
#   STKVN_PRICES = copy(Compo)
#   STKVN_PRICES = merge(STKVN_PRICES[, -c('new')], STKVN_SHARES[, .(code, date, new=sharesout)], all.x = T, by=c('code', 'date'))
#   STKVN_PRICES[!is.na(new) & is.na(sharesout), sharesout:=new]
#   STKVN_PRICES[!is.na(new) & is.na(shares), shares:=new]
#   GC_SILENT()
#   
#   LastTrading      = CCPR_LAST_TRADING_DAY_VN(max(17, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
#   STKVN_PRICES     = STKVN_PRICES[date<=LastTrading]
#   
#   STKVN_PRICES     = STKVN_PRICES[market %in% list('HSX', 'HNX')]
#   MinDate          = min(STKVN_PRICES$date)
#   MinDate          = '2000-12-31'
#   
#   Start.Date       = max(STKVN_PRICES[year(date)==year(MinDate)]$date)
#   Compo            = STKVN_PRICES[date>=Start.Date][order(date)]
#   Compo[date==Start.Date, rt:=0]
#   
#   Compo = Compo[order(code, date)][!is.na(rt)]
#   
#   Compo %>%
#     group_by(code) %>%
#     mutate(wgtrt.unit = order_by(date,cumprod(1+rt))) -> Compo
#   Compo = setDT(Compo)
#   My.Kable(Compo)
#   # Compo
#   Compo[, rtc_adj:=wgtrt.unit/wgtrt.unit[1], by='code']
#   
#   Compo[, rtc_inv:=rtc_adj/rtc_adj[.N], by='code']
#   
#   Compo[, close_end:=close[.N], by='code']
#   Compo[, close_adj:=close_end*rtc_inv, by='code']
#   
#   Compo = Compo[order(code, date)]
#   Compo = Compo[, rt_adj:=(close_adj/shift(close_adj))-1, by='code']
#   Compo = Compo[, lchange:=(close-shift(close)), by='code']
#   Compo = Compo[order(code, date)]
#   My.Kable(Compo[, .(market, code, date, reference, close_end, close, close_adj, close_unadj, lchange, rt, wgtrt.unit, rtc_adj, rtc_inv, rt_adj)], Nb=10)
#   
#   Compo = TRAINEE_SMOOTH_BEFOREAFTER_EQUAL(pFile="", pData=Compo, InCol="shares", ByGroup="code", ToReplace=T, ToReSave="")
#   Compo = TRAINEE_SMOOTH_BEFOREAFTER_EQUAL(pFile="", pData=Compo, InCol="sharesout", ByGroup="code", ToReplace=T, ToReSave="")
#   Compo = TRAINEE_SMOOTH_BEFOREAFTER_EQUAL(pFile="", pData=Compo, InCol="shareslis", ByGroup="code", ToReplace=T, ToReSave="")
#   Compo = Compo[order(code, date)]
#   
#   Last_compo = unique(Compo[order(code, -date)], by=c('code','date'))[, .(code, date, last_close=close, last_change=lchange)]
#   Compo = merge(Compo[, -c('last_close', 'last_change')], Last_compo, all.x=T, by=c('code', 'date'))
#   
#   STKVN_FINAL = Compo[, .(market, code, date, sharesout, shares, shareslis, reference, close_end, close, close_adj, close_unadj, last_close, rt, wgtrt.unit, rtc_adj, rtc_inv, rt_adj, last_change)]
#   STKVN_FINAL[, capibvnd:=sharesout*close_unadj/1000000000]
#   STKVN_FINAL[, change:=(close_unadj-reference)]
#   STKVN_FINAL[, varpc:=100*rt]
#   
#   STKVN_FINAL_DAHBOARD = STKVN_FINAL[, .(market, code, date, reference, shares, sharesout, close, close_unadj, last_close, last_change, change, last_capibvnd=capibvnd, capibvnd, close_adj, rt, rt_adj, varpc)]
#   My.Kable.TB(STKVN_FINAL_DAHBOARD[date == max(date)], Nb=5)
#   
#   CCPR_SAVERDS(STKVN_FINAL_DAHBOARD, 'S:/STKVN/PRICES/FINAL/', 'IFRC_CCPR_STKVN_FOR_DASHBOARD.rds', ToSummary = T, SaveOneDrive = T)
# }

# ==================================================================================================
DBL_CONVERT_CURRENCY = function(pOption = 'PERFORMANCE', ToForce = F){
  # ------------------------------------------------------------------------------------------------
  DBL_RELOAD_INSREF()
  # INDEXES_DATA_CLEAN_OPTIMISE(Option = 'FILL_ALL_DAYS', Folder = UData, FileName = 'IFRC_CUR_4INDEX.rds', Source = '')
  list_file = setDT(fread('S:/CCPR/DATA/DASHBOARD_LIVE/list_files_beq_ind_history.txt'))
  switch(pOption,
         'PERFORMANCE' = {
           Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
           File_Compare = "dbl_ind_performance.rds"
         },
         'CHART_MONTH' = {
           Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
           File_Compare = "ccpi_dashboard_indbeq_all_month_history.rds"
         },
         'CHART_QUARTER' = {
           Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
           File_Compare = "ccpi_dashboard_indbeq_all_quarter_history.rds"
         },
         'CHART_YEAR' = {
           Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
           File_Compare = "ccpi_dashboard_indbeq_all_year_history.rds"
         },
         'RISK' = {
           Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
           File_Compare = "dbl_ind_risk.rds"
         }
  )
  dt_rep = DBL_REPORT_DATA_UPDATE_LIST_FILE(Chart_Type = 'CHART_MONTH')
  
  for (i in 1:nrow(list_file)){
    # i = 1
    Folder_Index = list_file$folder[i]
    File_Index = list_file$filename[i]
    # Folder = 'S:/CCPR/DATA/'
    # File = 'CCPR_WORLDINDEXES_HISTORY.rds'
    pc_run = unlist(strsplit(as.character(list_file$pc_to_run[i]), ","))
    if (length(pc_run) == 0){
      if (file.exists(paste0(Folder_Index,File_Index)))
      {
        summary_index = setDT(fread(paste0(Folder_Index,gsub(".rds","_summary.txt",File_Index))))
        
        if (any(grepl("VND$",summary_index$code)) | (any(grepl("USD$",summary_index$code)))){
          
          WRITE_SUMMARY_FULLPATH(paste0(Folder_Compare,File_Compare))
          summary_compare = setDT(fread(paste0(Folder_Compare,gsub(".rds","_summary.txt",File_Compare))))
          
          out_compare = summary_compare[code %in% summary_index$code & !grepl("VND$",code) & !grepl("USD$",code)]
          CUR_VND = summary_compare[code %in% summary_index$code & grepl("VND$",code)]
          CUR_USD = summary_compare[code %in% summary_index$code & grepl("USD$",code)]
          if (nrow(CUR_VND) > 0 & nrow(CUR_USD) > 0 & max(CUR_USD$date) < max(CUR_VND$date)){
            TO_DO_VND = T
          } else {
            TO_DO_VND = F
          }
          if (nrow(out_compare) > 0){
            # max_date_df1 = out_compare %>%
            #   group_by(code) %>%
            #   summarize(max_date1 = max(date))
            max_date_df1 = unique(out_compare[, max_date1 := max(date), by = 'code'], by = 'code')[, .(code, max_date1)]
            
            max_date_df1 = as.data.table(max_date_df1)
            max_date_df1[, code_prefix := substr(code, 1,nchar(code) - 3)]
            
            # max_date_df2 = summary_index[grepl("VND$",code)] %>%
            #   group_by(code) %>%
            #   summarize(max_date2 = max(date))
            
            # max_date_df2 = summary_index %>%
            #   group_by(code) %>%
            #   summarize(max_date2 = max(date))
            max_date_df2 = unique(summary_index[grepl("VND$",code)][, max_date2 := max(date), by = 'code'], by = 'code')[, .(code, max_date2)]
            
            max_date_df2 = as.data.table(max_date_df2)
            max_date_df2[, code_prefix := substr(code, 1,nchar(code) - 3)]
            
            merge_data = merge(max_date_df1[, -c('code')], max_date_df2[, -c('code')], all.x = T, by = 'code_prefix')
            # max_date_df2 = max(summary_index[grepl("VND$",code)]$date)
            
            all_equal = all(merge_data$max_date1 == merge_data$max_date2)
            
            if (ToForce || (max(summary_index$date) > max(out_compare$date)) || !all_equal)
            {
              if (TO_DO_VND){
                try(DBL_INDEXES_CONVERT_CURRENCIES_FROM_VND (IND_FOLDER      = Folder_Index, IND_FILENAME  = File_Index,
                                                             Save_Folder     = Folder_Index, Save_File     = File_Index,
                                                             CUR_FILE_NAME   = 'IFRC_CUR_4INDEX.rds',
                                                             IND_CURS_LIST   = list('USD', 'GBP', 'EUR', 'JPY', 'SGD', 'AUD', 'CAD', 'KRW', 'HKD', 'CNY'),
                                                             IncludeOnly_VND = T))
              } else {
                data_one = try(DBL_INDEXES_CONVERT_CURRENCIES_FROM_USD(IND_FOLDER    = Folder_Index, IND_FILENAME = File_Index,IND_PREFIX_NAME = '',
                                                                       Save_Folder   = Folder_Index, Save_File     = File_Index,
                                                                       IND_CURS_LIST = list('GBP', 'EUR', 'JPY', 'SGD', 'VND', 'AUD', 'CAD', 'KRW', 'HKD', 'CNY')))
              }
            } else {
              CATln_Border("ALL DATA HAS ALREADY UPDATED!!!!!")
            }
          }
          else {
            if (TO_DO_VND){
              try(DBL_INDEXES_CONVERT_CURRENCIES_FROM_VND (IND_FOLDER      = Folder_Index, IND_FILENAME  = File_Index,
                                                           Save_Folder     = Folder_Index, Save_File     = File_Index,
                                                           CUR_FILE_NAME   = 'IFRC_CUR_4INDEX.rds',
                                                           IND_CURS_LIST   = list('USD', 'GBP', 'EUR', 'JPY', 'SGD', 'AUD', 'CAD', 'KRW', 'HKD', 'CNY'),
                                                           IncludeOnly_VND = T))
            } else {
              data_one = try(DBL_INDEXES_CONVERT_CURRENCIES_FROM_USD(IND_FOLDER    = Folder_Index, IND_FILENAME = File_Index,IND_PREFIX_NAME = '',
                                                                     Save_Folder   = Folder_Index, Save_File     = File_Index,
                                                                     IND_CURS_LIST = list('GBP', 'EUR', 'JPY', 'SGD', 'VND', 'AUD', 'CAD', 'KRW', 'HKD', 'CNY')))
            }
          }
        } else {
          CATln_Border("ALL DATA DOES NOT HAVE VND OR USD IN CODE!!!!!")
        }
      }
    }
  }
}


# ==================================================================================================
DBL_FIX_CURRENCY = function(){
  # ------------------------------------------------------------------------------------------------
  DBL_RELOAD_INSREF()
  INDEXES_DATA_CLEAN_OPTIMISE(Option = 'FILL_ALL_DAYS', Folder = UData, FileName = 'IFRC_CUR_4INDEX.rds', Source = '')
  
  LIST_INDEX_NAME = c ('LOGISTICS', 'BROKERAGE', 'PETROLIMEX','REALESTATE', 
                       'RETAIL', 'BANK', 'HEALTHCARE', 'ENERGY', 'FINANCE', 'AI'  )
  for (INDEX in LIST_INDEX_NAME) {
    # INDEX = "BROKERAGE"
    switch ( INDEX, 
             'LOGISTICS'       = { PREFIX = 'log' } ,
             'BROKERAGE'       = { PREFIX = 'brk' } ,
             'PETROLIMEX'      = { PREFIX = 'pet' } ,
             'REALESTATE'      = { PREFIX = 'rst' } ,
             'RETAIL'          = { PREFIX = 'rtl' } ,
             'BANK'            = { PREFIX = 'bnk' } ,
             'HEALTHCARE'      = { PREFIX = 'hlc' } ,
             'ENERGY'          = { PREFIX = 'eny' } ,
             'FINANCE'         = { PREFIX = 'fin' } ,
             'AI'              = { PREFIX = 'ari' }  )
    
    # Fr_File_Path = paste0('S:/STKVN/INDEX_2024/', 'beq_ind', PREFIX, 'als_history.rds')
    x = CCPR_READRDS("S:/STKVN/INDEX_2024/", paste0('beq_ind', PREFIX, 'als_history.rds'), ToRestore = T)
    y = x[grepl("VND$",code)]
    CCPR_SAVERDS(y, "S:/STKVN/INDEX_2024/","beq_indxxxalsvnd_history.rds")
    
    DBL_INDEXES_CONVERT_CURRENCIES_FROM_VND (IND_FOLDER    = paste0('S:/STKVN/INDEX_2024/'), IND_FILENAME  = 'beq_indxxxalsvnd_history.rds',
                                             Save_Folder   = paste0('S:/STKVN/INDEX_2024/'), Save_File     = paste0('beq_ind', PREFIX, 'als_history.rds'),
                                             CUR_FILE_NAME = 'BEQ_CUR_4INDEX.rds',
                                             IND_CURS_LIST = list('USD', 'GBP', 'EUR', 'JPY', 'SGD', 'AUD', 'CAD', 'KRW', 'HKD', 'CNY')) 
    
    # x = CCPR_READRDS("S:/STKVN/INDEX_2024/",  paste0('beq_ind', PREFIX, 'als_history.rds'), ToRestore = T)
    # y = x[grepl("CWPRUSD$",code) & date > as.Date('2014-07-31') - 10 & date < as.Date('2014-07-31') + 10]
    # My.Kable.All(y[,.(code,date,close)])
  }
  
  
  x = CCPR_READRDS("S:/BEQ_WEB/WOMEN_CEO/", "vnmwceo_merge_2023.rds", ToRestore = T)
  y = x[grepl("VND$",code)]
  CCPR_SAVERDS(y, "S:/BEQ_WEB/WOMEN_CEO/","vnmwceo_merge_2023vnd.rds")
  DBL_INDEXES_CONVERT_CURRENCIES_FROM_VND (IND_FOLDER    = paste0("S:/BEQ_WEB/WOMEN_CEO/"), IND_FILENAME  = 'vnmwceo_merge_2023vnd.rds',
                                           Save_Folder   = paste0("S:/BEQ_WEB/WOMEN_CEO/"), Save_File     = "vnmwceo_merge_2023.rds",
                                           CUR_FILE_NAME = 'BEQ_CUR_4INDEX.rds',
                                           IND_CURS_LIST = list('USD', 'GBP', 'EUR', 'JPY', 'SGD', 'AUD', 'CAD', 'KRW', 'HKD', 'CNY')) 
  
  
  FolderAll = "S:/CCPR/DATA/"
  FileAll = "IFRC_CCPR_INDWCEOIN_ALL_HISTORY.rds"
  ToForce = T
  LIST_ISO3 = list ('AUS' ,	'BEL' ,	'CHN' ,	'DEU', 	'FRA' ,	'HKG' ,	'IDN' ,	'IRL' ,	'ITA' ,	'JPN' ,	'MYS' ,	'NLD' ,	'NOR' ,	'PRT' ,	'SGP' ,	'THA', 	'TWN', 'USA')
  
  for (iso3 in sample(LIST_ISO3))
  {
    Folder_Index = paste0("S:/WCEO_INDEXES/",iso3,"/")
    File_Index = paste0("ifrc_ccpr_indwceo",tolower(iso3),"_history.rds")
    Folder_Compare = paste0("S:/WCEO_INDEXES/",iso3,"/")
    File_Compare = paste0("ifrc_ccpr_indwceo",tolower(iso3),"_all_history.rds")
    if (file.exists(paste0(Folder_Index,File_Index)))
    {
      summary_index = setDT(fread(paste0(Folder_Index,gsub(".rds","_summary.txt",File_Index))))
      
      WRITE_SUMMARY_FULLPATH(paste0(Folder_Compare,File_Compare))
      summary_compare = setDT(fread(paste0(Folder_Compare,gsub(".rds","_summary.txt",File_Compare))))
      
      out_compare = summary_compare[!grepl("USD$",code)]
      if (ToForce || (max(summary_index$date) > max(out_compare$date)))
      {
        data_one = try(DBL_INDEXES_CONVERT_CURRENCIES_FROM_USD(IND_FOLDER    = Folder_Index, IND_FILENAME = File_Index,IND_PREFIX_NAME = '',
                                                               Save_Folder   = Folder_Compare, Save_File     = File_Compare,
                                                               IND_CURS_LIST = list('GBP', 'EUR', 'JPY', 'SGD', 'VND', 'AUD', 'CAD', 'KRW', 'HKD', 'CNY')))
        
        data_all = CHECK_CLASS(try(CCPR_READRDS(FolderAll, FileAll, ToRestore = T)))
        if (nrow(data_all)>0)
        {
          data_all = rbind(data_all[!code %in% data_one$code], data_one,fill=T)
          
          try(CCPR_SAVERDS(data_all, FolderAll, FileAll, ToSummary = T, SaveOneDrive = T))
        }
      }
    }
    
  }
}

# ==================================================================================================
DBL_REPAIR = function(Action = 'UPDATE_IFRC_CCPR_INDHOME_HISTORY')
{
  # ------------------------------------------------------------------------------------------------
  switch(Action,
         'UPDATE_IFRC_CCPR_INDHOME_HISTORY' = {
           # Update DATA for WORLD INDEXES
           uda = CCPR_READRDS(UData, 'EFRC_INDHOME_HISTORY.RDS', ToKable = T, ToRestore = T)
           uda[, last := close]
           uda[, change := (close-shift(close)), by='code']
           uda[, rt:= close/shift(close) - 1, by='code']
           uda[, varpc:=100*rt, by='code']
           CCPR_SAVERDS(uda, UData, 'EFRC_INDHOME_HISTORY.RDS', ToSummary = T, SaveOneDrive = T)
           TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr = UData, File_Fr = 'EFRC_INDHOME_HISTORY.RDS', 
                                                                 Folder_To = 'S:/CCPR/DATA/WORLD_INDEXES/', File_To = 'IFRC_CCPR_INDHOME_HISTORY.rds', 
                                                                 RemoveNA = T, NbDays = 0, pCondition = '', MinPercent = 1, ToForce = F, IncludedOnly = T) 
           
           if (SUMMARY_DATE(paste0('S:/CCPR/DATA/WORLD_INDEXES/IFRC_CCPR_INDHOME_HISTORY.rds')) > SUMMARY_DATE(paste0('S:/CCPR/DATA/IFRC_CCPR_INDHOME_HISTORY.rds')))
           {
             x = CCPR_READRDS('S:/CCPR/DATA/WORLD_INDEXES/', 'IFRC_CCPR_INDHOME_HISTORY.rds', ToKable = T, ToRestore =  T)
             My.Kable.Min(x[order(date)])
             CCPR_SAVERDS(x, 'S:/CCPR/DATA/', 'CCPR_WORLDINDEXES_HISTORY.rds', ToSummary = T, SaveOneDrive = T)
           }
           try( TRAINEE_DASHBOARD_CALCULATE_STATS_BLOCK  (pOption = 'WORLD_EQUITIES_INDEX', Fr_Data = data.table(),  Fr_Folder = 'S:/CCPR/DATA/',
                                               Fr_File = 'CCPR_WORLDINDEXES_HISTORY.rds',
                                               To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'ccpi_dashboard_ind_world.rds',
                                               DateLimit = Sys.Date() ,
                                               ToAddBenchmark = T, REF_LIST = list ('INDSPX', 'INDVNINDEX','CMDGOLD'),
                                               ToSave = T, ToUpload = T , pHost = 'dashboard_live_bat') )
           
         },
         'REPAIR_DATA_FOR_WTI' = {
           IFRC_CCPR_INVESTMENT_HISTORY (MaxDate=Sys.Date())
         },
         'REPAIR_VIETNAM_STOCK_DATA' = {
          try(TRAINEE_STKVN_FOR_DASHBOARD())
          try( TRAINEE_DASHBOARD_CALCULATE_STATS_BLOCK  (pOption = 'CCPI_VIETNAM_STOCKS', Fr_Data = data.table(),  Fr_Folder = 'S:/STKVN/PRICES/FINAL/', 
                                                        Fr_File = 'IFRC_CCPR_STKVN_FOR_DASHBOARD.RDS',
                                                        To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'ccpi_dashboard_stkvn.rds', 
                                                        DateLimit = Sys.Date()  , ToAddBenchmark = T, REF_LIST = list ('INDSPX', 'INDVNINDEX','CMDGOLD'),
                                                        ToSave = T, ToUpload = T , pHost = 'dashboard_live_bat') )          
         },
         'REPAIR_CURRENCY' = {
           try(DBL_FIX_CURRENCY())
         },
         'REPAIR_MARKET_CHARTS' = {
           try(DASHBOARD_CCPI_MARKET_CHARTS (phost = 'dashboard_live', ToDownload = T, ToUpload = T, ToSave   = T))
         },
         # 'REPAIR_CCPR_DASHBOARD_MARKET' = {
         #   Action = 'REPAIR_CCPR_DASHBOARD_MARKET'
         #   CATln_Border(paste(Action,'-', SYS.TIME()))
         #   if (ToForce || SUMMARY_DATE(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/', 	'ccpi_dashboard_markets.rds')) < SYSDATETIME(1))
         #   {
         #     try (MAINTENANCE_DASHBOARD (Action = 'DASHBOARD_CCPI_MARKET_CHARTS',  Minutes= 0))
         #   }
         # },
         'REPAIR_MARKET_FOREX' = {
           CATln_Border(paste(Action,'-', SYS.TIME()))
           if (ToForce || SUMMARY_DATE(paste0('S:/CCPR/DATA/DAY/FOREX/', 	'download_yah_forex_intraday_today.rds')) < SYSDATETIME(1))
           {
             try(TRAINEE_DOWNLOAD_MARKET_CUSTOM (pOption = 'CURRENCIES',ToDownload = T, ToSave = T, ToUpload = T, pHost = 'dashboard_live') )
           }
         },
         
         'REPAIR_CCPR_DASHBOARD_WORLD_EQUITIES' = {
           Action = 'REPAIR_CCPR_DASHBOARD_WORLD_EQUITIES'
           CATln_Border(paste(Action,'-', SYS.TIME()))
           if (ToForce || SUMMARY_DATE(paste0('S:/CCPR/DATA/', 	'	ccpr_worldindexes_history.rds')) < SYSDATETIME(1))
           {
             try( TRAINEE_DASHBOARD_LIVE_DIV  (pOptions = 'WORLD_EQUITIES_INDEX', Minutes = 0) )
           }
         },
         'REPAIR_CCPR_DASHBOARD_WTI' = {
           Action = 'REPAIR_CCPR_DASHBOARD_WTI'
           CATln_Border(paste(Action,'-', SYS.TIME()))
           if (ToForce || SUMMARY_DATE(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/', 	'ifrc_ccpr_investment_history.rds')) < SYSDATETIME(1))
           {
             try( TRAINEE_DASHBOARD_LIVE_DIV  (pOptions = 'WHERE_TO_INVEST', Minutes = 0) )
           }
         }
  )
  CATln_Border(paste('DASHBOARD_LIVE_REPAIR','-', SYS.TIME()))
  Monitor = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DASHBOARD_LIVE_REPAIR', pAction = "SAVE", NbSeconds = 3600, ToPrint = F))  

}

# ==================================================================================================
TRAINEE_DOWNLOAD_STB_PRICESBOARD = function(){
  # ------------------------------------------------------------------------------------------------
  List_SetChars = list('ABC', 'DEF', 'GHI', 'JKL', 'MNO', 'PQR', 'STU', 'VWXYZ')
  # List_SetChars = list('ABC')
  pMyPC = as.character(try(fread("C:/R/my_pc.txt", header = F)))
  
  LastTrading = CCPR_LAST_TRADING_DAY_VN(8, ToPrompt = T)
  for (SOURCE in sample(list('STB'))){
    for (MARKET in sample(list('HNX','HSX','UPC'))){
      for (pFirstChars in sample(List_SetChars)){
        x = NEW_LOOP_DOWNLOAD_CUSTOM_BY_CODE ( ranking    = '', FirstChars = pFirstChars, random = F,
                                               pMarket    = MARKET, 
                                               pFolder    = 'S:/STKVN/PRICES/DAY/',
                                               File_Pattern  = paste0('DOWNLOAD_',SOURCE,'_PRICESBOARD'),
                                               pDate      = LastTrading,
                                               List_Codes = list(),
                                               File_Codes = paste0("S:/CCPR/DATA/download_",tolower(MARKET),"_stkvn_ref_history.rds"),
                                               Action     = paste0('DOWNLOAD_',SOURCE,'_STKVN_PRICESDAY_BY_CODE'))
        
      }
    }
  }
  xAll = CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('DOWNLOAD_', 'STB', '_PRICESBOARD_', gsub('-','', LastTrading), '.rds'), ToKable = T, ToRestore = T)
  xAll = xAll[date == LastTrading]
  xAll[, timestamp := paste0(LastTrading,' 15:30:00')]
  xAll[, datetime := paste0(LastTrading,' 15:30:00')]
  xAll = UPDATE_UPDATED(xAll)
  CCPR_SAVERDS(xAll, 'S:/STKVN/PRICES/DAY/', paste0('DOWNLOAD_', 'STB', '_PRICESBOARD_', gsub('-','', LastTrading), '.rds'), ToSummary = T, SaveOneDrive = T)
  
}
# ==================================================================================================

TRAINEE_DOWNLOAD_FILE_FR_DASHBOARDLIVE = function (Folder_xls  = paste0(ODDrive, 'TRAINEE/') ,
                                                   File_xls    = 'CHECK_UPDATE_DASHBOARD_LIVE 1.xlsx', 
                                                   Folder_Save = 'S:/CCPR/DATA/DASHBOARD_LIVE/HOST_TO_LOCAL/'){
  FILE_XLS_DASHBOARD_LIVE = setDT(read.xlsx(paste0(Folder_xls, File_xls)))
  TABLES                  = as.list(FILE_XLS_DASHBOARD_LIVE$table)
  FILES_TODO              = as.data.table(FILE_XLS_DASHBOARD_LIVE[!is.na(upload_location) & !is.na(local_location)])
  for (table in TABLES) {
    x = try(TRAINEE_LOAD_SQL_TABLES_HOST (pTableSQL = table, pHost = 'dashboard_live', ToDisconnect=T, 
                                          SaveFolder = Folder_Save, ToKable=T))
  }
  
}
# ==================================================================================================
TRAINEE_REPORT_ACTIONSLIST = function (folder    = paste0(ODDrive, 'BeQ/TRAINING/'),
                                       file_name = 'BEQ_TRAINING.xlsx', 
                                       ToUpload = T) {
  # ------------------------------------------------------------------------------------------------
  action_list =  setDT(read.xlsx(paste0(folder, file_name),  sheet = "ACTIONS_LIST", startRow = 3))
  action_list[, DEADLINE := as.Date(DEADLINE, origin = "1899-12-30")]
  action_list[, id := seq(1, .N)]
  action_list = action_list[,. (id, NR, BY, STATUS, PRIORITY, DEADLINE, NAME)]
  saveRDS(action_list, 'S:/ACTIONS_LIST/actions_list.rds')
  
  if (nchar(ToUpload) > 0 )
  {
    UPLOAD_RDS_TABLE(pName = "intranet_dev",
                     pHost = "192.168.1.105",
                     pUser = "intranet_admin",
                     pPass = "admin@123456",
                     folder_name = "S:/ACTIONS_LIST/",
                     file_name = "actions_list.rds",
                     table_name = "process_actionslist",
                     key_column = "id")
  }
  My.Kable(action_list)
  return(action_list)
  
}
# ==================================================================================================

TRAINEE_REPORT_BATCH = function (folder = paste0(ODDrive, 'BeQ/CCPR/DATA/MONITOR/'),
                                 file_name = 'MANAGEMENT_RD_TEAM.xlsx', 
                                 ToUpload = T) {
  # ------------------------------------------------------------------------------------------------
  FILE =  setDT(read.xlsx(paste0(folder, file_name),  sheet = "BATCH_BY_PC", startRow = 3))
  FILE[, id := seq(1, .N)]
  saveRDS(FILE, 'S:/ACTIONS_LIST/management_batch.rds')
  
  if (nchar(ToUpload) > 0 )
  {
    UPLOAD_RDS_TABLE(pName = "intranet_dev",
                     pHost = "192.168.1.105",
                     pUser = "intranet_admin",
                     pPass = "admin@123456",
                     folder_name = "S:/ACTIONS_LIST/",
                     file_name = "managemet_batch.rds",
                     table_name = "process_managementbatch",
                     key_column = "id")
  }
  My.Kable(FILE)
  return(FILE)
  
}


# ==================================================================================================
TRAINEE_INTEGRATION_INTRADAY_BY_FILES_QUALITY = function(File_Folder = UData, 
                                                         File_Fr = 'DOWNLOAD_BIN_BAS_INTRADAY_DAY.rds', 
                                                         File_To = 'DOWNLOAD_BIN_BAS_INTRADAY_HISTORY.rds', 
                                                         RemoveNA = T, NbDays = 0, pCondition = '', MinPercent = 1, ToForce = F) {
  # ------------------------------------------------------------------------------------------------
  
  GC_SILENT()
  DATACENTER_LINE_BORDER_BEGINEND(paste('IFRC_CCPR_INTEGRATION_INTRADAY_BY_FILES_QUALITY to :',File_To), 'BEGIN')
  
  TO_DO = try(TRAINEE_SUMMARY_DATE_COMPARE_UPDATES_PERCENT_BY_CODE(Folder_Fr = File_Folder, Folder_To = File_Folder, 
                                                                   File_Fr   = File_Fr, 
                                                                   File_To   = File_To, MinPercent = MinPercent, EchoOn = F))
  if (ToForce || TO_DO)
  {
    IFRC_SLEEP(2)
    try(TRAINEE_INTEGRATION_INTRADAY_BY_FILES(File_Folder = File_Folder, File_Fr = File_Fr, File_To = File_To, 
                                              RemoveNA = RemoveNA, NbDays = NbDays, pCondition = pCondition))
  }
  DATACENTER_LINE_BORDER_BEGINEND(paste('IFRC_CCPR_INTEGRATION_INTRADAY_BY_FILES_QUALITY to :',File_To), 'END')
  GC_SILENT()
}

# ==================================================================================================
TRAINEE_INTEGRATION_INTRADAY_BY_FILES = function(File_Folder = UData, 
                                                 File_Fr = 'DOWNLOAD_BIN_BAS_INTRADAY_DAY.rds', 
                                                 File_To = 'DOWNLOAD_BIN_BAS_INTRADAY_HISTORY.rds', RemoveNA = T, NbDays = 0, pCondition = '') {
  # ------------------------------------------------------------------------------------------------
  GC_SILENT()
  DATACENTER_LINE_BORDER_BEGINEND(paste('IFRC_CCPR_INTEGRATION_INTRADAY_BY_FILES to :',File_To), 'BEGIN')
  Data_Fr     = CHECK_CLASS(try(CCPR_READRDS(File_Folder, File_Fr, ToKable = T, ToRestore = T)))
  if (nrow(Data_Fr)>0)
  {
    if (File_Fr != File_To)
    {
      Data_To     = CHECK_CLASS(try(CCPR_READRDS(File_Folder, File_To, ToKable = T, ToRestore = T)))
    } else { Data_To =  data.table()}
    Data_To     = rbind(Data_To, Data_Fr, fill=T)
    My.Kable.Min(Data_To)
    if (nrow(Data_To)>0)
    {
      
      Data_To = unique(Data_To, by=c('code', 'date', 'timestamp'))[order(code, date, timestamp)]
      if (RemoveNA) { Data_To = Data_To[!is.na(code) & !is.na(date)]}
      if (NbDays!=0)
      {
        # Data_To[, x.maxdate:=max(date), by='code']
        Data_To = Data_To[order(code, -date)]
        Data_To[, x.nr:=seq.int(1,.N), by='code']
        
        Data_To = Data_To[x.nr<=NbDays]
        Data_To = Data_To[order(code, date, timestamp)]
        Data_To$x.nr = NULL
      }
      
      Data_To = MERGE_DATASTD_BYCODE(Data_To, pOption='FINAL')
      if (nchar(pCondition)>0) {  Data_To = Data_To[eval(parse(text = pCondition))] }
      # Data_To[type!='STK', close_adj:=close]
      # Data_To[type!='STK' & !is.na(rt), rt:=(close/shift(close))-1, by='code']
      CATln_Border("MERGED DATA")
      My.Kable.Min(Data_To[order(date)])
    }
    if (nrow(Data_To)>0)
    {
      try(CCPR_SAVERDS(Data_To, File_Folder, File_To, ToKable = T, ToSummary = T, SaveOneDrive = T))
    }
  }
  DATACENTER_LINE_BORDER_BEGINEND(paste('IFRC_CCPR_INTEGRATION_INTRADAY_BY_FILES to :',File_To), 'END')
  GC_SILENT()
}

# ==================================================================================================
TRAINEE_SUMMARY_DATE_COMPARE_UPDATES_PERCENT_BY_CODE = function(Folder_Fr=UData, Folder_To=UData, 
                                                                File_Fr='efrc_stkworld_forindex.rds', File_To='efrc_stktop1000_forindex.rds', MinPercent=1, EchoOn=F) {
  # ------------------------------------------------------------------------------------------------
  # Updated = 2022-03-30
  # EchoOn=T
  # Folder_Fr=UData; Folder_To=UData
  # File_Fr='download_rts_ind_history.rds'; File_To='download_rts_indin_history.rds'; MinPercent=1; EchoOn=F
  Sum.Quality = 0
  Decision = F
  Sum.From = try(DATACENTER_LOAD_TEXTFILE(Folder_Fr, gsub('.rds', '_summary.txt', tolower(File_Fr))))
  Sum.To   = try(DATACENTER_LOAD_TEXTFILE(Folder_To, gsub('.rds', '_summary.txt', tolower(File_To))))
  if ( all(class(Sum.From)!='try-error') & all(class(Sum.To)!='try-error'))
  {
    # if (max(Sum.From$date) > max(Sum.To$date))
    # {
    #   # Sum.Quality = 100
    # } else {
    Sum.From = unique(Sum.From, by='code')
    Sum.To   = unique(Sum.To, by='code')
    
    if (nrow(Sum.From)>0 & nrow(Sum.To)>0)
    {
      DateFrom = max(Sum.From[!is.na(date)]$date)
      DateTo   = max(Sum.To[!is.na(date)]$date)
      
      Sum.To   = merge(Sum.To[, -c('date_from')], Sum.From[, .(code, date_from=date)], all.x=T, by='code') # add date_from to Sum_To
      Sum.ToFr = Sum.To[!is.na(date_from)]
      # Sum.ToFr = MERGE_DATASTD_BYCODE(Sum.ToFr)
      if (EchoOn) { My.Kable.MaxCols(Sum.ToFr) }
      Sum.Quality = 100*nrow(Sum.To[date<as.Date(substr(date_from,1,10))])/nrow(Sum.To) # Updated %
      
      Condition_Date    = max(Sum.ToFr[!is.na(date_from)]$date_from)>max(Sum.ToFr[!is.na(date)]$date)
      Condition_Percent = Sum.Quality > MinPercent
      
      DateToFr   = max(Sum.ToFr[!is.na(date)]$date)
      
      # CATln(paste("QUALITY UPDATE TODO : ", File_Fr, ">", File_To, "=", Format.Number(Sum.Quality,2), '%', DateFrom, "/", DateTo, DateToFr), top_border = T, p_border=T)
      Decision = Condition_Date | Condition_Percent
      CATln('')
      CATln_Border(paste("QUALITY UPDATE TODO : ", File_Fr, ">", File_To, "=", Format.Number(Sum.Quality,2), '%', DateFrom, "/", DateTo, DateToFr, '>>> TODO =', Decision))
    } else { Sum.Quality = 0 }
    # }
  } 
  return(Decision)
}

# ==================================================================================================
PRICESBOARD_TO_UDATA_INTRADAY = function(pSource = 'STB') {
  # ------------------------------------------------------------------------------------------------
  
  LastTrading      = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
  File_PricesBoard = paste0('DOWNLOAD_', pSource, '_PRICESBOARD_', gsub('-','', LastTrading), '.rds')
  CATln_Border(File_PricesBoard)
  Data_PricesBoard = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', File_PricesBoard)))
  My.Kable(Data_PricesBoard)
  
  Check_Data = Data_PricesBoard[!is.na(last) & date == LastTrading]
  
  if (nrow(Check_Data)>0)
  {
    File_Prices = paste0('DOWNLOAD_', pSource, '_STKVN_INTRADAY_TODAY', '.rds')
    CATln_Border(File_Prices)
    Data_Prices = CHECK_CLASS(try(CCPR_READRDS(UData, File_Prices)))
    My.Kable.Min(Data_Prices)
    
    if (nrow(Data_Prices)>0)
    {
      CloseDateTime = paste(LastTrading, '15:45:00')
      Data_PricesBoard[, timestamp:=updated]
      Data_PricesBoard[timestamp>CloseDateTime, timestamp:=CloseDateTime]
      Data_PricesBoard[, datetime:=timestamp]

      if ('last' %in% names(Data_Prices)) { Data_Prices[, close:=last] } else {Data_Prices[, last:=close_adj] }
      Data_Prices = Data_Prices[!is.na(last) & date == LastTrading]
      Data_Prices = rbind(Data_Prices, Data_PricesBoard[date==LastTrading], fill=T)
      Data_Prices = unique(Data_Prices, by=c('code', 'date', 'timestamp'), fromLast=T)
      Data_Prices[, close:=last]
      Data_Prices[is.na(close), close:=reference]
      Data_Prices[is.na(volume), volume:=0]
      Data_Prices[, rt:=(close/reference)-1]
      My.Kable.Min(Data_Prices[order(date)])
      
      if (nrow(Data_Prices)>0)
      {
        Data_Prices = MERGE_DATASTD_BYCODE(Data_Prices, pOption='FINAL')
        try(CCPR_SAVERDS(Data_Prices, UData, File_Prices, ToSummary = T, SaveOneDrive = T))
        
      }
    }
  }
}

# ==================================================================================================
CLEAN_SOURCE = function(l_data) {
  # ------------------------------------------------------------------------------------------------
  my_data = l_data
  if ('source' %in% names(my_data))
  {
    my_data[source=="Q",   source:="QDL"]
    my_data[source=="RTS", source:="EIK"]
    my_data[source=="E",   source:="EIK"]
    my_data[source=="B",   source:="BLG"]
    my_data[source=="H",   source:="IFRC"]
    my_data[source=="EURONEXT", source:="ENX"]
  }
  return(my_data)
}
#===================================================================================================
CLEAN_FIELDS_DATATABLE = function(pData) {
  # ------------------------------------------------------------------------------------------------
  if ('updated' %in% names(pData) && typeof(pData$update)!='character')
  {
    CATln('Cleaning Fields Datatable ...')
    pData[, updated:=as.character(updated)]
    # str(pData)
  }
  if ('updaed' %in% names(pData)) { pData = pData[, -c('updaed')]}
  if ('volume' %in% names(pData)) { 
    pData[, volume:=as.numeric(volume)]
    pData[volume<1, volume:=0]
  }
  if ('updated' %in% names(pData)) { pData[, updated:=as.character(updated)] }
  if ('timestampvn' %in% names(pData)) { pData[, timestampvn:=as.character(timestampvn)] }
  if ('timestamp' %in% names(pData)) { pData[, timestamp:=as.character(timestamp)] }
  if ('timestamputc' %in% names(pData)) { pData[, timestamputc:=as.character(timestamputc)] }
  
  return(pData)
}
#===================================================================================================
IFRC_CCPR_DATACENTER_INTEGRATION_CODE_DATE_SOURCE_BYLIST = function(p_folder, p_datatable=data.table(),
         list_filefrom=list("download_hsx_stkvn_ref.rds", "download_hnx_stkvn_ref.rds",
                            "download_upc_stkvn_ref.rds"),
         p_fileto    = "download_mlt_stkvn_ref.rds",
         IntegrateTo = "download_mlt_stkvn_ref_history.rds",
         MaxDate     = T) {
  # ------------------------------------------------------------------------------------------------
  DBL_RELOAD_INSREF()
  # if (!exists("ins_ref")) { ins_ref = DATACENTER_LOADDTRDS_UDATA('efrc_ins_ref.rds') }
  
  # IFRC_RELEASE_MEMORY("",F)
  DATACENTER_LINE_BORDER(paste("MHM_DATACENTER_INTEGRATION_CODE_DATE_SOURCE_BYLIST : ", p_fileto))
  OK=F
  
  if (nrow(p_datatable)>0) {
    OK = T
    dt_from = p_datatable
    dt_from = CLEAN_FIELDS_DATATABLE(dt_from)
  } else {
    dt_from = data.table()
    
    for (p_filefrom in list_filefrom)
    {
      # p_filefrom = list_filefrom[[1]]
      # p_filefrom = "efrc_indifrctemp_history.rds"
      # p_filefrom = "download_ssi_stkvn_ref_day.rds"
      if (file.exists(paste0(p_folder, p_filefrom)))
      {
        # dt_from1    = try(DATACENTER_LOADDTRDS(paste0(p_folder, p_filefrom), T))
        dt_from1    = try(CCPR_READRDS(p_folder, p_filefrom, ToKable = T, ToRestore = T))
        dt_from1    = CLEAN_FIELDS_DATATABLE(dt_from1)
        # str(dt_from1)
        x = (all(class(dt_from1)!="try-error"))
        if (all(class(dt_from1)!="try-error") & nrow(dt_from1)>0)
          # if (all(class(dt_from1)!="try-error"))
        {
          if (nrow(dt_from1)>0)
          {
            if (!'source' %in% names(dt_from1) & 'market' %in% names(dt_from1)) { dt_from1[, source:=market] }
            # My.Kable(dt_from1)
            # str(dt_from1)
            if ("timestamp"   %in% names(dt_from1)) {dt_from1[, timestamp  := as.character(timestamp)]}
            if ("timestampvn" %in% names(dt_from1)) {dt_from1[, timestampvn  := as.character(timestampvn)]}
            if ("updated"     %in% names(dt_from1)) {dt_from1[, updated  := as.character(updated)]}
            
            if ("source" %in% colnames(dt_from1)) {dt_from1  = CLEAN_SOURCE(dt_from1)}
            if ("updated" %in% colnames(dt_from1)) { dt_from1[, updated:=as.character(updated)] }
            dt_from1[, date:=as.Date(date)]
            # if (nchar(DateMin)>0) { dt_from1 = dt_from1[date>=as.Date(DateMin)]}
            dt_from   = rbind(dt_from, dt_from1, fill=T)
            dt_from   = unique(dt_from, by=c("code", "date", "source"))
            # print("done")
          }
        }
      }
    }
    
    if (nrow(dt_from)>0) { OK = T; dt_from = unique(dt_from, by=c("code", "date", "source"))} else { OK=F}
  }
  
  # My.Kable(dt_from[order(-date)])
  
  if (OK)
  {
    if (MaxDate)
    {
      pMaxDate = max(dt_from[!is.na(date)]$date)
      dt_from  = dt_from[date==pMaxDate]
      My.Kable.MaxCols(dt_from)
    }
    
    if (!file.exists(paste0(p_folder, p_fileto))) {
      dt_to = data.table()
    } else {
      dt_to      = CCPR_READRDS(p_folder, p_fileto, ToRestore = T)
      dt_to      = CLEAN_FIELDS_DATATABLE(dt_to)
    }
    dt_common  = dt_from
    dt_common = CLEAN_FIELDS_DATATABLE(dt_common)
    # My.Kable(dt_to[order(-date)])
    n0         = nrow(dt_to)
    
    # CommonFields = intersect(names(dt_to), names(dt_from))
    dt_to = rbind(dt_to, dt_common, fill=T)[!is.na(code) & !is.na(date) & !is.na(source)]
    dt_to[, code:=toupper(trimws(code))]
    # str(dt_to)
    if ("source" %in% names(dt_to)) { dt_to = CLEAN_SOURCE(dt_to)}
    dt_to = unique(rbind(dt_to, dt_common, fill=T), by=c("code", "date", "source"), fromLast=T)[order(code, date, source)]
    
    # PrintQ(max(dt_to$date))
    catrep("=", 125)
    if ("x" %in% colnames(dt_to)) {dt_to$x = NULL}
    
    if ("close" %in% names(dt_to)) { dt_to[, close:=as.numeric(close)]}
    # WRITE_SUMMARY_CURRENT_DUO(dt_to, p_fileto)
    My.Kable.MaxCols(dt_to)

    # xs = WRITE_SUMMARY_CURRENT_DUO (p_folder, my.data=dt_to, x.file=p_fileto, silent=F, ToSaveDuo=T)
    CCPR_SAVERDS(dt_to, p_folder, p_fileto, ToSummary = T, SaveOneDrive = T)
    # DATACENTER_SAVEDTRDS(dt_to, paste0(p_folder, p_fileto))
    n1         = nrow(dt_to)
    DATACENTER_LINE_BORDER(paste("NEW RECORDS = ", Format.Number(n1-n0,0)))
    
    if (nchar(IntegrateTo)>0)
    {
      Data_Old =  CCPR_READRDS(p_folder, IntegrateTo, ToRestore = T)
      # Data_Old = DATACENTER_LOADDTRDS(paste0(p_folder, IntegrateTo), F)
      Data_Old = unique(rbind(Data_Old, dt_to, fill=T), by=c("code", "date", "source"), fromLast=T)[order(code, date, source)]
      My.Kable.MaxCols(Data_Old)
      # DATACENTER_SAVEDTRDS(Data_Old, paste0(p_folder, IntegrateTo))
      CCPR_SAVERDS(Data_Old, p_folder, IntegrateTo, ToSummary = T, SaveOneDrive = T)
      # xs = WRITE_SUMMARY_CURRENT_DUO (p_folder, my.data=Data_Old, x.file=IntegrateTo, silent=F, ToSaveDuo=T)
    }
  }
  rm(xs, dt_from, dt_common); IFRC_RELEASE_MEMORY("",F)
  return(dt_to)
}


# ==============================================================================
FIX_LIQUIDITY_INTEGRATION_VOLUME = function(Folder_Fr = CCPRData,                    File_Fr = 'DOWNLOAD_SSI_STKVN_PRICES.rds', 
                                            Folder_To = CCPRData,                    File_To = 'DOWNLOAD_SSI_STKVN_LIQUIDITY.rds') {
  # ------------------------------------------------------------------------------------------------
  Data_Fr = CCPR_READRDS(Folder_Fr, File_Fr, ToKable=T)
  if (nrow(Data_Fr)> 0)
  {
    Data_To = CCPR_READRDS(Folder_To, File_To, ToKable=T)
    if ('volume' %in% names(Data_Fr)) { Data_Final1 = unique(Data_Fr[!is.na(volume)], by=c('code', 'date')) } else { Data_Final1 = data.table() }
    if (nrow(Data_To)> 0)
    {
      if ('volume' %in% names(Data_To)) { Data_Final2 = unique(Data_To[!is.na(volume)], by=c('code', 'date')) } else { Data_Final2 = data.table() }
    } else {
      Data_Final2 = data.table() 
    }
    
    if (nrow(Data_Final1)>0) { Data_Final1[, volume:=as.numeric(gsub(',','', volume))] }
    if (nrow(Data_Final2)>0) { Data_Final2[, volume:=as.numeric(gsub(',','', volume))] }
    Final_Data = unique(rbind(Data_Final1, Data_Final2, fill=T), by=c('code', 'date'))
    My.Kable(Final_Data[order(date, code)][,  .(source, code, date, volume)])
    if (nrow(Final_Data)>0) { CCPR_SAVERDS(Final_Data, Folder_To, File_To, ToSummary = T, SaveOneDrive = T) }
  }
}

# ==============================================================================
FIX_INTEGRATION_SHARESOUT = function(Folder_Fr = paste0(CCPRData, 'DAY/'), File_Fr = 'CCPR_DOWNLOAD_SSI_STKVN_SHARES.rds', 
                                     Folder_To = CCPRData,                    File_To = 'DOWNLOAD_SSI_STKVN_SHARES_DAY.rds') {
  # ------------------------------------------------------------------------------------------------
  Data_Fr = CHECK_CLASS(try(CCPR_READRDS(Folder_Fr, File_Fr, ToKable=T, ToRestore = T)))
  # Data_Fr[, sharesout := shareout]
  # Data_Fr[, shareout := NULL]

  if (nrow(Data_Fr)> 0)
  {
    Data_To = CHECK_CLASS(try(CCPR_READRDS(Folder_To, File_To, ToKable=T, ToRestore = T)))
    if ('sharesout' %in% names(Data_Fr)) { Data_Final1 = unique(Data_Fr[!is.na(sharesout)], by=c('code', 'date')) } else { Data_Final1 = data.table() }
    if (nrow(Data_To)> 0)
    {
      if ('sharesout' %in% names(Data_To)) { Data_Final2 = unique(Data_To[!is.na(sharesout)], by=c('code', 'date')) } else { Data_Final2 = data.table() }
    } else {
      Data_Final2 = data.table() 
    }
    if (nrow(Data_Final1)>0) { Data_Final1[, sharesout:=as.numeric(gsub(',','', sharesout))] }
    if (nrow(Data_Final2)>0) { Data_Final2[, sharesout:=as.numeric(gsub(',','', sharesout))] }
    Final_Data = unique(rbind(Data_Final1, Data_Final2, fill=T), by=c('code', 'date'))
    if (nrow(Final_Data)>0) 
    { 
      My.Kable(Final_Data[order(date, code)][,  .(source, code, date, sharesout)])
      CCPR_SAVERDS(Final_Data, Folder_To, File_To, ToSummary = T, SaveOneDrive = T)
    }
    # My.Kable(Final_Data[order(date, code)][,  .(source, code, date, sharesout)])
    # if (nrow(Final_Data)>0) { CCPR_SAVERDS(Final_Data, Folder_To, File_To, ToSummary = T, SaveOneDrive = T) }
  }
}

# ==================================================================================================
FIX_UDATA = function(Action='DOWNLOAD_HSX_STKVN_SNAPSHOT_DAY', pPROBA = 80)  {
  # ------------------------------------------------------------------------------------------------
  switch(Action,
         'DOWNLOAD_EXC_SHARES_HISTORY' = {
           # pPROBA = 80
           for (pMarket in list('HSX', 'HNX', 'UPC'))
           {
              # pMarket = 'UPC'
              ToCheckDate = (runif(1,1,100)>50)
              if (SUMMARY_DATE(paste0(UData, paste0('download_',pMarket,'_stkvn_shares_day.rds'))) < SYSDATETIME(9) ||  !ToCheckDate)
              {
                LastTrading      = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
                x                = CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('download_',pMarket,'_stkvn_shares_', gsub('-','',LastTrading), '.rds'), ToKable = T)
                str(x)
                x                = MERGE_DATASTD_BYCODE(x, pOption = 'FINAL')
                My.Kable(x)
                # CCPR_SAVERDS(x, 'S:/STKVN/PRICES/DAY/', paste0('download_hnx_stkvn_snapshot_', gsub('-','',LastTrading), '.rds'), ToSummary = T, SaveOneDrive = F)
                CCPR_SAVERDS(x, UData, paste0('download_',pMarket,'_stkvn_shares_day.rds'), ToSummary = T, SaveOneDrive = F)
              }

             try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = UData, 
                                                                       File_Fr     = paste0('download_', pMarket, '_stkvn_shares_day.rds'), 
                                                                       Folder_To   = UData,                  
                                                                       File_To     = paste0('download_', pMarket, '_stkvn_shares_history.rds'), 
                                                                       pCondition  = '', 
                                                                       RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>90)))

             try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = UData, 
                                                                        File_Fr     = paste0('download_', pMarket, '_stkvn_shares_day.rds'), 
                                                                        Folder_To   = UData,                  
                                                                        File_To     = paste0('download_', 'EXC', '_stkvn_shares_day.rds'), 
                                                                        pCondition  = '', 
                                                                        RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>90)))       
           }
           try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = UData, 
                                                                     File_Fr     = paste0('download_', 'EXC', '_stkvn_shares_day.rds'), 
                                                                     Folder_To   = UData,                  
                                                                     File_To     = paste0('download_', 'EXC', '_stkvn_shares_history.rds'), 
                                                                     pCondition  = '', 
                                                                     RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>90)))
         },
         'DOWNLOAD_EXC_STKVN_SHARES' = {
            for (pMarket in list('HSX', 'HNX', 'UPC'))
            {
              ToCheckDate = (runif(1,1,100)>50)
              if (SUMMARY_DATE(paste0(UData, paste0('download_',pMarket,'_stkvn_shares_day.rds'))) < SYSDATETIME(9) ||  !ToCheckDate)
              {
                LastTrading      = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
                x                = CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('download_',pMarket,'_stkvn_shares_', gsub('-','',LastTrading), '.rds'), ToKable = T)
                str(x)
                x                = MERGE_DATASTD_BYCODE(x, pOption = 'FINAL')
                My.Kable(x)
                # CCPR_SAVERDS(x, 'S:/STKVN/PRICES/DAY/', paste0('download_hnx_stkvn_snapshot_', gsub('-','',LastTrading), '.rds'), ToSummary = T, SaveOneDrive = F)
                CCPR_SAVERDS(x, UData, paste0('download_',pMarket,'_stkvn_shares_day.rds'), ToSummary = T, SaveOneDrive = F)
              }              
            }  
         },
         'DOWNLOAD_EXC_STKVN_PRICES' = {
           ToCheckDate = (runif(1,1,100)>50)
           if (file.exists(UData))
           {
             for (pMarket in list('HSX', 'HNX', 'UPC')){
              if (SUMMARY_DATE(paste0(UData, paste0('download_',pMarket,'_stkvn_prices.rds'))) < SYSDATETIME(8) ||  !ToCheckDate)
              {
                LastTrading      = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
                x                = CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('download_',pMarket,'_pricesboard_', gsub('-','',LastTrading), '.rds'), ToKable = T, ToRestore = T)
                # str(x)
                x                = MERGE_DATASTD_BYCODE(x, pOption = 'FINAL')
                My.Kable(x)
                # CCPR_SAVERDS(x, 'S:/STKVN/PRICES/DAY/', paste0('download_hnx_stkvn_snapshot_', gsub('-','',LastTrading), '.rds'), ToSummary = T, SaveOneDrive = F)
                CCPR_SAVERDS(x, UData, paste0('download_',pMarket,'_stkvn_prices.rds'), ToSummary = T, SaveOneDrive = T)
                                                                          
              }
               
               try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = UData, 
                                                                         File_Fr     = paste0('download_',pMarket,'_stkvn_prices.rds'), 
                                                                         Folder_To   = UData,                  
                                                                         File_To     = paste0('download_',pMarket,'_stkvn_prices_history.rds'), 
                                                                         pCondition  = '', 
                                                                         RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=T))
               
               try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = UData, 
                                                                         File_Fr     = paste0('download_',pMarket,'_stkvn_prices.rds'), 
                                                                         Folder_To   = UData,                  
                                                                         File_To     = paste0('download_',pMarket,'_stkvn_history.rds'), 
                                                                         pCondition  = '', 
                                                                         RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=T))
               
               
               try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = UData, 
                                                                         File_Fr     = paste0('download_', pMarket, '_stkvn_prices.rds'), 
                                                                         Folder_To   = UData,                  
                                                                         File_To     = paste0('download_', 'exc', '_stkvn_prices.rds'), 
                                                                         pCondition  = '', 
                                                                         RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=T))    
             }
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = UData, 
                                                                      File_Fr     = paste0('download_', 'exc', '_stkvn_prices.rds'), 
                                                                      Folder_To   = UData,                  
                                                                      File_To     = paste0('download_', 'exc', '_stkvn_history.rds'), 
                                                                      pCondition  = '', 
                                                                      RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=T))
            x = CCPR_READRDS(UData, paste0('download_', 'exc', '_stkvn_history.rds'), ToKable = T, ToRestore = T)
            CCPR_SAVERDS(x, UData, paste0('download_', 'exc', '_stkvn_history.rds'), ToSummary = T, SaveOneDrive = T)
           }          
         },
         'DOWNLOAD_EXC_STKVN_REF' = {
          for (pMarket in list('HSX', 'HNX', 'UPC')){
              ToCheckDate = (runif(1,1,100)>50)
              if (SUMMARY_DATE(paste0(UData, paste0('download_',pMarket,'_STKVN_REF.rds'))) < SYSDATETIME(9) ||  !ToCheckDate)
              {
                LastTrading      = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
                x                = CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('download_',pMarket,'_stkvn_shares_', gsub('-','',LastTrading), '.rds'), ToKable = T)
                str(x)
                x                = MERGE_DATASTD_BYCODE(x, pOption = 'FINAL')
                My.Kable(x)
                # CCPR_SAVERDS(x, 'S:/STKVN/PRICES/DAY/', paste0('download_hnx_stkvn_snapshot_', gsub('-','',LastTrading), '.rds'), ToSummary = T, SaveOneDrive = F)
                CCPR_SAVERDS(x, UData, paste0('download_',pMarket,'_STKVN_REF.rds'), ToSummary = T, SaveOneDrive = F)
              }
          
            x = CCPR_READRDS(UData, paste0('DOWNLOAD_', pMarket, '_STKVN_REF.rds'), ToKable = T, ToRestore = T)
            CCPR_SAVERDS(x, UData, paste0('DOWNLOAD_', pMarket, '_STKVN_REF.rds'), ToSummary = T, SaveOneDrive = T)

            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData, 
                                                  File_Fr     = paste0('DOWNLOAD_', pMarket, '_STKVN_REF.rds'),
                                                  File_To     = paste0('DOWNLOAD_', pMarket, '_STKVN_REF_HISTORY.rds'),
                                                  RemoveNA    = T, NbDays = 0, pCondition = 'type=="STK"', MinPercent = 0, 
                                                  ToForce     = ifelse(pPROBA>75,T,F))) 

            x = CCPR_READRDS(UData, paste0('DOWNLOAD_', pMarket, '_STKVN_REF_HISTORY.rds'), ToKable = T, ToRestore = T)
            CCPR_SAVERDS(x, UData, paste0('DOWNLOAD_', pMarket, '_STKVN_REF_HISTORY.rds'), ToSummary = T, SaveOneDrive = T)

            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData, 
                                                              File_Fr     = paste0('DOWNLOAD_', pMarket, '_STKVN_REF.rds'),
                                                              File_To     = paste0('DOWNLOAD_', 'EXC', '_STKVN_REF.rds'),
                                                              RemoveNA    = T, NbDays = 0, pCondition = 'type=="STK"', MinPercent = 0, 
                                                              ToForce     = ifelse(pPROBA>75,T,F)))
          }
          try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData, 
                                                  File_Fr     = paste0('DOWNLOAD_', 'EXC', '_STKVN_REF.rds'),
                                                  File_To     = paste0('DOWNLOAD_', 'EXC', '_STKVN_REF_HISTORY.rds'),
                                                  RemoveNA    = T, NbDays = 0, pCondition = 'type=="STK"', MinPercent = 0, 
                                                  ToForce     = T))
            x = CCPR_READRDS(UData, paste0('download_', 'exc', '_STKVN_REF_HISTORY.rds'), ToKable = T, ToRestore = T)
            CCPR_SAVERDS(x, UData, paste0('download_', 'exc', '_STKVN_REF_HISTORY.rds'), ToSummary = T, SaveOneDrive = T)
         },
         'DOWNLOAD_SOURCE_LIQUIDITY' = {
           
           for (pSource in list('CAF','C68', 'STB', 'VND', 'HSX', 'HNX', 'UPC', 'EXC'))
           {
             if (file.exists(UData) ) {
               try(FIX_LIQUIDITY_INTEGRATION_VOLUME(Folder_Fr = UData, File_Fr = paste0('DOWNLOAD_', pSource, '_STKVN_PRICES.rds'), 
                                                    Folder_To = UData, File_To = paste0('DOWNLOAD_', pSource, '_STKVN_LIQUIDITY.rds')))
               
               try(FIX_LIQUIDITY_INTEGRATION_VOLUME(Folder_Fr = UData, File_Fr = paste0('DOWNLOAD_', pSource, '_STKVN_LIQUIDITY.rds'), 
                                                    Folder_To = UData, File_To = paste0('DOWNLOAD_', pSource, '_STKVN_LIQUIDITY_HISTORY.rds'))) 
             }
           }
           
         },
         'DOWNLOAD_MLT_STKVN_REF' = {
          x = IFRC_CCPR_DATACENTER_INTEGRATION_CODE_DATE_SOURCE_BYLIST(p_folder      = UData, 
                                                                        p_datatable   = data.table(),
                                                                        list_filefrom = list(
                                                                          "download_hsx_stkvn_ref.rds",
                                                                          "download_hnx_stkvn_ref.rds",
                                                                          "download_upc_stkvn_ref.rds"
                                                                        ),
                                                                        p_fileto      = "download_mlt_stkvn_ref.rds",
                                                                        IntegrateTo   = "download_mlt_stkvn_ref_history.rds",
                                                                        MaxDate       = T)
          for (pMarket in list('HSX', 'HNX', 'UPC')){
                x                = CCPR_READRDS(UData, paste0('download_',pMarket,'_stkvn_liquidity.rds'), ToKable = T, ToRestore = T)
                # str(x)
                My.Kable(x)
                # CCPR_SAVERDS(x, 'S:/STKVN/PRICES/DAY/', paste0('download_hnx_stkvn_snapshot_', gsub('-','',LastTrading), '.rds'), ToSummary = T, SaveOneDrive = F)
                CCPR_SAVERDS(x, UData, paste0('download_',pMarket,'_stkvn_liquidity.rds'), ToSummary = T, SaveOneDrive = T)
              
                try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = UData, 
                                                                          File_Fr     = paste0('download_',pMarket,'_stkvn_liquidity.rds'), 
                                                                          Folder_To   = UData,                  
                                                                          File_To     = paste0('download_',pMarket,'_stkvn_liquidity_history.rds'), 
                                                                          pCondition  = '', 
                                                                          RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=T))

                try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = UData, 
                                                                          File_Fr     = paste0('download_', pMarket ,'_stkvn_liquidity.rds'), 
                                                                          Folder_To   = UData,                  
                                                                          File_To     = paste0('download_','exc','_stkvn_liquidity.rds'), 
                                                                          pCondition  = '', 
                                                                          RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=T))

            }

            
          try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = UData, 
                                                                    File_Fr     = paste0('download_','exc','_stkvn_liquidity.rds'), 
                                                                    Folder_To   = UData,                  
                                                                    File_To     = paste0('download_','exc','_stkvn_liquidity_history.rds'), 
                                                                    pCondition  = '', 
                                                                    RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=T))

          x = IFRC_CCPR_DATACENTER_INTEGRATION_CODE_DATE_SOURCE_BYLIST(p_folder      = UData, 
                                                                        p_datatable   = data.table(),
                                                                        list_filefrom = list(
                                                                          "download_hsx_stkvn_liquidity.rds",
                                                                          "download_hnx_stkvn_liquidity.rds",
                                                                          "download_upc_stkvn_liquidity.rds"
                                                                        ),
                                                                        p_fileto      = "download_mlt_stkvn_liquidity.rds",
                                                                        IntegrateTo   = "download_mlt_stkvn_liquidity_history.rds",
                                                                        MaxDate       = T)
          
          x = IFRC_CCPR_DATACENTER_INTEGRATION_CODE_DATE_SOURCE_BYLIST(p_folder      = UData, 
                                                                       p_datatable   = data.table(),
                                                                       list_filefrom = list(
                                                                         "download_hsx_stkvn_prices.rds",
                                                                         "download_hnx_stkvn_prices.rds",
                                                                         "download_upc_stkvn_prices.rds"
                                                                       ),
                                                                       p_fileto      = "download_mlt_stkvn_prices.rds",
                                                                       IntegrateTo   = "download_mlt_stkvn_history.rds",
                                                                       MaxDate       = T)
          
          x = IFRC_CCPR_DATACENTER_INTEGRATION_CODE_DATE_SOURCE_BYLIST(p_folder      = UData, 
                                                                       p_datatable   = data.table(),
                                                                       list_filefrom = list(
                                                                         "download_hsx_stkvn_shares_day.rds",
                                                                         "download_hnx_stkvn_shares_day.rds",
                                                                         "download_upc_stkvn_shares_day.rds"
                                                                       ),
                                                                       p_fileto      = "download_mlt_stkvn_shares_day.rds",
                                                                       IntegrateTo   = "download_mlt_stkvn_shares_history.rds",
                                                                       MaxDate       = T)

          x = IFRC_CCPR_DATACENTER_INTEGRATION_CODE_DATE_SOURCE_BYLIST(p_folder      = UData,
                                                                       p_datatable   = data.table(),
                                                                       list_filefrom = list(
                                                                         'DOWNLOAD_HSX_STKVN_INTRADAY_DAY.rds',
                                                                         'DOWNLOAD_HNX_STKVN_INTRADAY_DAY.rds',
                                                                         'DOWNLOAD_UPC_STKVN_INTRADAY_DAY.rds'
                                                                       ),
                                                                       p_fileto      = "download_mlt_stkvn_intraday_day.rds",
                                                                       IntegrateTo   = "download_mlt_stkvn_intraday_history.rds",
                                                                       MaxDate       = T)
         },
         'DOWNLOAD_STB_STKVN' = {
           # REF
           ToCheckDate = (runif(1,1,100)>50)
           
           LastTrading = CCPR_LAST_TRADING_DAY_VN(8, ToPrompt = T)

           if (SUMMARY_DATE(paste0(UData, 'download_stb_stkvn_ref.rds')) < SYSDATETIME(8) | !ToCheckDate){
             
             x = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('DOWNLOAD_', 'STB', '_PRICESBOARD_', gsub('-','', LastTrading), '.rds'), ToKable = T, ToRestore = T)))
             x = x[date == max(date, na.rm = T)]
             Data_Old = CHECK_CLASS(try(CCPR_READRDS(UData, 'download_stb_stkvn_ref.rds', ToKable = T, ToRestore = T)))
             x = merge(x, Data_Old[, .(code, iso2, country, continent, type, name, last, shares, sharesout, fcat, scat, home, sample, isin)], all.x = T, by = 'code')
             
             CCPR_SAVERDS(x, UData, 'download_stb_stkvn_ref.rds', ToSummary = T, SaveOneDrive = T)
           }
           
           try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData, 
                                                              File_Fr     = paste0('DOWNLOAD_', 'STB', '_STKVN_REF.rds'),
                                                              File_To     = paste0('DOWNLOAD_', 'STB', '_STKVN_REF_HISTORY.rds'),
                                                              RemoveNA    = T, NbDays = 0, pCondition = '', MinPercent = 0, 
                                                              ToForce     = ifelse(pPROBA>75,T,F))) 
           
           if (SUMMARY_DATE(paste0(UData, 'download_stb_stkvn_prices.rds')) < SYSDATETIME(8) | !ToCheckDate){
             
             x = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('DOWNLOAD_', 'STB', '_PRICESBOARD_', gsub('-','', LastTrading), '.rds'), ToKable = T, ToRestore = T)))
             x = x[date == max(date, na.rm = T)]
             Data_Old = CHECK_CLASS(try(CCPR_READRDS(UData, 'download_stb_stkvn_prices.rds', ToKable = T, ToRestore = T)))
             x = merge(x, Data_Old[, .(code, iso2, country, continent, type, name, last, shares, sharesout, fcat, scat, home, sample, isin, capiusd, timestamp)], all.x = T, by = 'code')
             x = unique(x, by = c("code", "date"))
             CCPR_SAVERDS(x, UData, 'download_stb_stkvn_prices.rds', ToSummary = T, SaveOneDrive = T)
           }
           
           try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData, 
                                                              File_Fr     = paste0('DOWNLOAD_', 'STB', '_STKVN_prices.rds'),
                                                              File_To     = paste0('DOWNLOAD_', 'STB', '_STKVN_HISTORY.rds'),
                                                              RemoveNA    = T, NbDays = 0, pCondition = '', MinPercent = 0, 
                                                              ToForce     = ifelse(pPROBA>75,T,F))) 
           
          if (SUMMARY_DATE(paste0(UData, 'DOWNLOAD_STB_STKVN_PRICES.rds')) >
              SUMMARY_DATE(paste0(UData, 'DOWNLOAD_STB_STKVN_LIQUIDITY_HISTORY.rds')) | !ToCheckDate)
          {
            x = CHECK_CLASS(try(CCPR_READRDS(UData, 'DOWNLOAD_STB_STKVN_PRICES.rds', ToKable = T, ToRestore = T)))
            if ('volume' %in% names(x) && nrow(x[!is.na(volume)])>0)
            {
              Data_old = CHECK_CLASS(try(CCPR_READRDS(UData, 'DOWNLOAD_STB_STKVN_LIQUIDITY.rds', ToKable = T, ToRestore = T)))
              Data_old = rbind(Data_old, x, fill=T)
              if (nrow(Data_old)>0)
              {
                Data_old = unique(Data_old, by=c('code', 'date'), fromLast = T)
                My.Kable.Min(Data_old[order(date, code)])
                Data_old = UPDATE_UPDATED(Data_old)
                try(CCPR_SAVERDS(Data_old, UData, 'DOWNLOAD_STB_STKVN_LIQUIDITY.rds', ToSummary = T, SaveOneDrive = T))
              }
              Data_hst = CHECK_CLASS(try(CCPR_READRDS(UData, 'DOWNLOAD_STB_STKVN_LIQUIDITY_HISTORY.rds', ToKable = T, ToRestore = T)))
              Data_hst = rbind(Data_hst, Data_old, fill=T)
              if (nrow(Data_hst)>0)
              {
                Data_hst = unique(Data_hst, by=c('code', 'date'), fromLast = T)
                My.Kable.Min(Data_hst[order(date, code)])
                Data_hst = UPDATE_UPDATED(Data_hst)
                try(CCPR_SAVERDS(Data_hst, UData, 'DOWNLOAD_STB_STKVN_LIQUIDITY_HISTORY.rds', ToSummary = T, SaveOneDrive = T))
              }
            }
          }
         },
         'DOWNLOAD_INTRADAY_HISTORY' = {
           # list('HNX', 'UPC')
           LIST_SOURCE_INTRADAY_LINUX = c(LIST_SOURCES_PRICESBOARD, 'HSX', 'HNX', 'UPC', 'STB')
           for (pSource in LIST_SOURCE_INTRADAY_LINUX){
            # pSource = 'STB'
             try(PRICESBOARD_TO_UDATA_INTRADAY (pSource = pSource))
             
             try(TRAINEE_INTEGRATION_INTRADAY_BY_FILES_QUALITY(File_Folder = UData, 
                                                               File_Fr = paste0('DOWNLOAD_', pSource, '_STKVN_INTRADAY_TODAY.rds'), 
                                                               File_To = paste0('DOWNLOAD_', pSource, '_STKVN_INTRADAY_DAY.rds'), 
                                                               RemoveNA = T, NbDays = 0, pCondition = '', MinPercent = 0, 
                                                               ToForce = ifelse(pPROBA>75,T,F)))
             
             try(TRAINEE_INTEGRATION_INTRADAY_BY_FILES_QUALITY(File_Folder = UData, 
                                                               File_Fr = paste0('DOWNLOAD_', pSource, '_STKVN_INTRADAY_DAY.rds'), 
                                                               File_To = paste0('DOWNLOAD_', pSource, '_STKVN_INTRADAY_HISTORY.rds'), 
                                                               RemoveNA = T, NbDays = 0, pCondition = '', MinPercent = 0, 
                                                               ToForce = ifelse(pPROBA>75,T,F)))
           }
           
         },
         'DOWNLOAD_SOURCE_STKVN_PRICES' = {
          LIST_SOURCE_PRICES_LINUX = list('CAF', 'C68', 'VND')
          for (i in (1:length(LIST_SOURCE_PRICES_LINUX))){
            # i = 2
           ToCheckDate = (runif(1,1,100)>50)
            
            pSource = LIST_SOURCE_PRICES_LINUX[[i]]
            if (SUMMARY_DATE(paste0(UData, paste0('download_',pSource,'_stkvn_prices.rds'))) < SYSDATETIME(8) | !ToCheckDate)
            {
              LastTrading      = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
              x                = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('download_',pSource,'_pricesboard_', gsub('-','',LastTrading), '.rds'), ToKable = T)))
              # str(x)
              if (nrow(x) > 0){
                x                = MERGE_DATASTD_BYCODE(x, pOption = 'FINAL')
                My.Kable(x)
                # CCPR_SAVERDS(x, 'S:/STKVN/PRICES/DAY/', paste0('download_hnx_stkvn_snapshot_', gsub('-','',LastTrading), '.rds'), ToSummary = T, SaveOneDrive = F)
                CCPR_SAVERDS(x, UData, paste0('download_',pSource,'_stkvn_prices.rds'), ToSummary = T, SaveOneDrive = T)
              }
              # CCPR_SAVERDS(x, UData, paste0('download_',pSource,'_stkvn_ref.rds'), ToSummary = T, SaveOneDrive = F)
            }
            if (SUMMARY_DATE(paste0(UData, paste0('download_',pSource,'_stkvn_ref.rds'))) < SYSDATETIME(8) |!ToCheckDate)
            {
              LastTrading      = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
              x                = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('download_',pSource,'_pricesboard_', gsub('-','',LastTrading), '.rds'), ToKable = T)))
              # str(x)
              if (nrow(x) > 0){
                x                = MERGE_DATASTD_BYCODE(x, pOption = 'FINAL')
                My.Kable(x)
                # CCPR_SAVERDS(x, 'S:/STKVN/PRICES/DAY/', paste0('download_hnx_stkvn_snapshot_', gsub('-','',LastTrading), '.rds'), ToSummary = T, SaveOneDrive = F)
                # CCPR_SAVERDS(x, UData, paste0('download_',pSource,'_stkvn_prices.rds'), ToSummary = T, SaveOneDrive = F)
                CCPR_SAVERDS(x, UData, paste0('download_',pSource,'_stkvn_ref.rds'), ToSummary = T, SaveOneDrive = T)
              }
            }
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = UData, 
                                                                      File_Fr     = paste0('download_',pSource,'_stkvn_prices.rds'), 
                                                                      Folder_To   = UData,                  
                                                                      File_To     = paste0('download_',pSource,'_stkvn_history.rds'), 
                                                                      pCondition  = '', 
                                                                      RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=T))
            
            # x = CCPR_READRDS(UData, paste0('download_',pSource,'_stkvn_history.rds'), ToKable = T, ToRestore = T)
            # CCPR_SAVERDS(x, UData, paste0('download_',pSource,'_stkvn_history.rds'), ToSummary = T, SaveOneDrive = T)
            
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = UData, 
                                                                      File_Fr     = paste0('download_',pSource,'_stkvn_ref.rds'), 
                                                                      Folder_To   = UData,                  
                                                                      File_To     = paste0('download_',pSource,'_stkvn_ref_history.rds'), 
                                                                      pCondition  = '', 
                                                                      RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=T))
            
            # x = CCPR_READRDS(UData, paste0('download_',pSource,'_stkvn_ref_history.rds'), ToKable = T, ToRestore = T)
            # CCPR_SAVERDS(x, UData, paste0('download_',pSource,'_stkvn_ref_history.rds'), ToSummary = T, SaveOneDrive = T)
          }          
         },
         'DOWNLOAD_SOURCE_STKVN_SHARES' = {
           LastTrading      = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
           
           for (pSource in list('CAF', 'C68', 'STB', 'VND'))
           {
             # pSource = 'VND'
             if (file.exists(UData) ) {
               FIX_INTEGRATION_SHARESOUT(Folder_Fr = 'S:/STKVN/PRICES/DAY/', File_Fr = paste0('download_',pSource,'_stkvn_shares_',gsub('-','',LastTrading),'.rds'), 
                                                                    Folder_To = UData,                  File_To = paste0('DOWNLOAD_',      pSource, '_STKVN_SHARES_DAY.rds'))
                                                                    
               FIX_INTEGRATION_SHARESOUT(Folder_Fr = 'S:/STKVN/PRICES/DAY/', File_Fr = paste0('download_',pSource,'_stkvn_shares_day.rds'), 
                                                                    Folder_To = UData,               File_To = paste0('DOWNLOAD_',      pSource, '_STKVN_SHARES_DAY.rds'))                                                                    
               
               FIX_INTEGRATION_SHARESOUT(Folder_Fr = UData, File_Fr = paste0('DOWNLOAD_',      pSource, '_STKVN_SHARES_DAY.rds'), 
                                                                    Folder_To = UData,                   File_To = paste0('DOWNLOAD_',pSource, '_STKVN_SHARES_HISTORY.rds'))
             }
           }
         }

  )
}


# ==================================================================================================
DEV_UPDATE_BEQ_INDVN_HISTORY = function(ToReset = F) {
  # ------------------------------------------------------------------------------------------------
  # ToReset = T
  if (ToReset) { file.remove(paste0('S:/STKVN/INDEX_2024/', 'beq_indvn_history.rds')) }
  for (index_prefix in list('ARI', 'BNK', 'LOG', 'HLC', 'RST', 'RTL', 'BRK', 'ENY', 'FIN'))
  {
    try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = 'S:/STKVN/INDEX_2024/',  
                                                              File_Fr     = paste0('beq_ind', index_prefix, 'als_history.rds'), 
                                                              Folder_To   = 'S:/STKVN/INDEX_2024/',  
                                                              File_To     = 'beq_indvn_history.rds', 
                                                              pCondition  = '', 
                                                              RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=T))
    
  }
  x = CCPR_READRDS('S:/STKVN/INDEX_2024/', 'beq_indvn_history.rds', ToKable = T, ToRestore = T)
  My.Kable(x)
  return(x)
}

# ==================================================================================================
CHECK_UPLOAD_DATA_DASHBOARD_LIVE = function ( ) {
  # ------------------------------------------------------------------------------------------------
  FILE_XLS_DASHBOARD_LIVE = setDT(read.xlsx(paste0('S:/STKVN/INDEX_2024/', 'CHECK_UPDATE_DASHBOARD_LIVE 1.xlsx')))
  file_vnind              = FILE_XLS_DASHBOARD_LIVE[delay == 0 & !is.na(upload_location) & !is.na(local_location)]
  list_folder             = unique(file_vnind$local_location)
  INDEX_VND               = file_vnind[local_location == 'S:/STKVN/INDEX_2024/']
  #try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=My_action, pAction="COMPARE", NbSeconds=Minutes, ToPrint=F))
  Minutes   = (0*60)
  
  for (folder in list_folder)
  {
    # folder = 'S:/STKVN/INDEX_2024/'
    x    = file_vnind[local_location == folder]
    if ( nrow(x) > 0)
    {
      for (k in 1:nrow(x) )
      {
        # k = 1
        Folder_fr = folder
        File_fr   = x[k]$local_file
        Folder_to = x[k]$upload_location
        File_to   = x[k]$upload_file
        table     = x[k]$table
        # table = 'ccpi_dashboard_indvn'
        ## LOAD DATA TU HOST
        y = try(TRAINEE_LOAD_SQL_TABLES_HOST (pTableSQL = c(table), pHost = 'dashboard_live', ToDisconnect=T,  SaveFolder = 'S:/DASHBOARD_LIVE/HOST/', ToKable=T) )
        delay_host  = as.numeric(LastTrading - y[[1]]$date)
        delay_local = SUMMARY_DATE(paste0(Folder_fr, File_fr))
        
        x[, ":=" (delay_host = delay_host, delay_local = delay_local) ]
        ### COMPARE LOCAL / HOST
        
      }
    }
  }
  for (pOption in list('LOGISTICS', 'AI', 'PETROLIMEX', 'REALESTATE', 'RETAIL', 'BANK', 'HEALTHCARE', 'ENERGY', 'FINANCE'))
  {
    switch ( pOption, 
             'LOGISTICS'       = { pFactor = 'log' } ,
             'BROKERAGE'       = { pFactor = 'brk' } ,
             'PETROLIMEX'      = { pFactor = 'pet' } ,
             'REALESTATE'      = { pFactor = 'rst' } ,
             'RETAIL'          = { pFactor = 'rtl' } ,
             'BANK'            = { pFactor = 'bnk' } ,
             'HEALTHCARE'      = { pFactor = 'hlc' } ,
             'ENERGY'          = { pFactor = 'eny' } ,
             'FINANCE'         = { pFactor = 'fin' } ,
             'AI'              = { pFactor = 'ari' } 
    )
    CATln('CLEAN COL...')
    CATln_Border(pOption)
    X = DASHBOARD_LIVE_COPY_INDEX (PROBA     = 0, 
                                   Folder_fr = 'S:/STKVN/INDEX_2024/',
                                   File_fr   = paste0('beq_ind', pFactor, 'als_history.rds'),
                                   Folder_to = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                   File_to   = paste0('ccpi_dashboard_ind',pFactor, '.rds'))
    
    compare = COMPARE_SUMMARY_DATE(Folder_fr = Folder_fr, File_fr = File_fr, Folder_to = Folder_to, File_to = File_to)
    if (all(compare))
    {
      try (CALCULATE_UPLOAD_BEQ_INDEX_2024 (INDEX.NAME= pOption, ToCheckDate = T, ToUpload = F , Minutes = Minutes))
    }
    
  }
  
  
  
  My.Kable(x)
  return(x)
}

# ==================================================================================================
DASHBOARDLIVE_ADD_COL_CODE = function (Folder = 'S:/CCPR/DATA/DASHBOARD/',
                                       File   = 'ccpi_dashboard_markets.rds', 
                                       col_replace = 'name'){
  # ------------------------------------------------------------------------------------------------
  
  data = CHECK_CLASS(try(CCPR_READRDS(Folder, File, ToKable = T, ToRestore = T)))
  if (col_replace %in% names(data) & !'code' %in% names(data))
  {
    data[, code := get(col_replace)]
  }
  try(CCPR_SAVERDS(data, Folder, File, ToSummary = T ))
  My.Kable(data)
  return(data)
  
}

# ==================================================================================================
TRAINEE_REPORT_DASHBOARLIVE = function (Folder_xls  = paste0(ODDrive, 'TRAINEE/'),
                                        File_xls    = 'CHECK_UPDATE_DASHBOARD_LIVE 1.xlsx', 
                                        Folder_Save = 'S:/STKVN/PRICES/REPORTS/',
                                        File_name   =  'report_upload_dashboard_live.rds', 
                                        PREFIX_LIST = list('LOCAL', 'UPLOAD', 'HOST'),
                                        ToUpload    = T ){
  # ------------------------------------------------------------------------------------------------
  FILE_XLS_DASHBOARD_LIVE = setDT(read.xlsx(paste0(Folder_xls, File_xls)))
  FILES_TODO              = as.data.table(FILE_XLS_DASHBOARD_LIVE[!is.na(upload_location) & !is.na(local_location)])
  FILES_TODO[, nr := seq.int(1, .N)]
  LOCAL                   = data.table()
  UPLOAD                  = data.table()
  HOST                    = data.table()
  LOCAL_UPLOAD_HOST              = data.table()
  REPORT                  = data.table()
  DBL_DATA_NOK            = data.table()
  
  for (PREFIX in PREFIX_LIST)
  {
    switch (PREFIX,
            'LOCAL' = {
              #FILES_TODO               = unique(FILES_TODO, by = c('local_file', 'local_location'))
              FILES_TODO$local_summary = as.Date(NA)
              FILES_TODO$local_delay   = as.numeric(NA)
              for ( k in 1:nrow(FILES_TODO))
              {
                # k = 6
                summary_file = SUMMARY_DATE(paste0(FILES_TODO[k]$local_location, FILES_TODO[k]$local_file))
                if (summary_file != '1900-01-01' & summary_file != -Inf)
                {
                  x  = FILES_TODO[k]$delay 
                  FILES_TODO[k]$local_summary = summary_file
                  if (x == 1)
                  {
                    FILES_TODO[k]$local_delay   = as.numeric(SYSDATETIME(FILES_TODO[k]$hour) - FILES_TODO[k]$local_summary)
                  }else{
                    FILES_TODO[k]$local_delay   = as.numeric(CCPR_LAST_TRADING_DAY_VN(FILES_TODO[k]$hour) - FILES_TODO[k]$local_summary)
                  }
                  
                  if (FILES_TODO[k]$local_delay < 0 )
                  {
                    FILES_TODO[k]$local_delay = 0
                  }
                }
              }
              My.Kable.All(FILES_TODO[,. (local_location, local_file, local_summary, local_delay)])
              LOCAL = FILES_TODO[,. (nr, div, code_div, local_location, local_file, local_summary, local_delay)]
              LOCAL[, type := PREFIX]
            }, 
            'UPLOAD' = {
              #FILES_TODO                = unique(FILES_TODO, by = c('upload_file', 'upload_location'))
              FILES_TODO$upload_summary = as.Date(NA)
              FILES_TODO$upload_delay   = as.numeric(NA)
              for ( k in 1:nrow(FILES_TODO))
              {
                summary_file = SUMMARY_DATE(paste0(FILES_TODO[k]$upload_location, FILES_TODO[k]$upload_file))
                if (summary_file != '1900-01-01' & summary_file != -Inf)
                {
                  
                  x  = FILES_TODO[k]$delay 
                  FILES_TODO[k]$upload_summary = summary_file
                  if (x == 1)
                  {
                    FILES_TODO[k]$upload_delay   = as.numeric(SYSDATETIME(FILES_TODO[k]$hour) - FILES_TODO[k]$upload_summary)
                  }else{
                    FILES_TODO[k]$upload_delay   = as.numeric(CCPR_LAST_TRADING_DAY_VN(FILES_TODO[k]$hour) - FILES_TODO[k]$upload_summary)
                  }
                  
                  if (FILES_TODO[k]$upload_delay < 0 )
                  {
                    FILES_TODO[k]$upload_delay = 0
                  }
                }
              }
              My.Kable.All(FILES_TODO[,. (upload_location, upload_file, upload_summary, upload_delay)])
              UPLOAD = FILES_TODO[,. (div, code_div, upload_location, upload_file, upload_summary, upload_delay)]
              UPLOAD[, type := PREFIX]
            },
            'HOST' = {
              
              # FILES_TODO                = unique(FILES_TODO, by = c('host_file', 'host_location'))
              FILES_TODO$host_summary = as.Date(NA)
              FILES_TODO[, host_summary := as.Date(host_summary)]
              FILES_TODO$host_delay   = as.numeric(NA)
              for ( k in 1:nrow(FILES_TODO))
              {
                 # k = 6
                # pFileName = paste0(FILES_TODO[k]$host_location, FILES_TODO[k]$host_file)
                summary_file = SUMMARY_DATE(paste0(FILES_TODO[k]$host_location, FILES_TODO[k]$host_file))
                if (summary_file != '1900-01-01' & summary_file != -Inf)
                {
                  x  = FILES_TODO[k]$delay 
                  FILES_TODO[k]$host_summary = summary_file
                  if (x == 1)
                  {
                    FILES_TODO[k]$host_delay   = as.numeric(SYSDATETIME(FILES_TODO[k]$hour) - FILES_TODO[k]$host_summary)
                  }else{
                    FILES_TODO[k]$host_delay   = as.numeric(CCPR_LAST_TRADING_DAY_VN(FILES_TODO[k]$hour) - FILES_TODO[k]$host_summary)
                  }
                  
                  if (FILES_TODO[k]$host_delay < 0 )
                  {
                    FILES_TODO[k]$host_delay = 0
                  }
                }
              }
              My.Kable.All(FILES_TODO[,. (host_location, host_file, host_summary, host_delay)])
              HOST = FILES_TODO[,. (div, code_div, host_location, host_file, host_summary, host_delay)]
              HOST[, type := PREFIX]
            }
    )
    
  }
  
  LOCAL  = LOCAL[, -c('div', 'type')]
  UPLOAD = UPLOAD[, -c('div', 'type')]
  HOST   = HOST[, -c('div', 'type')]

  REPORT = cbind(LOCAL[, -c('div', 'type')], UPLOAD[, -c('div', 'type', 'code_div')], HOST[, -c('div', 'type', 'code_div')])
  REPORT = REPORT[order(nr)]
  REPORT[, nr := as.numeric(nr)]
  REPORT[, id := as.numeric(seq.int(1, .N))]
  REPORT = UPDATE_UPDATED(REPORT)
  My.Kable.All(REPORT)
  
  # DBL_DATA_NOK = REPORT[is.na(upload_delay)]
  # try(CCPR_SAVERDS(DBL_DATA_NOK, Folder_Save, 'DBL_DATA_NOK.rds'))
  try(CCPR_SAVERDS(REPORT, Folder_Save, File_name))
  
  if ( nchar(ToUpload) > 0 )
  {
    x = UPLOAD_RDS_TABLE (pName = "intranet_dev",
                          pHost = "192.168.1.105",
                          pUser = "intranet_admin",
                          pPass = "admin@123456",
                          folder_name = "S:/STKVN/PRICES/REPORTS/",
                          file_name = "report_upload_dashboard_live.rds",
                          table_name = "process_dashboardlive",
                          key_column = "id")
  }
  return (REPORT)
}

# ==================================================================================================
DASHBOARD_LIVE_COPY_INDEX = function (PROBA     = 90, 
                                      Folder_fr = 'S:/STKVN/INDEX_2024/',
                                      File_fr   = paste0('beq_ind', pFactor, 'als_history.rds'),
                                      Folder_to = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                      File_to   = paste0('ccpi_dashboard_ind',pFactor, '.rds'),
                                      pData = data.table()){
  # ------------------------------------------------------------------------------------------------
  ToForce = runif(1,1,100) > PROBA
  compare = COMPARE_SUMMARY_DATE(Folder_fr = Folder_fr, File_fr = File_fr, Folder_to = Folder_to, File_to = File_to)
  DATA_FR = pData
  if (ToForce || compare[1] || (nrow(DATA_FR) > 0))
  {
    CATln('CLEAN COLUMNS...')
    DATA_FR = CHECK_CLASS(try(CCPR_READRDS(Folder_fr, File_fr, ToKable =T, ToRestore = T)))
    DATA_FR = DATA_FR [order(code, date)]
    if (nrow(DATA_FR) > 0)
    {
      if (!'name' %in% names(DATA_FR) & 'short_name' %in% names(DATA_FR))
      {
        DATA_FR[, name := short_name]
      }
      if (!'change' %in% names(DATA_FR))
      {
        DATA_FR [, change := close - shift(close), by = 'code']
      }
      if (!'rt' %in% names(DATA_FR))
      {
        DATA_FR [, rt := (close / shift(close)) - 1, by = 'code']
      }
      if (!'varpc' %in% names(DATA_FR))
      {
        DATA_FR [, varpc := (rt * 100), by = 'code']
      }
      if (!'var' %in% names(DATA_FR))
      {
        DATA_FR [, var := rt , by = 'code']
      }
      DATA_FR = UPDATE_UPDATED(DATA_FR)
      My.Kable(DATA_FR)
      try(CCPR_SAVERDS(DATA_FR, Folder_to, File_to, ToSummary = T, SaveOneDrive = T))
      CATln(paste0(Folder_to, File_to, ': COPIED'))
      catrep('-', 120)
    }
  }
  My.Kable(DATA_FR)
  return(DATA_FR)
}


# ==================================================================================================
COMPARE_SUMMARY_DATE = function(Folder_fr = 'S:/STKVN/INDEX_2024/', File_fr = 'beq_indlogals_history.rds',
                                Folder_to = 'S:/CCPR/DATA/DASHBOARD_LIVE/', File_to = 'ccpi_dashboard_indlog.rds'){
  # ------------------------------------------------------------------------------------------------
  
  Summary_fr = SUMMARY_DATE(paste0(Folder_fr, File_fr))
  Summary_to = SUMMARY_DATE(pFileName = paste0(Folder_to, File_to),  ToBrowse=T, DateOnly=T)
  
  OK_fr = Summary_fr != "1900-01-01"
  OK_to = Summary_to != "1900-01-01"
  
  Summary_fr_sup_to = Summary_fr > Summary_to
  print(c(Summary_fr_sup_to, OK_fr, OK_to))
  return (c(Summary_fr_sup_to, OK_fr, OK_to))
  
}
# ==================================================================================================
FIND_FINAL = function(row, sources, prefix) {
  # ------------------------------------------------------------------------------------------------
  switch(prefix,
         'FREEFLOAT' = {
           values = c()
           
           for (source in sources) {
             ff_col = paste0(source, '_ff_5pc')
             if (ff_col %in% names(row) && !is.na(row[[ff_col]]) && as.numeric(row[[ff_col]]) >= 0) {
               values = c(values, as.numeric(row[[ff_col]]))
             }
           }
           
           if (length(values) == 0) {
             return(NA)
           }
           
           value_counts = table(values)
           
           most_common_values = as.numeric(names(value_counts[value_counts == max(value_counts)]))
           
           if (length(most_common_values) == 1) {
             return(most_common_values[1])
           }
           
           for (source in sources) {
             ff_col = paste0(source,'_ff_5pc')
             if (ff_col %in% names(row) && !is.na(row[[ff_col]]) && as.numeric(row[[ff_col]]) %in% most_common_values) {
               return(as.numeric(row[[ff_col]]))
             }
           }
           
           return(NA)
         },
         'SHARESOUT' = {
           values = c()
           
           for (source in sources) {
             ff_col = paste0(source)
             if (ff_col %in% names(row) && !is.na(row[[ff_col]]) && as.numeric(row[[ff_col]]) >= 0) {
               values = c(values, as.numeric(row[[ff_col]]))
             }
           }
           
           if (length(values) == 0) {
             return(NA)
           }
           
           value_counts = table(values)
           
           most_common_values = as.numeric(names(value_counts[value_counts == max(value_counts)]))
           
           if (length(most_common_values) == 1) {
             return(most_common_values[1])
           }
           
           for (source in sources) {
             ff_col = paste0(source)
             if (ff_col %in% names(row) && !is.na(row[[ff_col]]) && as.numeric(row[[ff_col]]) %in% most_common_values) {
               return(as.numeric(row[[ff_col]]))
             }
           }
           
           return(NA)
         })
  
}

# ==================================================================================================
CONVERT_IFEXIST_FIELDS_DEV = function(pData=ins_last, AsType="numeric",
                                      List_Fields=list("cf_last", "cf_netchange")) {
  # ------------------------------------------------------------------------------------------------
  for (k in 1:length(List_Fields))
  {
    # k = 1
    pField = List_Fields[[k]]
    switch(AsType,
           "numeric"      = { pParseText = paste0(pField,":=as.numeric(", pField, ")") },
           "date"         = { pParseText = paste0(pField,":=as.Date(", pField, ")") },
           "character"    = { pParseText = paste0(pField,":=as.character(", pField, ")") }
    )
    if (pField %in% names(pData)) { 
      pData[, eval(parse(text = pParseText))] 
      if (AsType == 'character')
      {
        pParseTextNA = paste0(pField,":=ifelse(nchar(", pField, ")>0,", pField,",as.character(NA))") 
        pData[, eval(parse(text = pParseTextNA))]
      }
    }
  }
  # str(pData)
  return(pData)
}

# ==================================================================================================
TRAINEE_REPORT_FREEFLOAT = function(File_Folder='S:/STKVN/PRICES/DAY/', 
                                    Prefix = 'FREEFLOAT', 
                                    STKVN=T,
                                    LIST_SOURCES = list('EXC', 'VND', '24H','DNSE'),
                                    Pre_Date = '', Save = 'DRF') 
{
  # ------------------------------------------------------------------------------------------------
  LastTrading     = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
  FF_5PC = function(x) {
    ifelse((is.na(x) | x < 0), NA, ceiling(x / 5) * 5)
  }
  
  FF_10PC = function(x) {
    ifelse((is.na(x) | x < 0), NA, ceiling(x / 10) * 10)
  }
  
  
  # LIST_SOURCES = list('EXC', 'VND', '24H', 'DNSE')
  # Prefix = 'FREEFLOAT'
  # STKVN=T
  # File_Folder='S:/STKVN/PRICES/DAY/'
  # Pre_Date = ''
  Final_Data   = data.table()
  LIST_FREE_FLOAT              = list()
  
  DBL_RELOAD_INSREF()
  if (nchar(Pre_Date) != 0) {
    LastTrading = Pre_Date
  }else{
    LastTrading      = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
  }
  
  for (k in 1:length(LIST_SOURCES))
  {
    # k = 2
    pSource = LIST_SOURCES[[k]]
    
    File_Name_Day    = paste0('DOWNLOAD_', pSource, ifelse(STKVN,'_STKVN',''), '_', Prefix, '_DAY.rds')
    CATln_Border(paste0('DAY  = ', File_Name_Day))
    Data_Day         = CHECK_CLASS(try(CCPR_READRDS(File_Folder, File_Name_Day, ToKable = T, ToRestore = T)))
    
    File_Name_Date   = paste0('DOWNLOAD_', pSource, ifelse(STKVN,'_STKVN',''), '_', Prefix, '_', gsub('-','', LastTrading), '.rds')
    CATln_Border(paste0('DATE = ', File_Name_Date))
    Data_Date        = CHECK_CLASS(try(CCPR_READRDS(File_Folder, File_Name_Date, ToKable = T, ToRestore = T)))
    
    Data_All = rbind(Data_Day, Data_Date, fill=T)
    
    if (nrow(Data_All)>0) 
    {
      if (pSource == 'EXC'){
        Data_All[, dataTrading := as.Date(updated) ]
        Data_All = Data_All[dataTrading==LastTrading][, .(source = pSource, ticker = gsub('STKVN','',code),codesource = gsub('STKVN','',code), code, free_float = freefloat, date = dataTrading, updated, market = pSource)]
        Data_All = merge(Data_All[, -c('name')], ins_ref[, .(code, name=short_name)], all.x=T, by='code')
        LIST_FREE_FLOAT[[k]] = Data_All
      }
      else {
        Data_All = Data_All[date==LastTrading]
        Data_All[market == 'UPCOM', market := 'UPC']
        Data_All[market == 'HOSE', market := 'HSX']
        Data_All = unique(Data_All, by=c('code', 'date'))
        Data_All = merge(Data_All[, -c('name')], ins_ref[, .(code, name=short_name)], all.x=T, by='code')
        LIST_FREE_FLOAT[[k]] = Data_All
      }
    }
  }
  
  DATA_ALL = rbindlist(LIST_FREE_FLOAT, fill = T)
  DATA_ALL = DATA_ALL[, -c('close')]
  
  LastTrading     = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
  Prv_date        = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T, Option ='prev_date'))
  
  LIST_CODE_BY_EXC_PRV         = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('list_code_by_exc_', gsub('-', '', Prv_date), '.rds') )))
  if (nrow(LIST_CODE_BY_EXC_PRV) > 0){
    LIST_CODE_BY_EXC_PRV[, date := as.Date(date)]
  }
  
  LIST_CODE_BY_EXC_DAY         = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('list_code_by_exc_', gsub('-', '', LastTrading), '.rds') )))
  
  if (nrow(LIST_CODE_BY_EXC_DAY) > 0){
    LIST_CODE_BY_EXC_DAY[, date := as.Date(date)]
  }
  
  LIST_CODE_BY_EXC             = unique(rbind(LIST_CODE_BY_EXC_DAY, LIST_CODE_BY_EXC_PRV, fill = T)[order(-date)], by = 'code')
  
  DATA_ALL[, market := NULL]
  DATA_ALL = merge(DATA_ALL, LIST_CODE_BY_EXC[, .(codesource,market)], by = 'codesource', all.x = T)
  
  
  DATA_ALL = unique(DATA_ALL[date == LastTrading], by = c("source", "code"))
  # DATA_ALL_SHARES_DAY = merge(DATA_ALL_SHARES_DAY[, -c('name')], ins_ref[,. (name = short_name, code )], all.x = T, by = 'code')
  DATA_ALL = DATA_ALL[!is.na(free_float) & !is.na(market) & !is.na(source)]
  
  day = gsub("-", "", LastTrading)
  file_name_sources  = paste0("DATA_SOURCE_BY_FREEFLOAT_", day, ".rds")
  try(CCPR_SAVERDS(DATA_ALL, "S:/STKVN/PRICES/", file_name_sources))
  
  DATA_ALL_DAY_REPORT = unique(DATA_ALL[order(-date)][, .(source , market, date)], by = c("source", "market"))[, LastTrading := LastTrading]
  DATA_ALL_DAY_REPORT = DATA_ALL_DAY_REPORT [, nd := as.numeric(date - LastTrading)]
  DATA_ALL_DAY_REPORT = unique(DATA_ALL_DAY_REPORT, by = c("source", "market"))
  
  report_freefloat_date = setDT(spread(DATA_ALL_DAY_REPORT[,.(n = as.numeric(date - LastTrading)), by=c('source', 'market')], key='source', value='n'))
  report_freefloat_date = report_freefloat_date[order(market)][, ':='( updated = substr(Sys.time(),1,19) ,dataset = Prefix)]
  My.Kable.All(report_freefloat_date)
  
  report_freefloat_number = setDT(spread(DATA_ALL[,.(n = .N), by=c('source', 'market')], key='source', value='n'))[order(market)][, ':='( updated = substr(Sys.time(),1,19) ,dataset = Prefix)]
  My.Kable.All(report_freefloat_number)
  
  total_codes_by_market = LIST_CODE_BY_EXC[, .(total_codes = .N), by = .(market)]
  
  report_long = melt(report_freefloat_number, id.vars = c("market", "updated", "dataset"), 
                     variable.name = "source", value.name = "freefloat_number")
  
  merged_data = merge(report_long, total_codes_by_market, by = "market", all.x = TRUE)
  
  merged_data[, percent_freefloat := (freefloat_number / total_codes) * 100]
  
  merged_data[, percent_freefloat := pmin(100, round(percent_freefloat, 0))]
  
  # report_freefloat_percent = dcast(merged_data, market + updated + dataset ~ source, value.var = "percent_freefloat")
  
  report_freefloat_percent = merged_data[order(source)][, .(dataset,source, market, percent_freefloat)]
  # My.Kable.All(report_freefloat_percent)
  
  
  
  DATA_TEST = DATA_ALL[, .(dataset = Prefix, ticker, code, date, source, free_float)]
  DATA_TEST[is.na(source), source := 'EXC']
  
  # setnames(DATA_TEST, old = "free_float", new = paste0(source, '_ff'))
  stat  = setDT(spread(DATA_TEST, key = 'source', value = free_float))
  
  stat = stat %>%
    rename_with(~ paste0(., "_ff"), -c(dataset, ticker, code, date))
  
  # stat_5pc[VND_ff<0]
  # stat = stat %>%
  #   mutate(across(-c(dataset, ticker, code, date), ~ replace_na(., 0)))
  
  stat_5pc = stat %>%
    mutate(across(-c(dataset, ticker, code, date), 
                  .names = "{.col}_5pc", 
                  .fns = ~ FF_5PC(.))) 
  
  stat_10pc = stat %>%
    mutate(across(-c(dataset, ticker, code, date), 
                  .names = "{.col}_10pc", 
                  .fns = ~ FF_10PC(.)))
  
  Final_Data = merge(stat_5pc, stat_10pc, by = names(stat), all = TRUE)
  Final_Data[, final := apply(Final_Data, 1, function(row) FIND_FINAL(row, LIST_SOURCES, prefix = 'FREEFLOAT'))]
  # Final_Data[is.na(final)]
  DATA_FREEFLOAT = Final_Data
  market_prvdate = CHECK_CLASS(CCPR_READRDS('S:/STKVN/PRICES/FINAL/', paste0('DATA_FINAL_BY_MARKET_', gsub('-', '', Prv_date, '.rds'))))
  market_today   = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/FINAL/', 'DATA_FINAL_BY_MARKET_DAY.rds')))
  market         = rbind(market_prvdate, market_today, fill = T)
  market         = unique(market[order(-date)], by = 'code')
  DATA_FREEFLOAT = merge(DATA_FREEFLOAT , market[,. (code, market)], by = 'code', all.x = T)
  
  DATA_FINAL_BY_DATASET = DATA_FREEFLOAT[!is.na(final)][,. (code, date, freefloat = final, datetime = SYS.TIME() )]
  DATA_NOK              = DATA_ALL[!code %in% DATA_FINAL_BY_DATASET$code]
  
  MISSING_DOWNLOAD = LIST_CODE_BY_EXC[!code %in% DATA_FINAL_BY_DATASET$code]
  xlistff = list()
  if (nrow(MISSING_DOWNLOAD) > 0)
  {
    try(CCPR_SAVERDS(MISSING_DOWNLOAD, 'S:/STKVN/PRICES/DAY/', 'list_code_missing_ff.rds'))

    for ( k in 1:nrow(MISSING_DOWNLOAD))
    {
      # k = 1
      code = gsub('STKVN', '', MISSING_DOWNLOAD[k]$code)
      if ((CHECK_TIMEBETWEEN('10:00' , '18:00'))) 
      {
        msMarket = MISSING_DOWNLOAD[k]$market
        x = CHECK_CLASS(try(CCPR_READRDS("S:/CCPR/DATA/", paste0("download_",msMarket,"_stkvn_ref_history.rds"))))
        x = unique(rbind(x, MISSING_DOWNLOAD[k], fill = T), by = c('code', 'date'))
        try(CCPR_SAVERDS(x, "S:/CCPR/DATA/", paste0("download_",msMarket,"_stkvn_ref_history.rds"), ToSummary = T, SaveOneDrive = T))
      }
      for (i in 1:length(LIST_SOURCES))
      {
        # i = 1
        psource = LIST_SOURCES[[i]]
        if (psource == '24H') {psource_Download = '24HMONEY'} else {psource_Download = psource}
        My_Data = CHECK_CLASS(try(TRAINEE_DOWNLOAD_SOURCE_FREEFLOATPC (pSource = psource_Download, pCode  = code)))
        if (nrow(My_Data)>0)
        {
          My_Data[, ":="(dataset ='FREEFLOAT', source = psource, codesource = code, updated = substr(as.character(Sys.time()),1,19))] 
          xlistff[[paste0(code, psource)]] = My_Data
        }
      }
      
    }
    z = rbindlist(xlistff, fill = T)
    z = setDT(spread(z, key = 'source', value = 'free_float'))
    
    required_cols <- c("24H", "DNSE", "VND")
    for (col in required_cols) {
      if (!col %in% colnames(z)) {
        z[, (col) := NA]
      }
    }
    
    z_merged <- z[, .(
      code = first(code),
      date = first(date),
      updated = max(updated),
      dataset = first(dataset),
      `24H` = na.omit(`24H`)[1],
      DNSE = na.omit(DNSE)[1],
      VND = na.omit(VND)[1]
    ), by = .(ticker, codesource)]
    
    
    z_merged_stat_5pc = z_merged %>%
      mutate(across(-c(dataset, ticker, code, date, codesource, updated), 
                    .names = "{.col}_5pc", 
                    .fns = ~ FF_5PC(.))) 
    
    z_merged_stat_10pc = z_merged %>%
      mutate(across(-c(dataset, ticker, code, date, codesource, updated), 
                    .names = "{.col}_10pc", 
                    .fns = ~ FF_10PC(.)))
    
    z_new = merge(z_merged_stat_5pc, z_merged_stat_10pc, by = names(z_merged), all = TRUE)
    # z_new[, final := apply(z_new, 1, function(row) FIND_FINAL(row, LIST_SOURCES, prefix = 'FREEFLOAT'))]

    z_merged = z_new
    columns_to_check      = setdiff(names(z_merged), c("dataset", "date", "code", "market", 'updated', 'codesource', 'ticker'))
    
    
    z_merged[, final := apply(.SD, 1, function(x) {
      table_x <- table(x)
      if (length(table_x) == 0) {
        return(NA)
      } else if (all(is.na(x))) {
        return(NA)
      } else {
        max_count <- max(table_x[!is.na(names(table_x))], na.rm = TRUE)
        if (max_count == 0) {
          return(NA)
        } else {
          return(names(table_x)[which.max(table_x)])
        }
      }
    }), .SDcols = columns_to_check]
    
    print(z_merged)
    z_merged [final < 0 | is.na(final), final := as.numeric(NA)]

    z_merged = merge(z_merged, LIST_CODE_BY_EXC[,. (code, market )], by = 'code')
    DATA_FREEFLOAT= rbind(DATA_FREEFLOAT, z_merged , fill = T)
    # DATA_FREEFLOAT= DATA_FREEFLOAT[!is.na(final) ]
    DATA_FINAL_BY_DATASET = DATA_FREEFLOAT[!is.na(final)][,. (code, date, freefloat = final, datetime = SYS.TIME() )]
    DATA_NOK              = DATA_ALL[!code %in% DATA_FINAL_BY_DATASET$code][, datetime := SYS.TIME()]
    try(CCPR_SAVERDS(DATA_NOK, 'S:/STKVN/PRICES/DAY/', 'list_code_missing_ff.rds'))
    if (nrow(DATA_NOK) > 0)
    {
      DATA_NOK[, final := 100]
      DATA_FREEFLOAT = rbind(DATA_FREEFLOAT, DATA_NOK[,. (code, date, freefloat = final, datetime)], fill = T)
      DATA_FINAL_BY_DATASET = rbind(DATA_FINAL_BY_DATASET, DATA_NOK[,. (code, date, freefloat = final, datetime)], fill = T)
    }
  }
  
  REPORT = DATA_FREEFLOAT
  list_market = list('UPC', 'HSX', 'HNX')
  report_freefloat_percent = data.table()
  columns_to_check                    = setdiff(names(REPORT), c("dataset", "date", "code","market", "ticker", "value"))
  if (nrow(REPORT) > 0) 
  {
    # if (type != 'MARKET') 
    # {
    for (col in columns_to_check) 
    {
      # col = "final"
      for (i in 1:length(list_market))
      {
        # i = 1
        dt_filtered = data.table()
        mar         = list_market[[i]]
        dt_filtered = REPORT[, value := get(col)]
        dt_filtered = REPORT[!is.na(value) & market == mar]
        num         = as.numeric(nrow(dt_filtered))
        denom       = as.numeric(nrow(LIST_CODE_BY_EXC[market == mar]))
        percent     = round((num / denom) * 100, 2)
        report_freefloat_percent = rbind(report_freefloat_percent, data.table(dataset = Prefix, source = col, market = mar, percentage = percent), fill = T)
      }
    }
  }
  matched_rows = report_freefloat_percent[apply(report_freefloat_percent[, c("source")], 1, function(x) any(grepl("_ff_5pc", x) | grepl('final', x))), , drop=FALSE]
  report_freefloat_percent = matched_rows[, source := gsub('_ff_5pc','',source)]
  report_freefloat_percent = setDT(spread(report_freefloat_percent, key = 'source', value = 'percentage'))
  report_freefloat_percent$final[report_freefloat_percent$final > 100] = 100
  
  FINAL_DATA     = DATA_FREEFLOAT
  FINAL_DATA     = UPDATE_UPDATED(FINAL_DATA)
  FINAL_REPORT   = report_freefloat_percent
  FINAL_REPORT   = UPDATE_UPDATED(FINAL_REPORT)
  FINAL_FINAL    = DATA_FINAL_BY_DATASET
  FINAL_FINAL    = UPDATE_UPDATED(FINAL_FINAL)
  FINAL_NOTOK    = DATA_NOK
  FINAL_NOTOK    = UPDATE_UPDATED(FINAL_NOTOK)
  # Save = "DRF"
  
  if ( nchar(Save) > 0)
  {
    if (grepl('D', Save))
    {
      try(CCPR_SAVERDS(FINAL_DATA,'S:/STKVN/PRICES/DAY/', 'DATA_BY_FREEFLOAT.rds'))
      try(CCPR_SAVERDS(DATA_NOK, 'S:/STKVN/PRICES/DAY/', 'DATA_NOT_OK_BY_FREEFLOAT.rds'))
      
    }
    if (grepl('R', Save)) 
    {
      try(CCPR_SAVERDS(report_freefloat_date,'S:/STKVN/PRICES/REPORTS/', 'report_date_by_freefloat.rds'))
      try(CCPR_SAVERDS(report_freefloat_number,'S:/STKVN/PRICES/REPORTS/', 'report_number_by_freefloat.rds'))
      try(CCPR_SAVERDS(report_freefloat_percent,'S:/STKVN/PRICES/REPORTS/', 'report_percent_by_freefloat.rds'))
    }
    if (grepl('F', Save))
    {
      try(CCPR_SAVERDS(FINAL_FINAL,'S:/STKVN/PRICES/FINAL/', 'DATA_FINAL_BY_FREEFLOAT_DAY.rds'))
      try(CCPR_SAVERDS(FINAL_FINAL,'S:/STKVN/PRICES/FINAL/DAY/', paste0('DATA_FINAL_BY_FREEFLOAT_', gsub('-','', LastTrading),'.rds')))
    }
  }
  
  return(list(FINAL_DATA, FINAL_REPORT, FINAL_FINAL, FINAL_NOTOK))
}

# ==================================================================================================

TRAINEE_REPORT_SHARES = function (File_Folder = 'S:/STKVN/PRICES/DAY/',
                                  LIST_SOURCES = list('EXC', 'VND', 'MBS', 'VPS','TVSI', 'CAF', 'BSC','VST', 'C68',  'STB', 'DCL'),
                                  Save = 'DRF'){
  # ------------------------------------------------------------------------------------------------
  # File_Folder = 'S:/STKVN/PRICES/DAY/'
  # LIST_SOURCES = list('EXC', 'VND', 'MBS', 'VPS','TVSI', 'CAF', 'BSC','VST', 'C68',  'STB', 'DCL')
  LastTrading     = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
  DBL_RELOAD_INSREF()
  LIST_SHARES = list()
  FINAL_DATA           = data.table()
  FINAL_REPORT         = data.table()
  FINAL_FINAL          = data.table()
  FINAL_NOTOK          = data.table()
  Prefix = 'SHARESOUT'
  for (k in 1:length(LIST_SOURCES))
  {
    # # STKVN SHARES FILES 
    # k = 2
    pSource = LIST_SOURCES[[k]]
    
    File_Name_Day    = paste0('DOWNLOAD_', pSource, '_STKVN_SHARES_DAY.rds')
    CATln_Border(paste0('DAY  = ', File_Name_Day))
    Data_Day         = CHECK_CLASS(try(CCPR_READRDS(File_Folder, File_Name_Day, ToKable = T, ToRestore = T)))
    
    File_Name_Date   = paste0('DOWNLOAD_', pSource, '_STKVN_SHARES_', gsub('-','', LastTrading), '.rds')
    CATln_Border(paste0('DATE = ', File_Name_Date))
    Data_Date        = CHECK_CLASS(try(CCPR_READRDS(File_Folder, File_Name_Date, ToKable = T, ToRestore = T)))
    
    Data_All_Shares = rbind(Data_Day, Data_Date, fill=T)
    
    if (nrow(Data_All_Shares)>0)
    {
      Data_All_Shares  = Data_All_Shares[date==LastTrading]
      Data_All_Shares = unique(Data_All_Shares, by=c('code', 'market'))
      LIST_SHARES[[k]] = Data_All_Shares
    }
    
  }
  
  DATA_ALL_SHARES_DAY = rbindlist(LIST_SHARES, fill = T)
  LastTrading     = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
  Prv_date        = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T, Option ='prev_date'))
  LIST_CODE_BY_EXC_PRV         = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('list_code_by_exc_', gsub('-', '', Prv_date), '.rds') )))
  if (nrow(LIST_CODE_BY_EXC_PRV) > 0){
    LIST_CODE_BY_EXC_PRV[, date := as.Date(date)]
  }
  LIST_CODE_BY_EXC_DAY         = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('list_code_by_exc_', gsub('-', '', LastTrading), '.rds') )))
  if (nrow(LIST_CODE_BY_EXC_DAY) > 0){
    LIST_CODE_BY_EXC_DAY[, date := as.Date(date)]
  }
  LIST_CODE_BY_EXC             = unique(rbind(LIST_CODE_BY_EXC_DAY, LIST_CODE_BY_EXC_PRV, fill = T)[order(-date)], by = 'code')
  
  DATA_ALL_SHARES_DAY[, market := NULL]
  DATA_ALL_SHARES_DAY = merge(DATA_ALL_SHARES_DAY, LIST_CODE_BY_EXC[, .(codesource,market)], by = 'codesource', all.y = T)
  DATA_ALL_SHARES_DAY[, code := paste0('STKVN',codesource)]

  list_market = list('UPC', 'HSX', 'HNX')
  UDATA = data.table()
  xlist = list()
  for (k in 1: length(list_market)) 
  {
    pmarket = list_market[[k]]
    x = CHECK_CLASS(try(CCPR_READRDS(UData, paste0('DOWNLOAD_', pmarket, '_STKVN_SHARES_DAY.rds') )))
    if (nrow(x) > 0) 
    {
      x                = x[, .(code, market, date, sharesout, shareslis)][!is.na(sharesout)]
      xlist[[pmarket]] = x
    }
  }
  UDATA = rbindlist(xlist, fill = T)
  UDATA[, codesource := gsub('STKVN', '', code)]
  
  
  
  MISSING_SHARESOUT   = DATA_ALL_SHARES_DAY[is.na(sharesout)]
  if (nrow(MISSING_SHARESOUT) > 0)
  {
    DATA_ALL_SHARES_DAY = DATA_ALL_SHARES_DAY[!code %in% MISSING_SHARESOUT$code]
    MISSING_SHARESOUT   = merge(MISSING_SHARESOUT[,. (code)], UDATA, by = 'code', all.x = T)
    MISSING_SHARESOUT   = unique(MISSING_SHARESOUT[!is.na(code) & !is.na(sharesout)], by = 'code')
    DATA_ALL_SHARES_DAY = rbind(DATA_ALL_SHARES_DAY, MISSING_SHARESOUT, fill = T)
    
  }
  DATA_ALL_SHARES_DAY = unique(DATA_ALL_SHARES_DAY[date == LastTrading], by = c("source", "code"))
  # DATA_ALL_SHARES_DAY = merge(DATA_ALL_SHARES_DAY[, -c('name')], ins_ref[,. (name = short_name, code )], all.x = T, by = 'code')
  DATA_ALL_SHARES_DAY = DATA_ALL_SHARES_DAY[!is.na(sharesout) & !is.na(market)]
  day = gsub("-", "", LastTrading)
  file_name_sources  = paste0("DATA_SOURCE_BY_SHARES_", day, ".rds")
  try(CCPR_SAVERDS(DATA_ALL_SHARES_DAY, "S:/STKVN/PRICES/", file_name_sources))
  
  
  DATA_ALL_DAY_REPORT = unique(DATA_ALL_SHARES_DAY[order(-date)][, .(source , market, date)], by = c("source", "market"))[, LastTrading := LastTrading]
  DATA_ALL_DAY_REPORT = DATA_ALL_DAY_REPORT [, nd := as.numeric(date - LastTrading)]
  DATA_ALL_DAY_REPORT = unique(DATA_ALL_DAY_REPORT, by = c("source", "market"))
  
  # report_shares_date  = setDT(spread(DATA_ALL_DAY_REPORT[,.(n = as.numeric(date - LastTrading)), by=c('source', 'market')], key='source', value='n'))[order(market)][, ':='( updated = substr(Sys.time(),1,19) ,dataset = 'SHARESOUT')]
  # My.Kable.All(report_shares_date)
  # 
  # DATA_ALL_SHARES_NUMBER = unique(DATA_ALL_SHARES_DAY[order(date == LastTrading), by = c("source", "code")])
  # report_shares_number = setDT(spread(DATA_ALL_SHARES_NUMBER[,.(n = .N), by=c('source', 'market')], key='source', value='n'))[order(market)][, ':='( updated = substr(Sys.time(),1,19) ,dataset = 'SHARESOUT')]
  # My.Kable.All(report_shares_number)
  # 
  # # CCPR_SAVERDS(report_shares_date,'S:/SHINY/STKVN/REPORT_PRICESBOARD/', 'report_day_sources_shares_date.rds')
  # # CCPR_SAVERDS(report_shares_number,'S:/SHINY/STKVN/REPORT_PRICESBOARD/', 'report_day_sources_shares_number.rds')
  # 
  # total_codes_by_market = LIST_CODE_BY_EXC[, .(total_codes = .N), by = .(market)]
  # 
  # report_long = melt(report_shares_number, id.vars = c("market", "updated", "dataset"), 
  #                    variable.name = "source", value.name = "shares_number")
  # 
  # merged_data = merge(report_long, total_codes_by_market, by = "market", all.x = TRUE)
  # 
  # merged_data[, percent_shares := (shares_number / total_codes) * 100]
  # 
  # merged_data[, percent_shares := pmin(100, round(percent_shares, 0))]
  # 
  # report_shares_percent = dcast(merged_data, market + updated + dataset ~ source, value.var = "percent_shares")
  # 
  # # report_shares_percent = merged_data[order(source)][, .(dataset, source, market, percent_shares)]
  # My.Kable.All(report_shares_percent)
  
  
  DATA_TEST = DATA_ALL_SHARES_DAY[, .(dataset = Prefix, ticker = codesource, code, date, source, final = sharesout )]
  DATA_TEST[is.na(source), source := 'EXC']
  # setnames(DATA_TEST, old = "free_float", new = paste0(source, '_ff'))
  stat  = setDT(spread(DATA_TEST, key = 'source', value = final))
  stat[, final := apply(stat, 1, function(row) FIND_FINAL(row, LIST_SOURCES, prefix = 'SHARESOUT'))]
  DATA_SHARESOUT = stat
  
  MISSING_CODE_SHARES = LIST_CODE_BY_EXC[!code %in% DATA_SHARESOUT$code]
  data_final_day = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/FINAL/', 'DATA_FINAL_BY_SHARESOUT_DAY.rds', ToKable = T)))
  if (nrow(data_final_day) > 0 )
  {
    data_final_day[, final := sharesout]
    z = data_final_day[code %in% MISSING_CODE_SHARES$code][, ':=' (dataset = 'SHARESOUT', source = 'EXC')]
    DATA_SHARESOUT= rbind(DATA_SHARESOUT, z[,. (dataset, ticker = gsub('STKVN', '', code),  code, final)] , fill = T)
    DATA_FINAL_BY_DATASET = DATA_SHARESOUT[!is.na(final)][,. (code, date, final)]
    DATA_NOK              = DATA_ALL_SHARES_DAY[!code %in% DATA_FINAL_BY_DATASET$code]
  }
  MISSING_CODE_SHARES = LIST_CODE_BY_EXC[!code %in% DATA_SHARESOUT$code]
  xlist_shares = list()
  if (nrow(MISSING_CODE_SHARES) > 0)
  {
    try(CCPR_SAVERDS(MISSING_CODE_SHARES, 'S:/STKVN/PRICES/DAY/', 'missing_code_sharesout.rds'))
    for (k in 1:nrow(MISSING_CODE_SHARES))
    {
      # k = 1
      code = gsub('STKVN', '', MISSING_CODE_SHARES[k]$code)
      My_Data = CHECK_CLASS(try( TRAINEE_DOWNLOAD_HNX_STKVN_SHARES_BY_CODE (pCode = code) ))
      if (nrow(My_Data)>0)
      {
        My_Data[, ":="(dataset ='SHARESOUT', source='EXC', codesource = code, updated = substr(as.character(Sys.time()),1,19))] 
        xlist_shares[[code]] = My_Data
      }
    }
    z = rbindlist(xlist_shares, fill = T)
    z[, final := sharesout]
    DATA_SHARESOUT= rbind(DATA_SHARESOUT, zDATA_SHARESOUT= rbind(DATA_SHARESOUT, z[,. (dataset, ticker = gsub('STKVN', '', code),  code, final)] , fill = T) , fill = T)
    DATA_FINAL_BY_DATASET = DATA_SHARESOUT[!is.na(final)][,. (code, date, sharesout = final, datetime = SYS.TIME() )]
    DATA_NOK              = DATA_ALL_SHARES_DAY[!code %in% DATA_FINAL_BY_DATASET$code]
    
  }
  
  market_prvdate = CHECK_CLASS(CCPR_READRDS('S:/STKVN/PRICES/FINAL/', paste0('DATA_FINAL_BY_MARKET_', gsub('-', '', Prv_date, '.rds'))))
  market_today   = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/FINAL/', 'DATA_FINAL_BY_MARKET_DAY.rds')))
  market         = rbind(market_prvdate, market_today, fill = T)
  market         = unique(market[order(-date)], by = 'code')
  DATA_SHARESOUT = merge(DATA_SHARESOUT , market[,. (code, market)], by = 'code', all.x = T)
  
  
  REPORT = DATA_SHARESOUT
  list_market = list('UPC', 'HSX', 'HNX')
  report_sharesout_percent = data.table()
  columns_to_check                    = setdiff(names(REPORT), c("dataset", "date", "code","market", "ticker", "value", "source"))
  if (nrow(REPORT) > 0) 
  {
    # if (type != 'MARKET') 
    # {
    for (col in columns_to_check) 
    {
      # col = "EXC"
      for (i in 1:length(list_market))
      {
        # i = 1
        dt_filtered = data.table()
        mar         = list_market[[i]]
        dt_filtered = REPORT[, value := get(col)]
        dt_filtered = REPORT[!is.na(value) & market == mar]
        num         = as.numeric(nrow(dt_filtered))
        denom       = as.numeric(nrow(LIST_CODE_BY_EXC[market == mar]))
        percent     = round((num / denom) * 100, 2)
        report_sharesout_percent = rbind(report_sharesout_percent, data.table(dataset = Prefix, source = col, market = mar, percentage = percent), fill = T)
      }
    }
  }
  report_sharesout_percent = setDT(spread(report_sharesout_percent, key = 'source', value = 'percentage'))
  report_sharesout_percent[final > 100, final := 100]
  
  DATA_FINAL_BY_DATASET = DATA_SHARESOUT[!is.na(final)][,. (code, date, sharesout = final, datetime = SYS.TIME() )]
  DATA_NOK              = DATA_ALL_SHARES_DAY[!code %in% DATA_FINAL_BY_DATASET$code]
  
  FINAL_DATA     = DATA_SHARESOUT
  FINAL_DATA     = UPDATE_UPDATED(FINAL_DATA)
  FINAL_REPORT   = report_sharesout_percent
  FINAL_REPORT   = UPDATE_UPDATED(FINAL_REPORT)
  FINAL_FINAL    = DATA_FINAL_BY_DATASET
  FINAL_FINAL    = UPDATE_UPDATED(FINAL_FINAL)
  FINAL_NOTOK    = DATA_NOK
  FINAL_NOTOK    = UPDATE_UPDATED(FINAL_NOTOK)
  
  
  if (nchar(Save) > 0)
  {
    if (grepl('D', Save))
    {
      try(CCPR_SAVERDS(FINAL_DATA,'S:/STKVN/PRICES/DAY/', 'DATA_BY_SHARESOUT.rds'))
      try(CCPR_SAVERDS(DATA_NOK, 'S:/STKVN/PRICES/DAY/', 'DATA_NOT_OK_BY_SHARESOUT.rds'))
      
    }
    if (grepl('R', Save)) 
    {
      try(CCPR_SAVERDS(report_shares_date,'S:/STKVN/PRICES/REPORTS/', 'report_date_by_sharesout.rds'))
      try(CCPR_SAVERDS(report_shares_number,'S:/STKVN/PRICES/REPORTS/', 'report_number_by_sharesout.rds'))
      try(CCPR_SAVERDS(report_sharesout_percent,'S:/STKVN/PRICES/REPORTS/', 'report_percent_by_sharesout.rds'))
    }
    if (grepl('F', Save))
    {
      try(CCPR_SAVERDS(FINAL_FINAL,'S:/STKVN/PRICES/FINAL/', 'DATA_FINAL_BY_SHARESOUT_DAY.rds'))
      try(CCPR_SAVERDS(FINAL_FINAL,'S:/STKVN/PRICES/FINAL/DAY/', paste0('DATA_FINAL_BY_SHARESOUT_', gsub('-','', LastTrading),'.rds')))
    }
  }
  return(list(FINAL_DATA, FINAL_REPORT, FINAL_FINAL, FINAL_NOTOK))
}



# ==================================================================================================
TRAINEE_FINAL_BY_DATASET_DAILY = function(File_Folder='S:/STKVN/PRICES/DAY/', STKVN=F,
                                          Dataset = 'VOLUME',
                                          LIST_SOURCES = list('VND', 'MBS', 'VPS','TVSI', 'CAF', 'BSC','VST', 'C68', 'EXC', 'STB', 'DCL'),
                                            Prefix = 'PRICESBOARD', 
                                          Save = '', ToKable = F, Final_Folder = 'S:/STKVN/PRICES/FINAL/',
                                          Intraday = ''){
  # ------------------------------------------------------------------------------------------------
  Start.Time  = Sys.time()
  DBL_RELOAD_INSREF()
  CURRENT_TIME = substr(Sys.time(), 12,13)
  
  LastTrading     = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
  Prv_date        = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T, Option ='prev_date'))
  
  
  LIST_PRICES_BOARD = list()
  LIST_SHARES       = list()
  LIST_DIVIDEND     = list()
  Flag              = 0
  
  
  FINAL_LIST           = list()
  DATA_ALL_PRICESBOARD = data.table()
  DATA_FINAL           = data.table()
  FINAL_DATA           = data.table()
  FINAL_REPORT         = data.table()
  FINAL_FINAL          = data.table()
  FINAL_NOTOK          = data.table()
  
  
  OPTION               = Dataset
  if (CURRENT_TIME > 11 & Dataset == 'REFERENCE_OPEN') {
    CATln_Border(paste0('It is past 12pm.  FULLY DATA FINAL OPEN' ))
    DATA_FINAL_OPEN = CHECK_CLASS(try(CCPR_READRDS("S:/STKVN/PRICES/FINAL/","data_final_open_day.rds", ToKable = T, ToRestore = T)))
    # My.Kable(DATA_FINAL_OPEN)
    DATA_FINAL_OPEN = unique(DATA_FINAL_OPEN[as.POSIXct(updated)>=as.POSIXct(paste0(LastTrading,"09:15:00"))][order(updated)],by="code")
    FINAL_FINAL     = DATA_FINAL_OPEN
    return (FINAL_FINAL)
  }else{
    if (Dataset == "CLOSE" & CURRENT_TIME < 16)
    {
      Dataset = 'LAST'
      Flag = 1
    }
    
    List_Fields  = list("reference_open","reference_close", "market", "last", "change", "var", "varpc", "close", "volume", "rt", "rtd")
    
    LIST_CODE_BY_EXC_PRV         = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('list_code_by_exc_', gsub('-', '', Prv_date), '.rds') )))
    if (nrow(LIST_CODE_BY_EXC_PRV) > 0){
      LIST_CODE_BY_EXC_PRV[, date := as.Date(date)]
    }
    LIST_CODE_BY_EXC_DAY         = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('list_code_by_exc_', gsub('-', '', LastTrading), '.rds') )))
    if (nrow(LIST_CODE_BY_EXC_DAY) > 0){
      LIST_CODE_BY_EXC_DAY[, date := as.Date(date)]
    }
    
    LIST_CODE_BY_EXC             = unique(rbind(LIST_CODE_BY_EXC_DAY, LIST_CODE_BY_EXC_PRV, fill = T)[order(-date)], by = 'code')
    
    
    if( tolower(Dataset) %in% List_Fields)
    {
      OPTION = 'PRICESBOARD'
    }
    
    switch (OPTION,
            'PRICESBOARD' = {
              Prefix = 'PRICESBOARD'
              for (k in 1:length(LIST_SOURCES))
              {
                
                pSource = LIST_SOURCES[[k]]
                # PRICESBOARD FILES 
                File_Name_Day    = paste0('DOWNLOAD_', pSource, ifelse(STKVN,'_STKVN',''), '_', Prefix, '_DAY.rds')
                CATln_Border(paste0('DAY  = ', File_Name_Day))
                Data_Day         = CHECK_CLASS(try(CCPR_READRDS(File_Folder, File_Name_Day, ToKable = T, ToRestore = T)))
                
                File_Name_Date   = paste0('DOWNLOAD_', pSource, ifelse(STKVN,'_STKVN',''), '_', Prefix, '_', gsub('-','', LastTrading), '.rds')
                CATln_Border(paste0('DATE = ', File_Name_Date))
                Data_Date        = CHECK_CLASS(try(CCPR_READRDS(File_Folder, File_Name_Date, ToKable = T, ToRestore = T)))
                
                Data_All_Pricesboard = rbind(Data_Day, Data_Date, fill=T)
                
                if (nrow(Data_All_Pricesboard)>0) 
                {
                  Data_All_Pricesboard = Data_All_Pricesboard[date==LastTrading]
                  Data_All_Pricesboard[market == 'UPCOM', market := 'UPC']
                  Data_All_Pricesboard[market == 'HOSE', market := 'HSX']
                  Data_All_Pricesboard = unique(Data_All_Pricesboard, by=c('code', 'date'))
                  Data_All_Pricesboard = merge(Data_All_Pricesboard[, -c('name')], ins_ref[, .(code, name=short_name)], all.x=T, by='code')
                  LIST_PRICES_BOARD[[k]] = Data_All_Pricesboard
                }
              }
              
              DATA_ALL_PRICESBOARD         = rbindlist(LIST_PRICES_BOARD, fill = T)
              DATA_ALL_PRICESBOARD         = merge(DATA_ALL_PRICESBOARD[, -c('market')], LIST_CODE_BY_EXC[,. (code, market)], all.x = T, by = 'code')
              DATA_ALL_PRICESBOARD         = DATA_ALL_PRICESBOARD[code %in% LIST_CODE_BY_EXC$code]
              
              if (CURRENT_TIME > 11 )
              {
                DATA_FINAL_OPEN = CHECK_CLASS(try(CCPR_READRDS("S:/STKVN/PRICES/FINAL/","data_final_open_day.rds")))
                # My.Kable(DATA_FINAL_OPEN[code == 'STKVNTC6'])
                DATA_FINAL_OPEN = unique(DATA_FINAL_OPEN[as.POSIXct(updated)>=as.POSIXct(paste0(LastTrading,"09:15:00"))][order(updated)],by="code")
                if (nrow(DATA_FINAL_OPEN) > 0){
                  DATA_ALL_PRICESBOARD = merge(DATA_ALL_PRICESBOARD, DATA_FINAL_OPEN[,. (code, date, reference_open)], by = c('code', 'date'), all.x = T)
                } else {
                  DATA_FINAL_OPEN = CHECK_CLASS(try(CCPR_READRDS("S:/STKVN/PRICES/FINAL/","data_final_open_day.rds")))
                  DATA_ALL_PRICESBOARD = merge(DATA_ALL_PRICESBOARD, DATA_FINAL_OPEN[,. (code, date, reference_open)], by = c('code', 'date'), all.x = T)
                }
              }
              
              
              DATA_ALL_PRICESBOARD[, change := (as.numeric(last) - as.numeric(reference))]
              DATA_ALL_PRICESBOARD[!is.na(reference) & is.na(last) & is.na(volume), volume:=0]
              DATA_ALL_PRICESBOARD[!is.na(reference) & volume == 0, last := reference]
              DATA_ALL_PRICESBOARD[volume == 0, close := last]
              DATA_ALL_PRICESBOARD[!is.na(reference) & is.na(volume) & !is.na(last) & last == reference, volume := 0]
              DATA_ALL_PRICESBOARD[, varpc := as.numeric(100*as.numeric(change)/as.numeric(reference))]
              DATA_ALL_PRICESBOARD[, rt := as.numeric(as.numeric(last)/as.numeric(reference) -1)]

              # DATA_ALL_PRICESBOARD[code == 'STKVNTC6']

              xList = list ()
              col = Dataset
              if (tolower(col) %in% names(DATA_ALL_PRICESBOARD)) 
              {
                if (col == 'MARKET'){
                  DATA = DATA_ALL_PRICESBOARD[, .(dataset = col, date, code, source, value = get(tolower(col)))]
                }else{
                  DATA = DATA_ALL_PRICESBOARD[, .(dataset = col, date, code,  market, source, value = get(tolower(col)))]
                }
                setnames(DATA, old = "value", new = tolower(col))
                stat  = setDT(spread(DATA, key = 'source', value = get(tolower(col))))
                xList[[k]] = stat
              }
              
              DATA_PRICESBOARD = rbindlist(xList, fill = T)
              # Intraday = 'INTRADAY'
              if (nchar(Intraday) > 0 & CURRENT_TIME < 16 & Dataset == 'LAST' & Flag == 1)
              {
                Dataset = 'CLOSE'
                DATA_PRICESBOARD[, dataset := 'CLOSE']
              }
              
              if ( Dataset == 'MARKET')
              {
                DATA_PRICESBOARD = unique(DATA_PRICESBOARD, by = c("code", "dataset"))
                x =  select(DATA_PRICESBOARD, -c('dataset', 'date', 'code'))
              }else{
                DATA_PRICESBOARD = unique(DATA_PRICESBOARD, by = c("code", "dataset"))
                x =  select(DATA_PRICESBOARD, -c('dataset', 'date', 'code', 'market'))
              }
              
              SOURCE_PRICESBOARD = as.character( as.list(names(x)))
              DATA_PRICESBOARD[, final := apply(.SD, 1, function(x) {
                table_x <- table(x)
                if (length(table_x) == 0) {
                  NA
                } else if (all(is.na(x))) {
                  NA
                } else {
                  max_count <- max(table_x[!is.na(names(table_x))], na.rm = TRUE)
                  if (max_count == 0) {
                    NA
                  } else {
                    names(table_x)[which.max(table_x)]
                  }
                }
              }), .SDcols =SOURCE_PRICESBOARD]
              My.Kable(DATA_PRICESBOARD)
              
              DATA_ALL_PRICESBOARD = unique(DATA_ALL_PRICESBOARD[order(source, code, -updated)], by = c('source', 'code'))
              x = list()
              
              
              if(Dataset != 'market'){
                fields_source  = union(c('code', 'date', 'market', 'source'), tolower(Dataset))
              }else{
                fields_source  = union(c('code', 'date', 'source'), tolower(Dataset)) 
              }
              
              
              DATA_ALL_DATASET_SOURCE = DATA_ALL_PRICESBOARD[, ..fields_source]
              day = gsub("-", "", LastTrading)
              file_name_sources  = paste0("DATA_SOURCE_BY_", Dataset, "_", day, ".rds")
              
              try(CCPR_SAVERDS(DATA_ALL_DATASET_SOURCE, "S:/STKVN/PRICES/", file_name_sources))
              
              num_columns = ncol(DATA_ALL_DATASET_SOURCE)
              DATA_ALL_DATASET_SOURCE$market = paste0(tolower(Dataset), "_", DATA_ALL_DATASET_SOURCE$market)
              DATA_ALL_DATASET_SOURCE = DATA_ALL_DATASET_SOURCE[,. (date, source, market)]
              DATA_ALL_DATASET_SOURCE[, updated := substr(Sys.time(), 1,19)]
              
              names(DATA_ALL_DATASET_SOURCE) = c("date", "source", "value", "updated")
              
              DATA = DATA_ALL_DATASET_SOURCE
              DATA[, LastTrading := LastTrading]
              DATA_ALL_DATE = unique(DATA[order(source, value, -date)], by = c('source', 'value'))
              DATA_ALL_DATE [, n := as.character(date - LastTrading)]
              DATA_ALL_DATE [n == 0, n := substr(updated, 12, 16)]
              
              report_date = setDT(spread(DATA_ALL_DATE[, .(n), by = .(source, value, date)], key = 'source', value = 'n'))[, ':='( updated = substr(Sys.time(),1,19) )]
              
              report_number = setDT(spread(DATA[,.(n=.N), by=c('date','source', 'value')], key='source', value='n'))[, updated := substr(Sys.time(), 1, 19)]
              report_number = unique(report_number[order(-date)], by = "value")
              file_name_report_number = paste0("REPORT_NUMBER_BY_", Dataset,"_.rds")
              file_name_report_date = paste0("REPORT_DATE_BY_", Dataset,".rds")
              
              
              REPORT = copy(DATA_PRICESBOARD)
              list_market = list('UPC', 'HSX', 'HNX')
              REPORT_PERCENT_DOWNLOAD_PRICESBOARD = data.table()
              columns_to_check                    = setdiff(names(REPORT), c("dataset", "date", "code", "market"))
              type = Dataset
              if (nrow(REPORT[dataset == type]) > 0) 
              {
                for (col in columns_to_check) 
                {
                  # col = "final"
                  for (i in 1:length(list_market))
                  {
                    # i = 2
                    mar         = list_market[[i]]
                    dt_filtered = REPORT[, value := get(col)]
                    if (Dataset == 'MARKET')
                    {
                      dt_filtered = REPORT[!is.na(value) & value == mar]
                    }else{
                      dt_filtered = REPORT[!is.na(value) & market == mar]
                    }

                    num         = as.numeric(nrow(dt_filtered))
                    denom       = as.numeric(nrow(LIST_CODE_BY_EXC[market == mar]))
                    percent     = round((num / denom) * 100, 2)
                    REPORT_PERCENT_DOWNLOAD_PRICESBOARD = rbind(REPORT_PERCENT_DOWNLOAD_PRICESBOARD, data.table(dataset = type, source = col, market = mar, percentage = percent), fill = T)
                  }
                }
              }
              
              
              REPORT_PERCENT_DOWNLOAD_PRICESBOARD = setDT(spread(REPORT_PERCENT_DOWNLOAD_PRICESBOARD, key = 'source', value = 'percentage'))
              My.Kable.All(REPORT_PERCENT_DOWNLOAD_PRICESBOARD)
              
              
              DATA_FINAL_BY_DATASET = DATA_PRICESBOARD[!is.na(final)][,. (dataset, date, code, final)]
              DATA_FINAL_BY_DATASET = unique(DATA_FINAL_BY_DATASET, by = c("dataset", "code"))
              DATA_FINAL_BY_DATASET = setDT(spread(DATA_FINAL_BY_DATASET, key = 'dataset', value = 'final'))
              setnames(DATA_FINAL_BY_DATASET, old = names(DATA_FINAL_BY_DATASET), new = tolower(names(DATA_FINAL_BY_DATASET)))
              DATA_FINAL_BY_DATASET = UPDATE_UPDATED(DATA_FINAL_BY_DATASET)
              DATA_FINAL_BY_DATASET[, datetime := SYS.TIME()]
              DATA_FINAL_BY_DATASET = CONVERT_IFEXIST_FIELDS_DEV (pData=DATA_FINAL_BY_DATASET, AsType="numeric",
                                                                  List_Fields=list("reference_open","reference_close", "reference", "sharesout", "last", "change", "varpc", "close", "volume", "freefloat", "rt"))
              DATA_NOK              = LIST_CODE_BY_EXC[!code %in% DATA_FINAL_BY_DATASET$code]
              
              file_name_day  = paste0("DATA_FINAL_BY_", Dataset, "_DAY.rds")
              file_name_date = paste0("DATA_FINAL_BY_", Dataset, paste0('_',gsub('-', '', LastTrading)), '.rds')
              
              if (nchar(Save) > 0) 
              {
                if (grepl('D', Save))
                {
                  try(CCPR_SAVERDS(DATA_PRICESBOARD, File_Folder, paste0("DATA_BY_", Dataset, ".rds")))
                  try(CCPR_SAVERDS(DATA_NOK, File_Folder, paste0("DATA_NOT_OK_BY_", Dataset, ".rds")))
                  
                }
                if (grepl('R', Save))
                {
                  try(CCPR_SAVERDS(report_number, "S:/STKVN/PRICES/REPORTS/", file_name_report_number))
                  try(CCPR_SAVERDS(report_date, "S:/STKVN/PRICES/REPORTS/", file_name_report_date))
                  try(CCPR_SAVERDS(REPORT_PERCENT_DOWNLOAD_PRICESBOARD, 'S:/STKVN/PRICES/REPORTS/', paste0("report_percent_by_", tolower(Dataset), ".rds") ))
                }
                if (grepl('F', Save))
                {
                  try(CCPR_SAVERDS(DATA_FINAL_BY_DATASET, Final_Folder, file_name_day))
                  try(CCPR_SAVERDS(DATA_FINAL_BY_DATASET, paste0(Final_Folder, 'DAY/'), file_name_date))
                }
                
              }
              if (Dataset != 'MARKET')
              {
                market_prvdate = CHECK_CLASS(CCPR_READRDS('S:/STKVN/PRICES/FINAL/', paste0('DATA_FINAL_BY_MARKET_', gsub('-', '', Prv_date, '.rds'))))
                market_today   = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/FINAL/', 'DATA_FINAL_BY_MARKET_DAY.rds')))
                market         = rbind(market_prvdate, market_today, fill = T)
                market         = unique(market[order(-date)], by = 'code')
                DATA_PRICESBOARD = merge(DATA_PRICESBOARD[, -c('market')] , market[,. (code, market)], by = 'code', all.x = T)
              }else{
                DATA_PRICESBOARD[, market := final]
              }
              FINAL_NOTOK   = DATA_NOK
              FINAL_FINAL   = DATA_FINAL_BY_DATASET
              FINAL_REPORT  = REPORT_PERCENT_DOWNLOAD_PRICESBOARD
              FINAL_DATA    = DATA_PRICESBOARD
              FINAL_LIST    = list(FINAL_DATA, FINAL_REPORT, FINAL_FINAL, FINAL_NOTOK)
            }, 
            'SHARESOUT' = {
              FINAL_LIST = try(TRAINEE_REPORT_SHARES (File_Folder = 'S:/STKVN/PRICES/DAY/',
                                                      LIST_SOURCES = list('EXC', 'VND', 'MBS', 'VPS','TVSI', 'CAF', 'BSC','VST', 'C68',  'STB', 'DCL'),
                                                      Save = 'DRF'))
              
            },
            'FREEFLOAT' = {
              # FINAL_LIST = try(TRAINEE_REPORT_FREEFLOAT (File_Folder = 'S:/STKVN/PRICES/DAY/',
              #                                            LIST_SOURCES = list('EXC', 'VND', 'DNSE', '24H'),
              #                                            Save = 'DRF'))
              
              FINAL_LIST = try(TRAINEE_REPORT_FREEFLOAT(File_Folder='S:/STKVN/PRICES/DAY/', 
                                                        Prefix = 'FREEFLOAT', 
                                                        STKVN=T,
                                                        LIST_SOURCES = list('EXC', 'VND', '24H','DNSE'),
                                                        Pre_Date = '', Save = 'DRF') )
              
            }, 
            'DIVIDEND' = {
              
              LIST_DIVIDEND = list()
              for (k in 1:length(LIST_SOURCES)) 
              {
                # # STKVN DIVIDEND FILES 
                # pSource = 'CAF'
                pSource          = LIST_SOURCES[[k]]
                File_Name_Day    = paste0('DOWNLOAD_', pSource, '_STKVN_DIVIDEND_DAY.rds')
                CATln_Border(paste0('DAY  = ', File_Name_Day))
                Data_Day         = CHECK_CLASS(try(CCPR_READRDS(File_Folder, File_Name_Day, ToKable = T, ToRestore = T)))
                
                File_Name_Date   = paste0("DOWNLOAD_",pSource,"_STKVN_DIVIDEND_",gsub("-","",LastTrading),".rds")
                CATln_Border(paste0('DATE = ', File_Name_Date))
                Data_Date        = CHECK_CLASS(try(CCPR_READRDS(File_Folder, File_Name_Date, ToKable = T, ToRestore = T)))
                
                Data_All_Dvidend = rbind(Data_Day, Data_Date, fill=T)
                
                if (nrow(Data_All_Dvidend)>0)
                {
                  Data_All_Dvidend = Data_All_Dvidend[date==LastTrading]
                  Data_All_Dvidend = unique(Data_All_Dvidend, by=c('code', 'market'))
                  LIST_DIVIDEND[[k]] = Data_All_Dvidend
                }
              }
              # DATA_ALL_DIVIDEND = Data_All_Dvidend
              DATA_ALL_DIVIDEND    = rbindlist(LIST_DIVIDEND, fill = T)
              DATA_DIVIDEND = data.table()
              if (nrow(DATA_ALL_DIVIDEND) > 0)
              {
                DATA_ALL_DIVIDEND[, dividend := as.numeric(close)]
                if ('market' %in% names(DATA_ALL_DIVIDEND)){
                  DATA_ALL_DIVIDEND = merge(DATA_ALL_DIVIDEND[, -c('market')], LIST_CODE_BY_EXC[,. (code, market )], all.x = T, by = 'code')
                  
                }else{
                  DATA_ALL_DIVIDEND = merge(DATA_ALL_DIVIDEND, LIST_CODE_BY_EXC[,. (code, market )], all.x = T, by = 'code')
                  
                }
                
                DATA_ALL_DIVIDEND = unique(DATA_ALL_DIVIDEND [!is.na(code) & !is.na(dividend) ] [,. (date, source,market, code, dividend)], by = c('code', 'source'))
                
                day = gsub("-", "", LastTrading)
                file_name_sources  = paste0("DATA_SOURCE_BY_DIVIDEND_", day, ".rds")
                try(CCPR_SAVERDS(DATA_ALL_DIVIDEND, "S:/STKVN/PRICES/", file_name_sources))
                
                DATA_ALL_DAY_REPORT = unique(DATA_ALL_DIVIDEND[order(-date)][, .(source , market, date)], by = c("source", "market"))[, LastTrading := LastTrading]
                DATA_ALL_DAY_REPORT = DATA_ALL_DAY_REPORT [, nd := as.numeric(date - LastTrading)]
                DATA_ALL_DAY_REPORT = unique(DATA_ALL_DAY_REPORT, by = c("source", "market"))
                
                report_dividend_date  = setDT(spread(DATA_ALL_DAY_REPORT[,.(n = as.numeric(date - LastTrading)), by=c('source', 'market')], key='source', value='n'))[order(market)][, ':='( updated = substr(Sys.time(),1,19) ,dataset = 'DIVIDEND')]
                My.Kable.All(report_dividend_date)
                
                DATA_ALL_DIVIDEND_NUMBER = unique(DATA_ALL_DIVIDEND[order(date == LastTrading), by = c("source", "code")])
                report_dividend_number = setDT(spread(DATA_ALL_DIVIDEND_NUMBER[,.(n = .N), by=c('source', 'market')], key='source', value='n'))[order(market)][, ':='( updated = substr(Sys.time(),1,19) ,dataset = 'DIVIDEND')]
                My.Kable.All(report_dividend_number)
                
                
                
                DATA_DIVIDEND  = setDT(spread(DATA_ALL_DIVIDEND[,. (source,market, code,date, dividend)], key = 'source', value = 'dividend'))
                DATA_DIVIDEND[, dataset := 'DIVIDEND']
                REPORT = copy(DATA_DIVIDEND)
                
                y =  select(DATA_DIVIDEND, -c('code', 'market', 'date'))
                SOURCE_DIVIDEND = as.character( as.list(names(y)))
                
                DATA_DIVIDEND[, final := apply(.SD, 1, function(y) {
                  table_y <- table(y)
                  if (length(table_y) == 0) {
                    NA
                  } else if (all(is.na(y))) {
                    NA
                  } else {
                    max_count <- max(table_y[!is.na(names(table_y))], na.rm = TRUE)
                    if (max_count == 0) {
                      NA
                    } else {
                      names(table_y)[which.max(table_y)]
                    }
                  }
                }), .SDcols =SOURCE_DIVIDEND]
                
                My.Kable(DATA_DIVIDEND)
                
                DATA_DIVIDEND = DATA_DIVIDEND[!is.na(final) & (code %in% LIST_CODE_BY_EXC$code)]
                DATA_DIVIDEND = unique(DATA_DIVIDEND, by = c("code"))
                DATA_DIVIDEND = UPDATE_UPDATED(DATA_DIVIDEND) 
                
                REPORT = copy(DATA_DIVIDEND)
                list_market = list('UPC', 'HSX', 'HNX')
                report_dividend_percent = data.table()
                columns_to_check                    = setdiff(names(REPORT), c("dataset", "date", "code", "market"))
                type = Dataset
                if (nrow(REPORT) > 0) 
                {
                  # if (type != 'MARKET') 
                  # {
                  for (col in columns_to_check) 
                  {
                    # col = "CAF"
                    for (i in 1:length(list_market))
                    {
                      # i = 1
                      mar         = list_market[[i]]
                      dt_filtered = REPORT[, value := get(col)]
                      if (Dataset == 'MARKET')
                      {
                        dt_filtered = REPORT[!is.na(value) & value == mar]
                      }else{
                        dt_filtered = REPORT[!is.na(value) & market == mar]
                      }
                      
                      num         = as.numeric(nrow(dt_filtered))
                      denom       = as.numeric(nrow(LIST_CODE_BY_EXC[market == mar]))
                      percent     = round((num / denom) * 100, 2)
                      report_dividend_percent = rbind(report_dividend_percent, data.table(dataset = type, source = col, market = mar, percentage = percent), fill = T)
                    }
                  }
                }
                report_dividend_percent = setDT(spread(report_dividend_percent, key = 'source', value = 'percentage'))
                
                
                market_prvdate = CHECK_CLASS(CCPR_READRDS('S:/STKVN/PRICES/FINAL/', paste0('DATA_FINAL_BY_MARKET_', gsub('-', '', Prv_date, '.rds'))))
                market_today   = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/FINAL/', 'DATA_FINAL_BY_MARKET_DAY.rds')))
                market         = rbind(market_prvdate, market_today, fill = T)
                market         = unique(market[order(-date)], by = 'code')
                DATA_DIVIDEND  = merge(DATA_DIVIDEND[, -c("market")] , market[,. (code, market)], by = 'code', all.x = T)
                
                
                DATA_FINAL_BY_DATASET = DATA_DIVIDEND[!is.na(final)][,. (code, date, final , datetime = SYS.TIME() )]
                DATA_FINAL_BY_DATASET[, dividend := as.numeric(round(as.numeric(final), 2))]
                DATA_NOK              = DATA_ALL_DIVIDEND[!code %in% DATA_FINAL_BY_DATASET$code]
                
                FINAL_DATA     = DATA_DIVIDEND
                FINAL_DATA     = UPDATE_UPDATED(FINAL_DATA)
                FINAL_REPORT   = report_dividend_percent
                FINAL_REPORT   = UPDATE_UPDATED(FINAL_REPORT)
                FINAL_FINAL    = DATA_FINAL_BY_DATASET
                FINAL_FINAL    = UPDATE_UPDATED(FINAL_FINAL)
                FINAL_NOTOK    = DATA_NOK
                FINAL_NOTOK    = UPDATE_UPDATED(FINAL_NOTOK)
                
                FINAL_LIST = list(FINAL_DATA, FINAL_REPORT, FINAL_FINAL, FINAL_NOTOK)
                file_name_day = paste0("DATA_FINAL_BY_", Dataset, "_DAY.rds")
                file_name_date = paste0("DATA_FINAL_BY_", Dataset, gsub('-', '', LastTrading), '.rds')
                if ( nchar(Save) > 0)
                {
                  if (grepl('D', Save))
                  {
                    try(CCPR_SAVERDS(FINAL_DATA,'S:/STKVN/PRICES/DAY/', 'DATA_BY_DIVIDEND.rds'))
                    try(CCPR_SAVERDS(DATA_NOK, 'S:/STKVN/PRICES/DAY/', 'DATA_NOT_OK_BY_DIVIDEND.rds'))
                    
                  }
                  if (grepl('R', Save)) 
                  {
                    try(CCPR_SAVERDS(report_dividend_date,'S:/STKVN/PRICES/REPORTS/', 'report_date_by_dividend.rds'))
                    try(CCPR_SAVERDS(report_dividend_number,'S:/STKVN/PRICES/REPORTS/', 'report_number_by_dividend.rds'))
                    try(CCPR_SAVERDS(report_dividend_percent,'S:/STKVN/PRICES/REPORTS/', 'report_percent_by_dividend.rds'))
                  }
                  if (grepl('F', Save))
                  {
                    try(CCPR_SAVERDS(FINAL_FINAL,'S:/STKVN/PRICES/FINAL/', 'DATA_FINAL_BY_DIVIDEND_DAY.rds'))
                    try(CCPR_SAVERDS(FINAL_FINAL,'S:/STKVN/PRICES/FINAL/DAY/', paste0('DATA_FINAL_BY_DIVIDEND_', gsub('-','', LastTrading),'.rds')))
                  }
                }
                
                
              }
            }  
            
    )}
  
  # FINAL_LIST = list(FINAL_DATA, FINAL_REPORT, FINAL_FINAL, FINAL_NOTOK)
  if (length(FINAL_LIST) > 3)
  {
    FINAL_DATA   = FINAL_LIST[[1]]
    FINAL_REPORT = FINAL_LIST[[2]]
    FINAL_FINAL  = FINAL_LIST[[3]]
    FINAL_NOTOK  = FINAL_LIST[[4]]
    
    if (ToKable)
    {
      My.Kable(FINAL_DATA)
      CATln_Border('STAT')
      # My.Kable(FINAL_DATA[,. (n =.N), by = 'market'])
      STAT = FINAL_DATA[,. (n =.N), by = 'market']
      Total = FINAL_DATA[,. (n =.N)]
      My.Kable.All(rbind(STAT, Total, fill = T))
      CATln_Border('NOT OK')
      My.Kable(FINAL_DATA[is.na(final)])
    }
  }
  EndTime = Sys.time()
  CATln_Border(paste0('DURATION =' , Format.Number( difftime(EndTime, Start.Time, units ='secs'), 2) ))
  
  return(FINAL_LIST)
}

# market = TRAINEE_FINAL_BY_DATASET_DAILY(File_Folder='S:/STKVN/PRICES/DAY/', STKVN=F,
#                                         Dataset = 'MARKET',
#                                         LIST_SOURCES = list('VND', 'MBS', 'VPS','TVSI', 'CAF', 'BSC','VST', 'C68', 'EXC', 'STB', 'DCL'),
#                                         Save = "DFR", ToKable = T,
#                                         Final_Folder = 'S:/STKVN/PRICES/FINAL/')
# 
# # 
# # 
# SHARESOUT = TRAINEE_FINAL_BY_DATASET_DAILY(File_Folder='S:/STKVN/PRICES/DAY/', STKVN=F,
#                                            Dataset = 'SHARESOUT',
#                                            LIST_SOURCES = list('VND', 'MBS', 'VPS','TVSI', 'CAF', 'BSC','VST', 'C68', 'EXC', 'STB', 'DCL'),
#                                            Save = "DRF", ToKable = T)
# # 
# # # 
# FREEFLOAT = TRAINEE_FINAL_BY_DATASET_DAILY(File_Folder='S:/STKVN/PRICES/DAY/', STKVN=F,
#                                            Dataset = 'FREEFLOAT',
#                                            LIST_SOURCES = list('EXC', 'VND', '24H','DNSE'),
#                                            Save = "DRF", ToKable = T)
# #
# # # 
# DIVIDEND = TRAINEE_FINAL_BY_DATASET_DAILY(File_Folder='S:/STKVN/PRICES/DAY/', STKVN=F,
#                                           Dataset = 'DIVIDEND',
#                                           LIST_SOURCES = list('VND', 'MBS', 'VPS','TVSI', 'CAF', 'BSC','VST', 'C68', 'EXC', 'STB', 'DCL'),
#                                           Save = "DRF", ToKable = T)
# # # 
# LAST = TRAINEE_FINAL_BY_DATASET_DAILY(File_Folder='S:/STKVN/PRICES/DAY/', STKVN=F,
#                                       Dataset = 'LAST',
#                                       LIST_SOURCES = list('VND', 'MBS', 'VPS','TVSI', 'CAF', 'BSC','VST', 'C68', 'EXC', 'STB', 'DCL'),
#                                       Save = "F", ToKable = T)
# # 
# # 
# # 
# VARPC = TRAINEE_FINAL_BY_DATASET_DAILY(File_Folder='S:/STKVN/PRICES/DAY/', STKVN=F,
#                                        Dataset = 'VARPC',
#                                        LIST_SOURCES = list('VND', 'MBS', 'VPS','TVSI', 'CAF', 'BSC','VST', 'C68', 'EXC', 'STB', 'DCL'),
#                                        Save = "", ToKable = T)
#SESSION = 'INTRADAY'
# ==================================================================================================
TRAINEE_REPORT_AND_FINAL_BY_SESSION = function (SESSION = 'OPEN') {
  # ------------------------------------------------------------------------------------------------
  Start.Time  = Sys.time()
  DBL_RELOAD_INSREF()
  CURRENT_TIME = substr(Sys.time(), 12,13)
  
  mypc            =  as.character(try(fread("C:/R/my_pc.txt", header = F)))
  LastTrading     = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
  Prv_date        = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T, Option ='prev_date'))
  LIST_GENERAL    = list('MARKET', 'SHARESOUT', 'DIVIDEND',  'RT',  'FREEFLOAT')
  REPORT_SESSION  = data.table()
  
  switch(SESSION,
         'OPEN' = {
           LIST_DATASET = list('REFERENCE_OPEN', 'MARKET', 'SHARESOUT','LAST', 'DIVIDEND', 'VAR', 'CHANGE', 'VARPC', 'RT', 'FREEFLOAT')
         }, 
         'CLOSE' = {
           LIST_DATASET = list('REFERENCE_CLOSE','LAST', 'CLOSE', 'VOLUME','VAR', 'CHANGE', 'VARPC')
         },
         'INTRADAY' = {
           LIST_DATASET = list('LAST', 'CLOSE', 'VOLUME','VAR', 'CHANGE', 'VARPC')
         })
  
  xLIST = list()
  x = list()
  for (k in 1:length(LIST_DATASET) )
  {
    
    # k = 1
    DATASET = LIST_DATASET[[k]]
    if(SESSION == 'OPEN')
    {
      
      x = try(TRAINEE_FINAL_BY_DATASET_DAILY(File_Folder='S:/STKVN/PRICES/DAY/', STKVN=F,
                                             Dataset = DATASET,
                                             LIST_SOURCES = list('VND', 'MBS', 'VPS','TVSI', 'CAF', 'BSC','VST', 'C68', 'EXC', 'STB', 'DCL', '24H', 'DNSE'),
                                             Save = "DRF", ToKable = T,
                                             Intraday = ''))
    }else{
      
      x = try(TRAINEE_FINAL_BY_DATASET_DAILY(File_Folder='S:/STKVN/PRICES/DAY/', STKVN=F,
                                             Dataset = DATASET,
                                             LIST_SOURCES = list('VND', 'MBS', 'VPS','TVSI', 'CAF', 'BSC','VST', 'C68', 'EXC', 'STB', 'DCL', '24H', 'DNSE'),
                                             Save = "DRF", ToKable = T,
                                             Intraday = 'INTRADAY'))
    }
    
    if (all(class(x) !=  "try-error"))
    {
      if (length(x) > 0 )
      {
        report = x[[2]]
        report[, datetime := SYS.TIME()]
        xLIST[[DATASET]] = report
      }
    }else{
      
      error_table = data.table()
      error_table[, ":=" (pc = mypc, dataset = DATASET)]
      error_table = UPDATE_UPDATED(error_table)
      fwrite(error_table, 'S:/STKVN/PRICES/REPORTS/error_process_intranet.txt', sep = '\t', col.names = TRUE)
    }
    
  }
  REPORT_SESSION = rbindlist(xLIST, fill =T)
  if (SESSION != 'OPEN')
  {
    x = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/REPORTS/', 'report_percent_by_reference_open.rds')))
    if(nrow(x) > 0)
    {
      REPORT_SESSION = rbind(REPORT_SESSION, x, fill = T)
    }
    for (k  in 1:length(LIST_GENERAL)) 
    {
      dataset = LIST_GENERAL[[k]]
      x = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/REPORTS/', paste0('report_percent_by_', tolower(dataset), '.rds'))))
      if(nrow(x) > 0)
      {
        REPORT_SESSION = rbind(REPORT_SESSION, x, fill = T)
      }
    }
  }
  
  REPORT_SESSION = REPORT_SESSION[, -c("updated")]
  REPORT_SESSION [, date := LastTrading]
  REPORT_SESSION = REPORT_SESSION %>% 
    select(dataset,date, market, datetime, everything(), final) %>% 
    select(-final, final)
  REPORT_SESSION = UPDATE_UPDATED(REPORT_SESSION)
  REPORT_SESSION[, PC := mypc]
  
  # SESSION = 'OPEN'
  My.Kable.All(REPORT_SESSION)
  file_name = paste0('REPORT_PERCENT_', SESSION, '.rds')

  try(CCPR_SAVERDS(REPORT_SESSION, 'S:/STKVN/PRICES/REPORTS/', file_name))
  return(REPORT_SESSION)
}
# x = TRAINEE_REPORT_AND_FINAL_BY_SESSION(SESSION = 'OPEN') 
# ==================================================================================================
MERGE_FINAL_ALL_BY_SESSION = function (SESSION = 'INTRADAY',   folder_file  = 'S:/STKVN/PRICES/FINAL/'){
  # ------------------------------------------------------------------------------------------------
  
  LIST_DATASET = list('MARKET', 'REFERENCE_OPEN', 'SHARESOUT','LAST', 'DIVIDEND', 'CHANGE', 'VARPC', 'RT', 'FREEFLOAT', 'CLOSE', 'VOLUME')
  list_data    = list()
  data_all     = data.table()
  mypc         =  as.character(try(fread("C:/R/my_pc.txt", header = F)))
  
  for(k in 1:length(LIST_DATASET))
  {
    #  k = 5
    dataset   = LIST_DATASET[[k]]
    
    file_name = paste0('data_final_by_', tolower(dataset), '_day.rds')
    My_data   = CHECK_CLASS(try(CCPR_READRDS(folder_file, file_name, ToKable = T)))
    if ( k == 1)
    {
      data_all = My_data
    }else{
      col_names = setdiff(names(My_data), c("dataset", "date", "updated", "market", "datetime", "final"))
      col_names = c(col_names)
      data_all  = merge(data_all, My_data[,..col_names], by = 'code', all.x = T)
    }
    
  }
  # SESSION = 'CLOSE'
  file_name_data   = paste0('data_all_final_', tolower(SESSION), '_day.rds')
  file_name_report = paste0('report_all_final_', tolower(SESSION),'_day.rds')
  x = try(CCPR_SAVERDS(data_all, 'S:/STKVN/PRICES/FINAL/', file_name_data))
  MERGE_DAY_TO_HISTORY (File_Folder = 'S:/STKVN/PRICES/FINAL/',
                        File_Name = file_name_data,
                        File_History = '')
  
  
  col    = c(setdiff(names(data_all), c("dataset", "date", "updated",  "datetime", "final", "codesource", "source", "code")))
  report = data_all[,..col]
  
  percent_non_na = round(as.numeric(colSums(!is.na(report)) / nrow(report) * 100), 2)
  percent_non_na_report = data.table(dataset = names(report), final = round(percent_non_na, 2))
  percent_non_na_report[, dataset := toupper(dataset)]
  percent_non_na_report[, type := 'FINAL DAY']
  percent_non_na_report = UPDATE_UPDATED(percent_non_na_report)
  percent_non_na_report[, PC := mypc]
  
  
  
  data_history_all = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/FINAL/', paste0('data_all_final_',tolower(SESSION), '_history.rds'))))
  data_history_day = data_history_all[date == LastTrading]
  
  report_history_all = data_history_all[,..col]
  
  history_all = round(as.numeric(colSums(!is.na(report_history_all)) / nrow(report_history_all) * 100), 2)
  # history_all[is.na(change)]
  history_all = data.table(dataset = names(report_history_all), final = round(history_all, 2))
  history_all[, dataset := toupper(dataset)]
  history_all[, type := 'HISTORY ALL']
  history_all = UPDATE_UPDATED(history_all)
  history_all[, PC := mypc]
  
  
  report_history_day = data_history_day[,..col]
  
  history_day = round(as.numeric(colSums(!is.na(report_history_day)) / nrow(report_history_day) * 100), 2)
  history_day = data.table(dataset = names(report_history_day), final = round(history_day, 2))
  history_day[, dataset := toupper(dataset)]
  history_day[, type := 'HISTORY DAY']
  history_day = UPDATE_UPDATED(history_day)
  history_day[, PC := mypc]
  
  try(CCPR_SAVERDS(percent_non_na_report, 'S:/STKVN/PRICES/REPORTS/', file_name_report))
  try(CCPR_SAVERDS(history_all, 'S:/STKVN/PRICES/REPORTS/', paste0('report_history_all_', tolower(SESSION), '.rds')))
  try(CCPR_SAVERDS(history_day, 'S:/STKVN/PRICES/REPORTS/', paste0('report_history_day_', tolower(SESSION), '.rds')))
  
  My.Kable.All(percent_non_na_report)
  My.Kable.All(history_all)
  My.Kable.All(history_day)
  return(percent_non_na_report)
}

# MERGE_FINAL_ALL_BY_SESSION(SESSION = 'OPEN',   folder_file  = 'S:/STKVN/PRICES/FINAL/')

# x = MERGE_FINAL_ALL_BY_SESSION (SESSION = 'INTRADAY')
# ==================================================================================================
INIT_PROCESS_DATASET = function(columns = list('REFERENCE_OPEN', 'MARKET', 'SHARESOUT','LAST', 'DIVIDEND', 'CHANGE', 'VARPC','CLOSE', 'VOLUME',"RT", 'FREEFLOAT'),
                                type = NA){
  # ------------------------------------------------------------------------------------------------
  nr      = rep(2, 3)
  date    = LastTrading
  process = c("OPEN", "INTRADAY", "CLOSE")
  nrp     = as.numeric(NA)
  id      = NA
  process_dataset = data.table(id = id, nr = nr, nrp = nrp, date = date, type = type, process = process)
  for (col in columns) {
    process_dataset[[col]] = NA
  }
  process_dataset[["updated"]] = NA
  process_dataset[["PC"]]      = NA
  return (process_dataset)
}
# process_dataset = INIT_PROCESS_DATASET(columns = list('REFERENCE_OPEN', 'MARKET', 'SHARESOUT','LAST', 'DIVIDEND', 'CHANGE', 'VARPC','CLOSE', 'VOLUME',"RT", 'FREEFLOAT'),
#                               type = NA)

# ==================================================================================================

GET_SESSION = function(process_dataset, session_list = list("OPEN", "INTRADAY", "CLOSE"),
                       folder_name = "S:/STKVN/PRICES/REPORTS/", ptype = 'FINAL ALL'){
  # ------------------------------------------------------------------------------------------------
  MyPC = toupper(TRAINEE_GET_MYPC())  #getMyPC()
  # folder_name = "S:/STKVN/PRICES/REPORTS/"
  # file_name = "process_dataset_4upload.rds"
  # new_data = try(CCPR_READRDS(folder_name, file_name, ToKable = T))
  # My.Kable.All(new_data)
  switch (ptype,
          'DATASET' = {
            
            for (i in 1:length(session_list))
            {
              # i = 3
              session   = session_list[[i]]
              file_name = paste0('report_percent_', tolower(session), '.rds')
              last_ud   = file.info(paste0(folder_name,file_name))$mtime
              last_ud   = substr(last_ud, 1, 10)
              if (!is.na(last_ud) & last_ud == Sys.Date())
              {  
                report_file = CHECK_CLASS(try(CCPR_READRDS(folder_name, file_name, ToKable = T)))
                updated     = report_file[['updated']][1]
                mypc        = report_file[['PC']][1]
                if (nrow(report_file) > 0)
                {
                  datasets = unique(report_file$dataset)
                  for(k in 1:length(datasets)){
                    # k = 1
                    pdataset      = datasets[[k]]
                    final_dataset = report_file[dataset == pdataset, (final)]
                    total         = round(mean(final_dataset, na.rm = TRUE), 2)
                    
                    process_dataset[[pdataset]][i]  = total
                    process_dataset[['updated']][i] = updated
                    # process_dataset[i, PC := MyPC]
                    process_dataset[['PC']][i]      = mypc
                  }
                }
              }
            }
            process_dataset[, type := ptype]
            
            return (process_dataset)
          },
          'FINAL ALL' = {
            for (i in 1:length(session_list))
            {
              # i = 1
              session   = session_list[[i]]
              file_name = paste0('report_all_final_', tolower(session),'_day.rds')
              last_ud   = file.info(paste0(folder_name,file_name))$mtime
              last_ud   = substr(last_ud, 1, 10)
              if (!is.na(last_ud) & last_ud == Sys.Date())
              {  
                report_file = try(readRDS(paste0("S:/STKVN/PRICES/REPORTS/report_all_final_", tolower(session),"_day.rds")))
                updated     = report_file[['updated']][1]
                mypc        = report_file[['PC']][1]
                
                if (nrow(report_file) > 0)
                {
                  datasets = unique(report_file$dataset)
                  for(k in 1:length(datasets)){
                    # k = 1
                    pdataset      = datasets[[k]]
                    final_dataset = report_file[dataset == pdataset, (final)]
                    process_dataset[[pdataset]][i]  = final_dataset
                    process_dataset[['updated']][i] = updated
                    process_dataset[['PC']][i]      = mypc
                    
                  }
                }
              }
            }
            process_dataset[, type := ptype]
            return (process_dataset)
          },
          'HISTORY' = {
            data = data.table()
            
            for (session_history in list("HISTORY DAY", "HISTORY ALL"))
            {
              # session_history = "HISTORY DAY"
              for (i in 1:length(session_list))
              {
                # i = 1
                session   = session_list[[i]]
                
                z = tolower(session_history)
                z = gsub(" ", "_", z)
                
                file_name = paste0('report_', z ,'_', tolower(session),'.rds')
                last_ud   = file.info(paste0(folder_name,file_name))$mtime
                last_ud   = substr(last_ud, 1, 10)
                if (!is.na(last_ud) & last_ud == Sys.Date())
                {  
                  report_file = try(CCPR_READRDS(folder_name, file_name))
                  updated     = report_file[['updated']][1]
                  mypc        = report_file[['PC']][1]
                  
                  if (nrow(report_file) > 0)
                  {
                    datasets = unique(report_file$dataset)
                    for(k in 1:length(datasets)){
                      # k = 1
                      pdataset      = datasets[[k]]
                      
                      final_dataset = report_file[dataset == pdataset, (final)]
                      process_dataset[[pdataset]][i]  = final_dataset
                      process_dataset[['updated']][i] = updated
                      process_dataset[['PC']][i]      = mypc
                      
                    }
                  }
                }
              }
              
              process_dataset[, type := session_history]
              data =  rbind(data, process_dataset, fill = T)
              
              
            }
            process_dataset = data
            
            return (process_dataset)
          }
  )
  
}
# process_dataset = as.data.table(INIT_PROCESS_DATASET(columns, type = ptype))
# x = GET_SESSION (process_dataset, session_list = list("OPEN", "INTRADAY", "CLOSE"),
#                            folder_name = "S:/STKVN/PRICES/REPORTS/", ptype = 'DATASET')
# 
# x = GET_SESSION (process_dataset, session_list = list("OPEN", "INTRADAY", "CLOSE"),
#                  folder_name = "S:/STKVN/PRICES/REPORTS/", ptype = 'FINAL ALL')

# 
# x = GET_SESSION (process_dataset, session_list = list("OPEN", "INTRADAY", "CLOSE"),
#                        folder_name = "S:/STKVN/PRICES/REPORTS/", ptype = 'HISTORY')
# ==================================================================================================
UPLOAD_RDS_TABLE = function(pName = "intranet_dev",
                            pHost = "27.71.235.71",
                            pUser = "intranet_admin",
                            pPass = "admin@123456",
                            folder_name = "S:/STKVN/PRICES/REPORTS/",
                            file_name = "process_dataset_4upload.rds",
                            table_name = "process_dataset",
                            key_column = "id") {
  db_connections = dbListConnections(MySQL())
  if (length(db_connections) > 0)
  {
    for (conn in db_connections) {
      dbDisconnect(conn)
    }
    CATln_Border(paste0("=== DONE CLOSE ALL CONNECTIONS ==="))
  }
  
  CATln_Border("UPLOAD_RDS_TABLE")
  database = dbConnect(RMySQL::MySQL(), 
                       dbname = pName, 
                       host = pHost, 
                       port = 3306,
                       user = pUser, 
                       password = pPass)
  
  new_data = readRDS(paste0(folder_name, file_name))
  print(new_data)
  tables = dbListTables(database)
  colnames(new_data) = make.unique(colnames(new_data))
  
  if (!'id' %in% colnames(new_data)) {
    new_data$id = 1:nrow(new_data)
  }
  
  if (!(table_name %in% tables))
  {
    dbWriteTable(database, name = table_name, value = new_data, row.names = FALSE, overwrite = TRUE)
  }
  else
  {
    columns_new_data = names(new_data)
    columns_db = dbListFields(database, table_name)
    if (!identical(columns_new_data, columns_db))
    {
      dbWriteTable(database, name = table_name, value = new_data, row.names = FALSE, overwrite = TRUE)
    }
    else
    {
      db_data = dbReadTable(database, table_name)
      if (nrow(db_data) != nrow(new_data))
      {
        dbWriteTable(database, name = table_name, value = new_data, row.names = FALSE, overwrite = TRUE)
      }
      else
      {
        for (i in 1:ncol(new_data))
        {
          for(j in 1:nrow(new_data))
          {
            column_name = colnames(new_data)[i]
            new_value = new_data[[i]][j]
            db_value = db_data[[i]][j]
            if(is.na(new_value))
            {
              update_query = sprintf("UPDATE %s SET `%s` = NULL WHERE %s = %d", 
                                     table_name, 
                                     column_name, 
                                     key_column, 
                                     new_data[[key_column]][j])
              CATln_Border(update_query)
              dbExecute(database, update_query)
            }
            if ((is.na(db_value) & !is.na(new_value)) || (!is.na(db_value) & !is.na(new_value) & new_value != db_value))
            {
              update_query = sprintf("UPDATE %s SET `%s` = '%s' WHERE %s = %d",
                                     table_name,
                                     column_name,
                                     new_value,
                                     key_column,
                                     new_data[[key_column]][j])
              CATln_Border(update_query)
              dbExecute(database, update_query)
              My.Kable.All(new_data)
            }
          }
        }
      }
    }
  }
  
  
  
  
  CATln_Border(paste0("=== DONE UPLOAD TABLE === ", substr(as.character(Sys.time()), 1, 19), " ==="))
  dbDisconnect(database)
}
# UPLOAD_RDS_TABLE = function(pName = "intranet_dev",
#                             pHost = "192.168.1.105",
#                             pUser = "intranet_admin",
#                             pPass = "admin@123456",
#                             folder_name = "S:/STKVN/PRICES/REPORTS/",
#                             file_name = "process_dataset_4upload.rds",
#                             table_name = "process_dataset",
#                             key_column = "id") {
#   CATln_Border("UPLOAD_RDS_TABLE")
# 
#   database = dbConnect(RMySQL::MySQL(), 
#                       dbname = pName, 
#                       host = pHost, 
#                       port = 3306,
#                       user = pUser, 
#                       password = pPass)
# 
#   new_data = readRDS(paste0(folder_name, file_name))
#   print(new_data)
#   tables = dbListTables(database)
#   colnames(new_data) = make.unique(colnames(new_data))
#   if (!(table_name %in% tables))
#   {
#     dbWriteTable(database, name = table_name, value = new_data, row.names = FALSE, overwrite = TRUE)
#   }
#   else
#   {
#     columns_new_data = names(new_data)
#     columns_db = dbListFields(database, table_name)
#     if (!identical(columns_new_data, columns_db))
#     {
#       dbWriteTable(database, name = table_name, value = new_data, row.names = FALSE, overwrite = TRUE)
#     }
#     else
#     {
#       db_data = dbReadTable(database, table_name)
#       if (nrow(db_data) != nrow(new_data))
#       {
#         dbWriteTable(database, name = table_name, value = new_data, row.names = FALSE, overwrite = TRUE)
#       }
#       else
#       {
#         for (i in 1:ncol(new_data))
#         {
#           for(j in 1:nrow(new_data))
#           {
#             column_name = colnames(new_data)[i]
#             new_value = new_data[[i]][j]
#             db_value = db_data[[i]][j]
#             if(is.na(new_value))
#             {
#               update_query = sprintf("UPDATE %s SET `%s` = NULL WHERE %s = %d", 
#                                      table_name, 
#                                      column_name, 
#                                      key_column, 
#                                      new_data[[key_column]][j])
#               CATln_Border(update_query)
#               dbExecute(database, update_query)
#             }
#             if ((is.na(db_value) & !is.na(new_value)) || (!is.na(db_value) & !is.na(new_value) & new_value != db_value))
#             {
#               update_query = sprintf("UPDATE %s SET `%s` = '%s' WHERE %s = %d",
#                                      table_name,
#                                      column_name,
#                                      new_value,
#                                      key_column,
#                                      new_data[[key_column]][j])
#               CATln_Border(update_query)
#               dbExecute(database, update_query)
#               My.Kable.All(new_data)
#             }
#           }
#         }
#       }
#     }
#   }
#   dbDisconnect(database)
# 
# }
# ==================================================================================================
CREATE_PROCESS_DATASET = function (folder_name = "S:/STKVN/PRICES/REPORTS/",
                                   session_list = list('OPEN', 'INTRADAY', 'CLOSE'),
                                   process_table_file = "process_dataset_4upload.rds",
                                   columns = list('REFERENCE_OPEN', 'MARKET', 'SHARESOUT','LAST', 'DIVIDEND', 'CHANGE', 'VARPC','CLOSE', 'VOLUME',"RT", 'FREEFLOAT'),
                                   list_type = list('DATASET', 'FINAL ALL', 'HISTORY') ){
  # ------------------------------------------------------------------------------------------------
  
  xList = list()
  process_table = data.table()
  
  for (k in 1:length(list_type)) {
    # k = 1
    ptype = list_type[[k]]
    process_type  = as.data.table(INIT_PROCESS_DATASET(columns, type = ptype))
    process_type = as.data.table(GET_SESSION (process_dataset = process_type, session_list = list("OPEN", "INTRADAY", "CLOSE"),
                                              folder_name = "S:/STKVN/PRICES/REPORTS/", ptype = ptype))
    
    process_type = process_type[, -c("REFERENCE_CLOSE")]
    
    if (nrow(process_type) > 0) {
      process_type[, nrp := k]
      xList[[ptype]] = process_type
    }
    print(xList)
    
  }
  process_table = rbindlist(xList, fill = T)
  process_table[, id := seq(1:.N)] 
  
  
  
  try(CCPR_SAVERDS(process_table, folder_name, process_table_file))
  My.Kable.All(process_table)
  return (process_table)
}

# ==================================================================================================
PRICESBOARD_TO_HISTORY = function(pSource = 'VND', ToSave = T, YYYYMMDD = '20240606', FromPricesHistory = F) {
  # ------------------------------------------------------------------------------------------------
  # pSource = 'VND'; ToSave = T; YYYYMMDD = '20240605'
  # pSource = 'CAF'; ToSave = T; YYYYMMDD = '20240605'
  # pSource = 'VST'; ToSave = T; YYYYMMDD = '20240605'
  if (FromPricesHistory)
  {
    File_Name      = paste0('download_', pSource, '_pricesboard_history.rds')
  } else {
    File_Name      = paste0('download_', pSource, '_pricesboard_', YYYYMMDD, '.rds')
    
    # directory <- "S:/STKVN/PRICES/DAY"
    # all_files <- list.files(directory, full.names = TRUE)
    # base_files <- basename(all_files)
    # pattern <- "download_.*pricesboard_\\d{8}\\.rds"
    # matching_files <- grep(pattern, base_files, value = TRUE)
    # File_Name_List <- grep(tolower(pSource), matching_files, value = TRUE)
  }
  
  File_Data      = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', File_Name, ToKable = T, ToRestore = T)))
  My.Kable(File_Data)
  File_History   = paste0('download_', pSource, '_pricesboard_history.rds')
  History_Data   = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', File_History, ToKable = T, ToRestore = T)))
  History_Data   = rbind(History_Data, File_Data, fill=T)
  
  # for (File_Name in File_Name_List){
  #   File_Data      = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', File_Name, ToKable = T, ToRestore = T)))
  #   My.Kable(File_Data)
  
  #   History_Data   = rbind(History_Data, File_Data, fill=T)
  # } 
  # History_Data[, X1 := NULL]
  
  
  if (nrow(File_Data)>0 & nrow(History_Data)>0)
  {
    History_Data = unique(History_Data, by=c('code', 'date'), fromLast = T)
    
    History_Data = History_Data[source==pSource]
    My.Kable(History_Data)
    if (ToSave)
    {
      try(CCPR_SAVERDS(History_Data, 'S:/STKVN/PRICES/DAY/', File_History, ToSummary = T, SaveOneDrive = T))
    }
  }
  
  return(History_Data)
}
# x = PRICESBOARD_TO_HISTORY(pSource = 'VND', ToSave = T, YYYYMMDD = '', FromPricesHistory = F) 

# ==================================================================================================
PRICESDAY_TO_HISTORY = function(pSource = 'STB', ToSave = T, YYYYMMDD = '20240621', FromPricesHistory = F) {
  # ------------------------------------------------------------------------------------------------
  # pSource = 'VND'; ToSave = T; YYYYMMDD = '20240605'
  # pSource = 'CAF'; ToSave = T; YYYYMMDD = '20240605'
  # pSource = 'C68'; ToSave = T; YYYYMMDD = '20240605'
  if (FromPricesHistory)
  {
    File_Name      = paste0('download_', pSource, '_stkvn_prices_history.rds')
  } else {
    File_Name      = paste0('download_', pSource, '_stkvn_pricesday_', YYYYMMDD, '.rds')
  }
  
  File_Data      = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', File_Name, ToKable = T, ToRestore = T)))
  
  if (pSource == 'STB'){
    File_Data[, varpc :=100*(close_adj/shift(close_adj)-1)]
    File_Data[, rt := rt_calculated]
    File_Data[, ticker := codesource]
  }

  My.Kable(File_Data)
  
  File_History   = paste0('download_', pSource, '_stkvn_pricesday_history.rds')
  History_Data   = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', File_History, ToKable = T, ToRestore = T)))
  
  if (pSource == 'STB'){
    History_Data[, varpc :=100*(close_adj/shift(close_adj)-1)]
    History_Data[, rt := rt_calculated]
    History_Data[, ticker := codesource]
  }

  History_Data   = rbind(History_Data, File_Data, fill=T)
  
  LastTrading     = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
  Prv_date        = CCPR_LAST_TRADING_DAY_VN(max(16, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T, Option ='prev_date'))
  LIST_CODE_BY_EXC_PRV         = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('list_code_by_exc_', gsub('-', '', Prv_date), '.rds') )))
  if (nrow(LIST_CODE_BY_EXC_PRV) > 0){
    LIST_CODE_BY_EXC_PRV[, date := as.Date(date)]
  }
  LIST_CODE_BY_EXC_DAY         = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('list_code_by_exc_', gsub('-', '', LastTrading), '.rds') )))
  if (nrow(LIST_CODE_BY_EXC_DAY) > 0){
    LIST_CODE_BY_EXC_DAY[, date := as.Date(date)]
  }  
  LIST_CODE_BY_EXC             = unique(rbind(LIST_CODE_BY_EXC_DAY, LIST_CODE_BY_EXC_PRV, fill = T)[order(-date)], by = 'code')
  
  History_Data[, market := NULL]
  History_Data = merge(History_Data, LIST_CODE_BY_EXC[, .(codesource,market)], by = 'codesource', all.x = T)

  if (nrow(File_Data)>0 & nrow(History_Data)>0)
  {
    History_Data = unique(History_Data, by=c('code', 'date'), fromLast = T)
    switch(pSource,
           'VND' = { Fields_list = c('source', 'ticker', 'codesource', 'code', 'market', 'date', 'volume', 'close', 'close_adj', 'varpc', 'rt', 'updated')},
           'C68' = { Fields_list = c('source', 'ticker', 'codesource', 'code', 'market', 'date', 'volume', 'close', 'close_adj', 'varpc', 'rt', 'updated')},
           'CAF' = { Fields_list = c('source', 'ticker', 'codesource', 'code', 'market', 'date', 'volume', 'close', 'close_adj', 'varpc', 'rt', 'updated')},
           { Fields_list = c('source', 'ticker', 'codesource', 'code', 'market', 'date', 'volume', 'close', 'close_adj', 'varpc', 'rt', 'updated')}
    )
    File_Data      = File_Data[, ..Fields_list]
    switch(pSource,
           'VND' = { 
             History_Data[, close:=close_adj] 
             # History_Data[, close_unadj:=close_adj] 
           },
           'C68' = { History_Data[, close:=close_adj] },
           'CAF' = { History_Data[, close:=close_adj] },
           { History_Data[, close:=close_adj] }
    )
    History_Data = History_Data[source==pSource]
    My.Kable(History_Data)
    if (ToSave)
    {
      try(CCPR_SAVERDS(History_Data, 'S:/STKVN/PRICES/DAY/', File_History, ToSummary = T, SaveOneDrive = T))
    }
  }
  
  return(History_Data)
}
# x = PRICESDAY_TO_HISTORY(pSource = 'DNSE', ToSave = T, YYYYMMDD = '20240610', FromPricesHistory = F) 
# x = PRICESDAY_TO_HISTORY(pSource = 'CAF', ToSave = T, YYYYMMDD = '20240610', FromPricesHistory = F) 
# x = PRICESDAY_TO_HISTORY(pSource = 'VPS', ToSave = T, YYYYMMDD = '20240610', FromPricesHistory = F) 
# x = PRICESDAY_TO_HISTORY(pSource = 'C68', ToSave = T, YYYYMMDD = '20240610', FromPricesHistory = F) 
# x = PRICESDAY_TO_HISTORY(pSource = 'VND', ToSave = T, YYYYMMDD = '20240610', FromPricesHistory = F)
# ==================================================================================================
MERGE_PRICESDAY_FIRSTCHARS = function(pSource   = 'VND', YYYYMMDD = '20240605')
  # ------------------------------------------------------------------------------------------------
{
  Data_List     = list() 
  File_Day      = tolower(paste0('download_', pSource, '_stkvn_pricesday', '_', YYYYMMDD, '.rds'))
  Day_Data      = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', File_Day, ToKable = T, ToRestore = T)))
  
  for (k in 1:length(LIST_FIRST_CHARS))
  {
    FirstChars     = LIST_FIRST_CHARS[[k]]
    CATln('')
    
    File_Name      = tolower(paste0('download_', pSource, '_stkvn_pricesday_', trimws(tolower(FirstChars)), '_', YYYYMMDD, '.rds'))
    
    CATln_Border(paste(FirstChars,'=', File_Name))
    CATln('')
    File_Data      = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', File_Name, ToKable = T, ToRestore = T)))
    
    My.Kable(File_Data)
    if (nrow(File_Data)>0) { Data_List[[k]] = File_Data }
  }
  Data_All = rbindlist(Data_List, fill=T)
  if (nrow(Data_All)>0)
  {
    Data_All = unique(rbind(Day_Data, Data_All, fill=T), by=c('code','date'), fromLast=T)
    Data_All = Data_All[source==pSource]
    File_Day = paste0('download_', pSource, '_stkvn_pricesday_', YYYYMMDD, '.rds')
    My.Kable(Data_All)
    try(CCPR_SAVERDS(Data_All, 'S:/STKVN/PRICES/DAY/', File_Day, ToSummary = T, SaveOneDrive = T))
  }
}

# ==================================================================================================
# LastTrading  = CCPR_LAST_TRADING_DAY_VN(max(16, as.numeric(substr(Sys.time(),12,13))), ToPrompt = T)
PROCESS_PRICES = function(File_Folder  = 'S:/STKVN/PRICES/DAY/',
                          STKVN        = T,
                          Prefix       = 'PRICESDAY',
                          ToSave       = T){
  # ------------------------------------------------------------------------------------------------      
                            # STKVN        = F
                                                      # Prefix       = 'PRICESBOARD'

                    
  switch(Prefix, 
         'PRICESDAY' = {
           LastTrading  = CCPR_LAST_TRADING_DAY_VN(max(18, as.numeric(substr(Sys.time(),12,13))), ToPrompt = T)
           LIST_SOURCES = list( 'VND', 'CAF', 'VPS','C68','DNSE','STB')
           File_name    = 'process_pricesday.rds'
         },
         'PRICESBOARD' = {
           LastTrading  = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13))), ToPrompt = T)
           LIST_SOURCES = LIST_SOURCES_PRICESBOARD
           File_name    = 'process_pricesboard.rds'
           
         })
  LIST = list()
  for (k in 1:length(LIST_SOURCES))
  {
    # # STKVN PRICESDAY FILES 
    # ------------------------------------------------------------------------------------------------
    #k = 6
    pSource = LIST_SOURCES[[k]]
    
    File_Name_Date    = paste0('DOWNLOAD_', pSource, ifelse(STKVN,'_STKVN',''), '_', Prefix, '_', gsub("-", "", LastTrading), '.rds')
    CATln_Border(paste0('DATE  = ', File_Name_Date))
    Data_Date         = CHECK_CLASS(try(CCPR_READRDS(File_Folder, File_Name_Date, ToKable = T, ToRestore = T)))
    # File_Name_Day    = paste0('DOWNLOAD_', pSource, '_PRICESBOARD_', 'DAY', '.rds')
    # CATln_Border(paste0('DAY  = ', File_Name_Day))
    # Data_Day         = CHECK_CLASS(try(CCPR_READRDS(File_Folder, File_Name_Day, ToKable = T, ToRestore = T)))
    File_Name_History   = paste0('DOWNLOAD_', pSource, ifelse(STKVN,'_STKVN',''), '_', Prefix, '_HISTORY.rds')
    CATln_Border(paste0('HISTORY = ', File_Name_History))
    Data_History        = CHECK_CLASS(try(CCPR_READRDS(File_Folder, File_Name_History, ToKable = T, ToRestore = T)))

    date_summary        = data.table()
    history_summary        = data.table()
    
    # download_c68_stkvn_pricesday_history[,.(code,date=as.Date(as.character(date)))][!is.na(date)][order(date)]
    if (nrow(Data_Date) > 0){Data_Date[, date:=as.Date(as.character(date))]}
    if (nrow(Data_History) > 0){Data_History[, date:=as.Date(as.character(date))]}
    
    # Data_History[, date:=as.Date(as.character(date))]
    
    # Summarize data
    if (nrow(Data_Date) > 0) {
      date_summary = data.table(
        File = 'DATE',
        Source = pSource,
        'DATA CODES' = n_distinct(Data_Date$code),
        'DATA START' = min(Data_Date$date, na.rm = TRUE),
        'DATA END' = max(Data_Date$date, na.rm = TRUE)
      )
    }
    
    if (nrow(Data_History) > 0) {
      history_summary = data.table(
        File = 'HISTORY',
        Source = pSource,
        'DATA CODES' = n_distinct(Data_History$code),
        'DATA START' = min(Data_History$date, na.rm = TRUE),
        'DATA END' = max(Data_History$date, na.rm = TRUE)
      )
    }
    
    data_summaries = rbind(date_summary, history_summary, fill = TRUE)
    # data_summaries = rbind(data_summaries, day_summary, fill = TRUE)
    
    LIST[[k]] = data_summaries
  }
  
  # Combine all summaries into one data frame
  final_summary = rbindlist(LIST, fill=T)
  final_summary = unique(final_summary, by = c("File", "Source"))
  # final_summary = setDT(spread(final_summary, key = 'Source', value = 'DATA CODES'))
  Data_codes = final_summary[, c('File', 'Source', 'DATA CODES')]
  Data_codes = setDT(spread(Data_codes, key = 'Source', value = 'DATA CODES'))
  Data_codes       = cbind(data.table(dataset='DATA CODES'), Data_codes)
  Data_codes[, pnr := 1]
  Data_codes = Data_codes[, lapply(.SD, as.character)]
  
  Data_start = final_summary[, c('File', 'Source', 'DATA START')]
  Data_start = setDT(spread(Data_start, key = 'Source', value = 'DATA START'))
  Data_start       = cbind(data.table(dataset='DATA START'), Data_start)
  Data_start[, pnr := 2]
  
  Data_start = Data_start[, lapply(.SD, as.character)]
  
  Data_end = final_summary[, c('File', 'Source', 'DATA END')]
  Data_end = setDT(spread(Data_end, key = 'Source', value = 'DATA END'))
  Data_end       = cbind(data.table(dataset='DATA END'), Data_end)
  Data_end[, pnr := 3]
  Data_end = Data_end[, lapply(.SD, as.character)]
  
  final_report = rbind(Data_codes,Data_start, fill = T)
  final_report = rbind(final_report,Data_end, fill = T)
  
  final_report = final_report[order(File, pnr)]
  final_report[, updated := SYS.TIME()]
  final_report[, id:=seq.int(1,.N)]
  final_report[, date := LastTrading]
  
  final_report <- final_report %>%
    select(dataset, File, date, everything())
  
  if (ToSave){
    CCPR_SAVERDS(final_report, "S:/STKVN/PRICES/REPORTS/", File_name, ToSummary = T, SaveOneDrive = T)
  }
  My.Kable.All(final_report)
  
  return (final_report)
}

# x = MERGE_PRICESDAY_FIRSTCHARS(pSource   = 'DNSE', YYYYMMDD = '20240607')
# ==================================================================================================
UPLOAD_PROCESS = function(file_folder  = 'S:/STKVN/PRICES/REPORTS/',
                          file_name    = 'process_pricesday.rds')
  # ------------------------------------------------------------------------------------------------
{
  library(data.table)
  library(DBI)
  library(RMySQL)
  library(RMariaDB)
  library(DT)
  connection <- dbConnect(
    dbDriver("MySQL"), 
    host = "192.168.1.105", 
    port = 3306,
    user = "intranet_admin", 
    password = "admin@123456", 
    db = "intranet_dev"
  )
  #data = readRDS("S:/SHINY/REPORT/REPORT_WCEO.rds")
  # data = readRDS("S:/STKVN/PRICES/REPORTS/process_pricesday.rds")
  data = readRDS(paste0(file_folder, file_name))
  # data[, c("id"):=NULL] 
  data = data[, -c('pnr')]
  #result = dbWriteTable(connection, "process_wceo", data[3,], append = TRUE, row.names = FALSE)
  result = dbWriteTable(connection, gsub('.rds','',file_name), data, row.names = FALSE, overwrite = TRUE)
  print(result)
  dbDisconnect(connection)
}

# ==================================================================================================
REPORT_WCEO = function(Action='CHECK DOWNLOAD DATA', ToSave = T,  SaveTo = '',
                       list_country = list('AUS', 'FRA', 'BEL', 'HKG', 'CHN', 'IDN', 'IRL', 'ITA', 'JPN', 'MYS', 'NOR', 'SGP', 'THA', 'TWN', 'PRT', 'DEU', 'NLD'))
{
  Display_All = data.table()
  switch(Action,
         'CHECK DOWNLOAD DATA' = {
           
           Data_list = list()
           k = 0
           for (pCountry in list_country)
           {
             k = k+1
             Final_Data = data.table()
             rm(data, NCodes, NDates, NDate1)
             # pCountry  = 'SGP'
             # pCountry  = 'FRA'
             File_folder = paste0('S:/WCEO_INDEXES/', pCountry, '/')
             File_name = paste0('download_', pCountry, '_yah_prices_final_day.rds')
             # File_folder = paste0('S:/SHINY/INDEX/WOMENCEO/', pCountry, '/')
             #  File_name = 'PRICES_FINAL_DAY.rds'
             data      = CHECK_CLASS(try(CCPR_READRDS(File_folder, File_name, ToKable = T)))
             
             if (nrow(data)>0)
             {
               NCodes    = nrow(unique(data[!is.na(codesource)], by='codesource'))
               NDates    = data[!is.na(codesource)][order(codesource, date)][, .(n=.N, start=date[1], end=date[.N]), by='codesource']
               NDate1    = NDates[, .(records=sum(n), start=min(start), end=max(end))]
               Final_Data = cbind(data.table(iso3=pCountry), data.table(codes=NCodes), NDate1)
               My.Kable.All(Final_Data)
               Data_list[[k]] = Final_Data
             } else{
               Data_list[[k]] = data.table(iso3=pCountry)
             }
           }
           All_List     = rbindlist(Data_list, fill=T)
           Display_All  = data.table()
           
           # Number ............................................................................................
           Display_List = All_List[, .(iso3, date=as.character(codes))]
           names_data   = as.vector(Display_List$iso3)
           data         = setDT(transpose(Display_List)[-1,])
           names(data)  = names_data
           data        = cbind(data.table(dataset='DATA CODES'), data)
           My.Kable(data)
           Display_All = rbind(Display_All, data, fill=T)
           
           # Dates ............................................................................................
           Display_List = All_List[, .(iso3, date=as.character(start))]
           names_data   = as.vector(Display_List$iso3)
           data         = setDT(transpose(Display_List)[-1,])
           names(data)  = names_data
           data        = cbind(data.table(dataset='DATA START'), data)
           My.Kable(data)
           Display_All = rbind(Display_All, data, fill=T)
           
           Display_List = All_List[, .(iso3, date=as.character(end))]
           names_data   = as.vector(Display_List$iso3)
           data         = setDT(transpose(Display_List)[-1,])
           names(data)  = names_data
           data        = cbind(data.table(dataset='DATA END'), data)
           My.Kable(data)
           Display_All = rbind(Display_All, data, fill=T)
           
           # Updated ............................................................................................
           Display_All [, updated := SYS.TIME()]
           Display_All [, id := (1:.N)]
           Display_All [, date := Sys.Date()]
           Display_All <- Display_All %>%
             select(dataset, date, everything())
           
           My.Kable.All(Display_All)
         }
  )
  
  if (ToSave) { saveRDS (Display_All, SaveTo)
    CATln(''); CATln_Border(paste('SAVED TO: ',SaveTo) ); CATln('')
  }
  if (nrow(Display_All)>0) { 
    CATln(''); CATln_Border('FINALLY '); CATln('')
    My.Kable.All(Display_All)}
  return(Display_All)
}

# ==================================================================================================
REPORT_WCEO_INDEX = function(Action='CHECK INDEX DATA', SaveTo = '',
                             list_country = list('AUS', 'FRA', 'BEL', 'HKG', 'CHN', 'IDN', 'IRL', 'ITA', 'JPN', 'MYS', 'NOR', 'SGP', 'THA', 'TWN', 'PRT', 'DEU', 'NLD'),
                             Folder_data = 'S:/WCEO_INDEXES/')
{
  # ------------------------------------------------------------------------------------------------
  Display_All = data.table()
  vector_country <- unlist(list_country)
  list_country <- sort(vector_country)
  switch(Action,
         'CHECK WCEO DATA' = {
           
           Data_list = list()
           k = 0
           for (pCountry in list_country)
           {
             k = k+1
             Final_Data = data.table()
             rm(data, NCodes, NDates, NDate1)
             # pCountry  = 'DEU'
             # pCountry  = 'FRA'
             # File_name = 'PRICES_FINAL_DAY.rds'
             # download_nld_yah_prices_final_day
             File_name = paste0('download_', pCountry, '_yah_prices_final_day.rds')
             data      = CHECK_CLASS(try(CCPR_READRDS(paste0(Folder_data, pCountry, '/'), File_name, ToKable = T)))
             
             if (nrow(data)>1)
             {
               NCodes    = nrow(unique(data[!is.na(codesource)], by='codesource'))
               NDates    = data[!is.na(codesource)][order(codesource, date)][, .(n=.N, start=date[1], end=date[.N]), by='codesource']
               if (!is.null(NDates))
               {
                 NDate1    = NDates[, .(records=sum(n), start=min(start), end=max(end))]
                 Final_Data = cbind(data.table(iso3=pCountry), data.table(codes=NCodes), NDate1)
                 My.Kable.All(Final_Data)
                 Data_list[[k]] = Final_Data
               }
               else{
                 #   # NDate1    = NDates[, .(records = as.numeric(NA), start = as.Date(NA), end = as.Date(NA))]
                 #   # Final_Data = cbind(data.table(iso3=pCountry), data.table(codes=as.numeric(NA)), NDate1)
                 #   # My.Kable.All(Final_Data)
                 Data_list[[k]] = Final_Data
               }
             }
           }
           All_List     = rbindlist(Data_list, fill=T)
           Display_All  = data.table()
           
           # Number ............................................................................................
           Display_List = All_List[, .(iso3, date=as.character(codes))]
           names_data   = as.vector(Display_List$iso3)
           data         = setDT(transpose(Display_List)[-1,])
           names(data)  = names_data
           data        = cbind(data.table(dataset='DATA CODES'), data)
           My.Kable(data)
           Display_All = rbind(Display_All, data, fill=T)
           
           # Dates ............................................................................................
           Display_List = All_List[, .(iso3, date=as.character(start))]
           names_data   = as.vector(Display_List$iso3)
           data         = setDT(transpose(Display_List)[-1,])
           names(data)  = names_data
           data        = cbind(data.table(dataset='DATA START'), data)
           My.Kable(data)
           Display_All = rbind(Display_All, data, fill=T)
           
           Display_List = All_List[, .(iso3, date=as.character(end))]
           names_data   = as.vector(Display_List$iso3)
           data         = setDT(transpose(Display_List)[-1,])
           names(data)  = names_data
           data        = cbind(data.table(dataset='DATA END'), data)
           My.Kable(data)
           Display_All = rbind(Display_All, data, fill=T)
           
           My.Kable.All(Display_All)
         },
         'CHECK WCEO INDEXES' = {
           
           Data_list = list()
           k = 0
           for (pCountry in list_country)
           {
             k = k+1
             Final_Data = data.table()
             rm(data, NCodes, NDates, NDate1)
             # pCountry  = 'SGP'
             # pCountry  = 'USA'
             # File_name = 'PRICES_FINAL_DAY.rds'
             # File_name = paste0('ifrc_ccpr_indwceo', pCountry, '_history.rds')
             # ifrc_ccpr_indwceonld_history.rds
             # File_name = paste0('wceo_', pCountry, '_ind_history.rds')
             File_name = paste0('ifrc_ccpr_indwceo', pCountry, '_history.rds')
             data      = CHECK_CLASS(try(CCPR_READRDS(paste0(Folder_data, pCountry, '/'), File_name, ToKable = T)))
             
             if (nrow(data)>0)
             {
               NCodes    = nrow(unique(data[!is.na(code)], by='code'))
               NDates    = data[!is.na(code)][order(code, date)][, .(n=.N, start=date[1], end=date[.N]), by='code']
               NDate1    = NDates[, .(records=sum(n), start=min(start), end=max(end))]
               Final_Data = cbind(data.table(iso3=pCountry), data.table(codes=NCodes), NDate1)
               My.Kable.All(Final_Data)
               Data_list[[k]] = Final_Data
             } else{
               Data_list[[k]] = data.table(iso3=pCountry)
             }
           }
           All_List     = rbindlist(Data_list, fill=T)
           Display_All  = data.table()
           
           # Number ............................................................................................
           Display_List = All_List[, .(iso3, date=as.character(codes))]
           names_data   = as.vector(Display_List$iso3)
           data         = setDT(transpose(Display_List)[-1,])
           names(data)  = names_data
           data        = cbind(data.table(dataset='INDEX CODES'), data)
           My.Kable(data)
           Display_All = rbind(Display_All, data, fill=T)
           
           # Dates ............................................................................................
           Display_List = All_List[, .(iso3, date=as.character(start))]
           names_data   = as.vector(Display_List$iso3)
           data         = setDT(transpose(Display_List)[-1,])
           names(data)  = names_data
           data        = cbind(data.table(dataset='INDEX START'), data)
           My.Kable(data)
           Display_All = rbind(Display_All, data, fill=T)
           
           Display_List = All_List[, .(iso3, date=as.character(end))]
           names_data   = as.vector(Display_List$iso3)
           data         = setDT(transpose(Display_List)[-1,])
           names(data)  = names_data
           data        = cbind(data.table(dataset='INDEX END'), data)
           My.Kable(data)
           Display_All = rbind(Display_All, data, fill=T)
           
           My.Kable.All(Display_All)
         },
         'NUMBER_COMPO_CODE' = {
           Data_list = list()
           k = 0
           for (pCountry in list_country)
           {
             k = k+1
             Final_Data = data.table()
             rm(data, NCodes, NDates, NDate1)
             # pCountry  = 'SGP'
             # pCountry  = 'USA'
             # File_name = 'PRICES_FINAL_DAY.rds'
             # File_name = paste0('ifrc_ccpr_indwceo', pCountry, '_history.rds')
             # ifrc_ccpr_indwceonld_history.rds
             # File_name = paste0('wceo_', pCountry, '_ind_history.rds')
             File_name   = paste0('ifrc_ccpr_indwceo', tolower(pCountry), '_compo.rds') 
             data      = CHECK_CLASS(try(CCPR_READRDS(paste0('S:/SHINY/INDEX/WOMENCEO/', pCountry, '/'), File_name, ToKable = T)))
             
             if (nrow(data)>0)
             {
               NCodes    = nrow(unique(data[!is.na(codesource)], by='codesource'))
               NDates    = data[!is.na(codesource)][order(codesource, date)][, .(n=.N, start=date[1], end=date[.N]), by='codesource']
               NDate1    = NDates[, .(records=sum(n), start=min(start), end=max(end))]
               Final_Data = cbind(data.table(iso3=pCountry), data.table(codes=NCodes), NDate1)
               My.Kable.All(Final_Data)
               Data_list[[k]] = Final_Data
             } else{
               Data_list[[k]] = data.table(iso3=pCountry)
             }
           }
           All_List     = rbindlist(Data_list, fill=T)
           Display_All  = data.table()
           # Number ............................................................................................
           Display_List = All_List[, .(iso3, date=as.character(codes))]
           names_data   = as.vector(Display_List$iso3)
           data         = setDT(transpose(Display_List)[-1,])
           names(data)  = names_data
           data        = cbind(data.table(dataset='COMPO CODES'), data)
           My.Kable(data)
           Display_All = rbind(Display_All, data, fill=T)
         }
  )
  
  if (nchar(SaveTo) >0) { saveRDS (Display_All, SaveTo)
    CATln(''); CATln_Border(paste('SAVED TO: ',SaveTo) ); CATln('')
  }
  if (nrow(Display_All)>0) { 
    CATln(''); CATln_Border('FINALLY '); CATln('')
    My.Kable.All(Display_All)}
  return(Display_All)
}


# ==================================================================================================
REPORT_PROCESS_PRICES = function(Prefix = 'PRICESDAY', pOption = ''){
  # ------------------------------------------------------------------------------------------------
  switch(Prefix,
         'PRICESDAY' = {
           # MERGE TO HISTORY
           LastTrading  = CCPR_LAST_TRADING_DAY_VN(max(18, as.numeric(substr(Sys.time(),12,13))), ToPrompt = T)
           
           pPROBA = 0
           for (pSource in list('VND', 'CAF', 'C68', 'DNSE', 'VPS', 'STB'))
           {
             # pSource = 'C68'
             if (runif(1,1,100)>pPROBA)
             {
               try(MERGE_PRICESDAY_FIRSTCHARS(pSource   = pSource, YYYYMMDD = gsub('-','', LastTrading)))
               try(PRICESDAY_TO_HISTORY(pSource   = pSource, ToSave = T, YYYYMMDD = gsub('-','', LastTrading), FromPricesHistory = F))
               IFRC_SLEEP(10)
             }
           }
           # REPORT PROCESS PRICESDAY
           x = PROCESS_PRICES(File_Folder  = 'S:/STKVN/PRICES/DAY/',
                              STKVN        = T,
                              Prefix       = 'PRICESDAY',
                              ToSave = T)
           Sys.sleep(5)
           
           #  UPLOAD INTRANET
           UPLOAD_PROCESS (file_folder  = 'S:/STKVN/PRICES/REPORTS/',
                           file_name    = 'process_pricesday.rds')
           TRAINEE_MONITOR_EXECUTION_SUMMARY (pOption='SCRIPT > REPORT_PROCESS_PRICESDAY', pAction="SAVE", NbSeconds=3600, ToPrint=F) 
         },
         'PRICESBOARD' = {
           # MERGE TO HISTORY
           LastTrading  = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13))), ToPrompt = T)
           pPROBA = 0
           for (pSource in LIST_SOURCES_PRICESBOARD)
           {
             if (runif(1,1,100)>pPROBA)
             {
               # try(MERGE_PRICESDAY_FIRSTCHARS(pSource   = pSource, YYYYMMDD = gsub('-','', LastTrading)))
               try(PRICESBOARD_TO_HISTORY(pSource   = pSource, ToSave = T, YYYYMMDD = gsub('-','', LastTrading), FromPricesHistory = F))
               
             }
           }
           # REPORT PROCESS PRICESDAY
           x = PROCESS_PRICES(File_Folder  = 'S:/STKVN/PRICES/DAY/',
                              STKVN        = F,
                              Prefix       = 'PRICESBOARD',
                              ToSave = T)
           #  UPLOAD INTRANET
           UPLOAD_PROCESS (file_folder  = 'S:/STKVN/PRICES/REPORTS/',
                           file_name    = 'process_pricesboard.rds')
           TRAINEE_MONITOR_EXECUTION_SUMMARY (pOption='SCRIPT > REPORT_PROCESS_PRICESBOARD', pAction="SAVE", NbSeconds=3600, ToPrint=F) 
           
         },
         'WCEO' = {
          #  data =  REPORT_WCEO (Action='CHECK DOWNLOAD DATA', ToSave = T,  SaveTo = 'S:/WCEO_INDEXES/REPORT_WCEO.rds',
          #                       list_country = list('AUS', 'FRA', 'BEL', 'HKG', 'CHN', 'IDN', 'IRL', 'ITA', 'JPN', 'MYS', 'NOR', 'SGP', 'THA', 'TWN', 'PRT', 'DEU', 'NLD'))
          REPORT_DATA  = data.table()
          REPORT_INDEX = data.table()
          switch(pOption,
                 'DATA' ={
                   REPORT_DATA = REPORT_WCEO_INDEX(Action='CHECK WCEO DATA', SaveTo = '',
                                                      list_country = list('AUS', 'FRA', 'BEL', 'HKG', 'CHN', 'IDN', 'IRL', 'ITA', 'JPN', 'MYS', 'NOR', 'SGP', 'THA', 'TWN', 'PRT', 'DEU', 'NLD'))
                 },
                 'INDEX' = {
                   REPORT_INDEX = REPORT_WCEO_INDEX(Action='CHECK WCEO INDEXES', SaveTo = '',
                                                      list_country = list('AUS', 'FRA', 'BEL', 'HKG', 'CHN', 'IDN', 'IRL', 'ITA', 'JPN', 'MYS', 'NOR', 'SGP', 'THA', 'TWN', 'PRT', 'DEU', 'NLD')) 
                 },
                 {
                   REPORT_DATA = REPORT_WCEO_INDEX(Action='CHECK WCEO DATA', SaveTo = '',
                                                   list_country = list('AUS', 'FRA', 'BEL', 'HKG', 'CHN', 'IDN', 'IRL', 'ITA', 'JPN', 'MYS', 'NOR', 'SGP', 'THA', 'TWN', 'PRT', 'DEU', 'NLD'))
                   REPORT_INDEX = REPORT_WCEO_INDEX(Action='CHECK WCEO INDEXES', SaveTo = '',
                                                    list_country = list('AUS', 'FRA', 'BEL', 'HKG', 'CHN', 'IDN', 'IRL', 'ITA', 'JPN', 'MYS', 'NOR', 'SGP', 'THA', 'TWN', 'PRT', 'DEU', 'NLD')) 
                   REPORT_COMPO = REPORT_WCEO_INDEX(Action='NUMBER_COMPO_CODE', SaveTo = '',
                                                    list_country = list('AUS', 'FRA', 'BEL', 'HKG', 'CHN', 'IDN', 'IRL', 'ITA', 'JPN', 'MYS', 'NOR', 'SGP', 'THA', 'TWN', 'PRT', 'DEU', 'NLD')) 
                 })
                   

          #  REPORT_INDEX_1[, id := (1:.N)]
          #  REPORT_INDEX_2[, id := (1:.N)]

          #  data = merge(REPORT_INDEX_1, REPORT_INDEX_2, by = c('dataset','id'), all.x = T)
           data = rbind(REPORT_COMPO, REPORT_DATA, REPORT_INDEX, fill = T)
           data[, date := SYSDATETIME(23)-1]
          #  data = data[order(id)]
           data[, id := (1:.N)]
           data = data %>%
                select(dataset, date, everything())
           data = UPDATE_UPDATED(data)
           library(data.table)
           library(DBI)
           library(RMySQL)
           library(RMariaDB)
           library(DT)
           connection <- dbConnect(
             dbDriver("MySQL"), 
             host = "192.168.1.105", 
             port = 3306,
             user = "intranet_admin", 
             password = "admin@123456", 
             db = "intranet_dev"
           )
           #data = readRDS("S:/SHINY/REPORT/REPORT_WCEO.rds")
           # data = readRDS("S:/STKVN/PRICES/REPORTS/process_pricesday.rds")
          #  data = readRDS('S:/WCEO_INDEXES/REPORT_WCEO.rds')
           # current_date <- Sys.Date()
           # cols_to_update <- setdiff(names(data), c("dataset", "date", "updated", "id"))
           # data[data$id == 3, cols_to_update] <- current_date
           
           My.Kable.All(data)
           # data[, c("id"):=NULL] 
           # data = data[, -c('pnr')]
           #result = dbWriteTable(connection, "process_wceo", data[3,], append = TRUE, row.names = FALSE)
           try(saveRDS(data, "S:/WCEO_INDEXES/report_process_in.rds"))
           result = dbWriteTable(connection, 'process_wceo', data, row.names = FALSE, overwrite = TRUE)
           print(result)
           dbDisconnect(connection)   
           try(TRAINEE_MONITOR_EXECUTION_SUMMARY (pOption='SCRIPT > REPORT_PROCESS_WCEO', pAction="SAVE", NbSeconds=3600, ToPrint=F)) 
         })
}

# ==================================================================================================
REPORT_MISC = function(ToForce = F){
  # ------------------------------------------------------------------------------------------------
  
  # TO_DO = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='SCRIPT > REPORT_PROCESS_PRICESBOARD', pAction = "COMPARE", NbSeconds = 900, ToPrint = F))
  
  # REPORT_PROCESS_PRICES (Prefix = 'PRICESDAY')
  # REPORT_PROCESS_PRICES (Prefix = 'PRICESBOARD')
  
  
  
  List_SetChars = list('ABC', 'DEF', 'GHI', 'JKL', 'MNO', 'PQR', 'STU', 'VWXYZ')
  LastTrading = CCPR_LAST_TRADING_DAY_VN(16, ToPrompt = T)
  
  for (SOURCE in sample(list( 'CAF','VND',  'VPS','C68','DNSE', 'STB'))){
    for (MARKET in sample(list('HNX','HSX','UPC'))){
      for (pFirstChars in sample(List_SetChars)){
        if (ToForce || CHECK_TIMEBETWEEN('19:00' , '23:00') || CHECK_TIMEBETWEEN('12:00' , '14:00') || CHECK_TIMEBETWEEN('01:00' , '05:00')){
          FirstChar = pFirstChars
          x = NEW_LOOP_DOWNLOAD_CUSTOM_BY_CODE ( ranking    = '', FirstChars = pFirstChars, random = F,
                                                 pMarket    = MARKET, 
                                                 pFolder    = 'S:/STKVN/PRICES/DAY/',
                                                 File_Pattern  = paste0('DOWNLOAD_',SOURCE,'_STKVN_PRICESDAY_',FirstChar),
                                                 pDate      = LastTrading,
                                                 List_Codes = list(),
                                                 File_Codes = paste0("S:/CCPR/DATA/download_",tolower(MARKET),"_stkvn_ref_history.rds"),
                                                 Action     = paste0('DOWNLOAD_',SOURCE,'_STKVN_PRICESDAY_BY_CODE'))
        }
      }
    }
  }
}
# ==================================================================================================
TRAINEE_FINAL_SESSION = function(pSESSION="INTRADAY", OPEN_Minute = 5, INTRADAY_Minute = 0,
                                        CLOSE_Minute = 5, COMPARE = T)
{
  # ------------------------------------------------------------------------------------------------
  # pSESSION="CLOSE"; EveryMin = 5
  # pSESSION="OPEN"
  LastTrading = CCPR_LAST_TRADING_DAY_VN(max(8, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
  DBL_RELOAD_INSREF()
  pMyPC = toupper( as.character(try(fread("C:/R/my_pc.txt", header = F))) )
  
  
  switch (pSESSION, 
          "OPEN" = {
            NB_SECONDS = OPEN_Minute * 60
          }, 
          "INTRADAY" = {
            NB_SECONDS = INTRADAY_Minute * 60
          },
          "CLOSE" = {
            NB_SECONDS = CLOSE_Minute * 60
          })
  if (COMPARE){
    TO_DO = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0('SCRIPT > TRAINEE_FINAL_SESSION_',pSESSION), pAction = "COMPARE", NbSeconds = NB_SECONDS, ToPrint = F))
  } else {
    TO_DO = T
  }
  if ((pMyPC %in% LIST_PC_FOR_STKVN_REPORT) && TO_DO){
    switch (pSESSION,
            "OPEN" = {
              # OPEN
              # try(TRAINEE_FINAL_OPEN_CLOSE_DAILY(File_Folder='S:/STKVN/PRICES/DAY/', Prefix = 'PRICESBOARD', 
              #                                    SESSION = pSESSION, STKVN=F,
              #                                    LIST_SOURCES = LIST_SOURCE_FOR_REPORT,
              #                                    Pre_Date = '',Force=T, FixShares=T))
              # try(TRAINEE_FINAL_4INDEX( pSESSION=pSESSION, FinalFolder = "S:/STKVN/PRICES/FINAL/", pDate = LastTrading))
              X = TRAINEE_REPORT_AND_FINAL_BY_SESSION(SESSION = 'OPEN')
              MERGE_FINAL_ALL_BY_SESSION (SESSION = 'OPEN')
            },
            "INTRADAY" = {
              X = TRAINEE_REPORT_AND_FINAL_BY_SESSION(SESSION = 'INTRADAY')
              MERGE_FINAL_ALL_BY_SESSION  (SESSION = 'INTRADAY')
            },
            "CLOSE" = {
              # CLOSE
              # try(TRAINEE_FINAL_OPEN_CLOSE_DAILY(File_Folder='S:/STKVN/PRICES/DAY/', Prefix = 'PRICESBOARD', 
              #                                    SESSION = pSESSION, STKVN=F,
              #                                    LIST_SOURCES = LIST_SOURCE_FOR_REPORT,
              #                                    Pre_Date = '',Force=F, FixShares=T))
              # try(TRAINEE_FINAL_4INDEX( pSESSION=pSESSION, FinalFolder = "S:/STKVN/PRICES/FINAL/", pDate = LastTrading))
              X = TRAINEE_REPORT_AND_FINAL_BY_SESSION(SESSION = 'CLOSE')
              MERGE_FINAL_ALL_BY_SESSION (SESSION = 'CLOSE')
            }
            
    )
    My_data = CREATE_PROCESS_DATASET(folder_name = "S:/STKVN/PRICES/REPORTS/",
                                     session_list = list('OPEN', 'INTRADAY', 'CLOSE'),
                                     process_table_file = "process_dataset_4upload.rds",
                                     columns = list('REFERENCE_OPEN', 'MARKET', 'SHARESOUT','LAST', 'DIVIDEND', 'CHANGE', 'VARPC','CLOSE', 'VOLUME',"RT", 'FREEFLOAT'),
                                     list_type = list('DATASET', 'FINAL ALL', 'HISTORY') )
    
    
    
    # x = UPLOAD_RDS_TABLE (pName = "intranet_dev",
    #                       pHost = "192.168.1.105",
    #                       pUser = "intranet_admin",
    #                       pPass = "admin@123456",
    #                       folder_name = "S:/STKVN/PRICES/REPORTS/",
    #                       file_name = "process_dataset_4upload.rds",
    #                       table_name = "process_dataset",
    #                       key_column = "id")
    
    
    library(data.table)
    library(DBI)
    library(RMySQL)
    library(RMariaDB)
    library(DT)
    connection <- dbConnect(
      dbDriver("MySQL"), 
      host = "192.168.1.105", 
      port = 3306,
      user = "intranet_admin", 
      password = "admin@123456", 
      db = "intranet_dev"
    )

    result = dbWriteTable(connection, 'process_dataset', My_data, row.names = FALSE, overwrite = TRUE)
    print(result)
    dbDisconnect(connection)  
    # IFRC_SLEEP(NB_SECONDS)
    Monitor = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0('SCRIPT > TRAINEE_FINAL_SESSION_',pSESSION), pAction="SAVE", NbSeconds=1, ToPrint=F))
   
  } else {
    CATln_Border("WAITING FOR NEXT LOOP")
  }
}

# ==================================================================================================


# ==================================================================================================
REPORT_IMBALANCE = function(){
  # ------------------------------------------------------------------------------------------------
  library(fst)
  
  report_batch = data.table()
  report_history = data.table()
  
  batch_manage = setDT(fread("S:/CCPR/DATA/MONITOR/TRAINEE_MONITOR_EXECUTION_SUMMARY.txt"))
  
  report_feargreed = batch_manage[grepl('FEARGREED',code) | grepl('DBL_PREMIUM',code)]
  
  report_feargreed[, delay := as.numeric(difftime(as.POSIXct(substr(Sys.time(),1,19)),
                                                  as.POSIXct(substr(updated,1,19)),units = "mins"))]
  
  report_feargreed[, status := ifelse(delay > 60, "NOT RUN", "STILL RUNNING")]

  
  report_imbalance = batch_manage[grepl('IMBALANCE',code)]
  
  report_imbalance[, delay := as.numeric(difftime(as.POSIXct(substr(Sys.time(),1,19)),
                                              as.POSIXct(substr(updated,1,19)),units = "mins"))]
  
  report_imbalance[, status := ifelse(delay > 60, "NOT RUN", "STILL RUNNING")]
  
  saveRDS(report_imbalance, 'S:/SHINY/IMBALANCES/BATCH/report_imbalance.rds')
  write_fst(report_imbalance, 'S:/SHINY/IMBALANCES/BATCH/report_imbalance.fst')
  
  saveRDS(report_feargreed, 'S:/SHINY/FEARGREED/BATCH/report_feargreed.rds')
  write_fst(report_feargreed, 'S:/SHINY/FEARGREED/BATCH/report_feargreed.fst')
  
  history = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/','DBL_IND_IMBALANCE_HISTORY.rds', ToRestore = T)))
  history = history[order(code,date)][, ':=' (min_date = min(date, na.rm = T), 
                                              max_date = max(date, na.rm = T)), by = 'code']
  
  report_history = unique(history[order(-date)][,.(code, name, close, min_date, max_date, imbalance_number, change, imbalance_level, updated)], by = 'code')
  saveRDS(report_history, 'S:/SHINY/IMBALANCES/HISTORY/report_history.rds')
  write_fst(report_history, 'S:/SHINY/IMBALANCES/HISTORY/report_history.fst')
}



