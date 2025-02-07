# ==================================================================================================
# DBL_INTRADAY_LIBRARY.R 2024-08-31 09:22
# ==================================================================================================

# ==================================================================================================
DBL_INTRADAY_LOOP_ONE = function(priority='top', ToCompare = F, EveryMinutes = 5) {
  # ------------------------------------------------------------------------------------------------
  switch(priority,
         'top' = {
           if (ToCompare)
           {
             TO_DO = TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DBL_INTRADAY_LOOP_ONE > TOP', pAction="COMPARE", NbSeconds=EveryMinutes*60, ToPrint=T)
           } else { TO_DO = T}
           if (TO_DO)
           {
             DATA     = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_INTRADAY_BY_LIST (frequency = "intraday", category = "", priority = "top",    minnb = 50)))
             DATA_DAY = DBL_INTEGRATE_INTRADAY_BY_TIMESTAMP ( DATA,
                                                              Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                              Save_File   = 'DBL_SOURCE_INTRADAY_1MN_DAY.rds')
             
             DATA     = CHECK_CLASS(try(DBL_DOWNLOAD_DNSE_INTRADAY_BY_LIST(frequency = "intraday", category = "", priority = "top",    minnb = 50)))
             DATA_DAY = DBL_INTEGRATE_INTRADAY_BY_TIMESTAMP ( DATA,
                                                              Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                              Save_File   = 'DBL_SOURCE_INTRADAY_1MN_DAY.rds')
             
             DATA     = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_INTRADAY_BY_LIST  (frequency = "day", category = "", priority = "top",    minnb = 200)))
             DATA     = CHECK_CLASS(try(DBL_DOWNLOAD_DNSE_INTRADAY_BY_LIST (frequency = "top", category = "", priority = "medium", minnb = 200)))
           }
         },
         
         'medium' = {
           if (ToCompare)
           {
             TO_DO = TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DBL_INTRADAY_LOOP_ONE > MEDIUM', pAction="COMPARE", NbSeconds=EveryMinutes*60, ToPrint=T)
           } else { TO_DO = T}
           if (TO_DO)
           {
             DATA     = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_INTRADAY_BY_LIST (frequency = "intraday", category = "", priority = "medium",  minnb = 500)))
             DATA_DAY = DBL_INTEGRATE_INTRADAY_BY_TIMESTAMP ( DATA,
                                                              Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                              Save_File   = 'DBL_SOURCE_INTRADAY_1MN_DAY.rds')
             
             DATA     = CHECK_CLASS(try(DBL_DOWNLOAD_DNSE_INTRADAY_BY_LIST(frequency = "intraday", category = "", priority = "medium",    minnb = 500)))
             DATA_DAY = DBL_INTEGRATE_INTRADAY_BY_TIMESTAMP ( DATA,
                                                              Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                              Save_File   = 'DBL_SOURCE_INTRADAY_1MN_DAY.rds')
             
             DATA     = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_INTRADAY_BY_LIST  (frequency = "day", category = "", priority = "medium",    minnb = 200)))
             DATA     = CHECK_CLASS(try(DBL_DOWNLOAD_DNSE_INTRADAY_BY_LIST (frequency = "day", category = "", priority = "medium",    minnb = 200)))
           }
         })
}

# ==================================================================================================
DBL_DOWNLOAD_YAH_INTRADAY_BY_LIST = function(frequency = "intraday", category = "", priority = "top", minnb = 1000) {
  # ------------------------------------------------------------------------------------------------
  # frequency = "day"; priority = "top"
  DATA_DAY = data.table()
  start_time = Sys.time()
  x = setDT(read.xlsx("S:/CCPR/DATA/LIST/LIST_PRODUCT_DUO.xlsx", sheet = 'FINAL'))
  fwrite(x, 'S:/CCPR/DATA/LIST/LIST_PRODUCT_DUO.txt', sep = '\t' )
  
  list_duo = setDT(fread('S:/CCPR/DATA/LIST/LIST_PRODUCT_DUO.txt'))
  list_duo = list_duo [!is.na(YAH_PRODUCT)]
  My.Kable(list_duo)
  if (nchar(category) > 0)
  {
    list_duo_fgd = list_duo[CATEGORY == category]
  } else
  {
    list_duo_fgd = unique(list_duo, by = "YAH_PRODUCT")
  }
  My.Kable(list_duo_fgd)
  if (nchar(priority) > 0)
  {
    list_duo_fgd = list_duo_fgd[PRIORITY == priority]
  }
  nbtodo = min(minnb, nrow(list_duo_fgd))
  if (nbtodo > 0)
  {
    list_todo = list_duo_fgd[1:nbtodo]
    
    My.Kable(list_todo)
    
    DATA     = data.table()
    DATA_ALL = data.table()
    XLIST    = list()
    
    for (k in 1: nrow(list_todo))
    {
      # k = 1
      pCode = list_todo[k]$YAH_PRODUCT
      CATln_Border(pCode)
      switch(frequency,
             "intraday" = {
               DATA  = CHECK_CLASS(try(DOWNLOAD_YAH_INTRADAY_DAY_BY_CODE(pCode = pCode, pInt = '1m')))
               if (nrow(DATA) > 0)
               {
                 #DATA        = DBL_IND_TO_CORRECT(DATA)
                 DATA        = CLEAN_TIMESTAMP(DATA)
                 XLIST [[k]] = DATA
               }
             },
             "day" = {
               DATA  = CHECK_CLASS(try(DOWNLOAD_YAH_INTRADAY_DAY_BY_CODE(pCode = pCode, pInt = '1d')))
               if (nrow(DATA) > 0)
               {
                 #DATA        = DBL_IND_TO_CORRECT(DATA)
                 DATA        = CLEAN_TIMESTAMP(DATA)
                 XLIST [[k]] = DATA
               }
             }
      )
      
    }
    switch(frequency,
           "intraday" = {
             
             DATA_ALL = rbindlist(XLIST, fill = T)
             
             My.Kable.Min(DATA_ALL[order(-date)])
             
             try(DBL_CCPR_SAVERDS(DATA_ALL, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'DBL_YAH_INTRADAY_1MN_TODAY.rds', ToSummary = T, SaveOneDrive = T))
             
             DATA_DAY = DBL_INTEGRATE_INTRADAY_BY_TIMESTAMP ( DATA_ALL,
                                                              Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                              Save_File   = 'DBL_YAH_INTRADAY_1MN_DAY.rds')
             
             
             DATA_HISTORY = DBL_INTEGRATE_INTRADAY_BY_TIMESTAMP ( DATA_ALL,
                                                                  Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                                  Save_File   = 'DBL_YAH_INTRADAY_1MN_HISTORY.rds')
             DATA_LAST = unique(DATA_DAY[order(code, -timestamp_vn)], by = c("code"))
             My.Kable.All(DATA_LAST[, -c("home", "symbol", "sample", "provider", "fcat", "updated", "capiusd", "date",
                                         "isin", "source", "open", "high", "low", "type", "datetime", "iso2", "cur", "timestamp")])
           },
           "day" = {
             
             DATA_ALL = rbindlist(XLIST, fill = T)
             
             My.Kable.Min(DATA_ALL[order(-date)])
             
             try(DBL_CCPR_SAVERDS(DATA_ALL, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'DBL_YAH_INTRADAY_DAY_TODAY.rds', ToSummary = T, SaveOneDrive = T))
             
             DATA_DAY = DBL_INTEGRATE_INTRADAY_BY_TIMESTAMP ( DATA_ALL,
                                                              Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                              Save_File   = 'DBL_YAH_INTRADAY_DAY_DAY.rds')
             
             
             DATA_HISTORY = DBL_INTEGRATE_INTRADAY_BY_TIMESTAMP ( DATA_ALL,
                                                                  Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                                  Save_File   = 'DBL_YAH_INTRADAY_DAY_HISTORY.rds')
           }
    )
  }
  DBL_DURATION(start_time)
  return (DATA_DAY)
}

# ==================================================================================================
DBL_DOWNLOAD_DNSE_INTRADAY_BY_LIST = function(frequency = "intraday", category = "", priority = "top", minnb = 1000) {
  # ------------------------------------------------------------------------------------------------
  # frequency = "day"; priority = "top"
  DATA_DAY = data.table()
  start_time = Sys.time()
  x = setDT(read.xlsx("S:/CCPR/DATA/LIST/LIST_PRODUCT_DUO.xlsx", sheet = 'FINAL'))
  fwrite(x, 'S:/CCPR/DATA/LIST/LIST_PRODUCT_DUO.txt', sep = '\t' )
  
  list_duo = setDT(fread('S:/CCPR/DATA/LIST/LIST_PRODUCT_DUO.txt'))
  list_duo = list_duo [!is.na(DNSE_PRODUCT)]
  My.Kable(list_duo)
  sort(names(list_duo))
  if (nchar(category) > 0)
  {
    list_duo_fgd = list_duo[CATEGORY == category]
  } else
  {
    list_duo_fgd = unique(list_duo, by = "DNSE_PRODUCT")
  }
  My.Kable(list_duo_fgd)
  if (nchar(priority) > 0)
  {
    list_duo_fgd = list_duo_fgd[PRIORITY == priority]
  }
  nbtodo = min(minnb, nrow(list_duo_fgd))
  if (nbtodo > 0)
  {
    list_todo = list_duo_fgd[1:nbtodo]
    
    My.Kable(list_todo)
    
    DATA     = data.table()
    DATA_ALL = data.table()
    XLIST    = list()
    
    for (k in 1: nrow(list_todo))
    {
      # k = 1
      pCode = list_todo[k]$DNSE_PRODUCT
      CODE = list_todo[k]$PRODUCT
      CATln_Border(pCode)
      switch(frequency,
             "intraday" = {
               # DOWNLOAD_DNSE_INTRADAY_DAY_BY_CODE = function(pCode='VNINDEX', pInt = '1m', code = "INDVNINDEX")
               DATA  = CHECK_CLASS(try(DOWNLOAD_DNSE_INTRADAY_DAY_BY_CODE(pCode = pCode, pInt = '1m', code = CODE)))
               if (nrow(DATA) > 0)
               {
                 #DATA        = DBL_IND_TO_CORRECT(DATA)
                 DATA        = CLEAN_TIMESTAMP(DATA)
                 XLIST [[k]] = DATA
               }
             },
             "day" = {
               DATA  = CHECK_CLASS(try(DOWNLOAD_DNSE_INTRADAY_DAY_BY_CODE(pCode = pCode, pInt = '1d', code = CODE)))
               if (nrow(DATA) > 0)
               {
                 #DATA        = DBL_IND_TO_CORRECT(DATA)
                 DATA        = CLEAN_TIMESTAMP(DATA)
                 XLIST [[k]] = DATA
               }
             }
      )
      
    }
    switch(frequency,
           "intraday" = {
             
             DATA_ALL = rbindlist(XLIST, fill = T)
             
             My.Kable.Min(DATA_ALL[order(-date)])
             
             try(DBL_CCPR_SAVERDS(DATA_ALL, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'DBL_DNSE_INTRADAY_1MN_TODAY.rds', ToSummary = T, SaveOneDrive = T))
             
             DATA_DAY = DBL_INTEGRATE_INTRADAY_BY_TIMESTAMP ( DATA_ALL,
                                                              Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                              Save_File   = 'DBL_DNSE_INTRADAY_1MN_DAY.rds')
             
             
             DATA_HISTORY = DBL_INTEGRATE_INTRADAY_BY_TIMESTAMP ( DATA_ALL,
                                                                  Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                                  Save_File   = 'DBL_DNSE_INTRADAY_1MN_HISTORY.rds')
             DATA_LAST = unique(DATA_DAY[order(code, -timestamp_vn)], by = c("code"))
             My.Kable.All(DATA_LAST[, -c("home", "symbol", "sample", "provider", "fcat", "updated", "capiusd", "date",
                                         "isin", "source", "open", "high", "low", "type", "datetime", "iso2", "cur", "timestamp")])
           },
           "day" = {
             
             DATA_ALL = rbindlist(XLIST, fill = T)
             
             My.Kable.Min(DATA_ALL[order(-date)])
             
             try(DBL_CCPR_SAVERDS(DATA_ALL, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'DBL_DNSE_INTRADAY_DAY_TODAY.rds', ToSummary = T, SaveOneDrive = T))
             
             DATA_DAY = DBL_INTEGRATE_DAY_BY_DATE ( DATA_ALL,
                                                    File_Folder = '',
                                                    File_Name   = '',
                                                    Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                    Save_File   = 'DBL_DNSE_INTRADAY_DAY_DAY.rds')
             
             
             DATA_HISTORY = DBL_INTEGRATE_DAY_BY_DATE ( DATA_DAY,
                                                        File_Folder = '',
                                                        File_Name   = '',
                                                        Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                        Save_File   = 'DBL_DNSE_INTRADAY_DAY_HISTORY.rds')
           }
    )
  }
  DBL_DURATION(start_time)
  return (DATA_DAY)
}

# ==================================================================================================
DBL_INTEGRATE_INTRADAY_BY_TIMESTAMP = function( pData,
                                                Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                Save_File   = 'DBL_YAH_INTRADAY_1MN_DAY.rds') {
  # ------------------------------------------------------------------------------------------------
  # pData = DATA_ALL
  DATA_OLD = CHECK_CLASS(try(DBL_CCPR_READRDS(Save_Folder, Save_File, ToKable = T)))
  DATA_OLD = rbind(DATA_OLD, pData, fill = T)
  DATA_OLD = unique(DATA_OLD, by = c('code', 'date', 'timestamp'), fromLast = T)
  
  if(nrow(DATA_OLD) > 0)
  {
    My.Kable.Min(DATA_OLD)
    try(DBL_CCPR_SAVERDS(DATA_OLD, Save_Folder, Save_File, ToSummary = T, SaveOneDrive = T))
  }
  return (DATA_OLD)
  
}

# ==================================================================================================
DBL_INTEGRATE_DAY_BY_DATE = function( pData,
                                      File_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                      File_Name   = 'DBL_YAH_INTRADAY_DAY_HISTORY.rds',
                                      Save_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                      Save_File   = 'DBL_SOURCE_INTRADAY_DAY_HISTORY.rds') {
  # ------------------------------------------------------------------------------------------------
  # pData = data.table()
  if (nrow(pData)==0)
  {
    pData = CHECK_CLASS(try(DBL_CCPR_READRDS(File_Folder, File_Name, ToKable = T)))
  }
  if (nrow(pData)>0)
  {
    DATA_OLD = CHECK_CLASS(try(DBL_CCPR_READRDS(Save_Folder, Save_File, ToKable = T)))
    DATA_OLD = rbind(DATA_OLD, pData, fill = T)
    DATA_OLD = unique(DATA_OLD, by = c('code', 'date'), fromLast = T)
  }
  
  if (nrow(DATA_OLD) > 0)
  {
    My.Kable.Min(DATA_OLD)
    try(DBL_CCPR_SAVERDS(DATA_OLD, Save_Folder, Save_File, ToSummary = T, SaveOneDrive = T))
  }
  return (DATA_OLD)
}

# ==================================================================================================
DOWNLOAD_YAH_INTRADAY_DAY_BY_CODE = function(pCode='BTC-USD', pInt = '1m') {
  # ------------------------------------------------------------------------------------------------
  # pInt = '1d'
  index_yah_final = data.table()
  if (pInt == "1d")
  {
    index_yah_day = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_OHLC_INTRADAY( pCodesource = pCode, pInterval = '1d', Hour_adjust = -5)))
    My.Kable(index_yah_day)
    if (nrow(index_yah_day)>0)
    {
      index_yah_day = index_yah_day[order(date)]
      index_yah_day[, pclose := shift(close)]
      index_yah_day[, varpc := 100*((close/shift(close))-1)]
      My.Kable(index_yah_day[, .(code, date, timestamp, timestamp_vn, close, varpc)])
      index_yah_final = index_yah_day
    }
    
  } else 
  {
    index_yah_intraday = data.table()
    index_yah_day = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_OHLC_INTRADAY( pCodesource = pCode, pInterval = '1d', Hour_adjust = -5)))
    My.Kable(index_yah_day)
    if (nrow(index_yah_day)>0)
    {
      index_yah_day = index_yah_day[order(date)]
      index_yah_day[, pclose := shift(close)]
      index_yah_day[, varpc := 100*((close/shift(close))-1)]
      My.Kable(index_yah_day[, .(code, date, timestamp, timestamp_vn, close, varpc)])
      
      # index_yah_intraday = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_OHLC_INTRADAY( pCodesource = yCode, pInterval = '1m', Hour_adjust = -5))) #1,563 records
      index_yah_intraday = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_OHLC_INTRADAY( pCodesource = pCode, pInterval = pInt, Hour_adjust = -5))) #1,563 records
      My.Kable(index_yah_intraday)
      if (nrow(index_yah_intraday)>0)
      {
        index_yah_intraday = index_yah_intraday[order(date, timestamp)]
        index_yah_intraday = merge(index_yah_intraday, index_yah_day[, .(date, pclose)], all.x=T, by='date')
        index_yah_intraday[, varpc := 100*((close/pclose)-1)]
        DBL_RELOAD_INSREF()
        index_yah_intraday = merge(index_yah_intraday[, -c('code')], ins_ref[!is.na(yah)][, .(codesource=yah, code)], all.x=T, by='codesource')
        My.Kable.TB(index_yah_intraday[, .(codesource, code, date, timestamp, timestamp_vn, close, pclose, varpc)])
        index_yah_final = index_yah_intraday
      }
    }
  }
  return(index_yah_final)
}

# ==================================================================================================
DOWNLOAD_DNSE_INTRADAY_DAY_BY_CODE = function(pCode='VNINDEX', pInt = '1m', code = "INDVNINDEX") {
  # ------------------------------------------------------------------------------------------------
  # pInt = '1d'
  index_dnse_final = data.table()
  if (pInt == "1d")
  {
    pType = substr(code, 1, 3)
    switch ( pType, 
             "IND" = {
               xType = "index"
             },
             "STK" = {
               xType = "stock"
             })
    index_dnse_day = CHECK_CLASS(try(DOWNLOAD_ENTRADE_INDEX_INTRADAY(type = xType, codesource = pCode, code = code,
                                                                     NbMinutes   = 30*24*60, OffsetDays  = 0,
                                                                     Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = '',
                                                                     Save_history = '')))
    index_dnse_day = DBL_IND_CALCULATE_RT_VAR_CHANGE(index_dnse_day)
    My.Kable(index_dnse_day)
    if (nrow(index_dnse_day)>0)
    {
      index_dnse_day = index_dnse_day[order(date)]
      index_dnse_day[, pclose := shift(close)]
      index_dnse_day[, varpc := 100*((close/shift(close))-1)]
      My.Kable(index_dnse_day[, .(code, date, timestamp, timestamp_vn, close, varpc)])
      index_dnse_final = index_dnse_day
    }
    
  } else 
  {
    pType = substr(code, 1, 3)
    switch ( pType, 
             "IND" = {
               xType = "index"
             },
             "STK" = {
               xType = "stock"
             })
    index_dnse_day = CHECK_CLASS(try(DOWNLOAD_ENTRADE_INDEX_DAY(type = xType, codesource = pCode, code = code,
                                                                NbMinutes   = 30*24*60, OffsetDays  = 0,
                                                                Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = '',
                                                                Save_history = '')))
    index_dnse_day = DBL_IND_CALCULATE_RT_VAR_CHANGE(index_dnse_day)
    My.Kable(index_dnse_day)
    if (nrow(index_dnse_day)>0)
    {
      index_dnse_day = index_dnse_day[order(date)]
      index_dnse_day[, pclose := shift(close)]
      index_dnse_day[, varpc := 100*((close/shift(close))-1)]
      My.Kable(index_dnse_day[, .(code, date, close, varpc)])
      index_dnse_final = index_dnse_day
    }
    
    index_dnse_intraday = data.table()
    # index_dnse_day = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_OHLC_INTRADAY( pCodesource = pCode, pInterval = '1d', Hour_adjust = -5)))
    # My.Kable(index_yah_day)
    
    # index_yah_intraday = CHECK_CLASS(try(DBL_DOWNLOAD_YAH_OHLC_INTRADAY( pCodesource = yCode, pInterval = '1m', Hour_adjust = -5))) #1,563 records
    index_dnse_intraday = CHECK_CLASS(try(DOWNLOAD_ENTRADE_INDEX_INTRADAY(type = xType, codesource = pCode, code = code,
                                                                          NbMinutes   = 30*24*60, OffsetDays  = 0,
                                                                          Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = '',
                                                                          Save_history = '')))
    # DBL_DOWNLOAD_YAH_OHLC_INTRADAY( pCodesource = pCode, pInterval = pInt, Hour_adjust = -5))) #1,563 records
    My.Kable(index_dnse_intraday)
    if (nrow(index_dnse_intraday)>0)
    {
      index_dnse_intraday = index_dnse_intraday[order(date, timestamp)]
      index_dnse_intraday = merge(index_dnse_intraday, index_dnse_day[, .(date, pclose)], all.x=T, by='date')
      index_dnse_intraday[, varpc := 100*((close/pclose)-1)]
      DBL_RELOAD_INSREF()
      # index_dnse_intraday = merge(index_dnse_intraday[, -c('code')], ins_ref[!is.na(yah)][, .(codesource=yah, code)], all.x=T, by='codesource')
      My.Kable.TB(index_dnse_intraday[, .(codesource, code, date, timestamp, timestamp_vn, close, pclose, varpc)])
      index_dnse_final = index_dnse_intraday
    }
  }
  
  
  return(index_dnse_final)
}

# ==================================================================================================
DBL_REPAIR_INSREF = function(EveryHours = 3) {
  # ------------------------------------------------------------------------------------------------
  x = TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DBL_REPAIR_INSREF', pAction="COMPARE", NbSeconds=EveryHours*60*60, ToPrint=T)
  ins.ref = DBL_CCPR_READRDS(UData, 'efrc_ins_ref.rds', ToKable = T, ToRestore = T)
  if (nrow (ins.ref) >0)
  {
    ins_ref = copy(ins.ref)
    assign("ins_ref", ins_ref, envir = .GlobalEnv)
  } else {
    
    
  }
  if (nrow (ins_ref) >0)
  {
    DBL_CCPR_SAVERDS(ins_ref, UData, 'efrc_ins_ref.rds', ToSummary = T, SaveOneDrive = T)
    DBL_CCPR_SAVERDS(ins_ref, SData, 'efrc_ins_ref.rds', ToSummary = T, SaveOneDrive = T)
    DBL_CCPR_SAVERDS(ins_ref, SData, 'dbl_ins_ref.rds', ToSummary = T, SaveOneDrive = T)
    DBL_CCPR_SAVERDS(ins_ref, SData, paste0('dbl_ins_ref_', gsub('-','', Sys.Date()), '.rds'), ToSummary = T, SaveOneDrive = T)
  }
  
  x = TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DBL_REPAIR_INSREF', pAction="SAVE", NbSeconds=EveryHours*60*60, ToPrint=T)
  
  # DBL_RELOAD_INSREF(ToForce = T)
  # if (exists('ins_ref'))
  # {
  #   
  # }
}


