# ==================================================================================================
# UDATA_LIBRARY.R 2024-06-10 14:45
# ==================================================================================================

UDATA_EXECUTE_LOOP = function()
{
  try(UDATA_EXECUTE(Action = 'HSX_HNX_UPC_STKVN_REF'))
  try(UDATA_EXECUTE(Action = 'LAST_SHARES_TO_UDATA'))
  
  try(UDATA_EXECUTE(Action = 'HSX_HNX_UPC_STKVN_SHARES_DAY_HISTORY'))
  try(UDATA_EXECUTE(Action = 'HSX_HNX_UPC_STKVN_PRICES_DAY_HISTORY'))
  try(UDATA_EXECUTE(Action = 'SOURCES_STKVN_LIQUIDITY_DAY_HISTORY'))
  try(UDATA_EXECUTE(Action = 'MISC'))
  try(UDATA_EXECUTE(Action = 'MLT'))
  try(UDATA_EXECUTE(Action = 'SOURCES_STKVN_INTRADAY_TODAY_HISTORY'))
  
  try(UDATA_EXECUTE(Action = 'PRICESBOARD_TO_UDATA_INTRADAY_AND_HISTORY'))
  try(UDATA_EXECUTE(Action = 'BAS_HISTORY'))
  try(UDATA_EXECUTE(Action = 'CURHOME'))
  
}

# ==================================================================================================
PRICESBOARD_TO_UDATA_INTRADAY_AND_HISTORY = function(pSource = 'CAF', ToIntegrate = T) {
  # ------------------------------------------------------------------------------------------------
  
  LastTrading      = CCPR_LAST_TRADING_DAY_VN(max(9, as.numeric(substr(Sys.time(),12,13)), ToPrompt = T))
  File_PricesBoard = paste0('DOWNLOAD_', pSource, '_PRICESBOARD_', gsub('-','', LastTrading), '.rds')
  CATln_Border(File_PricesBoard)
  Data_PricesBoard = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', File_PricesBoard)))
  My.Kable(Data_PricesBoard)
  Check_Data = Data_PricesBoard[!is.na(last) & date == LastTrading]
  
  if (nrow(Check_Data)>0)
  {
    Data_PricesBoard[, timestamp:=updated]
    Data_PricesBoard[, datetime:=updated]
    Max_Datetime = paste(LastTrading, '15:45:00')
    Data_PricesBoard[timestamp>Max_Datetime, timestamp:=Max_Datetime]
    Data_PricesBoard[datetime>Max_Datetime, datetime:=Max_Datetime]
    
    File_Prices  = paste0('DOWNLOAD_', pSource, '_STKVN_INTRADAY_DAY', '.rds')
    CATln_Border(File_Prices)
    Data_Prices = CHECK_CLASS(try(CCPR_READRDS(UData, File_Prices)))
    My.Kable(Data_Prices)
    
    if (nrow(Data_Prices)>0)
    {
      if ('last' %in% names(Data_Prices)) { Data_Prices[, close:=last] }
      Data_Prices = Data_Prices[!is.na(last) & date == LastTrading]
      Data_Prices = rbind(Data_Prices, Data_PricesBoard[date==LastTrading], fill=T)
      Data_Prices = unique(Data_Prices, by=c('code', 'date', 'timestamp'), fromLast=T)
      Data_Prices[, close:=last]
      Data_Prices[is.na(close), close:=reference]
      Data_Prices[is.na(volume), volume:=0]
      Data_Prices[, rt:=(close/reference)-1]
      Data_Prices = UPDATE_UPDATED(Data_Prices)
      My.Kable.Min(Data_Prices)
      
      if (nrow(Data_Prices)>0)
      {
        Data_Prices = MERGE_DATASTD_BYCODE(Data_Prices, pOption='FINAL')
        try(CCPR_SAVERDS(Data_Prices, UData, File_Prices, ToKable = T, SaveOneDrive = T))
        
        # 
        if (ToIntegrate)
        {
          File_HistoryBoard = paste0('DOWNLOAD_', pSource, '_STKVN_INTRADAY_HISTORY.rds')
          CATln_Border(File_HistoryBoard)
          Data_HistoryBoard = CHECK_CLASS(try(CCPR_READRDS(UData, File_HistoryBoard)))
          My.Kable.Min(Data_HistoryBoard)
          Data_HistoryBoard = rbind(Data_HistoryBoard, Data_Prices, fill=T)
          Data_HistoryBoard = unique(Data_HistoryBoard, by=c('code', 'date', 'timestamp'), fromLast=T)
          Data_HistoryBoard = MERGE_DATASTD_BYCODE(Data_HistoryBoard, pOption='FINAL')
          # Data_Prices[, close:=last]
          # Data_Prices[is.na(close), close:=reference]
          # Data_Prices[is.na(volume), volume:=0]
          # Data_Prices[, rt:=(close/reference)-1]
          Data_HistoryBoard = UPDATE_UPDATED(Data_HistoryBoard)
          My.Kable.Min(Data_HistoryBoard[order(date, datetime)])
          try(CCPR_SAVERDS(Data_HistoryBoard, UData, File_HistoryBoard, ToSummary = T, SaveOneDrive = T))
        }
      }
    }
  }
  Monitor = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0('PRICESBOARD_TO_UDATA_INTRADAY_AND_HISTORY >', pSource), pAction="SAVE", NbSeconds=1, ToPrint=F))
}

UDATA_EXECUTE = function(Action = 'HSX_HNX_UPC_STKVN_REF')
{
  switch (Action,
          
          'DOWNLOAD_SOURCE_INTRADAY' = {
            try(DEMO_INTRADAY_BY_SOURCE(pSource='BLG'))
            try(DEMO_INTRADAY_BY_SOURCE(pSource='YAH'))
            try(DEMO_INTRADAY_BY_SOURCE(pSource='RTS'))
            try(DEMO_INTRADAY_BY_SOURCE(pSource='GOG'))
            try(DEMO_INTRADAY_BY_SOURCE(pSource='BIN'))
          },
          
          'CURHOME'     = {
            # BLG, CURHOME
            try(MELI_MELO_SOURCE(pSource='BLG', pPrefix = 'CURHOME', CheckType='CUR',     Nb_Max = 10, NbDaysBack=10, pMethod='LONG', pSleep=1))
            try(IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData, 
                                                       File_Fr = 'DOWNLOAD_BLG_CURHOME_HISTORY.rds',
                                                       File_To = 'DOWNLOAD_BLG_CUR_HISTORY.rds',
                                                       RemoveNA = T, NbDays = 0, pCondition = 'type=="CUR"', MinPercent = 0, ToForce = T))
            
            try(MELI_MELO_SOURCE(pSource='INV', pPrefix = 'CURHOME', CheckType='CUR',     Nb_Max = 10, NbDaysBack=10, pMethod='LONG', pSleep=1))
            try(IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData, 
                                                       File_Fr = 'DOWNLOAD_INV_CURHOME_HISTORY.rds',
                                                       File_To = 'DOWNLOAD_INV_CUR_HISTORY.rds',
                                                       RemoveNA = T, NbDays = 0, pCondition = 'type=="CUR"', MinPercent = 0, ToForce = T))           
            
            try(MELI_MELO_SOURCE(pSource='YAH', pPrefix = 'CURHOME', CheckType='CUR',     Nb_Max = 10, NbDaysBack=10, pMethod='LONG', pSleep=1))
            try(IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData, 
                                                       File_Fr = 'DOWNLOAD_YAH_CURHOME_HISTORY.rds',
                                                       File_To = 'DOWNLOAD_YAH_CUR_HISTORY.rds',
                                                       RemoveNA = T, NbDays = 0, pCondition = 'type=="CUR"', MinPercent = 0, ToForce = T))           
            
            try(MELI_MELO_SOURCE(pSource='RTS', pPrefix = 'CURHOME', CheckType='CUR',     Nb_Max = 10, NbDaysBack=10, pMethod='LONG', pSleep=1))
            try(IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData, 
                                                       File_Fr = 'DOWNLOAD_RTS_CURHOME_HISTORY.rds',
                                                       File_To = 'DOWNLOAD_RTS_CUR_HISTORY.rds',
                                                       RemoveNA = T, NbDays = 0, pCondition = 'type=="CUR"', MinPercent = 0, ToForce = T))           
            
            try(MELI_MELO_SOURCE(pSource='GOG', pPrefix = 'CURHOME', CheckType='CUR',     Nb_Max = 10, NbDaysBack=10, pMethod='LONG', pSleep=1))
            try(IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData, 
                                                       File_Fr = 'DOWNLOAD_GOG_CURHOME_HISTORY.rds',
                                                       File_To = 'DOWNLOAD_GOG_CUR_HISTORY.rds',
                                                       RemoveNA = T, NbDays = 0, pCondition = 'type=="CUR"', MinPercent = 0, ToForce = T))
            
          },
          
          'BAS_HISTORY' = {
            # ALL, BAS
            for (pSource in sample(list('GOG', 'RTS', 'INV', 'BIN', 'BLG', 'NSD', 'MWH', 'EXC', 'PRV')))
            {
              try(CCPR_QUICK_INTEGRATION_DAY_FINAL(pFolder = UData, pData=data.table(),
                                                   File_Fr = paste0('DOWNLOAD_', pSource, '_CUR_HISTORY.rds'),
                                                   File_To = paste0('DOWNLOAD_', pSource, '_BAS_HISTORY.rds'),
                                                   pCondition='!is.na(close) & type !="STK" & type!="IND" & nchar(type)==3', 
                                                   IncludedOnly=F, CheckDateSummary=F, MaxDateOnly=F, SaveOneDrive=T, Merge_STD=F))
            }
            
            # INV, BAS
            try(CCPR_QUICK_INTEGRATION_DAY_FINAL(pFolder = UData, pData=data.table(),
                                                 File_Fr = 'DOWNLOAD_INV_BND_HISTORY.rds',
                                                 File_To = 'DOWNLOAD_IND_BAS_HISTORY.rds',
                                                 pCondition='!is.na(close) & type !="STK" & type!="IND" & nchar(type)==3',
                                                 IncludedOnly=F, CheckDateSummary=F, MaxDateOnly=F, SaveOneDrive=T, Merge_STD=F))
            
            # # BIN, BAS
            # try(CCPR_QUICK_INTEGRATION_DAY_FINAL(pFolder = UData, pData=data.table(),
            #                                      File_Fr = 'DOWNLOAD_BIN_CUR_HISTORY.rds',
            #                                      File_To = 'DOWNLOAD_BIN_BAS_HISTORY.rds',
            #                                      pCondition='!is.na(close) & type !="STK" & nchar(type)==3', IncludedOnly=F, CheckDateSummary=F, MaxDateOnly=F, SaveOneDrive=T, Merge_STD=F))
            
          },
          
          'PRICESBOARD_TO_UDATA_INTRADAY_AND_HISTORY' = {
          try(PRICESBOARD_TO_UDATA_INTRADAY_AND_HISTORY(pSource = 'HSX', ToIntegrate = T))
          try(PRICESBOARD_TO_UDATA_INTRADAY_AND_HISTORY(pSource = 'C68', ToIntegrate = T))
          try(PRICESBOARD_TO_UDATA_INTRADAY_AND_HISTORY(pSource = 'VND', ToIntegrate = T))
          try(PRICESBOARD_TO_UDATA_INTRADAY_AND_HISTORY(pSource = 'HNX', ToIntegrate = T))
          try(PRICESBOARD_TO_UDATA_INTRADAY_AND_HISTORY(pSource = 'UPC', ToIntegrate = T))
          },
          
          'LAST_SHARES_TO_UDATA' = {
            try(LAST_SHARES_TO_UDATA(pSource = 'C68', FieldCheck = 'sharesout', ToCheckDate = T, IntegrateHistory = T))
            try(LAST_SHARES_TO_UDATA(pSource = 'STB', FieldCheck = 'sharesout', ToCheckDate = T, IntegrateHistory = T))
            try(LAST_SHARES_TO_UDATA(pSource = 'CAF', FieldCheck = 'sharesout', ToCheckDate = T, IntegrateHistory = T))
          },
          
          'HSX_HNX_UPC_STKVN_REF' = {
            
            # ................................................................................................
            pOption = "download_hsx_stkvn_ref.rds" ; pHH = 8  ; ToCheckDate = T ### OK
            # ................................................................................................
            DATACENTER_LINE_BORDER(paste(pOption, ": Executing ..."))
            if (SUMMARY_DATE_RDS_UDATA(pOption) < SYSDATETIME(pHH) || !ToCheckDate)
            {
              hsx_ref = try(DOWNLOAD_HSX_STKVN_REF())                         # download_hsx_stkvn_ref.rds and summary.txt
              My.Kable.MaxCols(hsx_ref)
              IFRC_RELEASE_MEMORY("",F)
            } else { DATACENTER_LINE_BORDER(paste(pOption, ": DATA ALREADY UPDATED, Before", pHH)) ; CATln('')}
            
            
            # ..................................................................................................
            pOption = "download_hnx_stkvn_ref.rds" ; pHH = 9 ; ToCheckDate = F ### OK
            # ..................................................................................................
            DATACENTER_LINE_BORDER(paste(pOption, ": Executing ..."))
            if (SUMMARY_DATE_RDS_UDATA(pOption) < SYSDATETIME(pHH) || !ToCheckDate)
            {
              hnx_ref = try(TRAINEE_DOWNLOAD_HNX_STKVN_REF (SaveFolder="U:/EFRC/DATA/STKVN/DAY/REF/EXC/", nrl=30, ToIntegrateHistory=T))  # download_hnx_stkvn_ref.rds and summary.txt
              My.Kable(hnx_ref)
            } else { CATln_Border(paste('TRAINEE_DOWNLOAD_HNX_STKVN_REF =', 'ALREADY UPDATED'))}
            
            # ................................................................................................
            pOption = "download_upc_stkvn_ref.rds" ; pHH = 9 ### OK
            # ................................................................................................
            DATACENTER_LINE_BORDER(paste(pOption, ": Executing ..."))
            if (SUMMARY_DATE_RDS_UDATA(pOption) < SYSDATETIME(pHH) || !ToCheckDate)
            {
              upc_ref = try(TRAINEE_DOWNLOAD_UPC_STKVN_REF (SaveFolder="U:/EFRC/DATA/STKVN/DAY/REF/EXC/", nrl=30, ToIntegrateHistory=T))  # download_hnx_stkvn_ref.rds and summary.txt
              My.Kable(upc_ref)
            } else { CATln_Border(paste('TRAINEE_DOWNLOAD_UPC_STKVN_REF =', 'ALREADY UPDATED'))}
            
          },
          
          'HSX_HNX_UPC_STKVN_SHARES_DAY_HISTORY' = {
            ForceProba = 90
            pSource = 'HSX'
            for (pSource in list('HSX', 'HNX', 'UPC', 'EXC'))
            {
              x = try(UPDATE_DOWNLOAD_PREFIX_HISTORY(UData, 
                                                     FileNamePrice   = paste0('DOWNLOAD_', pSource, '_STKVN_REF.rds'),
                                                     FileNameHistory = paste0('DOWNLOAD_', pSource, '_STKVN_SHARES_HISTORY.rds'),
                                                     CheckField='sharesout', ToForce=(runif(1,1,100)>ForceProba)))
              try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                                 File_Fr     = paste0('DOWNLOAD_', pSource, '_STKVN_SHARES_HISTORY.rds'),
                                                                 File_To     = paste0('DOWNLOAD_', pSource, '_STKVN_SHARES_DAY.rds'),
                                                                 pCondition  = '',
                                                                 RemoveNA    = T, NbDays = 1, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
              
            }
            
            ForceProba = 90
            for (pSource in list('HSX', 'HNX', 'UPC', 'EXC'))
            {
              try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                                 File_Fr     = paste0('DOWNLOAD_', pSource, '_STKVN_REF.rds'),
                                                                 File_To     = paste0('DOWNLOAD_', pSource, '_STKVN_REF_HISTORY.rds'),
                                                                 pCondition  = '',
                                                                 RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
              
            }
          },
          
          'HSX_HNX_UPC_STKVN_PRICES_DAY_HISTORY' = {
            try(PRICESBOARD_TO_UDATA_PRICES_AND_HISTORY(pSource = 'HSX', ToIntegrate = T))
            try(PRICESBOARD_TO_UDATA_PRICES_AND_HISTORY(pSource = 'HNX', ToIntegrate = T))
            try(PRICESBOARD_TO_UDATA_PRICES_AND_HISTORY(pSource = 'UPC', ToIntegrate = T))
            
            # PRICES ...........................................................................................
            xOption    = 'TRAINEE_DEV_UDATA > PRICES'
            Minutes    = 5
            ForceProba = 50
            if (TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=xOption, pAction="COMPARE", NbSeconds=Minutes*60, ToPrint=F))
            {
              ForceProba = 90
              if (CHECK_TIMEBETWEEN('09:30' , '15:15') & wday(Sys.Date()) %in% c(2:6) )
              { 
                for (pSource in list('HSX', 'HNX', 'UPC', 'EXC', 'CAF', 'C68', 'VND'))
                {
                  try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                                     File_Fr     = paste0('DOWNLOAD_', pSource, '_STKVN_HISTORY.rds'),
                                                                     File_To     = paste0('DOWNLOAD_', pSource, '_STKVN_PRICES.rds'),
                                                                     pCondition  = '',
                                                                     RemoveNA    = T, NbDays = 1, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
                  
                }
              }
              try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=xOption, pAction="SAVE", NbSeconds=1, ToPrint=F))
            } else { CATln_Border(paste(xOption, 'ALREADY UPDATED'))}
          },
          
          'SOURCES_STKVN_LIQUIDITY_DAY_HISTORY' = {
            # LIQUIDITY ........................................................................................
            xOption    = 'TRAINEE_DEV_UDATA > LIQUIDITY'
            Minutes    = 0
            ForceProba = 50
            if (TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=xOption, pAction="COMPARE", NbSeconds=Minutes*60, ToPrint=F))
            {
              
              if (CHECK_TIMEBETWEEN('09:30' , '16:30') & wday(Sys.Date()) %in% c(2:6) )
              { 
                for (pSource in list('HSX', 'HNX', 'UPC', 'EXC', 'CAF', 'VND', 'C68'))
                {
                  x = try(UPDATE_DOWNLOAD_PREFIX_HISTORY(UData, 
                                                         FileNamePrice   = paste0('DOWNLOAD_', pSource, '_STKVN_PRICES.rds'),
                                                         FileNameHistory = paste0('DOWNLOAD_', pSource, '_STKVN_LIQUIDITY_HISTORY.rds'),
                                                         CheckField='volume', ToForce=(runif(1,1,100)>ForceProba)))
                  try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                                     File_Fr     = paste0('DOWNLOAD_', pSource, '_STKVN_LIQUIDITY_HISTORY.rds'),
                                                                     File_To     = paste0('DOWNLOAD_', pSource, '_STKVN_LIQUIDITY.rds'),
                                                                     pCondition  = '',
                                                                     RemoveNA    = T, NbDays = 1, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
                  
                }
              }
              try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=xOption, pAction="SAVE", NbSeconds=1, ToPrint=F))
            } else { CATln_Border(paste(xOption, 'ALREADY UPDATED'))}
          },
          'MISC' = {
            # ..................................................................................................
            pOption = "download_stb_stkvn_ref.rds" ; pHH = 9 ; ToCheckDate = T
            # ..................................................................................................
            xOption    = toupper(pOption)
            Minutes    = 0
            ForceProba = 50
            DATACENTER_LINE_BORDER(paste(pOption, ": Executing ..."))
            if (SUMMARY_DATE_RDS_UDATA(pOption) < SYSDATETIME(pHH) || !ToCheckDate)
            {
              try(IFRC_CCPR_UPDATE_STB_STKVN_REF())
            } else { DATACENTER_LINE_BORDER(paste(pOption, ": DATA ALREADY UPDATED, Before", pHH)) ; CATln('')}
            
            # CAF, PRICESBOARD .................................................................................
            x = CCPR_READRDS('S:/STKVN/PRICES/DAY/', 'download_caf_pricesboard_day.rds', ToKable = T, ToRestore = T)
            x = UPDATE_UPDATED(x)
            My.Kable(x)
            if ('market' %in% names(x)) { CCPR_SAVERDS(x, UData, 'DOWNLOAD_CAF_STKVN_REF.rds', ToSummary=T, SaveOneDrive = T) }
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                               File_Fr     = 'DOWNLOAD_CAF_STKVN_REF.rds',
                                                               File_To     = 'DOWNLOAD_CAF_STKVN_REF_HISTORY.rds',
                                                               pCondition  = '',
                                                               RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
            if ('volume' %in% names(x)) { CCPR_SAVERDS(x, UData, 'DOWNLOAD_CAF_STKVN_LIQUIDITY.rds', ToSummary=T, SaveOneDrive = T) }
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                               File_Fr     = 'DOWNLOAD_CAF_STKVN_LIQUIDITY.rds',
                                                               File_To     = 'DOWNLOAD_CAF_STKVN_LIQUIDITY_HISTORY.rds',
                                                               pCondition  = '',
                                                               RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
            
            # c68, PRICESBOARD .................................................................................
            x = CCPR_READRDS('S:/STKVN/PRICES/DAY/', 'download_C68_pricesboard_day.rds', ToKable = T, ToRestore = T)
            x = UPDATE_UPDATED(x)
            My.Kable(x)
            if ('market' %in% names(x)) { CCPR_SAVERDS(x, UData, 'DOWNLOAD_C68_STKVN_REF.rds', ToSummary=T, SaveOneDrive = T) }
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                               File_Fr     = 'DOWNLOAD_C68_STKVN_REF.rds',
                                                               File_To     = 'DOWNLOAD_C68_STKVN_REF_HISTORY.rds',
                                                               pCondition  = '',
                                                               RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
            if ('volume' %in% names(x)) { CCPR_SAVERDS(x, UData, 'DOWNLOAD_C68_STKVN_LIQUIDITY.rds', ToSummary=T, SaveOneDrive = T) }
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                               File_Fr     = 'DOWNLOAD_C68_STKVN_LIQUIDITY.rds',
                                                               File_To     = 'DOWNLOAD_C68_STKVN_LIQUIDITY_HISTORY.rds',
                                                               pCondition  = '',
                                                               RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
            
            
            # VND, PRICESBOARD .................................................................................
            x = CCPR_READRDS('S:/STKVN/PRICES/DAY/', 'download_VND_pricesboard_day.rds', ToKable = T, ToRestore = T)
            x = UPDATE_UPDATED(x)
            My.Kable(x)
            if ('market' %in% names(x)) { CCPR_SAVERDS(x, UData, 'DOWNLOAD_VND_STKVN_REF.rds', ToSummary=T, SaveOneDrive = T) }
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                               File_Fr     = 'DOWNLOAD_VND_STKVN_REF.rds',
                                                               File_To     = 'DOWNLOAD_VND_STKVN_REF_HISTORY.rds',
                                                               pCondition  = '',
                                                               RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
            if ('last' %in% names(x)) { CCPR_SAVERDS(x, UData, 'DOWNLOAD_VND_STKVN_PRICES.rds', ToSummary=T, SaveOneDrive = T) }
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                               File_Fr     = 'DOWNLOAD_VND_STKVN_PRICES.rds',
                                                               File_To     = 'DOWNLOAD_VND_STKVN_HISTORY.rds',
                                                               pCondition  = '',
                                                               RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
            
            if ('volume' %in% names(x)) { CCPR_SAVERDS(x, UData, 'DOWNLOAD_VND_STKVN_LIQUIDITY.rds', ToSummary=T, SaveOneDrive = T) }
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                               File_Fr     = 'DOWNLOAD_VND_STKVN_LIQUIDITY.rds',
                                                               File_To     = 'DOWNLOAD_VND_STKVN_LIQUIDITY_HISTORY.rds',
                                                               pCondition  = '',
                                                               RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
            
            
            x = CCPR_READRDS('S:/STKVN/PRICES/DAY/', 'download_VND_pricesboard_day.rds', ToKable = T, ToRestore = T)
            x = UPDATE_UPDATED(x)
            My.Kable(x)
            if ('market' %in% names(x)) { CCPR_SAVERDS(x, UData, 'DOWNLOAD_VND_STKVN_REF.rds', ToSummary=T, SaveOneDrive = T) }
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                               File_Fr     = 'DOWNLOAD_VND_STKVN_REF.rds',
                                                               File_To     = 'DOWNLOAD_VND_STKVN_REF_HISTORY.rds',
                                                               pCondition  = '',
                                                               RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
            if ('last' %in% names(x)) { CCPR_SAVERDS(x, UData, 'DOWNLOAD_VND_STKVN_PRICES.rds', ToSummary=T, SaveOneDrive = T) }
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                               File_Fr     = 'DOWNLOAD_VND_STKVN_PRICES.rds',
                                                               File_To     = 'DOWNLOAD_VND_STKVN_HISTORY.rds',
                                                               pCondition  = '',
                                                               RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
            # --------------------------------------------------------------------------------------------------
            pSource    = 'UPC'
            ForceProba = 50
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                               File_Fr     = paste0('DOWNLOAD_', pSource, '_STKVN_LIQUIDITY_HISTORY.rds'),
                                                               File_To     = paste0('DOWNLOAD_', pSource, '_STKVN_LIQUIDITY.rds'),
                                                               pCondition  = '',
                                                               RemoveNA    = T, NbDays = 1, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
            pSource    = 'UPC'
            ForceProba = 50
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY(File_Folder = UData,
                                                               File_Fr     = paste0('DOWNLOAD_', pSource, '_STKVN_LIQUIDITY.rds'),
                                                               File_To     = paste0('DOWNLOAD_', pSource, '_STKVN_LIQUIDITY_HISTORY.rds'),
                                                               pCondition  = '',
                                                               RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>ForceProba)))
          },
          'MLT' = {
            # MLT, STKVN, SHARES ...............................................................................  
            xOption    = 'TRAINEE_DEV_UDATA > MLT_SHARES'
            Minutes    = 30
            ForceProba = 50
            if (TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=xOption, pAction="COMPARE", NbSeconds=Minutes*60, ToPrint=F))
            {
              
              xf = try(MHM_DATACENTER_INTEGRATION_CODE_DATE_SOURCE_BYLIST(p_folder=UData, p_datatable=data.table(), 
                                                                          list_filefrom=list(
                                                                            "download_hsx_stkvn_shares_day.rds", 
                                                                            "download_hnx_stkvn_shares_day.rds", 
                                                                            "download_upc_stkvn_shares_day.rds" 
                                                                            
                                                                            # "download_vst_stkvn_ref.rds"
                                                                          ), 
                                                                          p_fileto    = "download_exc_stkvn_shares_day.rds", 
                                                                          IntegrateTo = "download_mlt_stkvn_shares_day.rds", 
                                                                          MaxDate     = T))
              
              xf = try(MHM_DATACENTER_INTEGRATION_CODE_DATE_SOURCE_BYLIST(p_folder=UData, p_datatable=data.table(), 
                                                                          list_filefrom=list(
                                                                            "download_caf_stkvn_shares_day.rds", 
                                                                            "download_c68_stkvn_shares_day.rds", 
                                                                            "download_vst_stkvn_shares_day.rds", 
                                                                            "download_stb_stkvn_shares_day.rds", 
                                                                            "download_hsx_stkvn_shares_day.rds", 
                                                                            "download_hnx_stkvn_shares_day.rds", 
                                                                            "download_upc_stkvn_shares_day.rds" 
                                                                            
                                                                            # "download_vst_stkvn_ref.rds"
                                                                          ), 
                                                                          p_fileto    = "", 
                                                                          IntegrateTo = "download_mlt_stkvn_shares_day.rds", 
                                                                          MaxDate     = T))
              
              xf = try(MHM_DATACENTER_INTEGRATION_CODE_DATE_SOURCE_BYLIST(p_folder=UData, p_datatable=data.table(), 
                                                                          list_filefrom=list(
                                                                            "download_mlt_stkvn_shares_day.rds" 
                                                                          ), 
                                                                          p_fileto    = "download_mlt_stkvn_shares_history.rds", 
                                                                          IntegrateTo = "download_mlt_stkvn_shares_history.rds", 
                                                                          MaxDate     = T))
              try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=xOption, pAction="SAVE", NbSeconds=1, ToPrint=F))
            } else { CATln_Border(paste(xOption, 'ALREADY UPDATED'))}
            
            xf = try(MHM_DATACENTER_INTEGRATION_CODE_DATE_SOURCE_BYLIST(p_folder=UData, p_datatable=data.table(), 
                                                                        list_filefrom=list(
                                                                          "download_hsx_stkvn_liquidity.rds", 
                                                                          "download_hnx_stkvn_liquidity.rds", 
                                                                          "download_upc_stkvn_liquidity.rds" 
                                                                          
                                                                          # "download_vst_stkvn_ref.rds"
                                                                        ), 
                                                                        p_fileto    = "download_exc_stkvn_liquidity.rds", 
                                                                        IntegrateTo = "download_mlt_stkvn_liquidity.rds", 
                                                                        MaxDate     = T))
            
            xf = try(MHM_DATACENTER_INTEGRATION_CODE_DATE_SOURCE_BYLIST(p_folder=UData, p_datatable=data.table(), 
                                                                        list_filefrom=list(
                                                                          "download_hsx_stkvn_prices.rds", 
                                                                          "download_hnx_stkvn_prices.rds", 
                                                                          "download_upc_stkvn_prices.rds" 
                                                                          
                                                                          # "download_vst_stkvn_ref.rds"
                                                                        ), 
                                                                        p_fileto    = "download_exc_stkvn_prices.rds", 
                                                                        IntegrateTo = "download_mlt_stkvn_prices.rds", 
                                                                        MaxDate     = T))
            
            xf = try(MHM_DATACENTER_INTEGRATION_CODE_DATE_SOURCE_BYLIST(p_folder=UData, p_datatable=data.table(), 
                                                                        list_filefrom=list(
                                                                          "download_mlt_stkvn_prices.rds" 
                                                                        ), 
                                                                        p_fileto    = "download_mlt_stkvn_history.rds", 
                                                                        IntegrateTo = "download_mlt_stkvn_history.rds", 
                                                                        MaxDate     = T))
            
            # MLT, STKVN, SHARES ...............................................................................  
            xOption    = 'TRAINEE_DEV_UDATA > SOURCE_INTRADAY'
            Minutes    = 30
            ForceProba = 50
            if (TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=xOption, pAction="COMPARE", NbSeconds=Minutes*60, ToPrint=F))
            {
              try(CCPR_SMART_INTEGRATION_INTRADAY(Folder_Fr=UData, Folder_To=UData, pData=data.table(), GroupBy = 'code x date x timestamp',
                                                  File_Fr='download_c68_stkvn_intraday_day.rds', File_To='download_c68_stkvn_intraday_history.rds',
                                                  pCondition='', IncludedOnly=F, CheckDateSummary=F, MaxDateOnly=F, NbDays=0, SaveOneDrive=F, Merge_STD=F, ToForce=F))
              
              try(CCPR_SMART_INTEGRATION_INTRADAY(Folder_Fr=UData, Folder_To=UData, pData=data.table(), GroupBy = 'code x date x timestamp',
                                                  File_Fr='download_c68_stkvn_intraday_history.rds', File_To='CheckDateSummary',
                                                  pCondition='', IncludedOnly=F, CheckDateSummary=F, MaxDateOnly=T, NbDays=0, SaveOneDrive=F, Merge_STD=F, ToForce=F))
              
              try(CCPR_SMART_INTEGRATION_INTRADAY(Folder_Fr=UData, Folder_To=UData, pData=data.table(), GroupBy = 'code x date x timestamp',
                                                  File_Fr='download_exc_stkvn_intraday_day.rds', File_To='download_exc_stkvn_intraday_history.rds',
                                                  pCondition='', IncludedOnly=F, CheckDateSummary=F, MaxDateOnly=F, NbDays=0, SaveOneDrive=F, Merge_STD=F, ToForce=F))
              try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=xOption, pAction="SAVE", NbSeconds=1, ToPrint=F))
            } else { CATln_Border(paste(xOption, 'ALREADY UPDATED'))}
            
          },
          
          'SOURCES_STKVN_INTRADAY_TODAY_HISTORY' = {
            pSource    = 'HSX'
            pSource    = 'HNX'
            pSource    = 'UPC'
            pSource    = 'CAF'
            pSource    = 'VND'
            pSource    = 'C68'
            pSource    = 'STB'
            pSource    = 'EXC'
            for (pSource in sample(list('HSX', 'HNX', 'UPC', 'EXC')))
            {
              xOption    = paste0('TRAINEE_DEV_UDATA > INTRADAY_HISTORY >', pSource)
              Minutes    = 30
              ForceProba = 50
              if (TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=xOption, pAction="COMPARE", NbSeconds=Minutes*60, ToPrint=F))
              {
                
                File_Intraday = paste0('download_', pSource, '_stkvn_intraday_today.rds')
                x             = CHECK_CLASS(try(CCPR_READRDS('S:/STKVN/PRICES/DAY/', paste0('download_', pSource, '_pricesboard_day.rds'), ToKable = T, ToRestore = T)))
                if (nrow(x)>0)
                {
                  My.Kable(x)
                  x[is.na(last), last:=reference]
                  x[is.na(close), close:=last]
                  UPDATE_UPDATED(x)
                  x[, timestamp:=updated]
                  x[, datetime:=updated]
                  My.Kable(x)
                  CCPR_SAVERDS(x, UData, File_Intraday, ToSummary=T, SaveOneDrive = F)
                  try(TRAINEE_CCPR_SMART_INTEGRATION_INTRADAY(Folder_Fr=UData, Folder_To=UData, pData=x, GroupBy = 'code x date x timestamp',
                                                              File_Fr='', File_To=paste0('download_', pSource, '_stkvn_intraday_history.rds'),
                                                              pCondition='', IncludedOnly=F, CheckDateSummary=F, MaxDateOnly=F, NbDays=0, SaveOneDrive=F, Merge_STD=F, ToForce=F))
                  try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=xOption, pAction="SAVE", NbSeconds=1, ToPrint=F))
                }
              } else { CATln_Border(paste(xOption, 'ALREADY UPDATED'))}
            }
          }
  )
}
