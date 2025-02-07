# ==================================================================================================
TRAINEE_DOWNLOAD_INDVN_BY_SOURCE = function(source = 'CAF', Save_Folder = 'S:/STKVN/INDEX_2024/', ToIntergrate = F){
  # ------------------------------------------------------------------------------------------------
  # pCode = 'hnx-index'
  switch (source,
          'CAF' = {
            list_index = list('VNINDEX', 'HNX-INDEX', 'VN30INDEX', 'HNX30-INDEX')
            xList = list()
            for (i in 1:length(list_index)){
              pCode = list_index[[i]]
              switch(pCode,
                     'HNX-INDEX'   = {code = 'INDVHINDEX'},
                     'HNX30-INDEX' = {code = 'INDHNX30'},
                     'VN30INDEX'   = {code = 'INDVN30'},
                     {code = paste0('IND',pCode)})
              CAF = TRAINEE_DOWNLOAD_CAF_INDVN_PRICES_BY_CODE(pCodesource = pCode, CodeInt = code) 
              if (all(class(CAF) != 'try-error')){
                xList[[i]] = CAF
              }
            }
            Final_Data = rbindlist(xList, fill = T)
            DATA_OLD = CCPR_READRDS(UData, 'DOWNLOAD_CAF_INDVN_PRICES_HISTORY.rds', ToKable = T)
            Final_Data = unique(rbind(DATA_OLD[code %in% list('INDVHINDEX','INDHNX30','INDVN30','VNINDEX')], Final_Data, fill=T), by=c('code', 'date'), fromLast = T)
          },
          'FANT' = {
            list_index = list('VNINDEX', 'HNXINDEX', 'VN30', 'HNX30')
            # pCode = 'HNXINDEX'
            xList = list()
            for (i in 1:length(list_index)){
              pCode = list_index[[i]]
              switch(pCode,
                     'HNXINDEX'   = {code = 'INDVHINDEX'},
                     {code = paste0('IND', pCode)})
              FANT = TRAINEE_DOWNLOAD_FANT_PRICES_BY_CODE (pCodesource = pCode, Starttime=8, STOCK = F, CodeInt=code,  merge_ins_ref = F)
              if (all(class(FANT) != 'try-error')){
                xList[[i]] = FANT
              }
            }
            Final_Data = rbindlist(xList, fill = T)
          },
          'VND' = {
            list_index = list('VNINDEX', 'HNX-INDEX', 'VN30', 'HNX30-INDEX')
            # pCode = 'HNXINDEX'
            xList = list()
            for (i in 1:length(list_index)){
              pCode = list_index[[i]]
              switch(pCode,
                     'HNX-INDEX'   = {code = 'INDVHINDEX'},
                     'HNX30-INDEX' = {code = 'INDHNX30'},
                     'VN30'   = {code = 'INDVN30'},
                     {code = paste0('IND',pCode)})
              VND = TRAINEE_DOWNLOAD_VND_VIETNAM_INDEX_BY_CODE (pCode = pCode, CodeInt = code)
              if (all(class(FANT) != 'try-error')){
                xList[[i]] = VND
              }
            }
            Final_Data = rbindlist(xList, fill = T)
          },
          'STB' = {
            Mydata = DOWNLOAD_STB_INDEX(codesource = "HOSTC",  code = "INDVNINDEX", pagemax = 50000, Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = 'DOWNLOAD_STB_VNI_PRICES.RDS')
            Mydata = DOWNLOAD_STB_INDEX(codesource = "HASTC",  code = "INDVHINDEX", pagemax = 50000, Save_folder = 'S:/STKVN/INDEX_2024/', Save_file = 'DOWNLOAD_STB_HNX_PRICES.RDS')
            
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = Save_Folder, File_Fr     = 'DOWNLOAD_STB_VNI_PRICES.RDS',
                                                                      Folder_To   = Save_Folder, File_To     = 'DOWNLOAD_STB_INDVN_PRICES.RDS',
                                                                      pCondition  = '',
                                                                      RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>50)))
            
            try(TRAINEE_IFRC_CCPR_INTEGRATION_BY_FILES_QUALITY_FR_TO (Folder_Fr   = Save_Folder, File_Fr     = 'DOWNLOAD_STB_HNX_PRICES.RDS',
                                                                      Folder_To   = Save_Folder, File_To     = 'DOWNLOAD_STB_INDVN_PRICES.RDS',
                                                                      pCondition  = '',
                                                                      RemoveNA    = T, NbDays = 0, MinPercent  = 0, ToForce=(runif(1,1,100)>50)))
            
          }
  )
  if (source != 'STB'){
    Final_Data = UPDATE_UPDATED(Final_Data)
    CCPR_SAVERDS(Final_Data, Save_Folder ,paste0('DOWNLOAD_',source,'_INDVN_PRICES.rds'), ToSummary = T, SaveOneDrive = T)
    if (CHECK_TIMEBETWEEN('17:00', '23:00') | ToIntergrate){
      if (file.exists(paste0(Save_Folder, paste0('DOWNLOAD_',source,'_INDVN_PRICES_HISTORY.rds')))){
        DATA_OLD = CCPR_READRDS(Save_Folder, paste0('DOWNLOAD_',source,'_INDVN_PRICES_HISTORY.rds'), ToKable = T, ToRestore = T)
        DATA_DAY = CCPR_READRDS(Save_Folder, paste0('DOWNLOAD_',source,'_INDVN_PRICES.rds'), ToKable = T, ToRestore = T)
        
        DATA_OLD = unique(rbind(DATA_OLD, DATA_DAY, fill=T), by=c('code', 'date'), fromLast = T)
        CCPR_SAVERDS(DATA_OLD, Save_Folder ,paste0('DOWNLOAD_',source,'_INDVN_PRICES_HISTORY.rds'), ToSummary = T, SaveOneDrive = T)
      } else {
        CCPR_SAVERDS(Final_Data, Save_Folder ,paste0('DOWNLOAD_',source,'_INDVN_PRICES_HISTORY.rds'), ToSummary = T, SaveOneDrive = T)
      }
    }
  }
}


# ==================================================================================================
TRAINEE_DOWNLOAD_CAF_INDVN_PRICES_BY_CODE = function(pCodesource = 'vnindex', CodeInt = 'INDVNINDEX')  {
  # ------------------------------------------------------------------------------------------------
  # pCode = 'XYZ'
  # pURLk = paste0('https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=', pCode, '&StartDate=30/12/2022&EndDate=30/12/2023&PageIndex=3&PageSize=20')
  # NbPagesBack = 20
  pCode     = pCodesource
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
        Final_Data = XData[, .(source='CAF', codesource=pCode, ticker=pCode,
                               date      = as.Date(ngay, '%d/%m/%Y'), 
                               open      = as.numeric(giamocua),
                               high      = as.numeric(giacaonhat),
                               low       = as.numeric(giathapnhat),
                               close_adj = as.numeric(giadieuchinh),
                               close=as.numeric(giadongcua),
                               change=as.numeric(word(thaydoi,1,1,'\\(')),
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
    Data_All[, codesource:=toupper(codesource)]
    Data_All[, code:=toupper(CodeInt)]
    My.Kable.TB(Data_All)
  } else {
    Data_All = data.table()
  }
  return(Data_All)
}
