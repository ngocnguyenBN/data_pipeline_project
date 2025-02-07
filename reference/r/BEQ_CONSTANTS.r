# UPDATED : 2024-01-26 10:50
NbCarLine    = 125
RData        = "R:/DATA/"
CCPR_Folder  = 'R:/R/CCPR/DATA/'
CCPRData     = 'R:/CCPR/DATA/'
UData  ='U:/EFRC/DATA/'
WPublic = 'Y:/LOCAL_WEB/'
# assign("UData", UData, envir = .GlobalEnv)
# ODDrive = 'd:/OneDrive/BeQ/'
ISO2_ASEANP3 = list('SG', 'VN', 'TH', 'MY', 'ID', 'PH', 'JP', 'CN', 'HK', 'KR') ; assign("ISO2_ASEANP3", ISO2_ASEANP3, envir = .GlobalEnv)
ISO2_ASEAN   = list('SG', 'VN', 'TH', 'MY', 'ID', 'PH')                         ; assign("ISO2_ASEAN", ISO2_ASEAN, envir = .GlobalEnv)


All_SOURCES_exEIKMLT     = list("INV", 'BIN', 'ENX', 'NSD', 'EXC', 'BLG', 'YAH', 'GOG', 'FTSE', 'DJSP', 'MSCI', 'STX', 'IFRC')
All_SOURCES_exEIK        = list("INV", 'BIN', 'ENX', 'NSD', 'EXC', 'BLG', 'YAH', 'GOG', 'FTSE', 'DJSP', 'MSCI', 'STX', 'IFRC', 'MLT')
All_SOURCES              = list("INV", 'BIN', 'ENX', 'NSD', 'EXC', 'BLG', 'YAH', 'GOG', 'EIK', 'FTSE', 'DJSP', 'MSCI', 'STX', 'IFRC', 'MLT')
All_CONTINENTS           = list("ASIA", "EUROPE", "AMERICA", "OCEANIA", "AFRICA")
All_SOURCES_exEIKIFRCMLT = setdiff(All_SOURCES, list('EIK','IFRC', 'MLT'))
IND_PROVIDER             = list("ENX", "NSD", "MSCI", 'STX', 'DJSP', 'IFRC')
IND_SOURCE               = list("GOG", "INV", "BIN", 'YAH', 'BLG', 'RTS')

List_PC_POWER            = list('3630', 'IREEDS')
assign("List_PC_POWER", List_PC_POWER, envir = .GlobalEnv)

Fields_Min_STD           = c('source', 'codesource', 'symbol', 'code', 'date', 'close', 'close_adj', 'close_unadj', 
                             'change', 'var', 'varpc', 'rt', 'volume', 'turnover', 'cur', 'curs', 'timestamp', 'updated')
Fields_Min_STD_DAY       = c('source', 'codesource', 'symbol', 'code', 'date', 'close', 'close_adj', 'close_unadj', 
                             'change', 'var', 'varpc', 'rt', 'volume', 'turnover', 'cur', 'curs', 'updated')
Fields_Min_STD_INTRADAY  = c('source', 'codesource', 'symbol', 'code', 'date', 'close', 'close_adj', 'close_unadj', 
                             'change', 'var', 'varpc', 'rt', 'volume', 'turnover', 'cur', 'curs', 'timestamp', 'datetime', 'updated')
assign("Fields_Min_STD", Fields_Min_STD, envir = .GlobalEnv)

IND_FIELDS_STD = c('type', 'fcat', 'country', 'iso2', 'continent', 'source', 'codesource', 'mic', 'isin', 'code', 'name', 
                   'date', 'close', 'open', 'high', 'low', 'currrency', 'cur', 'change', 'var', 'varpc', 'rt', 'home', 'sample', 'updated')
assign("IND_FIELDS_STD", IND_FIELDS_STD, envir = .GlobalEnv)
Position_Moniter_PC        = c(3,0)
Position_Moniter_File      = c(3,11)
Position_Moniter_Connexion = c(19,0)

List_IFRC_IND_CONTINENTS  = list("ASIA", "AMERICA", "AFRICA", "EUROPE", "OCEANIA")
List_IFRC_STK_CONTINENTS  = list("ASIA", "AMERICA", "AFRICA", "EUROPE", "OCEANIA")
List_IFRC_STK_REGIONS     = list("ASIA", "AMERICA", "AFRICA", "EUROPE", "OCEANIA", 'IN', 'VN', 'ASEAN', 'ASEANP3', 'WORLD')
List_IFRC_IND_REGIONS     = list("ASIA", "AMERICA", "AFRICA", "EUROPE", "OCEANIA", 'IN', 'VN', 'ASEAN', 'ASEANP3', 'WORLD', 'GLOBAL')
Dt_INDFCAT                = ins_ref[type=="IND" & !is.na(fcat), .(n=.N), by="fcat"][order(-n)][n>5]
List_IFRC_INDFCAT         = as.list(Dt_INDFCAT$fcat)

List_IFRC_SOURCES_MONITOR             = c('RTS', 'BLG', 'INV', 'YAH', 'BIN', 'GOG', 'NSD', 'IFRC', 'EFRC')
List_IFRC_SOURCES_MONITOR_ex_EFRCIFRC = c('RTS', 'BLG', 'INV', 'YAH', 'BIN', 'GOG', 'NSD')
List_IFRC_SOURCES_MONITOR_ex_EFRC     = c('RTS', 'BLG', 'INV', 'YAH', 'BIN', 'GOG', 'NSD', 'IFRC')
