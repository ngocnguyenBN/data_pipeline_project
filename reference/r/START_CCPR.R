# ==================================================================================================
# START_CCPR.R 2023-10-04 07:01
# ==================================================================================================

RELOAD_INSREF()

List_IFRC_IND_CONTINENTS  = list("ASIA", "AMERICA", "AFRICA", "EUROPE", "OCEANIA")
List_IFRC_STK_CONTINENTS  = list("ASIA", "AMERICA", "AFRICA", "EUROPE", "OCEANIA")
List_IFRC_STK_REGIONS     = list("ASIA", "AMERICA", "AFRICA", "EUROPE", "OCEANIA", 'IN', 'VN', 'ASEAN', 'ASEANP3', 'WORLD')
List_IFRC_IND_REGIONS     = list("ASIA", "AMERICA", "AFRICA", "EUROPE", "OCEANIA", 'IN', 'VN', 'ASEAN', 'ASEANP3', 'WORLD', 'GLOBAL')
Dt_INDFCAT                = ins_ref[type=="IND" & !is.na(fcat), .(n=.N), by="fcat"][order(-n)][n>5]
List_IFRC_INDFCAT         = as.list(Dt_INDFCAT$fcat)

print('============================================================================================', quote=F)
print('START_CCPR.R, END.', quote=F)
print('============================================================================================', quote=F)