# ==================================================================================================
# DBL_LIBRARY.R - 2024-08-07 16:42
# ==================================================================================================

# rm(list = ls())
# # ..................................................................................................
# source("c:/R/TRAINEE_CONFIG.r", echo=T)
# print(SDrive)
# print(ODDrive)
# print(ODLibrary)
# print(SDLibrary)
# # print(ODData)
# 
# source(paste0(SDLibrary, "TRAINEE_INTRO.R"),                                                 echo=F)
# 
# source(paste0(SDLibrary, "TRAINEE_LIBRARY.R"),                                               echo=F)


# ==================================================================================================
DBL_REPORT_ALERT = function(nb_minute = 10){
  # ------------------------------------------------------------------------------------------------
  # Monitor = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='START SCRIPT > REPORT_ALERT', pAction="SAVE", NbSeconds=1, ToPrint=F))
  
  pMyPC = toupper(as.character(try(fread("C:/R/my_pc.txt", header = F))))
  start_time = Sys.time(); dt_rep = list(); irun = 1
  
  # CHECK DRIVES
  if (!pMyPC %in% c("IFRC-TV"))
  {
    if (pMyPC %in% c("LOCAL", "BEQ-RD7", "BEQ-RD2"))
    {
      try(CHECK_DRIVE(list_check = c("SDATA", "SDRIVE")))
    }else{ try(CHECK_DRIVE(list_check = c("SDATA", "UDATA", "SDRIVE","TDRIVE", "UDRIVE"))) }
  }
  # CHECK PC
  try(CHECK_PC(pMinutes = 1*60))
  
  # if (pMyPC %in% c("BEQ-RD1", "BEQ-INDEX", "BEQ-RD5", "BEQ-RD3", "BEQ-RD4", "BEQ-RD6"))
  # {
  # TODO_ALERT = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='REPORT_ALERT', pAction="COMPARE", NbSeconds=nb_minute*60, ToPrint=F))
  ToContinue = T
  while(ToContinue){
    dt_rep[[irun]] = try(REPORT_ALERT_BATCH())
    irun = irun + 1 
    # TODO_ALERT = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0(pMyPC," = REPORT_ALERT"), pAction="COMPARE", NbSeconds=nb_minute*60, ToPrint=F))
    TODO_ALERT = difftime(Sys.time(), start_time, units = "secs")>=nb_minute*60
    if(TODO_ALERT){
      ToContinue = F
      # TODO_ALERT = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0(pMyPC," = REPORT_ALERT"), pAction="SAVE", NbSeconds=nb_minute*60, ToPrint=F))
    }
    IFRC_SLEEP(10)
  }
  
  # CHECK BATCHS
  dt_rep = rbindlist(dt_rep)
  check_batch = unique(dt_rep[order(-timestampvn_updated)], by = c("code","pc"))[order(status_delay, PRIORITY)]
  check_batch = UPDATE_UPDATED(check_batch)
  DBL_CCPR_SAVERDS(check_batch, "S:/REPORTS/","check_batch.rds", Nb_repeat = 1, pSleep = 1, SaveOneDrive = T)
  
  alert_dt = unique(dt_rep[status_alert %in% c("WARNING", "URGENCY")], by = c("code","pc"))
  
  To_report = alert_dt[!paste(code, pc) %in% paste(dt_rep[status_alert=="OK"]$code, dt_rep[status_alert=="OK"]$pc)]
  
  try(DBL_RESTART_STOPPED_BATCHS())
  
  CHECK_RESTART = CCPR_READRDS("S:/REPORTS/","check_batch_restarted.rds")
  if (nrow(CHECK_RESTART)>0)
  {
    To_report = merge(To_report,CHECK_RESTART[,.(code,pc,time_restarted)], by = c("code","pc"), all.x=T)
    To_report[,delay_restarted := round(as.numeric(difftime(as.POSIXct(substr(timestampvn_check,1,19)),
                                                            as.POSIXct(substr(time_restarted,1,19)),units = "mins")), 2)]
    To_report = To_report[delay_restarted > delay_max | is.na(time_restarted)]
  }
  if (nrow(To_report) > 0)
  {
    list_alert = setDT(fread("S:/REPORTS/list_alert.txt"))
    list_alert = list_alert[category=="ALERT BATCHS" & active==1]
    
    To_report = unique(To_report[order(-timestampvn_check)], by = c("code","pc","subject"))
    To_report = UPDATE_UPDATED(To_report)
    
    rep_dt = CCPR_READRDS("S:/REPORTS/", "report_alert_batch.rds")
    To_report = merge(To_report, rep_dt[,.(code, pc, timestampvn_alert = updated)], by=c("code","pc"), all.x=T)
    To_report[is.na(timestampvn_alert), timestampvn_alert:=Sys.time()-15*60]
    
    for (isbj in 1:length(unique(To_report$subject))) {
      if (difftime(Sys.time(), To_report[subject==unique(To_report$subject)[isbj]][1]$updated, units = "sec")>10*60)
      {
        rep_sbj = To_report[subject==unique(To_report$subject)[isbj]]
        sbj = paste(rep_sbj[1]$status_alert, rep_sbj[1]$subject)
        rep_sbj = rep_sbj[,-c("PRIORITY","subject","delay_max")]
        Text_prd = paste0(kable(rep_sbj, format = "markdown"), collapse = "<br/>")
        Text = paste0(sbj, "<br/>", "<pre>", Text_prd, "</pre>")
        
        # try(TELEGRAM_SEND_MSG(ChatID = paste0(list_alert[send_type=="TELEGRAM"]$receive_telegramid,collapse = ","), Text = gsub('<br/>','\n',Text),
        #                       TokenUser = 'VAYmbSr6CV9y2i8MrIoTutscFeAQUsl3yoAymZrchg2PdgrHEKsvdzuPwm0kzJW1'))
        # 
        # try(GMAIL_SEND_MSG(From = list_alert[send_type=="GMAIL"][1]$user_name,
        #                    To = paste0(list_alert[send_type=="GMAIL"]$receive_name,collapse = ";"),
        #                    Subject = sbj, Content = Text))
        
        try(SKYPE_SEND_MSG(ConversationID = list("19:962e2d85025b4d1882465b577faa2198@thread.v2"),
                           Message = Text))
      }
    }
    
    rep_dt = unique(rbind(rep_dt, To_report),by = c("code","pc","status_alert","updated"))
    rep_dt = rep_dt[as.Date(updated)==Sys.Date()]
    DBL_CCPR_SAVERDS(rep_dt, "S:/REPORTS/", "report_alert_batch.rds", Nb_repeat = 1, pSleep = 1, SaveOneDrive = T)
  }
  # } else{
  #   IFRC_SLEEP(5*60)
  #   
  #   # CHECK DRIVES
  #   if (pMyPC %in% c("LOCAL", "BEQ-RD7"))
  #   {
  #     try(CHECK_DRIVE(list_check = c("SDATA", "SDRIVE")))
  #   }else{ try(CHECK_DRIVE(list_check = c("SDATA", "UDATA", "SDRIVE","TDRIVE", "UDRIVE"))) }
  #   
  #   # CHECK PC
  #   try(CHECK_PC(pMinutes = 1*60))
  # }
  # Monitor = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='END SCRIPT > REPORT_ALERT', pAction="SAVE", NbSeconds=1, ToPrint=F))
  
}

# DBL_KILL_BATCH = function(batch_name) {
#   tasks = system("tasklist /fo csv /v", intern = TRUE)
#   tasks_df = setDT(read.csv(text = tasks, stringsAsFactors = FALSE))
#   target_pid = tasks_df$PID[tasks_df$Image.Name == "cmd.exe" & grepl(batch_name, tasks_df$Window.Title, ignore.case = TRUE)]
#   
#   if (length(target_pid) > 0) {
#     sapply(target_pid, function(pid) {
#       # system(paste("taskkill /F /PID", pid), intern = FALSE)
#       system(paste("taskkill -f /pid", pid), intern = FALSE)
#       
#       cat(sprintf("Tiến trình %d đã được dừng.\n", pid))
#     })
#   } else {
#     cat("Không tìm thấy tiến trình phù hợp.\n")
#   }
#   
#   
#   tasks = system("tasklist /fo csv /v", intern = TRUE)
#   tasks_df = setDT(read.csv(text = tasks, stringsAsFactors = FALSE))
#   target_pid = tasks_df$PID[tasks_df$Image.Name == "Rterm.exe" & grepl(batch_name, tasks_df$Window.Title, ignore.case = TRUE)]
#   
#   if (length(target_pid) > 0) {
#     sapply(target_pid, function(pid) {
#       # system(paste("taskkill /F /PID", pid), intern = FALSE)
#       system(paste("taskkill -f /pid", pid), intern = FALSE)
#       
#       cat(sprintf("Tiến trình %d đã được dừng.\n", pid))
#     })
#   } else {
#     cat("Không tìm thấy tiến trình phù hợp.\n")
#   }
#   
#   
# }

DBL_KILL_BATCH = function(batch_name) {
  wmic_output = system2("wmic", args = c("process", "where", "name='cmd.exe'", "get", "ProcessId,CommandLine", "/format:csv"), stdout = TRUE)
  wmic_df = setDT(read.csv(text = paste(wmic_output, collapse = "\n"), stringsAsFactors = FALSE))
  target_pid = wmic_df[grepl(batch_name, CommandLine, ignore.case = TRUE)]$ProcessId
  
  if (length(target_pid) > 0) {
    sapply(target_pid, function(pid) {
      # system(paste("taskkill /F /PID", pid), intern = FALSE)
      system2("taskkill", args = c("/f", "/t", "/pid", as.character(pid)))
      
      cat(sprintf("Tiến trình %d đã được dừng.\n", pid))
    })
  } else {
    cat("Không tìm thấy tiến trình phù hợp.\n")
  }
  
}

DBL_RESTART_STOPPED_BATCHS = function(){
  check_batch = CCPR_READRDS("S:/REPORTS/","check_batch.rds")
  
  #Do restart 
  BATCH_TO_RESTART = check_batch[status_delay == "NOT OK"]
  CHECK_RESTART = CCPR_READRDS("S:/REPORTS/","check_batch_restarted.rds")
  if (nrow(CHECK_RESTART)>0)
  {
    BATCH_TO_RESTART = merge(BATCH_TO_RESTART,CHECK_RESTART[,.(code,pc,time_restarted)], by = c("code","pc"), all.x=T)
    BATCH_TO_RESTART[,delay_restarted := round(as.numeric(difftime(as.POSIXct(substr(timestampvn_check,1,19)),
                                                                   as.POSIXct(substr(time_restarted,1,19)),units = "mins")), 2)]
    BATCH_TO_RESTART = BATCH_TO_RESTART[delay_restarted > delay_max | is.na(delay_restarted)]
  }
  #system(paste("cmd.exe /c S: &&", "start BATCH.bat"))
  if (nrow(BATCH_TO_RESTART) > 0)
  {
    pMyPC = toupper(as.character(try(fread("C:/R/my_pc.txt", header = F))))
    
    for (i in 1:nrow(BATCH_TO_RESTART))
    {
      # i = 1
      if (BATCH_TO_RESTART[i]$pc==pMyPC)
      {
        try(DBL_KILL_BATCH(batch_name = BATCH_TO_RESTART[i]$code))
        
        system(paste0("cmd.exe /c S: && start cmd /c S:/R/BATCH/",BATCH_TO_RESTART[i]$code,".bat"), wait = F)
        
        CATln(paste0(BATCH_TO_RESTART[i]$code,'.bat',' IS RESTARTED!'))
        check_batch[code == BATCH_TO_RESTART[i]$code & pc==BATCH_TO_RESTART[i]$pc,":="(note = 'RESTARTED', time_restarted = substr(Sys.time(),1,19))]
      }else{ check_batch[code == BATCH_TO_RESTART[i]$code & pc==BATCH_TO_RESTART[i]$pc,":="(note = as.character(NA), time_restarted = as.character(NA))] }
    }
  }else{  check_batch[,":="(note = as.character(NA), time_restarted = as.character(NA))] }
  SAVE_RESTARTED = check_batch[note == "RESTARTED"]
  DATA_OLD = CCPR_READRDS("S:/REPORTS/","check_batch_restarted.rds")
  DATA_NEW = rbind(DATA_OLD,SAVE_RESTARTED,fill = T)
  DATA_NEW = unique(DATA_NEW[order(-time_restarted)], by = c("code","pc","note"))
  DBL_CCPR_SAVERDS(DATA_NEW,"S:/REPORTS/","check_batch_restarted.rds", Nb_repeat = 1, pSleep = 1, SaveOneDrive = T)
  
  check_batch = check_batch[,.(code, nb_pc, pc, timestampvn_updated, timestampvn_check = substr(as.character(timestampvn_check),1,19),
                               delay_mins, status_delay)]
  check_batch = UPDATE_UPDATED(check_batch[,-c("updated")])
  
  library(fst)
  write_fst(check_batch, "S:/SHINY/REPORT/BATCH/report_batch.fst")
  DBL_CCPR_SAVERDS(check_batch, "S:/REPORTS/","report_batch.rds", Nb_repeat = 1, pSleep = 1, SaveOneDrive = T)
  return(check_batch)
}

REPORT_ALERT_BATCH_BY_SUBJECT = function (pFolder = "S:/CCPR/DATA/MONITOR/", pFile = "trainee_monitor_execution_summary.rds",
                                          pCode = list(), pOption = "ALERT_SIGNALS")
{
  RD_ALERTS = setDT(read.xlsx("D:/onedrive/DBL/RD_BATCH_MANAGEMENT.xlsx",sheet = "ALERT_SUBJECT"))
  BATCH = RD_ALERTS[ALERT_SUBJECT==pOption]$BATCH_NAME
  
  
  PRIORITY_LIST = setDT(read_xlsx("D:/onedrive/BeQ/CCPR/DATA/MONITOR/MANAGEMENT_RD_TEAM.xlsx", sheet = "BATCH_BY_PC", skip = 2))
  PRIORITY_LIST[, `BATCH TO RUN`:= gsub(".bat","",`BATCH TO RUN`)]
  PRIORITY_LIST[,code := `BATCH TO RUN`]
  
  PRIORITY_LIST = setDT(pivot_longer(PRIORITY_LIST[,-c("LOCATION","TOTAL","ACTIVE","BATCH TO RUN")],
                                     cols = -c("code","PRIORITY","delay_max"),
                                     names_to = "pc",
                                     values_to = "active"))
  PRIORITY_LIST = PRIORITY_LIST[active==1]
  
  
  MONITOR = DBL_CCPR_READRDS(pFolder,pFile)
  # MONITOR = DBL_CCPR_READRDS(pFolder, pFile)
  MONITOR = MONITOR[, -c("date","updated")]
  
  REPORT = setDT(pivot_longer(MONITOR,
                              cols = -c("code","codesource","source","close"),
                              names_to = "pc",
                              values_to = "timestampvn_updated"))
  REPORT = REPORT[!is.na(timestampvn_updated)]
  
  
  # REPORT = REPORT[grepl("^START SCRIPT", code)]
  if (length(pCode) > 0)
  {
    REPORT = REPORT[code %in% pCode]
  }
  REPORT = REPORT[grepl("^END SCRIPT|^START SCRIPT", code)]
  REPORT = REPORT[, code := gsub("END SCRIPT > |START SCRIPT > ","",code)]
  REPORT = unique(REPORT[order(-timestampvn_updated)], by=c("code","pc"))
  
  REPORT = merge(PRIORITY_LIST[,.(code,pc = toupper(pc), delay_max, PRIORITY)], REPORT, by = c('code',"pc"), all.x = T)
  # REPORT = merge(REPORT, PRIORITY_LIST[,.(code,pc = toupper(pc), delay_max, PRIORITY)], by = c('code',"pc"), all.x = T)
  REPORT = REPORT[!is.na(PRIORITY)]
  REPORT = REPORT[code %in% BATCH]
  REPORT[, ":="(minute_delay = delay_max, timestampvn_check = Sys.time())]
  REPORT[, delay_mins := round(as.numeric(difftime(as.POSIXct(substr(timestampvn_check,1,19)),
                                                   as.POSIXct(substr(timestampvn_updated,1,19)),units = "mins")), 2)]
  
  REPORT[, status_delay := ifelse(delay_mins > delay_max | is.na(delay_mins), "NOT OK", "OK")]
  REPORT[is.na(status_delay), status_delay := "NOT OK"]
  # NOT_OK = REPORT[status == "NOT OK"]$code
  # REPORT = REPORT[code %in% NOT_OK]
  REPORT[, nb_pc := (.N), by = 'code']
  
  
  
  # REPORT[number_pc_run >= 2, alert := ifelse(all(is.na(status) | all(status) != "OK"), 'URGENCY', 
  #                                            ifelse(any(status != "OK" | is.na(status)), 'WARNING', 'NO'))]
  # REPORT[number_pc_run == 1, alert := ifelse((is.na(status)), 'URGENCY',
  #                                            ifelse((status != "OK" | is.na(status)), 'WARNING', 'NO'))]
  
  if(all(REPORT$status_delay != "OK"))
  {
    REPORT[, status_alert := 'URGENCY']
  }else{
    REPORT[, status_alert := ifelse(all(status_delay != "OK"), 'WARNING', 'OK'), by = 'code']
  }
  
  REPORT[,timestampvn_check:=substr(as.character(timestampvn_check),1,19)]
  # REPORT = REPORT[,.(code, nb_pc, pc, timestampvn_updated, timestampvn_check = substr(as.character(timestampvn_check),1,19),
  #                    delay_mins, status_delay, status_alert, PRIORITY)]
  # REPORT = UPDATE_UPDATED(REPORT)
  
  return(REPORT)
}
# 
# REPORT_ALERT_BATCH = function()
# {
#   RD_ALERTS = setDT(read.xlsx("D:/onedrive/DBL/RD_BATCH_MANAGEMENT.xlsx",sheet = "ALERT_SUBJECT"))
#   SUBJECT = unique(RD_ALERTS$ALERT_SUBJECT)
#   xList = list()
#   for (k in 1:length(SUBJECT))
#   {
#     #k=3
#     Option = SUBJECT[k]
#     x = try(REPORT_ALERT_BATCH_BY_SUBJECT(pFolder = "S:/CCPR/DATA/MONITOR/", pFile = "trainee_monitor_execution_summary.rds",
#                                           pCode = list(), pOption = Option))
#     To_report = x[status_alert == "WARNING" | status_alert == "URGENCY"][order(PRIORITY)]
#     if (nrow(To_report) > 0)
#     {
#       list_alert = setDT(fread("S:/REPORTS/list_alert.txt"))
#       list_alert = list_alert[category=="ALERT BATCHS" & active==1]
#       
#       To_report = UPDATE_UPDATED(To_report[,-c("PRIORITY")])
#       sbj = paste(To_report[1]$status_alert, Option)
#       Text_prd = paste0(kable(To_report, format = "markdown"), collapse = "<br/>")
#       Text = paste0(sbj, "<br/>", "<pre>", Text_prd, "</pre>")
#       
#       try(TELEGRAM_SEND_MSG(ChatID = paste0(list_alert[send_type=="TELEGRAM"]$receive_telegramid,collapse = ","), Text = gsub('<br/>','\n',Text),
#                             TokenUser = 'VAYmbSr6CV9y2i8MrIoTutscFeAQUsl3yoAymZrchg2PdgrHEKsvdzuPwm0kzJW1'))
#       
#       try(GMAIL_SEND_MSG(From = list_alert[send_type=="GMAIL"][1]$user_name,
#                          To = paste0(list_alert[send_type=="GMAIL"]$receive_name,collapse = ";"),
#                          Subject = sbj, Content = Text))
#       
#       try(SKYPE_SEND_MSG(ConversationID = list("19:962e2d85025b4d1882465b577faa2198@thread.v2"),
#                          Message = Text))
#       
#       rep_dt = DBL_CCPR_READRDS("S:/REPORTS/", "report_alert_batch.rds")
#       rep_dt = unique(rbind(rep_dt, To_report),by = c("code","pc","updated"))
#       rep_dt = rep_dt[as.Date(updated)==Sys.Date()]
#       DBL_CCPR_SAVERDS(rep_dt, "S:/REPORTS/", "report_alert_batch.rds")
#     }
#     xList[[k]] = x
#   }
#   Intranet_report = rbindlist(xList, fill = T)
#   Intranet_report = unique(Intranet_report[order(-timestampvn_updated)], by = c("code","pc"))[order(status_delay, PRIORITY)]
#   Intranet_report = UPDATE_UPDATED(Intranet_report[,-c("PRIORITY","status_alert")])
#   
#   library(fst)
#   write_fst(Intranet_report, "S:/SHINY/REPORT/BATCH/check_batch.fst")
#   DBL_CCPR_SAVERDS(Intranet_report, "S:/REPORTS/","check_batch.rds")
#   
#   return (Intranet_report)
# }


REPORT_ALERT_BATCH = function()
{
  RD_ALERTS = setDT(read.xlsx("D:/onedrive/DBL/RD_BATCH_MANAGEMENT.xlsx",sheet = "ALERT_SUBJECT"))
  SUBJECT = unique(RD_ALERTS$ALERT_SUBJECT)
  xList = list()
  for (k in 1:length(SUBJECT))
  {
    #k=3
    Option = SUBJECT[k]
    x = try(REPORT_ALERT_BATCH_BY_SUBJECT(pFolder = "S:/CCPR/DATA/MONITOR/", pFile = "trainee_monitor_execution_summary.rds",
                                          pCode = list(), pOption = Option))
    x[,subject:=Option]
    # To_report = x[status_alert == "WARNING" | status_alert == "URGENCY"][order(PRIORITY)]
    # if (nrow(To_report) > 0)
    # {
    #   list_alert = setDT(fread("S:/REPORTS/list_alert.txt"))
    #   list_alert = list_alert[category=="ALERT BATCHS" & active==1]
    #   
    #   To_report = UPDATE_UPDATED(To_report[,-c("PRIORITY")])
    #   sbj = paste(To_report[1]$status_alert, Option)
    #   Text_prd = paste0(kable(To_report, format = "markdown"), collapse = "<br/>")
    #   Text = paste0(sbj, "<br/>", "<pre>", Text_prd, "</pre>")
    #   
    #   try(TELEGRAM_SEND_MSG(ChatID = paste0(list_alert[send_type=="TELEGRAM"]$receive_telegramid,collapse = ","), Text = gsub('<br/>','\n',Text),
    #                         TokenUser = 'VAYmbSr6CV9y2i8MrIoTutscFeAQUsl3yoAymZrchg2PdgrHEKsvdzuPwm0kzJW1'))
    #   
    #   try(GMAIL_SEND_MSG(From = list_alert[send_type=="GMAIL"][1]$user_name,
    #                      To = paste0(list_alert[send_type=="GMAIL"]$receive_name,collapse = ";"),
    #                      Subject = sbj, Content = Text))
    #   
    #   try(SKYPE_SEND_MSG(ConversationID = list("19:962e2d85025b4d1882465b577faa2198@thread.v2"),
    #                      Message = Text))
    #   
    #   rep_dt = DBL_CCPR_READRDS("S:/REPORTS/", "report_alert_batch.rds")
    #   rep_dt = unique(rbind(rep_dt, To_report),by = c("code","pc","updated"))
    #   rep_dt = rep_dt[as.Date(updated)==Sys.Date()]
    #   DBL_CCPR_SAVERDS(rep_dt, "S:/REPORTS/", "report_alert_batch.rds")
    # }
    xList[[k]] = x
  }
  Intranet_report = rbindlist(xList, fill = T)
  
  # dt_rep = unique(Intranet_report[order(-timestampvn_updated)], by = c("code","pc"))[order(status_delay, PRIORITY)]
  # dt_rep = UPDATE_UPDATED(dt_rep[,-c("PRIORITY","status_alert")])
  # 
  # library(fst)
  # write_fst(dt_rep, "S:/SHINY/REPORT/BATCH/check_batch.fst")
  # DBL_CCPR_SAVERDS(dt_rep, "S:/REPORTS/","check_batch.rds")
  
  return (Intranet_report)
}


#===================================================================================================
CHECK_DRIVE = function(list_check = c("config","my_pc", "py_download", "SDATA", "UDATA", "SDRIVE","TDRIVE", "UDRIVE")){
  # ------------------------------------------------------------------------------------------------
  if (file.exists('c:/r/my_pc.txt')) {
    MyPC = as.character(readLines('c:/r/my_pc.txt'))
    # MyPC = as.character(try(fread("C:/R/my_pc.txt", header = F)))
  } else {
    MyPC = paste0('GUEST ', sample(1:100, 1))
  }
  
  FileName = 'CHECK_MYPC.rds'
  pFolder  = paste0(SData, 'MONITOR/')
  ODDrive_Data = paste0(ODDrive, substr(pFolder, 1, 1), word(pFolder, 2, 2, ":"))
  CATln(ODDrive_Data)
  
  if (!file.exists(ODDrive_Data)) {
    try(dir.create(ODDrive_Data))
  }
  
  FullPath_Local = paste0(pFolder, FileName)
  FullPath_ODDrive = paste0(ODDrive_Data, FileName)
  
  Has_Config      = ifelse(file.exists('c:/r/config.r'), 'EXIST', 'NOT EXIST')
  Has_MyPC        = ifelse(file.exists('c:/r/my_pc.txt'), 'EXIST', 'NOT EXIST')
  Has_Py_Download = ifelse(file.exists('c:/python/savepage_nobrowser.py'), 'EXIST', 'NOT EXIST')
  Has_CCPR        = if_else(file.exists(SData), 'EXIST', 'NOT EXIST')
  Has_Udata       = if_else(file.exists(UData), 'EXIST', 'NOT EXIST')
  Has_SDrive      = if_else(file.exists('S:/'), 'EXIST', 'NOT EXIST')
  Has_RDrive      = if_else(file.exists('R:/'), 'EXIST', 'NOT EXIST')
  Has_UDrive      = if_else(file.exists('U:/'), 'EXIST', 'NOT EXIST')
  Has_TDrive      = if_else(file.exists('T:/'), 'EXIST', 'NOT EXIST')
  
  Data_One = data.table(
    mypc        = MyPC,
    date        = Sys.Date(),
    updated     = substr(Sys.time(), 1, 19),
    config      = Has_Config,
    my_pc       = Has_MyPC,
    py_download = Has_Py_Download,
    SDATA       = Has_CCPR,
    UDATA       = Has_Udata,
    SDRIVE      = Has_SDrive,
    RDRIVE      = Has_RDrive,
    TDRIVE      = Has_TDrive,
    UDRIVE      = Has_UDrive
  )
  
  Data_One = Data_One[, .SD, .SDcols = c("mypc", "date", "updated", list_check)]
  Data_One[, status:= ifelse(apply(Data_One[, ..list_check], 1, function(row) all(row == "EXIST")), "OK", "NOT_OK")]
  
  if (Data_One$status == 'NOT_OK'){
    columns_with_N = sapply(Data_One[,..list_check], function(col) any(col == "NOT EXIST"))
    columns_with_N = c(names(Data_One[,!..list_check]), names(columns_with_N[columns_with_N]))
    if(length(columns_with_N) > 0){
      list_alert = setDT(fread("S:/REPORTS/list_alert.txt"))
      list_alert = list_alert[category=="ALERT BATCHS" & active==1]
      
      data = Data_One[,..columns_with_N]
      
      sbj = "URGENCY ALERT DRIVES"
      Text_prd = paste0(kable(data, format = "markdown"), collapse = "<br/>")
      Text = paste0(sbj, "<br/>", "<pre>", Text_prd, "</pre>")
      
      try(TELEGRAM_SEND_MSG(ChatID = paste0(list_alert[send_type=="TELEGRAM"]$receive_telegramid,collapse = ","), Text = gsub('<br/>','\n',Text),
                            TokenUser = 'VAYmbSr6CV9y2i8MrIoTutscFeAQUsl3yoAymZrchg2PdgrHEKsvdzuPwm0kzJW1'))
      
      try(GMAIL_SEND_MSG(From = list_alert[send_type=="GMAIL"][1]$user_name, 
                         To = paste0(list_alert[send_type=="GMAIL"]$receive_name,collapse = ";"),
                         Subject = sbj, Content = Text))
      
      try(SKYPE_SEND_MSG(ConversationID = list("19:962e2d85025b4d1882465b577faa2198@thread.v2"),
                         Message = Text))
    }
  }
  check_drives = DBL_CCPR_READRDS("S:/REPORTS/","check_drives.rds")
  check_drives[,mypc:=toupper(mypc)]
  check_drives = unique(rbind(check_drives, Data_One[,mypc:=toupper(mypc)], fill=T)[order(-updated)], by="mypc")
  check_drives = UPDATE_UPDATED(check_drives)
  library(fst)
  write_fst(check_drives[order(status)], "S:/SHINY/REPORT/DRIVES/check_drives.fst")
  CCPR_SAVERDS(check_drives[order(status)], "S:/REPORTS/","check_drives.rds")
}

TELEGRAM_SEND_MSG = function(ChatID = '-4511417947', Text = '<strong>HELLO</strong>',
                             TokenUser = 'VAYmbSr6CV9y2i8MrIoTutscFeAQUsl3yoAymZrchg2PdgrHEKsvdzuPwm0kzJW1'){
  # param = paste0('{\"chat_ids\":\"',ChatID,'\",\"text\":\"',Text,'\",\"token_user\":\"',TokenUser,'\"}')
  param = list( chat_ids = ChatID, text = Text, token_user = TokenUser)
  header = c("Content-Type" = "application/json")
  x = content(POST("https://dashboardlive.ccpi.vn/api/send-telegram",body = param,
                   add_headers(header), encode = "json"),"text",encoding = "UTF-8")
  print(x)
  return(x)
}

GMAIL_SEND_MSG = function(From="ngvan.beqholdings@gmail.com", To="beqholdingsgroup@googlegroups.com",
                          Subject="Test",Content="Test"){
  # param = '{\"from\":\"ngvan.beqholdings@gmail.com\",\"to\":\"buuhuynh.beqholdings@gmail.com\",\"subject\":\"Testsendmailapi\",\"message\":\"TradeableindexVIX\",\"token\":\"vAymbSr6CV9y2i8MrIoTutscFeAQUsl3yoAymZrchg2PdgrHEKsvdzuPwm0kzJW1\"}'
  param = paste0('{\"from\":\"',From,'\",\"to\":\"',To,'\",\"subject\":\"',Subject,'\",\"message\":\"',Content,
                 '\",\"token\":\"vAymbSr6CV9y2i8MrIoTutscFeAQUsl3yoAymZrchg2PdgrHEKsvdzuPwm0kzJW1\"}')
  
  x = content(POST("https://dashboardlive.ccpi.vn/api/send-mail",body = param,
                   add_headers("Content-Type" = "application/json")),"text",encoding = "UTF-8")
  print(x)
  return(x)
}

SKYPE_SEND_MSG = function(ConversationID = list("29:1lsNr5SkPyc9ScpQpCOJb95Or8S7qOjBEPdc0PKm8nLujUmoKIDQzDDjo7UbpKU1a",
                                                "19:962e2d85025b4d1882465b577faa2198@thread.v2"),
                          Message = "Test", BotID = "a2c413c7-75a6-4f6b-80b3-a561649de35b"){
  # param = paste0('{\"chat_ids\":\"',ChatID,'\",\"text\":\"',Text,'\",\"token_user\":\"',TokenUser,'\"}')
  param = list( conversation_id = ConversationID, message = Message, 
                service_url = "https://smba.trafficmanager.net/apis/",
                bot_id = BotID)
  header = c("Content-Type" = "application/json")
  x = content(POST("https://dashboardlive.ccpi.vn/api/messages",body = param,
                   add_headers(header), encode = "json"),"text",encoding = "UTF-8")
  print(x)
  return(x)
}

#===================================================================================================
CHECK_PC = function(pMinutes = 30){
  # ------------------------------------------------------------------------------------------------
  # list("config","my_pc", "py_download", "SData", "udata", "sdrive","tdrive", "udrive")
  check_dri = try(CCPR_READRDS("S:/REPORTS/", "check_drives.rds"))
  
  check_dri[, check_time := Sys.time()]     
  check_dri[, delay_mins := as.numeric(difftime(as.POSIXct(substr(check_time,1,19)),
                                                as.POSIXct(substr(updated,1,19)), units = "mins"))]
  
  check_dri[, status:= ifelse(delay_mins < pMinutes, "OK", "FROZEN")]
  check_pc = check_dri[,.(mypc, last_activity_updated = updated, check_time = substr(check_time,1,19), delay_mins, status)]
  
  pc_alerted = try(CCPR_READRDS("S:/REPORTS/", "check_pc.rds"))
  
  check_pc[,timestampvn_alert:=pc_alerted[!is.na(timestampvn_alert)][1]$timestampvn_alert]
  
  check_pc = check_pc[!mypc %in% c("IFRC-TV")]
  
  if (nrow(check_pc[status == 'FROZEN']) > 0 & difftime(Sys.time(), check_pc[1]$timestampvn_alert, units = "sec")>30*60)
  {
    list_alert = setDT(fread("S:/REPORTS/list_alert.txt"))
    list_alert = list_alert[category=="ALERT BATCHS" & active==1]
    
    data = check_pc[status == 'FROZEN', -c("timestampvn_alert")]
    sbj = "URGENCY ALERT PC"
    Text_prd = paste0(kable(data, format = "markdown"), collapse = "<br/>")
    Text = paste0(sbj, "<br/>", "<pre>", Text_prd, "</pre>")
    
    # try(TELEGRAM_SEND_MSG(ChatID = paste0(list_alert[send_type=="TELEGRAM"]$receive_telegramid,collapse = ","), Text = gsub('<br/>','\n',Text),
    #                       TokenUser = 'VAYmbSr6CV9y2i8MrIoTutscFeAQUsl3yoAymZrchg2PdgrHEKsvdzuPwm0kzJW1'))
    
    # try(GMAIL_SEND_MSG(From = list_alert[send_type=="GMAIL"][1]$user_name, 
    #                    To = paste0(list_alert[send_type=="GMAIL"]$receive_name,collapse = ";"),
    #                    Subject = sbj, Content = Text))
    
    try(SKYPE_SEND_MSG(ConversationID = list("19:962e2d85025b4d1882465b577faa2198@thread.v2"),
                       Message = Text))
    
    check_pc[, timestampvn_alert:=SYS.TIME()]
  }
  
  check_pc = UPDATE_UPDATED(check_pc)
  library(fst)
  write_fst(check_pc[order(status)], "S:/SHINY/REPORT/PC/check_pc.fst")
  CCPR_SAVERDS(check_pc[order(status)], "S:/REPORTS/","check_pc.rds")
  
  return(check_pc)
}


TRAINEE_FINAL_SOURCES_DATASET = function(pData=data.table(code="",date="",source="",close=""),
                                         Dataset="DIVIDEND", KeySpread="source", ValueSpread="close",
                                         ExcludeCols = c("code","date")){
  dt_dataset = setDT(spread(pData, key = KeySpread, value = ValueSpread))
  x =  select(dt_dataset, -ExcludeCols)
  SOURCE_DATASET = as.character( as.list(names(x)))
  dt_dataset[, final := apply(.SD, 1, function(x) {
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
  }), .SDcols =SOURCE_DATASET]
  dt_dataset = dt_dataset[!is.na(final)][,":="(dataset = toupper(Dataset))]
  return(dt_dataset)
}

DBL_INTEGRATE_BY_SOURCE_PERIOD = function(List_Src = c("CNBC", "CBOE", "YAH", 'DNSE'), Period = "INTRADAY", IntegrateTo = "DAY"){
  # List_Src = c("CNBC", "CBOE", "YAH", "DNSE")
  # Period = "LAST"
  # IntegrateTo = "DAY"
  for (isrc in 1:length(List_Src)) {
    dt_notop = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_',List_Src[isrc],'_notop_',Period,'_today.rds'))
    if (nrow(dt_notop)>0)
    {
      if (Period=="DAY") { dt_notop[,timestamp:=date] }
      dt_notop = CLEAN_TIMESTAMP(dt_notop)
    }
    
    dt_top = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_',List_Src[isrc],'_top_',Period,'_today.rds'))
    if (nrow(dt_top)>0)
    {
      if (Period=="DAY") { dt_top[,timestamp:=date] }
      dt_top = CLEAN_TIMESTAMP(dt_top)
    }
    
    dt_ins = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_',List_Src[isrc],'_ins_',Period,'_today.rds'))
    if (nrow(dt_ins)>0)
    {
      if (Period=="DAY") { dt_ins[,timestamp:=date] }
      dt_ins = CLEAN_TIMESTAMP(dt_ins)
    }
    
    dt_ins = unique(rbind(dt_ins, dt_top, dt_notop, fill=T), by = c("code","timestamp"))
    # DBL_CCPR_SAVERDS(dt_ins, 'S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_',List_Src[isrc],'_ins_',Period,'_today.rds'))
    # dt_ins = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_',List_Src[isrc],'_ins_',Period,'_today.rds'))
    
    if (Period!="LAST")
    {
      dt_int = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_',List_Src[isrc],'_ins_',Period,'_',IntegrateTo,'.rds'))
      dt_int = CLEAN_TIMESTAMP(dt_int)
      
      switch (IntegrateTo,
              "DAY" = {
                if (Period=="INTRADAY")
                {
                  dt_int = unique(rbind(dt_ins, dt_int, fill=T), by = c("code","timestamp"))
                  dt_day = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_',List_Src[isrc],'_ins_day_',IntegrateTo,'.rds'))
                  dt_day[,mindate:=min(date),by = "code"]
                  dt_int = merge(dt_int[,-c("mindate")], unique(dt_day,by = "code")[,.(code, mindate)],by = "code",all.x=T)
                  dt_int = dt_int[date>=mindate]
                }else{
                  dt_int = unique(rbind(dt_ins, dt_int, fill=T), by = c("code","date"))[order(-date,code)]
                  dt_int[,nbdate:=seq(1,.N),by = "code"]
                  dt_int = dt_int[nbdate<=5][order(code,date)]
                }
              },
              "HISTORY" = {
                if (Period=="INTRADAY")
                {
                  dt_int = unique(rbind(dt_ins, dt_int, fill=T), by = c("code","timestamp"))
                }else{
                  dt_int = unique(rbind(dt_ins, dt_int, fill=T), by = c("code","date"))[order(code,date)]
                  dt_int = dt_int[!is.na(code) & !is.na(date)]
                }
              }
      )
      dt_int = UPDATE_UPDATED(dt_int[,-c('updated')])
      DBL_CCPR_SAVERDS(dt_int, 'S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_',List_Src[isrc],'_ins_',Period,'_',IntegrateTo,'.rds'),pSleep = 1, SaveOneDrive = T)
    }
  }
}

DBL_MERGE_INS_FREQUENCY = function(List_Src = c("CNBC", "CBOE", "YAH"), Period1 = "INTRADAY", Period2 = "HISTORY"){
  # List_Src = c("CNBC", "CBOE", "YAH", "DNSE")
  # Period1 = "LAST"
  # Period2 = "TODAY"
  dt_mlt = list()
  for (isrc in 1:length(List_Src)) {
    dt_notop = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_',List_Src[isrc],'_notop_',Period1,'_',Period2,'.rds'))
    if (nrow(dt_notop)>0)
    {
      if (Period1=="DAY") { dt_notop[,timestamp:=date] }
      dt_notop = CLEAN_TIMESTAMP(dt_notop)
    }
    
    dt_top = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_',List_Src[isrc],'_top_',Period1,'_',Period2,'.rds'))
    if (nrow(dt_top)>0)
    {
      if (Period1=="DAY") { dt_top[,timestamp:=date] }
      dt_top = CLEAN_TIMESTAMP(dt_top)
    }
    
    dt_ins = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_',List_Src[isrc],'_ins_',Period1,'_',Period2,'.rds'))
    if (Period1=="DAY") { dt_ins[,timestamp:=date] }
    dt_ins = CLEAN_TIMESTAMP(dt_ins)
    
    dt_ins = rbind(dt_ins, dt_top, dt_notop, fill=T)
    if ("codesource" %in% names(dt_ins)) { dt_ins[codesource=="@SP.1",code:="FUTESC1"] }
    dt_ins = unique(dt_ins, by = c("code","timestamp"))
    # dt_ins = unique(rbind(dt_top, fill=T), by = c("code","timestamp"))
    
    dt_mlt[[isrc]] = dt_ins
  }
  dt_mlt = rbindlist(dt_mlt, fill=T)
  dt_mlt = (dt_mlt[!is.na(timestamp) & !is.na(code)])
  
  if (nrow(dt_mlt)>0)
  {
    DBL_CCPR_SAVERDS(dt_mlt, 'S:/CCPR/DATA/DASHBOARD_LIVE/', paste0('dbl_multi_ins_',Period1,'_',Period2,'.rds'),pSleep = 1, SaveOneDrive = T)
    
    switch (Period1,
            "DAY" = {
              dt_src = unique(dt_mlt[,.(code, date = as.Date(timestamp))], by = c("code","date"))
              list_cols = c("open","high","low","close")
              for (icol in 1:length(list_cols)) {
                dt_col = dt_mlt[order(code,timestamp)]
                setnames(dt_col, list_cols[icol], "column")
                # dt_col = TRAINEE_FINAL_SOURCES_DATASET(pData=dt_col[,. (source, date, code, column)],
                #                                        Dataset=toupper(list_cols[icol]), KeySpread="source", ValueSpread="column",
                #                                        ExcludeCols = c("code","date"))
                
                dt_col = FINAL_VALUE(DATA_ALL = dt_mlt, DATASET = toupper(list_cols[icol]), order_list = list('CNBC', 'CBOE', 'YAH', 'DNSE',"NIKKEI", "BINANCE", "MCT", "STOXX"))
                
                dt_col[, final:=as.numeric(final)]
                
                dt_src = merge(dt_src, dt_col[,.(code, date, final)], by = c("code","date"), all.x=T)
                setnames(dt_src, "final", list_cols[icol])
                
              }
              dt_src = dt_src[order(code, date)]
              dt_src[,":="(timestamp = date, pclose = shift(close), change = close - shift(close),
                           rt = close/shift(close) -1, varpc = 100*(close/shift(close) -1)),by = "code"]
              dt_src = CLEAN_TIMESTAMP(dt_src)
              
              data_old = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', paste0('dbl_source_ins_',Period1,'_',Period2,'.rds'))))
              if (nrow(data_old) > 0){
                dt_src = MERGE_DATASTD_BYCODE(dt_src, pOption = 'FINAL')
                common_cols = intersect(names(dt_src), names(data_old))
                dt_src = dt_src[, ..common_cols]
                data_old = unique(rbind(data_old, dt_src, fill = T), by = c('code','date'))   
                data_old = UPDATE_UPDATED(data_old[,-c('updated')])
                if (Period2 == 'HISTORY'){
                  data_old = data_old[,..STRUCTURE_DAY_HISTORY]
                }
                DBL_CCPR_SAVERDS(data_old, 'S:/CCPR/DATA/DASHBOARD_LIVE/', paste0('dbl_source_ins_',Period1,'_',Period2,'.rds'), ToSummary = T, SaveOneDrive = T,pSleep = 1)
              }
              
            },
            "INTRADAY" = {
              dt_mlt[nchar(timestamp)<12, timestamp:=paste(timestamp,"00:00:00")]
              dt_mlt[,":="(timestamp = as.POSIXct(timestamp))]
              
              dt_src = unique(dt_mlt[order(-timestamp)], by = c("code","timestamp"))[order(code, timestamp)]
              dt_src = CLEAN_TIMESTAMP(dt_src)
              
              data_old = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', paste0('dbl_source_ins_',Period1,'_',Period2,'.rds'))))
              if (nrow(data_old) > 0){
                dt_src = MERGE_DATASTD_BYCODE(dt_src, pOption = 'FINAL')
                common_cols = intersect(names(dt_src), names(data_old))
                dt_src = dt_src[, ..common_cols]
                
                data_new = unique(rbind(data_old, dt_src, fill = T), by = c('code', 'timestamp'))    
                data_date = unique(data_new, by=c("code","date"))[order(-date)]
                data_date[, seqdate:=seq(1,.N), by="code"]
                data_new = merge(data_new, data_date[,.(code, date, seqdate)], by = c("code","date"), all.x=T)
                data_new = data_new[seqdate<=5]
                data_new[,":="(open = as.numeric(open), high = as.numeric(high), low = as.numeric(low), close = as.numeric(close))]
                
                # data_old = data_old[!(code=="FUTESC1" & source=="YAH" & date>="2024-09-11")]
                # data_old = data_old[!(code=="FUTNQC1" & source=="CNBC" & date>="2024-09-11")]
                # data_old = data_old[!(code=="FUTINDYMC1" & source=="CNBC" & date>="2024-09-11")]
                
                data_new = UPDATE_UPDATED(data_new[,-c('updated','seqdate')])
                
                DBL_CCPR_SAVERDS(data_new, 'S:/CCPR/DATA/DASHBOARD_LIVE/', paste0('dbl_source_ins_',Period1,'_',Period2,'.rds'), ToSummary = T, SaveOneDrive = T,pSleep = 1)
                
              }
            },
            "LAST" = {
              dt_mlt[nchar(timestamp)<12, timestamp:=paste(timestamp,"00:00:00")]
              dt_mlt[,":="(timestamp = as.POSIXct(timestamp))]
              
              dt_src = unique(dt_mlt[order(-timestamp)], by = c("code"))
              dt_src = CLEAN_TIMESTAMP(dt_src)
              
              data_old = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', paste0('dbl_source_ins_',Period1,'_',Period2,'.rds'))))
              if (nrow(data_old) > 0){
                dt_src = MERGE_DATASTD_BYCODE(dt_src, pOption = 'FINAL')
                common_cols = intersect(names(dt_src), names(data_old))
                dt_src = dt_src[, ..common_cols]
                data_old = unique(rbind(dt_src,data_old, fill = T)[order(-timestamp)], by = c('code'))    
                data_old = UPDATE_UPDATED(data_old[,-c('updated')])
                
                DBL_CCPR_SAVERDS(data_old, 'S:/CCPR/DATA/DASHBOARD_LIVE/', paste0('dbl_source_ins_',Period1,'_',Period2,'.rds'), ToSummary = T, SaveOneDrive = T,pSleep = 1)
              }
            }
    )
  }
}

# 
# DBL_MERGE_INS_FREQUENCY = function(List_Src = c("CNBC", "CBOE", "YAH"), Period1 = "DAY", Period2 = "HISTORY"){
#   # List_Src = c("CNBC", "CBOE", "YAH", "DNSE")
#   # Period1 = "DAY"
#   # Period2 = "HISTORY"
#   dt_mlt = list()
#   for (isrc in 1:length(List_Src)) {
#     dt_notop = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_',List_Src[isrc],'_notop_',Period1,'_',Period2,'.rds'))
#     if (nrow(dt_notop)>0)
#     {
#       if (Period1=="DAY") { dt_notop[,timestamp:=date] }
#       dt_notop = CLEAN_TIMESTAMP(dt_notop)
#     }
#     
#     dt_top = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_',List_Src[isrc],'_top_',Period1,'_',Period2,'.rds'))
#     if (nrow(dt_top)>0)
#     {
#       if (Period1=="DAY") { dt_top[,timestamp:=date] }
#       dt_top = CLEAN_TIMESTAMP(dt_top)
#     }
#     
#     dt_ins = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',  paste0('dbl_',List_Src[isrc],'_ins_',Period1,'_',Period2,'.rds'))
#     if (Period1=="DAY") { dt_ins[,timestamp:=date] }
#     dt_ins = CLEAN_TIMESTAMP(dt_ins)
#     
#     dt_ins = rbind(dt_ins, dt_top, dt_notop, fill=T)
#     if ("codesource" %in% names(dt_ins)) { dt_ins[codesource=="@SP.1",code:="FUTESC1"] }
#     dt_ins = unique(dt_ins, by = c("code","timestamp"))
#     # dt_ins = unique(rbind(dt_top, fill=T), by = c("code","timestamp"))
#     
#     dt_mlt[[isrc]] = dt_ins
#   }
#   dt_mlt = rbindlist(dt_mlt, fill=T)
#   dt_mlt = (dt_mlt[!is.na(timestamp) & !is.na(code)])
#   
#   if (nrow(dt_mlt)>0)
#   {
#     DBL_CCPR_SAVERDS(dt_mlt, 'S:/CCPR/DATA/DASHBOARD_LIVE/', paste0('dbl_multi_ins_',Period1,'_',Period2,'.rds'))
#     
#     switch (Period1,
#             "DAY" = {
#               dt_src = unique(dt_mlt[,.(code, date = as.Date(timestamp))], by = c("code","date"))
#               list_cols = c("open","high","low","close")
#               for (icol in 1:length(list_cols)) {
#                 dt_col = dt_mlt[order(code,timestamp)]
#                 setnames(dt_col, list_cols[icol], "column")
#                 dt_col = TRAINEE_FINAL_SOURCES_DATASET(pData=dt_col[,. (source, date, code, column)],
#                                                        Dataset=toupper(list_cols[icol]), KeySpread="source", ValueSpread="column",
#                                                        ExcludeCols = c("code","date"))
#                 dt_col[, final:=as.numeric(final)]
#                 
#                 dt_src = merge(dt_src, dt_col[,.(code, date, final)], by = c("code","date"), all.x=T)
#                 setnames(dt_src, "final", list_cols[icol])
#                 
#               }
#               dt_src = dt_src[order(code, date)]
#               dt_src[,":="(timestamp = date, pclose = shift(close), change = close - shift(close),
#                            rt = close/shift(close) -1, varpc = 100*(close/shift(close) -1)),by = "code"]
#             },
#             "INTRADAY" = {
#               dt_mlt[nchar(timestamp)<12, timestamp:=paste(timestamp,"00:00:00")]
#               dt_mlt[,":="(timestamp = as.POSIXct(timestamp))]
#               
#               dt_src = unique(dt_mlt[order(-timestamp)], by = c("code","timestamp"))[order(code, timestamp)]
#               dt_src = CLEAN_TIMESTAMP(dt_src)
#             },
#             "LAST" = {
#               dt_mlt[nchar(timestamp)<12, timestamp:=paste(timestamp,"00:00:00")]
#               dt_mlt[,":="(timestamp = as.POSIXct(timestamp))]
#               
#               dt_src = unique(dt_mlt[order(-timestamp)], by = c("code"))
#               dt_src = CLEAN_TIMESTAMP(dt_src)
#             }
#     )
#     DBL_CCPR_SAVERDS(dt_src, 'S:/CCPR/DATA/DASHBOARD_LIVE/', paste0('dbl_source_ins_',Period1,'_',Period2,'.rds'))
#   }
# }

#===================================================================================================
TRAINEE_CALCULATE_STKVN_LIQUIDITY = function (pData = data.table(), pFolder = 'S:/STKVN/PRICES/FINAL/',
                                              pFile   = 'IFRC_CCPR_STKVN_FOR_DASHBOARD.rds', EndDate  = SYSDATETIME(hour(Sys.time())), ToUpload = T, ToSave = T)  {
  # ------------------------------------------------------------------------------------------------
  #updated: 2024-06-04 18:55
  # pFolder='S:/STKVN/PRICES/FINAL/' ; pFile="IFRC_CCPR_STKVN_FOR_DASHBOARD.rds"
  # pData = data.table()
  # pFolder = 'S:/STKVN/INDEX_2024/'
  # pFile   = 'beq_indenyals_history.rds'
  # EndDate  = SYSDATETIME(hour(Sys.time()))
  if (nrow(pData ) > 1 ) 
  { 
    x = pData
    if ( !'close_adj' %in% colnames(pData) ) { x [, close_adj := close]}
    if ( !'code' %in% colnames(pData) ) { x [, code := codesource]}
  } else {
    if (pFile == 'ifrc_ccpr_investment_history.rds') {
      investment = DBL_CCPR_READRDS(pFolder, pFile) [order (-date)]
      investment [, rt:=((close/shift(close))-1)*100, by='code']
      investment [, ':=' (cur = NA, close_adj = close)]
      x = investment
    } else { 
      x = DBL_CCPR_READRDS(pFolder, pFile, ToRestore = T)
      My.Kable(x[order(date)])
      if ( !'close_adj' %in% colnames(x) ) { x [, close_adj := close]}
      if ( !'code' %in% colnames(x) ) { x [, code := codesource]}
    }
  }
  
  # STEP 1: READ FILE PRICES HISTORY
  # My.Kable(x)
  # colnames(x)
  GC_SILENT()
  # cleanse raw data
  x = unique(x, by = c('code', 'date'))
  
  # STEP 2: RE-CALCULATE DATA
  # Limit data maxdate
  Ind_History = x  [date <= EndDate]  
  My.Kable(Ind_History[, .(code, date, close)][order(date)])
  str(Ind_History)
  # Ind_History = Ind_History[code == 'STKVNAAA']
  #Calculate necessary column: change, varpc, yyyy, yyyymm, yyyyqn
  Ind_History = Ind_History[order(code, date)] 
  if (all (! c('change', 'varpc') %in% names (Ind_History))) {
    Ind_History[, ':='(change=close_adj-shift(close_adj), varpc=100*(close_adj/shift(close_adj)-1)), by='code']
  }
  Ind_History[,":="(yyyy=year(date),yyyymm=year(date)*100+month(date),yyyyqn=year(date)*100+floor((month(date)-1)/3)+1)]
  Ind_History = Ind_History[order(code, date)]
  My.Kable(Ind_History[, .(code, date, volume, sharesout, yyyy, yyyymm, yyyyqn)])
  
  Ind_History[, rt:=(close_adj/shift(close_adj))-1, by='code']
  Ind_History[, lnrt:=log(close_adj/shift(close_adj)), by='code']
  My.Kable(Ind_History[, .(code, date, yyyymm, yyyy, close_adj, rt, lnrt)])
  
  Ind_Dates = unique(Ind_History[, .(date, yyyy, yyyyqn, yyyymm)], by='date')[order(-date)]
  My.Kable(Ind_Dates)
  
  Ind_Dates_MM = unique(Ind_Dates[order(-date)], by='yyyymm')
  Ind_Dates_MM[, MM_NR:=seq.int(1, .N)]
  
  Ind_Dates_QQ = unique(Ind_Dates[order(-date)], by='yyyyqn')
  Ind_Dates_QQ[, QQ_NR:=seq.int(1, .N)]
  
  Ind_Dates_YY = unique(Ind_Dates[order(-date)], by='yyyy')
  Ind_Dates_YY[, YY_NR:=seq.int(1, .N)]
  
  
  Ind_History = merge(Ind_History[, -c('MM_NR')], Ind_Dates_MM[, .(yyyymm, MM_NR)], by='yyyymm')
  Ind_History = merge(Ind_History[, -c('QQ_NR')], Ind_Dates_QQ[, .(yyyyqn, QQ_NR)], by='yyyyqn')
  Ind_History = merge(Ind_History[, -c('YY_NR')], Ind_Dates_YY[, .(yyyy, YY_NR)], by='yyyy')
  # My.Kable(Ind_Overview[, .(code, date, yyyymm, yyyy, close_adj, rt, lnrt, MM_NR, QQ_NR, YY_NR)])
  
  
  Ind_Overview = unique(Ind_History[order(code, -date)], by=c('code')) #[, .(code, name, date, last=close_adj, last_change, last_varpc)]
  
  My.Kable(Ind_Overview[, .(code, date, yyyymm, yyyy, close_adj, rt, lnrt, MM_NR, QQ_NR, YY_NR)][order(code, -date)])
  
  #Merge data: month, quarter, year
  MinDays = 1
  # Ind_History[, LIQUID_MM := NULL]
  Ind_History[is.na(volume), volume := 0, by = c('code','date')]
  Ind_History[, velocity := volume/sharesout*100*250, by = c('code','date')]
  
  VOLAT_MM = setDT(Ind_History %>%
                     group_by(code, MM_NR) %>%
                     summarise(avg_velocity = mean(velocity, na.rm = TRUE)))
  VOLAT_MM[, n := .N, by = c('code','MM_NR')]
  
  VOLAT_MM[n<MinDays, avg_velocity:=as.numeric(NA)]
  My.Kable(VOLAT_MM[order(MM_NR, code)])
  
  VOLAT_6MM = VOLAT_MM[MM_NR <= 6][order(code, MM_NR)]
  My.Kable(setDT(spread(VOLAT_6MM[, .(code,  nr = paste0('liquid_M',(MM_NR)), avg_velocity)], key='nr', value='avg_velocity'))[order(-liquid_M1)])
  Res_Month = setDT(spread(VOLAT_6MM[, .(code,  nr = paste0('liquid_M',(MM_NR)), avg_velocity)], key='nr', value='avg_velocity'))[order(-liquid_M1)]
  # setnames(Res_Month, "liquid_M0", "liquid_mtd")
  Res_Month[, liquid_mtd := liquid_M1]
  My.Kable(Res_Month)
  cols = setdiff(names(Res_Month),'code')
  
  Res_Month[, paste0(cols, "_color") := lapply(.SD, function(x) {
    x[is.na(x)] = 0  
    rank_order = rank(x, ties.method = "min")  
    group = cut(rank_order, breaks = 3, labels = c("EF4444", "FDA124", "357A38"))
    group[x == 0] = NA  
    return(group)
  }), .SDcols = cols]
  
  VOLAT_QQ = setDT(Ind_History %>%
                     group_by(code, QQ_NR) %>%
                     summarise(avg_velocity = mean(velocity, na.rm = TRUE)))
  VOLAT_QQ[, n := .N, by = c('code','QQ_NR')]
  
  VOLAT_QQ[n<MinDays, avg_velocity:=as.numeric(NA)]
  My.Kable(VOLAT_QQ[order(QQ_NR, code)])
  
  VOLAT_6QQ = VOLAT_QQ[QQ_NR <= 6][order(code, QQ_NR)]
  My.Kable(setDT(spread(VOLAT_6QQ[, .(code,  nr = paste0('liquid_Q',(QQ_NR)), avg_velocity)], key='nr', value='avg_velocity'))[order(-liquid_Q1)])
  Res_Quarter = setDT(spread(VOLAT_6QQ[, .(code,  nr = paste0('liquid_Q',(QQ_NR)), avg_velocity)], key='nr', value='avg_velocity'))[order(-liquid_Q1)]
  # setnames(Res_Quarter, "liquid_Q0", "liquid_qtd")
  Res_Quarter[, liquid_qtd := liquid_Q1]
  
  My.Kable(Res_Quarter)
  
  cols = setdiff(names(Res_Quarter),list('code'))
  
  Res_Quarter[, paste0(cols, "_color") := lapply(.SD, function(x) {
    x[is.na(x)] = 0  
    rank_order = rank(x, ties.method = "min")  
    group = cut(rank_order, breaks = 3, labels = c("EF4444", "FDA124", "357A38"))
    group[x == 0] = NA  
    return(group)
  }), .SDcols = cols]
  
  VOLAT_YY = setDT(Ind_History %>%
                     group_by(code, YY_NR) %>%
                     summarise(avg_velocity = mean(velocity, na.rm = TRUE)))
  VOLAT_YY[, n := .N, by = c('code','YY_NR')]
  
  VOLAT_YY[n<MinDays, avg_velocity:=as.numeric(NA)]
  My.Kable(VOLAT_YY[order(YY_NR, code)])
  
  VOLAT_6YY = VOLAT_YY[YY_NR <= 6][order(code, YY_NR)]
  My.Kable(setDT(spread(VOLAT_6YY[, .(code,  nr = paste0('liquid_Y',(YY_NR)), avg_velocity)], key='nr', value='avg_velocity'))[order(-liquid_Y1)])
  Res_Year = setDT(spread(VOLAT_6YY[, .(code,  nr = paste0('liquid_Y',(YY_NR)), avg_velocity)], key='nr', value='avg_velocity'))[order(-liquid_Y1)]
  # setnames(Res_Year, "liquid_Y0", "liquid_ytd")
  Res_Year[, liquid_ytd := liquid_Y1]
  
  My.Kable(Res_Year)
  
  cols = setdiff(names(Res_Year),'code')
  
  Res_Year[, paste0(cols, "_color") := lapply(.SD, function(x) {
    x[is.na(x)] = 0  
    rank_order = rank(x, ties.method = "min")  
    group = cut(rank_order, breaks = 3, labels = c("EF4444", "FDA124", "357A38"))
    group[x == 0] = NA  
    return(group)
  }), .SDcols = cols]
  
  Ind_Overview = merge(Ind_Overview, Res_Month, all.x = T, by = 'code' )
  Ind_Overview = merge(Ind_Overview, Res_Quarter, all.x = T, by = 'code' )
  Ind_Overview = merge(Ind_Overview, Res_Year, all.x = T, by = 'code' )
  
  My.Kable(Ind_Overview[, .(code, date, volume, sharesout, liquid_mtd, liquid_qtd, liquid_ytd)])
  
  ToAddRef = T
  if (ToAddRef) {
    # Ind_Data = TRAINEE_MERGE_COL_FROM_REF (pData = Ind_Overview, 
    #                                        List_Fields = c('name','cur','wgtg','prtr', 'category', 'size', 'coverage'))
    # stk_mother = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOAD_LIVE/', 'dbl_stk_mother.rds', ToRestore = T)
    # Ind_Data = merge(Ind_Data[, -c('name','cur','wgtg','prtr', 'category', 'size', 'coverage')], stk_mother[, .(name,cur,wgtg,prtr,category,size,coverage)], by = 'code', all.x = T)
    
    Ind_Data = TRAINEE_MERGE_COL_FROM_SOURCE(pSource      = 'DBL_STK_MOTHER',pData  = Ind_Overview, 
                                             Folder_Fr    = '',       File_Fr   = '' , 
                                             ToSave       = F ,       Folder_To = '' ,       File_To = '' ,
                                             List_Fields  = c('name','cur','wgtg','prtr', 'category', 'size','coverage'))
  } else { Ind_Data = Ind_All}
  
  if (!'last' %in% names (Ind_Data)) {
    Ind_Data [, last:= close]
  }
  Ind_Data [, excess := 0]
  Ind_Data [, updated := SYS.TIME()]
  My.Kable(Ind_Data[, .(code, date, volume, sharesout, liquid_mtd, liquid_qtd, liquid_ytd)])
  
  if (ToSave){
    try(DBL_CCPR_SAVERDS(Ind_Data, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_stk_liquidity.rds', ToSummary = T, SaveOneDrive = T))
  }
  if (ToUpload){
    library(data.table)
    library(DBI)
    library(RMySQL)
    library(RMariaDB)
    library(DT)
    # user = "dashboard_user_login", password = "admin@beqholdings", host = '27.71.235.71', dbname = "ccpi_dashboard_login")
    connection <- dbConnect(
      dbDriver("MySQL"),
      host = "27.71.235.71",
      port = 3306,
      user = "dashboard_user_login",
      password = "admin@beqholdings",
      db = "ccpi_dashboard_login"
    )
    # 
    # connection <- dbConnect(
    #   dbDriver("MySQL"),
    #   host = "27.71.235.71",
    #   port = 3306,
    #   user = "dashboard_user_login_data",
    #   password = "admin@beqholdings",
    #   db = "dashboard_user_login_dev"
    # )
    
    #data = readRDS("S:/SHINY/REPORT/REPORT_WCEO.rds")
    # data = readRDS("S:/STKVN/PRICES/REPORTS/process_pricesday.rds")
    #  data = readRDS('S:/WCEO_INDEXES/REPORT_WCEO.rds')
    # current_date <- Sys.Date()
    # cols_to_update <- setdiff(names(data), c("dataset", "date", "updated", "id"))
    # data[data$id == 3, cols_to_update] <- current_date
    
    My.Kable(Ind_Data)
    # data[, c("id"):=NULL]
    # data = data[, -c('pnr')]
    #result = dbWriteTable(connection, "process_wceo", data[3,], append = TRUE, row.names = FALSE)
    result = dbWriteTable(connection, 'dbl_stk_liquidity', Ind_Data, row.names = FALSE, overwrite = TRUE)
    print(result)
    dbDisconnect(connection)
  }
  
  return(Ind_Data)
}

# ==================================================================================================
DBL_STK_FIGURES_BY_FILE = function (  pFolder = 'S:/STKVN/PRICES/FINAL/', pFile = paste0( 'IFRC_CCPR_STKVN_FOR_DASHBOARD.rds' ),
                                      ListCodes = c(),
                                      ToFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', ToFile  = paste0('all_figures.rds'),
                                      FolderMerge = 'S:/CCPR/DATA/DASHBOARD_LIVE/', FileMerge = 'dbl_stk_figures.rds',
                                      ToSave = T, ToIntergration = T,ToUpload = T,pHost = "dashboard_live")  {
  # ------------------------------------------------------------------------------------------------
  # pFolder = 'S:/STKVN/PRICES/FINAL/'; pFile = paste0( 'IFRC_CCPR_STKVN_FOR_DASHBOARD.rds' )
  # pFolder = UData; pFile = paste0( 'ifrc_beq_stkin_history.rds' )
  # pFolder = UData; pFile = paste0( 'download_yah_stkhome_history.rds' )
  
  # ListCodes = c("INDVNXSECLOGCWPRVND","INDVNXSECLOGEWPRVND"); CompoFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/'; CompoFile = "dbl_ind_compo.rds"
  # ToFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/'; ToFile  = paste0('all_figures.rds')
  # FolderMerge = 'S:/CCPR/DATA/DASHBOARD_LIVE/'; FileMerge = 'dbl_stk_figures.rds'
  # ToSave = T; ToIntergration = T; ToUpload = T ; pDate = Sys.Date() ; pHost = "dashboard_live"
  # FIGURES ........................................................................................
  IND_HISTORY = DBL_CCPR_READRDS(pFolder,pFile , ToKable=T, ToRestore = T)[order(code,-date)]
  GC_SILENT()
  if ('close_unadj' %in% names(IND_HISTORY) && all(!is.na(IND_HISTORY$close_unadj))){
    IND_HISTORY[, close := close_unadj]
  } else {
    if ('close_adj' %in% names(IND_HISTORY) && all(!is.na(IND_HISTORY$close_adj))){
      IND_HISTORY[, close := close_adj]
    } 
  }
  
  ind_bcm = DBL_CCPR_READRDS("U:/EFRC/DATA/","efrc_indhome_history.rds")
  IND_HISTORY = unique(rbind(IND_HISTORY, ind_bcm[code %in% c("INDSPX")],fill=T),by=c("code","date"))
  
  VNI = DBL_CCPR_READRDS( 'S:/STKVN/INDEX_2024/','DOWNLOAD_SOURCES_INDVN_HISTORY.rds')
  VNI[code == 'INDVNINDEX']
  
  IND_HISTORY = unique(rbind(IND_HISTORY,  VNI[code == 'INDVNINDEX'],fill=T),by=c("code","date"))
  
  
  codemother = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","dbl_stk_mother.rds")
  IND_HISTORY = merge(IND_HISTORY[,-c("coverage")], codemother[,.(code,coverage)],by="code",all.x=T)
  IND_HISTORY[coverage=="VIETNAM", codebm:="INDVNINDEX"]
  IND_HISTORY[coverage=="INTERNATIONAL", codebm:="INDSPX"]
  IND_HISTORY[,maxdate:=max(date),by="code"]
  IND_HISTORY = IND_HISTORY[date<=min(IND_HISTORY[code %in% c("INDVNINDEX","INDSPX")]$maxdate)]
  IND_HISTORY[,nbd:=seq(1,.N),by="code"]
  
  IND_VNI     = IND_HISTORY[code %in% c("INDVNINDEX","INDSPX")][order(code,date)]
  
  if (any(ListCodes!="")) { IND_HISTORY = IND_HISTORY[code %in% ListCodes] }
  IND_2023    = IND_HISTORY[nbd<=250][order(code,date)]
  
  
  
  # if ('close_unadj' %in% names(IND_VNI)){
  #   IND_VNI[, close := close_unadj]
  # } 
  
  # DBL_CCPR_SAVERDS(IND_VNI,('S:/CCPR/DATA/DASHBOARD/'), 'CCPR_INDVNI_HISTORY.rds' )
  IND_VNI     = IND_VNI[order(code, date)]
  
  IND_2023[, rt:=close/shift(close)-1, by='code']
  IND_2023[, lnrt:=log(close/shift(close)), by='code']
  
  IND_VNI[, rt:=close/shift(close)-1, by='code']
  IND_VNI[, lnrt:=log(close/shift(close)), by='code']
  
  IND_2023    = merge(IND_2023[, -c('benchmark')], IND_VNI[, .(codebm=code, date, benchmark=lnrt)], all.x=T, by=c('codebm','date'))
  My.Kable(IND_2023)
  IND_2023[, dlnrt:=lnrt-benchmark]
  
  
  # IND_HISTORY = DBL_CCPR_READRDS(paste0('R:/CCPR/DATA/DASHBOARD/'), 'ccpi_dashboard_indvn_history.rds', ToKable=T)
  DB_TEMPLATE = setDT(fread(paste0('S:/LIST/', 'template_stk_figures.txt')))
  DB_TEMPLATE_EMPTY = DB_TEMPLATE
  DB_TEMPLATE_EMPTY$value=NULL
  DB_TEMPLATE_EMPTY$value=as.character('')
  str(DB_TEMPLATE_EMPTY)
  
  IND_2023_STATS = IND_2023[, .(
    volatility=sqrt(250)*100*sqrt(var(lnrt, na.rm=T)),
    tracking_error=100*sqrt(var(dlnrt, na.rm=T))
  ), by='code']
  IND_2023_STATS = IND_2023_STATS[!is.na(volatility)]
  My.Kable(IND_2023_STATS)
  
  IND_HISTORY_STATS = IND_HISTORY[,.(
    min_close=round(min(close, na.rm=T),2), 
    avg_close=round(mean(close, na.rm=T),2), 
    max_close=round(max(close, na.rm=T),2)) , by=c('code')]
  
  # IND_HISTORY[code == 'STKVNYEG', .SD[which.min(close)], by = code]
  
  temp_min_close    = IND_HISTORY[, .SD[which.min(close)], by = code]
  IND_HISTORY_STATS = merge(IND_HISTORY_STATS, temp_min_close[, .(code, min_close_date = as.character(date))], by = "code", all.x = T)
  temp_max_close    = IND_HISTORY[, .SD[which.max(close)], by = code]
  IND_HISTORY_STATS = merge(IND_HISTORY_STATS, temp_max_close[, .(code, max_close_date = as.character(date))], by = "code", all.x = T)
  IND_HISTORY_STATS = merge(IND_HISTORY_STATS[, -c('volatility', 'tracking_error')], IND_2023_STATS, all.x=T, by='code')
  IND_HISTORY_STATS[, ':=' (min_close = min_close,
                            max_close = max_close,
                            volatility  = as.character(volatility ) )]
  # IND_COMPO = DBL_CCPR_READRDS(CompoFolder, CompoFile , ToKable=T, ToRestore = T)
  FIGURES_List = list()
  IND_HISTORY_STATS = IND_HISTORY_STATS[grepl('^STK', code)][!is.na(max_close_date) & !is.infinite(max_close) & !is.nan(avg_close) & !is.infinite(min_close)]
  IND_List     = unique(IND_HISTORY_STATS[grepl('^STK', code)]$code)
  
  # IND_List = unique(y$stock_code)
  for (icode in 1:length(IND_List))
  {
    # icode = 1
    # dt_compo = unique(IND_COMPO[gsub("CW.*|EW.*","",code_mother)==gsub("CW.*|EW.*","",IND_List[icode])],by="code")
    
    pData = IND_2023[code==IND_List[icode]][!is.na(lnrt) & !is.na(benchmark) & !is.nan(lnrt) & !is.infinite(lnrt)]
    # pData = IND_2023[code=='INDGI2BBA05CWPRVND'][!is.na(lnrt) & !is.na(benchmark)]
    if ( nrow (pData) > 1) {
      fit = lm(lnrt ~ benchmark, data=pData)
      
      beta  = fit$coefficients[2] # This prints out the beta
      alpha = fit$coefficients[1] # This prints out the beta
      
      
      DB_TEMPLATE_ONE = copy(DB_TEMPLATE_EMPTY)
      DB_TEMPLATE_ONE[, figure:=trimws(as.character(figure))]
      DB_TEMPLATE_ONE[, stock_code:=IND_List[[icode]]]
      DB_TEMPLATE_ONE[, code:=IND_List[[icode]]]
      
      DB_TEMPLATE_ONE[figure=='Lowest Close',          value:=as.character(format(IND_HISTORY_STATS[icode]$min_close,big.mark=",",scientific=FALSE))]
      DB_TEMPLATE_ONE[figure=='Lowest Close Date',          value:=as.character(IND_HISTORY_STATS[icode]$min_close_date)]
      # format(IND_HISTORY_STATS[icode]$min_close,big.mark=",",scientific=FALSE)
      DB_TEMPLATE_ONE[figure=='Highest Close',         value:=as.character(format(IND_HISTORY_STATS[icode]$max_close,big.mark=",",scientific=FALSE))]
      DB_TEMPLATE_ONE[figure=='Highest Close Date',         value:=as.character(IND_HISTORY_STATS[icode]$max_close_date)]
      
      DB_TEMPLATE_ONE[figure=='Volatility',            value:=as.character(paste(round(as.numeric(IND_HISTORY_STATS[icode]$volatility),2), '%'))]
      DB_TEMPLATE_ONE[figure=='Alpha',                 value:=as.character(round(alpha,6))]
      DB_TEMPLATE_ONE[figure=='Beta',                  value:=as.character(round(beta,6))]
      
      if ('capibvnd' %in% names(pData)){
        DB_TEMPLATE_ONE[figure=='Capitalisation*', value:=paste(as.character(format(round(sum(pData[date ==max(date)]$capibvnd,na.rm = T), 3),big.mark=",",scientific=FALSE)), 'Billions VND')]
      } else {
        DB_TEMPLATE_ONE[figure=='Capitalisation*', value:=as.character(NA)]
        
      }
      
      
      My.Kable.All(DB_TEMPLATE_ONE)
      FIGURES_List[[icode]] = DB_TEMPLATE_ONE } else {  FIGURES_List[[icode]] = data.table ()}
  }
  FIGURES_ALL = rbindlist(FIGURES_List)
  
  # FIGURES_ALL_1 = FIGURES_ALL
  # # FIGURES_ALL = unique(rbind(FIGURES_ALL_1, FIGURES_ALL , fill = T), by = c('stock_code', 'figure'))
  # FIGURES_ALL[stock_code == 'STKVNAME']
  # My.Kable(FIGURES_ALL[figure == 'Volatility' & grepl('NA', value)])
  # 
  # IND_HISTORY_STATS[, stock_code := code]
  # y =  FIGURES_ALL[stock_code %in% FIGURES_ALL[figure == 'Volatility' & grepl('NA', value)]$stock_code]
  # x = IND_HISTORY_STATS[stock_code %in% FIGURES_ALL[figure == 'Volatility' & grepl('NA', value)]$stock_code]
  
  
  if (ToSave ) { CCPR_SAVERDS (FIGURES_ALL, ToFolder , ToFile) }
  # saveRDS(FIGURES_ALL, paste0('R:/CCPR/DATA/DASHBOARD/', 'FIGURES_ALL.rds'))
  My_Data = FIGURES_ALL
  if (ToIntergration) { 
    if (file.exists (paste0(FolderMerge , FileMerge)) ) {
      Data_Old = CCPR_READRDS( FolderMerge , FileMerge, ToRestore = T)
    } else {Data_Old = data.table () }
    # SPECIFICATIONS_ALL
    My_Data = unique ( rbind (Data_Old[!stock_code %in% FIGURES_ALL$stock_code], FIGURES_ALL, fill =T), fromLast = T, by = c ('figure', 'stock_code') )
    # pDate = '2024-01-12'
    My_Data = UPDATE_UPDATED(My_Data[, -c('updated')])
    CCPR_SAVERDS(My_Data, FolderMerge , FileMerge )
  }
  
  if (ToUpload) {
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = pHost, l.tablename= gsub(".rds","",FileMerge),
                                     l.filepath= paste0(FolderMerge,  FileMerge ) ,
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
  }
  
  return (My_Data)
}

# ==================================================================================================
DBL_PARTNERS_SPECIFICATIONS_BY_FILE = function (pFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', pFile = 'dbl_partners_specification.rds' ,
                                                ToFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/' , ToFile  = 'all_partners_specifications.rds',
                                                FolderMerge = 'S:/CCPR/DATA/DASHBOARD_LIVE/', FileMerge = 'dbl_partners_specification.rds',
                                                ToSave = F, ToIntergration  = F, ToUpload = F , pHost = 'dashboard_live' ) {
  # ------------------------------------------------------------------------------------------------
  # SPECIFICATIONS :
  # pFolder = UData 
  # pFile = 'ifrc_beq_stkin_history.rds' 
  library('stringi')
  ALL_HISTORY = readRDS(paste0(pFolder, pFile))
  GC_SILENT()
  DBL_RELOAD_INSREF()
  
  IND_STATS = ALL_HISTORY
  DB_TEMPLATE = setDT(fread(paste0('S:/LIST/', 'template_partners_specifications.txt')))
  
  DB_TEMPLATE_EMPTY = DB_TEMPLATE
  # DB_TEMPLATE_EMPTY$value=NULL
  # DB_TEMPLATE_EMPTY$value=as.character('')
  X_List = list()
  
  for (k in 1:nrow(IND_STATS))
  {
    # k = 1
    DB_TEMPLATE_ONE = setDT(copy(DB_TEMPLATE_EMPTY))
    DB_TEMPLATE_ONE[, company:=IND_STATS[k]$title]
    DB_TEMPLATE_ONE[field=='Title',   value:=as.character(IND_STATS[k]$title)]
    DB_TEMPLATE_ONE[field=='Address',   value:=as.character(IND_STATS[k]$Address)]
    DB_TEMPLATE_ONE[field=='Phone',   value:=as.character(IND_STATS[k]$Phone)]
    DB_TEMPLATE_ONE[field=='Email',   value:=as.character(IND_STATS[k]$Email)]
    DB_TEMPLATE_ONE[field=='Subtitle',   value:=as.character(IND_STATS[k]$subtitle)]
    
    My.Kable.All(DB_TEMPLATE_ONE)
    X_List[[k]] = DB_TEMPLATE_ONE
  }
  
  SPECIFICATIONS_ALL = rbindlist(X_List, fill = T)
  My.Kable(SPECIFICATIONS_ALL)
  # saveRDS(SPECIFICATIONS_ALL, paste0('R:/CCPR/DATA/DASHBOARD/', 'INDETF_SPECIFICATIONS_ALL.rds'))
  # DBL_CCPR_SAVERDS(SPECIFICATIONS_ALL, 'R:/CCPR/DATA/DASHBOARD/', 'INDGICS_SPECIFICATIONS_ALL.rds')
  
  
  
  if (ToSave) { 
    if (file.exists (paste0( ToFolder , ToFile )) ) {
      data_old = CCPR_READRDS(ToFolder , ToFile, ToRestore = T)
    } else {data_old = data.table () }
    
    SPECIFICATIONS_ALL = unique(rbind(SPECIFICATIONS_ALL, data_old), by = c('field','company'))
    saveRDS (SPECIFICATIONS_ALL, paste0(ToFolder , ToFile))
  }
  
  My_Data = SPECIFICATIONS_ALL
  # ToMerge =T
  if (ToIntergration) { 
    if (file.exists (paste0( FolderMerge , FileMerge )) ) {
      Data_Old = CCPR_READRDS( FolderMerge , FileMerge, ToRestore = T)
    } else {Data_Old = data.table () }
    # SPECIFICATIONS_ALL
    My_Data = unique ( rbind (Data_Old[!stock_code %in% SPECIFICATIONS_ALL$stock_code], SPECIFICATIONS_ALL, fill =T), fromLast = T, by = c ('field', 'stock_code') )
    # pDate = '2024-01-12'
    My_Data = UPDATE_UPDATED(My_Data[, -c('updated')])
    saveRDS(My_Data, paste0(FolderMerge , FileMerge))
  }
  
  if (ToUpload) {
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = pHost, l.tablename= gsub(".rds","",FileMerge),
                                     l.filepath= tolower(paste0(FolderMerge , FileMerge)),
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=T))
  }
  return (My_Data)
}

# ==================================================================================================
DBL_STKIN_SPECIFICATIONS_BY_FILE = function (pFolder = UData, pFile = 'ifrc_beq_stkin_history.rds' ,
                                             List_Codes = c(),
                                             ToFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/' , ToFile  = 'all_stkin_specifications.rds',
                                             FolderMerge = 'S:/CCPR/DATA/DASHBOARD_LIVE/', FileMerge = 'dbl_stk_specifications.rds',
                                             ToSave = F, ToIntergration  = F, ToUpload = F , pHost = 'dashboard_live' ) {
  # ------------------------------------------------------------------------------------------------
  # SPECIFICATIONS :
  # pFolder = UData 
  # pFile = 'download_yah_stkhome_history.rds' 
  library('stringi')
  ALL_HISTORY = DBL_CCPR_READRDS(pFolder, pFile, ToKable=T, ToRestore = T)
  GC_SILENT()
  if (length (List_Codes)>0) {IND_HISTORY = ALL_HISTORY [code %in% List_Codes]} else {
    IND_HISTORY = unique(ALL_HISTORY[, .(code, ticker = codesource)])
  }
  DBL_RELOAD_INSREF()
  # IND_HISTORY = DBL_CCPR_READRDS(paste0('R:/CCPR/DATA/DASHBOARD/'), 'ifrc_ccpr_ind_gics_prices.rds', ToKable=T, ToRestore = T)
  stkmother = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',"dbl_stk_mother.rds")
  # IND_HISTORY = merge(IND_HISTORY, indmother[,.(code,new_name = name,new_shortname = short_name,prtr = version,wgtg,cur)], by="code", all.x=T)
  # IND_HISTORY[is.na(name) & !is.na(new_name), name:=new_name]
  # IND_HISTORY[is.na(short_name) & !is.na(new_shortname), short_name:=new_shortname]
  # ins_ref[code == 'STKVNAAA']
  # str(ins_ref)
  IND_HISTORY = merge(IND_HISTORY[,-c('name','date_ipo',  'website', 'ind_desc_e', 'isin', 'date_firsttrading')],
                      ins_ref[,.(code,name,date_ipo, website, ind_desc_e, isin, date_1strade = date_firsttrading)], by="code",all.x=T)  
  
  # sector = DBL_CCPR_READRDS('S:/STKVN/', 'download_yah_stk_sector_history.rds')
  # 
  # IND_HISTORY = merge(IND_HISTORY[, -c('gics_sector_name', 'gics_ind_name')], sector[, .(code, gics_sector_name = sector, gics_ind_name = industry)], by = 'code', all.x = T)
  IND_HISTORY = merge(IND_HISTORY[,-c('name', 'market', 'gics_sector_name', 'gics_ind_name', 'country', 'continent', 'size', 'cur')],
                      stkmother[,.(code,name, market, gics_sector_name = sector, gics_ind_name = industry, country, continent, size = category, cur)], by="code",all.x=T)  
  
  
  info = DBL_CCPR_READRDS('S:/STKVN/', 'download_yah_stk_info_history.rds')
  # info[, sum_capi:=sum(capiusd), by='date']
  # info[, cum_capi:=cumsum(capiusd), by='date']
  # info[, pc_capi:=100*cum_capi/sum_capi, by='date']
  # info[!is.na(sum_capi) & is.na(size), size := ifelse(pc_capi<=85, 'LARGE', ifelse(pc_capi>85 & pc_capi<=95, 'MID', 'SMALL'))]
  # info[is.na(sum_capi), size := NA]  # Set SIZE to NA when sum_capi is NA
  IND_HISTORY = merge(IND_HISTORY[, -c('ind_desc_e')], info[, .(code, ind_desc_e = description)], by = 'code', all.x = T)
  
  data_board = DBL_CCPR_READRDS('S:/R/DATA/BOARD/', 'download_all_board.rds')[, ticker:= codesource]
  data_ceo = unique(data_board[ceo == 1], by = 'codesource')
  
  IND_HISTORY = merge(IND_HISTORY[,-c('ceo')],
                      data_ceo[,.(ticker, ceo = name)], by="ticker",all.x=T)  
  
  data_cob = unique(data_board[cob == 1], by = 'codesource')
  
  IND_HISTORY = merge(IND_HISTORY[,-c('cob')],
                      data_cob[,.(ticker, cob = name)], by="ticker",all.x=T)  
  
  
  
  data_info = DBL_CCPR_READRDS('S:/R/DATA/BOARD/', 'download_all_info.rds')[, ticker:= codesource]
  
  IND_HISTORY = merge(IND_HISTORY[,-c('website')],
                      data_info[,.(ticker,address,phone_number,website)], by="ticker",all.x=T) 
  
  
  # IND_HISTORY[, phone_number := as.character(NA)]
  # IND_HISTORY[, address := as.character(NA)]
  # IND_HISTORY[, ceo := as.character(NA)]
  # IND_HISTORY[, cob := as.character(NA)]
  IND_HISTORY[, fax := as.character(NA)]
  
  # IND_HISTORY[, short_name:=name]
  # S:\STKVN\BOARD\WOMAN_CEO\
  
  IND_STATS = IND_HISTORY
  DB_TEMPLATE = setDT(fread(paste0('S:/LIST/', 'template_stk_specifications.txt')))
  
  DB_TEMPLATE_EMPTY = DB_TEMPLATE
  # DB_TEMPLATE_EMPTY$value=NULL
  # DB_TEMPLATE_EMPTY$value=as.character('')
  X_List = list()
  
  for (k in 1:nrow(IND_STATS))
  {
    # k = 1
    
    DB_TEMPLATE_ONE = copy(DB_TEMPLATE_EMPTY)
    DB_TEMPLATE_ONE[, stock_code:=IND_STATS[k]$code]
    DB_TEMPLATE_ONE[, code:=IND_STATS[k]$code]
    
    DB_TEMPLATE_ONE[field=='Symbol',   value:=as.character(IND_STATS[k]$ticker)]
    DB_TEMPLATE_ONE[field=='Company Name',   value:=as.character(IND_STATS[k]$name)]
    DB_TEMPLATE_ONE[field=='First Trade Date',   value:=as.character(IND_STATS[k]$date_1strade)]
    DB_TEMPLATE_ONE[field=='Exchange',   value:=as.character(IND_STATS[k]$market)]
    DB_TEMPLATE_ONE[field=='Sector',   value:=as.character(IND_STATS[k]$gics_sector_name)]
    DB_TEMPLATE_ONE[field=='Industry',   value:=as.character(IND_STATS[k]$gics_ind_name)]
    DB_TEMPLATE_ONE[field=='Address',   value:=as.character(IND_STATS[k]$address)]
    DB_TEMPLATE_ONE[field=='Country',   value:=as.character(IND_STATS[k]$country)]
    DB_TEMPLATE_ONE[field=='Continent',   value:=as.character(IND_STATS[k]$continent)]
    DB_TEMPLATE_ONE[field=='Phone Number',   value:=as.character(IND_STATS[k]$phone)]
    DB_TEMPLATE_ONE[field=='Website',   value:=as.character(IND_STATS[k]$website)]
    DB_TEMPLATE_ONE[field=='CEO',   value:=as.character(IND_STATS[k]$ceo)]
    DB_TEMPLATE_ONE[field=='COB',   value:=as.character(IND_STATS[k]$cob)]
    DB_TEMPLATE_ONE[field=='Size',   value:=as.character(IND_STATS[k]$size)]
    DB_TEMPLATE_ONE[field=='Currency',   value:=as.character(IND_STATS[k]$cur)]
    DB_TEMPLATE_ONE[field=='Fax',   value:=as.character(IND_STATS[k]$fax)]
    DB_TEMPLATE_ONE[field=='Description',   value:=as.character(IND_STATS[k]$ind_desc_e)]
    DB_TEMPLATE_ONE[field=='Isin',   value:=as.character(IND_STATS[k]$isin)]
    
    My.Kable.All(DB_TEMPLATE_ONE)
    X_List[[k]] = DB_TEMPLATE_ONE
  }
  
  SPECIFICATIONS_ALL = rbindlist(X_List, fill = T)
  # SPECIFICATIONS_ALL[, active := 1]
  My.Kable(SPECIFICATIONS_ALL[active == 1])
  # saveRDS(SPECIFICATIONS_ALL, paste0('R:/CCPR/DATA/DASHBOARD/', 'INDETF_SPECIFICATIONS_ALL.rds'))
  # DBL_CCPR_SAVERDS(SPECIFICATIONS_ALL, 'R:/CCPR/DATA/DASHBOARD/', 'INDGICS_SPECIFICATIONS_ALL.rds')
  
  
  
  if (ToSave) { 
    if (file.exists (paste0( ToFolder , ToFile )) ) {
      data_old = CCPR_READRDS(ToFolder , ToFile, ToRestore = T)
    } else {data_old = data.table () }
    
    SPECIFICATIONS_ALL = unique(rbind(SPECIFICATIONS_ALL, data_old[!stock_code %in% SPECIFICATIONS_ALL$stock_code], fill = T), by = c('field','stock_code'))
    CCPR_SAVERDS (SPECIFICATIONS_ALL, ToFolder , ToFile)
  }
  
  My_Data = SPECIFICATIONS_ALL
  # ToMerge =T
  if (ToIntergration) { 
    if (file.exists (paste0( FolderMerge , FileMerge )) ) {
      Data_Old = CCPR_READRDS( FolderMerge , FileMerge, ToRestore = T)
    } else {Data_Old = data.table () }
    # SPECIFICATIONS_ALL
    My_Data = unique ( rbind (Data_Old[!code %in% SPECIFICATIONS_ALL$code], SPECIFICATIONS_ALL, fill =T), fromLast = T, by = c ('field', 'stock_code') )
    # pDate = '2024-01-12'
    My_Data = UPDATE_UPDATED(My_Data[, -c('updated')])
    CCPR_SAVERDS(My_Data, FolderMerge , FileMerge)
  }
  
  if (ToUpload) {
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = pHost, l.tablename= gsub(".rds","",FileMerge),
                                     l.filepath= tolower(paste0(FolderMerge , FileMerge)),
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=T))
  }
  return (My_Data)
}

# ==================================================================================================
DBL_STKVN_SPECIFICATIONS_BY_FILE = function (pFolder = 'S:/STKVN/PRICES/FINAL/', pFile = 'IFRC_CCPR_STKVN_FOR_DASHBOARD.rds' ,
                                             List_Codes = c(),
                                             ToFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/' , ToFile  = 'all_stkvn_specifications.rds',
                                             FolderMerge = 'S:/CCPR/DATA/DASHBOARD_LIVE/', FileMerge = 'dbl_stk_specifications.rds',
                                             ToSave = F, ToIntergration  = F, ToUpload = F , pHost = 'dashboard_live' ) {
  # ------------------------------------------------------------------------------------------------
  # SPECIFICATIONS :
  library('stringi')
  ALL_HISTORY = CCPR_READRDS(pFolder, pFile, ToKable=T, ToRestore = T)
  GC_SILENT()
  if (length (List_Codes)>0) {IND_HISTORY = ALL_HISTORY [code %in% List_Codes]} else {
    IND_HISTORY = unique(ALL_HISTORY[, .(code, ticker)])
  }
  DBL_RELOAD_INSREF()
  # IND_HISTORY = DBL_CCPR_READRDS(paste0('R:/CCPR/DATA/DASHBOARD/'), 'ifrc_ccpr_ind_gics_prices.rds', ToKable=T, ToRestore = T)
  indmother = CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',"dbl_stk_mother.rds")
  # IND_HISTORY = merge(IND_HISTORY, indmother[,.(code,new_name = name,new_shortname = short_name,prtr = version,wgtg,cur)], by="code", all.x=T)
  # IND_HISTORY[is.na(name) & !is.na(new_name), name:=new_name]
  # IND_HISTORY[is.na(short_name) & !is.na(new_shortname), short_name:=new_shortname]
  # ins_ref[code == 'STKVNAAA']
  # str(ins_ref)
  IND_HISTORY = merge(IND_HISTORY[, -c('name','date_ipo', 'market', 'gics_sector_name', 'gics_ind_name', 'country', 'website', 'continent', 'size', 'cur', 'ind_desc_e', 'isin')],
                      indmother[,.(code,name,market,gics_sector_name = sector, gics_ind_name = industry,country,continent, size = category, cur)])
  
  IND_HISTORY = merge(IND_HISTORY[,-c('date_ipo', 'website', 'ind_desc_e', 'isin')],
                      ins_ref[,.(code,date_ipo, website, ind_desc_e, isin)], by="code",all.x=T)  
  
  data_ceo = CCPR_READRDS("S:/STKVN/BOARD/WOMAN_CEO/", "data_stkvn_ceo_day.rds", ToKable = T, ToRestore = T)
  IND_HISTORY = merge(IND_HISTORY[, -c('ceo', 'size')], data_ceo[, .(code, ceo = stri_trans_general(ceo, "Latin-ASCII"), size)], all.x = T, by = 'code')
  
  data_cob = CCPR_READRDS("S:/STKVN/BOARD/", "trainee_vnd_stkvn_board.rds", ToKable = T, ToRestore = T)
  IND_HISTORY = merge(IND_HISTORY[, -c('cob', 'people_name')], data_cob[cob == 1][, .(code, cob = stri_trans_general(people_name, "Latin-ASCII"))], all.x = T, by = 'code')
  
  data_info = CCPR_READRDS("S:/STKVN/INFO/", "download_vnd_stkvn_company_info.rds", ToKable = T, ToRestore = T)
  IND_HISTORY = merge(IND_HISTORY[, -c('website')], data_info[, .(code, website, address = en_address, phone, fax, email, date_1strade, date_1sprice)], all.x = T, by = 'code')
  
  # hnx = DBL_CCPR_READRDS(UData, 'download_hnx_stkvn_ref.rds', ToRestore = T)
  # hsx = DBL_CCPR_READRDS(UData, 'download_hsx_stkvn_ref.rds', ToRestore = T)
  # upc = DBL_CCPR_READRDS(UData, 'download_upc_stkvn_ref.rds', ToRestore = T)
  # data_firstdate = rbind(hnx, hsx, fill = T)
  # data_firstdate = rbind(data_firstdate, upc, fill = T)
  # 
  # IND_HISTORY = merge(IND_HISTORY[, -c('firstdate')], data_firstdate[, .(code, date_1strade)], all.x = T, by = 'code')
  
  # IND_HISTORY[, phone_number := as.character(NA)]
  # IND_HISTORY[, address := as.character(NA)]
  # IND_HISTORY[, ceo := as.character(NA)]
  # IND_HISTORY[, cob := as.character(NA)]
  
  # IND_HISTORY[, short_name:=name]
  # S:\STKVN\BOARD\WOMAN_CEO\
  
  IND_STATS = IND_HISTORY
  DB_TEMPLATE = setDT(fread(paste0('S:/CCPR/DATA/DASHBOARD/', 'template_stk_specifications.txt')))
  
  DB_TEMPLATE_EMPTY = DB_TEMPLATE
  # DB_TEMPLATE_EMPTY$value=NULL
  # DB_TEMPLATE_EMPTY$value=as.character('')
  X_List = list()
  
  for (k in 1:nrow(IND_STATS))
  {
    # k = 1
    
    DB_TEMPLATE_ONE = copy(DB_TEMPLATE_EMPTY)
    DB_TEMPLATE_ONE[, stock_code:=IND_STATS[k]$code]
    DB_TEMPLATE_ONE[, code:=IND_STATS[k]$code]
    
    DB_TEMPLATE_ONE[field=='Symbol',   value:=as.character(IND_STATS[k]$ticker)]
    DB_TEMPLATE_ONE[field=='Company Name',   value:=as.character(IND_STATS[k]$name)]
    DB_TEMPLATE_ONE[field=='First Trade Date',   value:=as.character(IND_STATS[k]$date_1strade)]
    DB_TEMPLATE_ONE[field=='First Trade Price',   value:=as.character(IND_STATS[k]$date_1sprice)]
    DB_TEMPLATE_ONE[field=='Exchange',   value:=as.character(IND_STATS[k]$market)]
    DB_TEMPLATE_ONE[field=='Sector',   value:=as.character(IND_STATS[k]$gics_sector_name)]
    DB_TEMPLATE_ONE[field=='Industry',   value:=as.character(IND_STATS[k]$gics_ind_name)]
    DB_TEMPLATE_ONE[field=='Address',   value:=as.character(IND_STATS[k]$address)]
    DB_TEMPLATE_ONE[field=='Country',   value:=as.character(IND_STATS[k]$country)]
    DB_TEMPLATE_ONE[field=='Continent',   value:=as.character(IND_STATS[k]$continent)]
    DB_TEMPLATE_ONE[field=='Phone Number',   value:=as.character(IND_STATS[k]$phone)]
    DB_TEMPLATE_ONE[field=='Website',   value:=as.character(IND_STATS[k]$website)]
    DB_TEMPLATE_ONE[field=='CEO',   value:=as.character(IND_STATS[k]$ceo)]
    DB_TEMPLATE_ONE[field=='COB',   value:=as.character(IND_STATS[k]$cob)]
    DB_TEMPLATE_ONE[field=='Size',   value:=as.character(IND_STATS[k]$size)]
    DB_TEMPLATE_ONE[field=='Currency',   value:=as.character(IND_STATS[k]$cur)]
    DB_TEMPLATE_ONE[field=='Fax',   value:=as.character(IND_STATS[k]$fax)]
    DB_TEMPLATE_ONE[field=='Description',   value:=as.character(IND_STATS[k]$ind_desc_e)]
    DB_TEMPLATE_ONE[field=='Isin',   value:=as.character(IND_STATS[k]$isin)]
    
    My.Kable.All(DB_TEMPLATE_ONE)
    X_List[[k]] = DB_TEMPLATE_ONE
  }
  
  SPECIFICATIONS_ALL = rbindlist(X_List, fill = T)
  My.Kable(SPECIFICATIONS_ALL[active == 1])
  # saveRDS(SPECIFICATIONS_ALL, paste0('R:/CCPR/DATA/DASHBOARD/', 'INDETF_SPECIFICATIONS_ALL.rds'))
  # DBL_CCPR_SAVERDS(SPECIFICATIONS_ALL, 'R:/CCPR/DATA/DASHBOARD/', 'INDGICS_SPECIFICATIONS_ALL.rds')
  
  
  
  if (ToSave) { 
    data_old = DBL_CCPR_READRDS(ToFolder , ToFile, ToRestore = T)
    SPECIFICATIONS_ALL = unique(rbind(SPECIFICATIONS_ALL, data_old), by = c('field','stock_code'))
    DBL_CCPR_SAVERDS (SPECIFICATIONS_ALL, ToFolder , ToFile)
  }
  
  My_Data = SPECIFICATIONS_ALL
  # ToMerge =T
  if (ToIntergration) { 
    if (file.exists (paste0( FolderMerge , FileMerge )) ) {
      Data_Old = CCPR_READRDS( FolderMerge , FileMerge, ToRestore = T)
    } else {Data_Old = data.table () }
    # SPECIFICATIONS_ALL
    My_Data = unique ( rbind (Data_Old[!stock_code %in% SPECIFICATIONS_ALL$stock_code], SPECIFICATIONS_ALL, fill =T), fromLast = T, by = c ('field', 'stock_code') )
    # pDate = '2024-01-12'
    My_Data = UPDATE_UPDATED(My_Data[, -c('updated')])
    CCPR_SAVERDS(My_Data, FolderMerge , FileMerge)
  }
  
  if (ToUpload) {
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = pHost, l.tablename= gsub(".rds","",FileMerge),
                                     l.filepath= tolower(paste0(FolderMerge , FileMerge)),
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=T))
  }
  return (My_Data)
}



# ==================================================================================================
DBL_IND_SPECIFICATIONS_BY_FILE = function (pFolder = 'S:/STKVN/INDEX_2024/', pFile = 'beq_indrtlals_history.rds' ,
                                           List_Codes = c(),CompoFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                           CompoFile = "dbl_ind_compo.rds",
                                           ToFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/' , ToFile  = 'all_specifications.rds',
                                           FolderMerge = 'S:/CCPR/DATA/DASHBOARD_LIVE/', FileMerge = 'dbl_ind_specifications.rds',
                                           ToSave = F, ToIntergration  = F, ToUpload = F , pHost = 'dashboard_live' ) {
  # ------------------------------------------------------------------------------------------------
  # SPECIFICATIONS :
  
  ALL_HISTORY = CCPR_READRDS(pFolder, pFile, ToKable=T, ToRestore = T)
  GC_SILENT()
  if (length (List_Codes)>0) {IND_HISTORY = ALL_HISTORY [code %in% List_Codes]} else {
    IND_HISTORY = ALL_HISTORY
  }
  # IND_HISTORY = DBL_CCPR_READRDS(paste0('R:/CCPR/DATA/DASHBOARD/'), 'ifrc_ccpr_ind_gics_prices.rds', ToKable=T, ToRestore = T)
  indmother = CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',"dbl_ind_mother.rds")
  # IND_HISTORY = merge(IND_HISTORY, indmother[,.(code,new_name = name,new_shortname = short_name,prtr = version,wgtg,cur)], by="code", all.x=T)
  # IND_HISTORY[is.na(name) & !is.na(new_name), name:=new_name]
  # IND_HISTORY[is.na(short_name) & !is.na(new_shortname), short_name:=new_shortname]
  if (pFile != 'ifrc_ccpr_investment_history.rds'){
    IND_HISTORY = merge(IND_HISTORY[,-c("name","short_name","wgtg","version","cur","coverage","category","group")],
                        indmother[,.(code,name,short_name,wgtg,version,cur,coverage,category,group)], by="code",all.x=T)  
  } else {
    IND_HISTORY = merge(IND_HISTORY[,-c("name","short_name","wgtg","version","cur","coverage","category","group")],
                        indmother[,.(code,name,short_name,wgtg,version,cur,coverage,category,group)], by="code",all.x=T)  
    IND_HISTORY[is.na(name), name := sub_name]
    IND_HISTORY[is.na(short_name), short_name := sub_name]
  }
  
  
  IND_HISTORY[, prtr := version]
  IND_HISTORY[, source:='IFRC']
  IND_HISTORY = IND_HISTORY[order(code, date)]
  IND_HISTORY[, base_value:=close[1], by='code']
  IND_HISTORY[, base_date:=date[1], by='code']
  # IND_HISTORY[, short_name:=name]
  
  DB_TEMPLATE = setDT(fread(paste0('S:/LIST/', 'template_specifications.txt')))
  DB_TEMPLATE_EMPTY = DB_TEMPLATE
  # DB_TEMPLATE_EMPTY$value=NULL
  # DB_TEMPLATE_EMPTY$value=as.character('')
  str(DB_TEMPLATE_EMPTY)
  
  IND_STATS    = IND_HISTORY[,.(full_name=name[1], short_name=short_name[1], start=date[1], start_value=close[1], cur = cur[1], prtr = prtr[1], wgtg = wgtg[1]) , by=c('code')]
  # IND_STATS[, cur:=substr(code, nchar(code)-2, nchar(code))]
  # IND_STATS[, prtr:=substr(code, nchar(code)-4, nchar(code)-3)]
  # IND_STATS[, wgtg:=substr(code, nchar(code)-6, nchar(code)-5)]
  
  X_List = list()
  
  for (k in 1:nrow(IND_STATS))
  {
    # k = 1
    
    DB_TEMPLATE_ONE = setDT(copy(DB_TEMPLATE_EMPTY))
    DB_TEMPLATE_ONE[, index_code:=IND_STATS[k]$code]
    DB_TEMPLATE_ONE[, code:=IND_STATS[k]$code]
    
    DB_TEMPLATE_ONE[field=='Full name',   value:=as.character(IND_STATS[k]$full_name)]
    DB_TEMPLATE_ONE[field=='Short name',  value:=as.character(IND_STATS[k]$short_name)]
    DB_TEMPLATE_ONE[field=='Composition criteria',  value:='Sector/Industry/Theme']
    switch(IND_STATS[k]$prtr,
           'PR' = { label = 'Price (PR)'},
           'TR' = { label = 'ToTal Return (TR)'},
           {label = ''}
    )
    DB_TEMPLATE_ONE[field=='Weighting',  value:=label]
    switch(IND_STATS[k]$wgtg,
           'EW' = { label = 'Equally Weighted (EW)'},
           'CW' = { label = 'Capitalisation Weighted (CW)'},
           {label = ''}
    )
    DB_TEMPLATE_ONE[field=='Weighting',  value:=label]
    
    switch(IND_STATS[k]$prtr,
           'PR' = { label = 'Price Index'},
           'TR' = { label = 'Total Return Index'},
           {label = ''}
    )
    DB_TEMPLATE_ONE[field=='Price or Total Return',  value:=label]
    
    label = IND_STATS[k]$cur
    DB_TEMPLATE_ONE[field=='Currency',  value:=label]
    DB_TEMPLATE_ONE[field=='Internal Code',  value:=IND_STATS[k]$code]
    DB_TEMPLATE_ONE[field=='Base Date',  value:=IND_STATS[k]$start]
    DB_TEMPLATE_ONE[field=='Base Value',  value:=IND_STATS[k]$start_value]
    DB_TEMPLATE_ONE[field=='History since',  value:=IND_STATS[k]$start]
    if (grepl("IFRC/BEQ",IND_STATS[k]$full_name))
    {
      DB_TEMPLATE_ONE[field=='Launching date', value:='2023-03-15']
    }else{
      DB_TEMPLATE_ONE[field=='Launching date', value:='']
    }
    My.Kable.All(DB_TEMPLATE_ONE)
    X_List[[k]] = DB_TEMPLATE_ONE
  }
  
  SPECIFICATIONS_ALL = rbindlist(X_List)
  My.Kable(SPECIFICATIONS_ALL)
  # saveRDS(SPECIFICATIONS_ALL, paste0('R:/CCPR/DATA/DASHBOARD/', 'INDETF_SPECIFICATIONS_ALL.rds'))
  # DBL_CCPR_SAVERDS(SPECIFICATIONS_ALL, 'R:/CCPR/DATA/DASHBOARD/', 'INDGICS_SPECIFICATIONS_ALL.rds')
  
  
  
  if (ToSave) {CCPR_SAVERDS (SPECIFICATIONS_ALL, ToFolder , ToFile)}
  My_Data = SPECIFICATIONS_ALL
  # ToMerge =T
  if (ToIntergration) { 
    if (file.exists (paste0( FolderMerge , FileMerge )) ) {
      Data_Old = CCPR_READRDS( FolderMerge , FileMerge, ToRestore = T)
    } else {Data_Old = data.table () }
    # SPECIFICATIONS_ALL
    My_Data = unique ( rbind (Data_Old[!index_code %in% SPECIFICATIONS_ALL$index_code], SPECIFICATIONS_ALL, fill =T), fromLast = T, by = c ('field', 'index_code') )
    # pDate = '2024-01-12'
    My_Data = UPDATE_UPDATED(My_Data[, -c('updated')])
    CCPR_SAVERDS(My_Data, FolderMerge , FileMerge)
  }
  
  if (ToUpload) {
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = pHost, l.tablename= gsub(".rds","",FileMerge),
                                     l.filepath= tolower(paste0(FolderMerge , FileMerge)),
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=T))
  }
  return (My_Data)
}

#===================================================================================================
TRAINEE_CALCULATE_FILE_RISK_NEW = function (pData = data.table(), pFolder = 'S:/STKVN/INDEX_2024/',
                                            pFile   = 'beq_indarials_history.rds', EndDate  = SYSDATETIME(hour(Sys.time())), ToUpload = T, ToSave = T)  {
  # ------------------------------------------------------------------------------------------------
  #updated: 2024-06-04 18:55
  # pFolder="S:/CCPR/DATA/DASHBOARD_LIVE/" ; pFile="ccpi_dashboard_indbeq_all_history.rds"
  # pData = data.table()
  # pFolder = 'S:/STKVN/INDEX_2024/'
  # pFile   = 'beq_indenyals_history.rds'
  # EndDate  = SYSDATETIME(hour(Sys.time()))
  if (nrow(pData ) > 1 ) 
  { 
    x = pData
    if ( !'close_adj' %in% colnames(pData) ) { x [, close_adj := close]}
    if ( !'code' %in% colnames(pData) ) { x [, code := codesource]}
  } else {
    if (pFile == 'ifrc_ccpr_investment_history.rds') {
      investment = DBL_CCPR_READRDS(pFolder, pFile) [order (-date)]
      investment [, rt:=((close/shift(close))-1)*100, by='code']
      investment [, ':=' (cur = NA, close_adj = close)]
      x = investment
    } else { 
      x = DBL_CCPR_READRDS(pFolder, pFile)
      My.Kable(x[order(date)])
      if ( !'close_adj' %in% colnames(x) ) { x [, close_adj := close]}
      if ( !'code' %in% colnames(x) ) { x [, code := codesource]}
    }
  }
  
  # STEP 1: READ FILE PRICES HISTORY
  # My.Kable(x)
  # colnames(x)
  GC_SILENT()
  # cleanse raw data
  x = unique(x, by = c('code', 'date'))
  
  # STEP 2: RE-CALCULATE DATA
  # Limit data maxdate
  Ind_History = x  [date <= EndDate]  
  My.Kable(Ind_History[, .(code, date, close)][order(date)])
  str(Ind_History)
  
  #Calculate necessary column: change, varpc, yyyy, yyyymm, yyyyqn
  Ind_History = Ind_History[order(code, date)]
  Ind_History = CALCULATE_CHANGE_RT_VARPC(Ind_History)
  
  if (all (! c('change', 'varpc') %in% names (Ind_History))) {
    Ind_History[, ':='(change=close_adj-shift(close_adj), varpc=100*(close_adj/shift(close_adj)-1)), by='code']
  }
  Ind_History[,":="(yyyy=year(date),yyyymm=year(date)*100+month(date),yyyyqn=year(date)*100+floor((month(date)-1)/3)+1)]
  Ind_History = Ind_History[order(code, date)]
  My.Kable(Ind_History[, .(code, date, close, yyyy, yyyymm, yyyyqn)])
  
  Ind_History[, rt:=(close_adj/shift(close_adj))-1, by='code']
  Ind_History[, lnrt:=log(close_adj/shift(close_adj)), by='code']
  My.Kable(Ind_History[, .(code, date, yyyymm, yyyy, close_adj, rt, lnrt)])
  
  Ind_Dates = unique(Ind_History[, .(date, yyyy, yyyyqn, yyyymm)], by='date')[order(-date)]
  My.Kable(Ind_Dates)
  
  Ind_Dates_MM = unique(Ind_Dates[order(-date)], by='yyyymm')
  Ind_Dates_MM[, MM_NR:=seq.int(1, .N)]
  
  Ind_Dates_QQ = unique(Ind_Dates[order(-date)], by='yyyyqn')
  Ind_Dates_QQ[, QQ_NR:=seq.int(1, .N)]
  
  Ind_Dates_YY = unique(Ind_Dates[order(-date)], by='yyyy')
  Ind_Dates_YY[, YY_NR:=seq.int(1, .N)]
  
  
  Ind_History = merge(Ind_History[, -c('MM_NR')], Ind_Dates_MM[, .(yyyymm, MM_NR)], by='yyyymm')
  Ind_History = merge(Ind_History[, -c('QQ_NR')], Ind_Dates_QQ[, .(yyyyqn, QQ_NR)], by='yyyyqn')
  Ind_History = merge(Ind_History[, -c('YY_NR')], Ind_Dates_YY[, .(yyyy, YY_NR)], by='yyyy')
  # My.Kable(Ind_Overview[, .(code, date, yyyymm, yyyy, close_adj, rt, lnrt, MM_NR, QQ_NR, YY_NR)])
  
  
  Ind_Overview = unique(Ind_History[order(code, -date)], by=c('code')) #[, .(code, name, date, last=close_adj, last_change, last_varpc)]
  
  My.Kable(Ind_Overview[, .(code, date, yyyymm, yyyy, close_adj, rt, lnrt, MM_NR, QQ_NR, YY_NR)][order(code, -date)])
  
  #Merge data: month, quarter, year
  
  MinDays = 1
  
  VOLAT_MM = Ind_History[, .(n=.N, date=date[.N], volat=100*sqrt(250)*sqrt(var(lnrt, na.rm=T))), by=c('code', 'MM_NR')]
  VOLAT_MM[n<MinDays, volat:=as.numeric(NA)]
  My.Kable(VOLAT_MM[order(MM_NR, code)])
  
  VOLAT_6MM = VOLAT_MM[MM_NR <= 6][order(code, MM_NR)]
  My.Kable(setDT(spread(VOLAT_6MM[, .(code,  nr = paste0('risk_M',(MM_NR)), volat)], key='nr', value='volat'))[order(-risk_M1)])
  Res_Month = setDT(spread(VOLAT_6MM[, .(code,  nr = paste0('risk_M',(MM_NR)), volat)], key='nr', value='volat'))[order(-risk_M1)]
  
  Res_Month =  Res_Month %>%
    mutate(risk_M1 = coalesce(risk_M2, risk_M3, risk_M4, risk_M5, risk_M6))
  
  Res_Month[, risk_mtd := risk_M1]
  # setnames(Res_Month, "risk_M0", "risk_mtd")
  My.Kable(Res_Month)
  # risk_cols = setdiff(names(Res_Month), list('code', 'risk_mtd'))
  # # dt = Res_Month
  # Res_Month[, paste0(risk_cols, "_level") := lapply(.SD, function(x) {
  #   ifelse(x > 0 & x <= 15, "LOW", 
  #          ifelse(x > 15 & x <= 30, "MEDIUM", 
  #                 ifelse(x > 30, "HIGH", NA)))
  # }), .SDcols = risk_cols]
  cols = setdiff(names(Res_Month),'code')
  
  Res_Month[, paste0(cols, "_level") := lapply(.SD, function(x) {
    x[is.na(x)] = 0  
    rank_order = rank(x, ties.method = "min")  
    group = cut(rank_order, breaks = 3, labels = c("LOW", "MEDIUM", "HIGH"))
    group[x == 0] = NA  
    return(as.character(group))
  }), .SDcols = cols]
  Res_Month[, (paste0(cols, "_level")) := lapply(.SD, as.character), .SDcols = paste0(cols, "_level")]
  
  VOLAT_QQ = Ind_History[, .(n=.N, date=date[.N], volat=100*sqrt(250)*sqrt(var(lnrt, na.rm=T))), by=c('code', 'QQ_NR')]
  VOLAT_QQ[n<MinDays, volat:=as.numeric(NA)]
  My.Kable(VOLAT_QQ[order(QQ_NR, code)])
  
  VOLAT_6QQ = VOLAT_QQ[QQ_NR <= 6][order(code, QQ_NR)]
  My.Kable(setDT(spread(VOLAT_6QQ[, .(code,  nr = paste0('risk_Q',(QQ_NR)), volat)], key='nr', value='volat'))[order(-risk_Q1)])
  Res_Quarter = setDT(spread(VOLAT_6QQ[, .(code,  nr = paste0('risk_Q',(QQ_NR)), volat)], key='nr', value='volat'))[order(-risk_Q1)]
  # setnames(Res_Quarter, "risk_Q0", "risk_qtd")
  Res_Quarter =  Res_Quarter %>%
    mutate(risk_Q1 = coalesce(risk_Q2, risk_Q3, risk_Q4, risk_Q5, risk_Q6))
  
  Res_Quarter[, risk_qtd := risk_Q1]
  
  # risk_cols = setdiff(names(Res_Quarter), list('code', 'risk_qtd'))
  # # dt = Res_Month
  # Res_Quarter[, paste0(risk_cols, "_level") := lapply(.SD, function(x) {
  #   ifelse(x > 0 & x <= 15, "LOW", 
  #          ifelse(x > 15 & x <= 30, "MEDIUM", 
  #                 ifelse(x > 30, "HIGH", NA)))
  # }), .SDcols = risk_cols]
  
  My.Kable(Res_Quarter)
  
  cols = setdiff(names(Res_Quarter),list('code'))
  
  Res_Quarter[, paste0(cols, "_level") := lapply(.SD, function(x) {
    x[is.na(x)] = 0  
    rank_order = rank(x, ties.method = "min")  
    group = cut(rank_order, breaks = 3, labels = c("LOW", "MEDIUM", "HIGH"))
    group[x == 0] = NA  
    return(as.character(group))
  }), .SDcols = cols]
  Res_Quarter[, (paste0(cols, "_level")) := lapply(.SD, as.character), .SDcols = paste0(cols, "_level")]
  
  VOLAT_YY = Ind_History[, .(n=.N, date=date[.N], volat=100*sqrt(250)*sqrt(var(lnrt, na.rm=T))), by=c('code', 'YY_NR')]
  VOLAT_YY[n<MinDays, volat:=as.numeric(NA)]
  My.Kable(VOLAT_YY[order(YY_NR, code)])
  
  VOLAT_6YY = VOLAT_YY[YY_NR <= 6][order(code, YY_NR)]
  My.Kable(setDT(spread(VOLAT_6YY[, .(code, nr = paste0('risk_Y',(YY_NR)), volat)], key='nr', value='volat'))[order(-risk_Y1)])
  Res_Year = setDT(spread(VOLAT_6YY[, .(code, nr = paste0('risk_Y',(YY_NR)), volat)], key='nr', value='volat'))[order(-risk_Y1)]
  # setnames(Res_Year, "risk_Y0", "risk_ytd")
  
  Res_Year =  Res_Year %>%
    mutate(risk_Y1 = coalesce(risk_Y2, risk_Y3, risk_Y4, risk_Y5, risk_Y6))
  
  Res_Year[, risk_ytd := risk_Y1]
  
  # risk_cols = setdiff(names(Res_Year), list('code', 'risk_ytd'))
  # # dt = Res_Month
  # Res_Year[, paste0(risk_cols, "_level") := lapply(.SD, function(x) {
  #   ifelse(x > 0 & x <= 15, "LOW", 
  #          ifelse(x > 15 & x <= 30, "MEDIUM", 
  #                 ifelse(x > 30, "HIGH", NA)))
  # }), .SDcols = risk_cols]
  
  My.Kable(Res_Year)
  cols = setdiff(names(Res_Year),'code')
  
  Res_Year[, paste0(cols, "_level") := lapply(.SD, function(x) {
    x[is.na(x)] = 0  
    rank_order = rank(x, ties.method = "min")  
    group = cut(rank_order, breaks = 3, labels = c("LOW", "MEDIUM", "HIGH"))
    group[x == 0] = NA  
    return(as.character(group))
  }), .SDcols = cols]
  Res_Year[, (paste0(cols, "_level")) := lapply(.SD, as.character), .SDcols = paste0(cols, "_level")]
  
  Ind_Overview = merge(Ind_Overview, Res_Month, all.x = T, by = 'code' )
  Ind_Overview = merge(Ind_Overview, Res_Quarter, all.x = T, by = 'code' )
  Ind_Overview = merge(Ind_Overview, Res_Year, all.x = T, by = 'code' )
  
  My.Kable(Ind_Overview[, .(code, date, close_adj, rt, lnrt, risk_mtd, risk_qtd, risk_ytd)])
  
  ToAddRef = T
  if (ToAddRef) {
    Ind_Data = TRAINEE_MERGE_COL_FROM_REF (pData = Ind_Overview, 
                                           List_Fields = c('name','cur','wgtg','prtr', 'category', 'size', 'coverage'))
  } else { Ind_Data = Ind_All}
  
  if (!'last' %in% names (Ind_Data)) {
    Ind_Data [, last:= close]
  }
  Ind_Data [, excess := 0]
  Ind_Data [, updated := SYS.TIME()]
  My.Kable(Ind_Data[, .(code, date, close_adj, rt, lnrt, risk_mtd, risk_qtd, risk_ytd)])
  
  if (ToSave){
    try(DBL_CCPR_SAVERDS(Ind_Data, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_ind_risk.rds', ToSummary = T, SaveOneDrive = T))
  }
  if (ToUpload){
    library(data.table)
    library(DBI)
    library(RMySQL)
    library(RMariaDB)
    library(DT)
    # user = "dashboard_user_login", password = "admin@beqholdings", host = '27.71.235.71', dbname = "ccpi_dashboard_login")
    connection <- dbConnect(
      dbDriver("MySQL"), 
      host = "27.71.235.71", 
      port = 3306,
      user = "dashboard_user_login", 
      password = "admin@beqholdings", 
      db = "ccpi_dashboard_login"
    )
    #data = readRDS("S:/SHINY/REPORT/REPORT_WCEO.rds")
    # data = readRDS("S:/STKVN/PRICES/REPORTS/process_pricesday.rds")
    #  data = readRDS('S:/WCEO_INDEXES/REPORT_WCEO.rds')
    # current_date <- Sys.Date()
    # cols_to_update <- setdiff(names(data), c("dataset", "date", "updated", "id"))
    # data[data$id == 3, cols_to_update] <- current_date
    
    My.Kable(Ind_Data)
    # data[, c("id"):=NULL] 
    # data = data[, -c('pnr')]
    #result = dbWriteTable(connection, "process_wceo", data[3,], append = TRUE, row.names = FALSE)
    result = dbWriteTable(connection, 'dbl_ind_risk', Ind_Data, row.names = FALSE, overwrite = TRUE)
    print(result)
    dbDisconnect(connection)   
  }
  
  
  return(Ind_Data)
}
# ==================================================================================================
TRAINEE_DASHBOARD_LIVE_COMPOSITION_2ND_NEW = function(pOption="ICB", ToSave=F, pHost="")  {
  # ------------------------------------------------------------------------------------------------
  DBL_RELOAD_INSREF()
  # COMPO_FOLDER = 'R:/CCPR/DATA/DASHBOARD/'
  # 
  # compo_files = c(list.files('R:/CCPR/DATA/DASHBOARD/','ifrc_.*_compo_4index'), 
  #                 list.files('S:/STKVN/INDEX_2024/','beq_.*_compo'))
  
  list_file_compo = setDT(fread('S:/CCPR/DATA/DASHBOARD_LIVE/list_files_compo.txt'))
  
  
  compo_all_cw_list = list()
  data_ind_code = data.table()
  
  # for (ifile in 1:length(compo_files)) {
  #   # ifile = 1
  #   File_Name = compo_files[ifile]
  #   Folder_Path = ifelse(grepl('compo_4index',File_Name), 'R:/CCPR/DATA/DASHBOARD/', 'S:/STKVN/INDEX_2024/')
  for (i in 1:nrow(list_file_compo)){
    # i = 1
    Folder_Path = list_file_compo$Folder[i]
    File_Name   = list_file_compo$File[i]
    
    xData = CHECK_CLASS(try(DBL_CCPR_READRDS(Folder_Path, File_Name, ToKable = T, ToRestore = T)))
    
    if ((nrow(xData) > 0) & ('index_code' %in% names(xData))){
      # data_ind_code = unique(rbind(data_ind_code, xData[, .(code, index_code)], fill = T), by = 'code', fromLast = T)
      
      # STEP 1: READ FILE PRICES HISTORY
      Data_All = xData
      
      pOption = list_file_compo$type[i]
      # STEP 3: ADD missing COLUMNS: market, sector, industry, size, ticker
      switch ( pOption, 
               'ICB'  = { icb      = readRDS("S:/CCPR/DATA/STKVN_ICB_ALL.rds")
               Data_All = merge(Data_All[, -c('market')], icb[,.(code, sector = toupper(icb1_name),  industry = toupper(icb2_name), market)], all.x= T, by = 'code')
               
               # DBL_CCPR_SAVERDS(icb, 'S:/CCPR/DATA/', 'STKVN_ICB_ALL.rds')
               }  ,
               'GICS' = { 
                 gics = setDT (read_xlsx('S:/CCPR/DATA/DASHBOARD/From_BloombergVN_GICS_en.xlsx'))
                 gics = TRAINEE_CLEAN_COLNAMES(gics)
                 gics [san == 'UPCOM', market := 'UPC']
                 gics [san == 'HOSE',  market := 'HSX']
                 gics [san == 'HNX',   market := 'HNX']
                 gics [, code := paste0('STKVN', ticker)]
                 Data_All = merge(Data_All[, -c('market')], gics[,.(code, sector = toupper(gics_1),  industry = toupper(gics_2), market)], all.x= T, by = 'code')
                 
                 # colnames(gics)
                 # gics [ ,. (code1 = code___5, name1 = gics_1 )]
               })
      
      if ('close_unadj' %in% names(Data_All) & all(!is.na(Data_All[date == max(date)]$close_unadj))){
        Data_All [, capivnd:= shares*close_unadj]
      } else{
        Data_All [, capivnd:= shares*close]
      }
      
      
      # cleanse raw data
      x = unique(xData, by = c('code', 'date'))
      
      
      # STEP 2: RE-CALCULATE DATA
      # Ind_History = x  [date <= Compo_Date]  
      # my_date = max (Ind_History$date)
      Ind_History = x[order(code, date)] 
      Ind_History[, ':='(change=close_unadj-shift(close_unadj), varpc=100*(close_unadj/shift(close_unadj)-1)), by='code']
      Ind_History[,":="(yyyy=year(date),yyyymm=year(date)*100+month(date),yyyyqn=year(date)*100+floor((month(date)-1)/3)+1),]
      Ind_Month = unique(Ind_History[order(code, -date)], by=c('code', 'yyyymm'))[order(code, date)]
      Ind_Month[, rtm:=(close_unadj/shift(close_unadj))-1, by='code']
      
      Ind_Year  = unique(Ind_History[order(code, -date)], by=c('code', 'yyyy'))[order(code, date)]
      Ind_Year[, rty:=(close_unadj/shift(close_unadj))-1, by='code']
      
      Ind_Quarter = unique(Ind_History[order(code, -date)], by=c('code', 'yyyyqn'))[order(code, date)]
      Ind_Quarter[, rtq:=(close_unadj/shift(close_unadj))-1, by='code']
      
      Ind_Overview = unique(Ind_History[order(code, -date)], by=c('code'))[, .(code, name, date, last=close_unadj, change, varpc)]
      Ind_Overview = merge(Ind_Overview, Ind_Month[, .(code, date, mtd=round(100*rtm,12))], all.x = T, by=c('code', 'date'))
      Ind_Overview = merge(Ind_Overview, Ind_Year[, .(code, date, ytd=round(100*rty,12))], all.x = T, by=c('code', 'date'))
      Ind_Overview = merge(Ind_Overview, Ind_Quarter[, .(code, date, qtd=round(100*rtq,12))], all.x = T, by=c('code', 'date'))
      My.Kable(Ind_Overview)
      
      
      Ind_Month6M = Ind_Month[order(code, -date)][, nr:=seq.int(1, .N), by='code'][nr<=6][, .(code, date, rtm, nr)]
      My.Kable(setDT(spread(Ind_Month6M[, .(code, nr = paste0('M',nr), rtm=round(100*rtm,12))], key='nr', value='rtm'))[order(-M1)])
      Res_Month = setDT(spread(Ind_Month6M[, .(code, nr = paste0('M',nr), rtm=round(100*rtm,12))], key='nr', value='rtm'))[order(-M1)]
      
      Ind_Year6Y = Ind_Year[order(code, -date)][, nr:=seq.int(1, .N), by='code'][nr<=6][, .(code, date, rty, nr)]
      Res_Year   = setDT(spread(Ind_Year6Y[, .(code, nr = paste0('Y',nr), rty=round(100*rty,12))], key='nr', value='rty'))[order(-Y1)]
      My.Kable(Res_Year)
      
      Ind_Quarter6Y = Ind_Quarter[order(code, -date)][, nr:=seq.int(1, .N), by='code'][nr<=6][, .(code, date, rtq, nr)]
      Res_Quarter   = setDT(spread(Ind_Quarter6Y[, .(code, nr = paste0('Q',nr), rtq=round(100*rtq,12))], key='nr', value='rtq'))[order(-Q1)]
      My.Kable(Res_Quarter)
      # End.time = Sys.time()
      
      Ind_All = merge(Ind_Overview,Res_Month, all.x = T, by = 'code' )
      Ind_All = merge(Ind_All,Res_Year, all.x = T, by = 'code' )
      Ind_All = merge(Ind_All,Res_Quarter, all.x = T, by = 'code' )
      Ind_All 
      
      # merge DATA_ALL and IND_ALL
      col_to_merge = setdiff(names(Ind_All), names(Data_All))
      Ind_All_to_merge = Ind_All %>% select('code', all_of(col_to_merge))
      
      Data_All_to_merge = unique(Data_All[order(-date)], by = c('code','index_code'))
      my_data = merge(Data_All_to_merge, Ind_All_to_merge, by = 'code', all.x = T)
      # Ind_All  = merge( Ind_All, y [,.(code, market, date, sector, industry)], all.x = T, by = c('code'))
      # Ind_All$name = NULL
      # 
      # xData = xData[date == max(date)]
      compo_all_cw_list[[i]] = my_data
      GC_SILENT()
    }
    
  }
  compo_all_cw = rbindlist(compo_all_cw_list, fill = T)
  # compo_all_cw$code_mother = NULL
  # compo_all_cw = merge(compo_all_cw[,-c("code_mother")],
  #                   ins_ref[type=="IND",.(code_mother=code, index_name=gsub(" PR| CW| \\(VND\\)","",name))],
  #                   by="index_name",all.x=T)
  compo_all_cw[,code_mother:=gsub("PRVND","CWPRVND",index_code)]
  # data_mother_code = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_ind_mother.rds', ToKable = T, ToRestore = T)))
  # My.Kable(data_mother_code)
  
  # compo_all_cw = merge(compo_all_cw, data_mother_code[, .(index_code = code, index_compo)], by = 'index_code', all.x = T)
  # My.Kable(compo_all_cw[index_code %in% gsub('CW','',data_mother_code$index_compo)])
  
  compo_all_cw[, capibnvnd:=shares*close_unadj/1000000000,by="code_mother"]
  compo_all_cw[, sum_capi:=sum(capibnvnd, na.rm=T),by="code_mother"]
  compo_all_cw[, nb:=.N,by="code_mother"]
  compo_all_cw[, wgt_pc:=100*capibnvnd/sum_capi,by="code_mother"]
  compo_all_cw[, ticker:=gsub('STKVN','', code)]
  
  compo_all_ew = copy(compo_all_cw)
  compo_all_ew[,":="(code_mother=gsub("CWPRVND","EWPRVND",code_mother), wgt_pc=100/.N),by="code_mother"]
  
  COMPO_4INDEX_FINAL = rbind(compo_all_cw, compo_all_ew, fill = T)
  
  COMPO_4INDEX_FINAL = COMPO_4INDEX_FINAL %>% dplyr::select('code_mother', 'nb', 'ticker', 'code', 'name', 'date', 'shares', 
                                                            'close_unadj', 'capibnvnd', 'wgt_pc', everything())
  COMPO_4INDEX_FINAL$IFRC_rt=NULL
  
  
  # MaxDate = max(my_short_list$date)
  STKVN_CLEANED = copy(COMPO_4INDEX_FINAL[!is.na(capibnvnd) & !is.na(code_mother)][order(code_mother,code, -date)])
  
  dt_stkvn = DBL_CCPR_READRDS('S:/STKVN/PRICES/FINAL/', 'final_stkvn_4index_day.rds')[order(-capivnd)]
  
  dt_stkvn[, sum_capi:=sum(capivnd), by='date']
  dt_stkvn[, cum_capi:=cumsum(capivnd), by='date']
  dt_stkvn[, pc_capi:=100*cum_capi/sum_capi, by='date']
  dt_stkvn[pc_capi<=85, size:='LARGE']
  dt_stkvn[pc_capi>85 & pc_capi<=95, size:='MID']
  dt_stkvn[pc_capi>95, size:='SMALL']
  # dt_stkvn$pc_capi = NULL
  # dt_stkvn$cum_capi = NULL
  # dt_stkvn$sum_capi = NULL
  
  STKVN_CLEANED = merge(STKVN_CLEANED[,-c("size")], dt_stkvn[,.(code,size)], by=c("code"), all.x=T)
  
  STKVN_CLEANED = STKVN_CLEANED [order (-capibnvnd)]
  
  # COMPO_4INDEX_FINAL = merge (COMPO_4INDEX_FINAL,STKVN_CLEANED , all.x = T, by = 'code' )
  Data_Old = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_ind_compo.rds', ToKable = T, ToRestore = T)
  Data_Old = unique(rbind(Data_Old[!index_code %in% STKVN_CLEANED$index_code], STKVN_CLEANED, fill = T), by = c('index_code','code','code_mother'), fromLast = T)
  My.Kable(Data_Old)
  
  Data_Old = UPDATE_UPDATED(Data_Old)
  
  # # str(STKVN_CLEANED)
  if (ToSave ) {
    DBL_CCPR_SAVERDS(Data_Old,'S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_ind_compo.rds' , ToSummary=T, SaveOneDrive = T) 
  }
  # STEP 4: UPLOAD DATA
  if( nchar(pHost)>0 ) {
    # pHost = 'dashboard_live'
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = pHost, l.tablename= gsub ('.rds','', tolower('dbl_ind_compo.rds') ),
                                     l.filepath= tolower(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/',  'dbl_ind_compo.rds') ),
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
  }
}

# TRAINEE_DASHBOARD_LIVE_COMPOSITION_2ND_NEW (pOption="ICB", ToSave=T, pHost="dashboard_live") 
# ==================================================================================================
TRAINEE_DASHBOARD_LIVE_COMPOSITION_2ND_OLD = function(pOption="ICB", ToSave=F, pHost="")  {
  # ------------------------------------------------------------------------------------------------
  DBL_RELOAD_INSREF()
  # COMPO_FOLDER = 'R:/CCPR/DATA/DASHBOARD/'
  compo_files = c(list.files('R:/CCPR/DATA/DASHBOARD/','ifrc_.*_compo_4index'), 
                  list.files('S:/STKVN/INDEX_2024/','beq_.*_compo'))
  compo_all_cw_list = list()
  data_ind_code = data.table()
  
  for (ifile in 1:length(compo_files)) {
    # ifile = 1
    File_Name = compo_files[ifile]
    Folder_Path = ifelse(grepl('compo_4index',File_Name), 'R:/CCPR/DATA/DASHBOARD/', 'S:/STKVN/INDEX_2024/')
    xData = CHECK_CLASS(try(DBL_CCPR_READRDS(Folder_Path, File_Name, ToKable = T, ToRestore = T)))
    
    if ((nrow(xData) > 0) & ('index_code' %in% names(xData))){
      # data_ind_code = unique(rbind(data_ind_code, xData[, .(code, index_code)], fill = T), by = 'code', fromLast = T)
      
      # STEP 1: READ FILE PRICES HISTORY
      Data_All = xData
      
      # STEP 3: ADD missing COLUMNS: market, sector, industry, size, ticker
      switch ( pOption, 
               'ICB'  = { icb      = readRDS("S:/CCPR/DATA/STKVN_ICB_ALL.rds")
               # DBL_CCPR_SAVERDS(icb, 'S:/CCPR/DATA/', 'STKVN_ICB_ALL.rds')
               }  ,
               'GICS' = { gics = setDT (read_xlsx('S:/CCPR/DATA/DASHBOARD/From_BloombergVN_GICS_en.xlsx'))
               gics = TRAINEE_CLEAN_COLNAMES(gics)
               gics [san == 'UPCOM', market := 'UPC']
               gics [san == 'HOSE',  market := 'HSX']
               gics [san == 'HNX',   market := 'HNX']
               # colnames(gics)
               # gics [ ,. (code1 = code___5, name1 = gics_1 )]
               })
      
      if ('close_unadj' %in% names(Data_All) & all(!is.na(Data_All[date == max(date)]$close_unadj))){
        Data_All [, capivnd:= shares*close_unadj]
      } else{
        Data_All [, capivnd:= shares*close]
      }
      
      Data_All = merge(Data_All[, -c('market')], icb[,.(code, sector = toupper(icb1_name),  industry = toupper(icb2_name), market)], all.x= T, by = 'code')
      
      # cleanse raw data
      x = unique(xData, by = c('code', 'date'))
      
      
      # STEP 2: RE-CALCULATE DATA
      # Ind_History = x  [date <= Compo_Date]  
      # my_date = max (Ind_History$date)
      Ind_History = x[order(code, date)] 
      Ind_History[, ':='(change=close_unadj-shift(close_unadj), varpc=100*(close_unadj/shift(close_unadj)-1)), by='code']
      Ind_History[,":="(yyyy=year(date),yyyymm=year(date)*100+month(date),yyyyqn=year(date)*100+floor((month(date)-1)/3)+1),]
      Ind_Month = unique(Ind_History[order(code, -date)], by=c('code', 'yyyymm'))[order(code, date)]
      Ind_Month[, rtm:=(close_unadj/shift(close_unadj))-1, by='code']
      
      Ind_Year  = unique(Ind_History[order(code, -date)], by=c('code', 'yyyy'))[order(code, date)]
      Ind_Year[, rty:=(close_unadj/shift(close_unadj))-1, by='code']
      
      Ind_Quarter = unique(Ind_History[order(code, -date)], by=c('code', 'yyyyqn'))[order(code, date)]
      Ind_Quarter[, rtq:=(close_unadj/shift(close_unadj))-1, by='code']
      
      Ind_Overview = unique(Ind_History[order(code, -date)], by=c('code'))[, .(code, name, date, last=close_unadj, change, varpc)]
      Ind_Overview = merge(Ind_Overview, Ind_Month[, .(code, date, mtd=round(100*rtm,12))], all.x = T, by=c('code', 'date'))
      Ind_Overview = merge(Ind_Overview, Ind_Year[, .(code, date, ytd=round(100*rty,12))], all.x = T, by=c('code', 'date'))
      Ind_Overview = merge(Ind_Overview, Ind_Quarter[, .(code, date, qtd=round(100*rtq,12))], all.x = T, by=c('code', 'date'))
      My.Kable(Ind_Overview)
      
      
      Ind_Month6M = Ind_Month[order(code, -date)][, nr:=seq.int(1, .N), by='code'][nr<=6][, .(code, date, rtm, nr)]
      My.Kable(setDT(spread(Ind_Month6M[, .(code, nr = paste0('M',nr), rtm=round(100*rtm,12))], key='nr', value='rtm'))[order(-M1)])
      Res_Month = setDT(spread(Ind_Month6M[, .(code, nr = paste0('M',nr), rtm=round(100*rtm,12))], key='nr', value='rtm'))[order(-M1)]
      
      Ind_Year6Y = Ind_Year[order(code, -date)][, nr:=seq.int(1, .N), by='code'][nr<=6][, .(code, date, rty, nr)]
      Res_Year   = setDT(spread(Ind_Year6Y[, .(code, nr = paste0('Y',nr), rty=round(100*rty,12))], key='nr', value='rty'))[order(-Y1)]
      My.Kable(Res_Year)
      
      Ind_Quarter6Y = Ind_Quarter[order(code, -date)][, nr:=seq.int(1, .N), by='code'][nr<=6][, .(code, date, rtq, nr)]
      Res_Quarter   = setDT(spread(Ind_Quarter6Y[, .(code, nr = paste0('Q',nr), rtq=round(100*rtq,12))], key='nr', value='rtq'))[order(-Q1)]
      My.Kable(Res_Quarter)
      # End.time = Sys.time()
      
      Ind_All = merge(Ind_Overview,Res_Month, all.x = T, by = 'code' )
      Ind_All = merge(Ind_All,Res_Year, all.x = T, by = 'code' )
      Ind_All = merge(Ind_All,Res_Quarter, all.x = T, by = 'code' )
      Ind_All 
      
      # merge DATA_ALL and IND_ALL
      col_to_merge = setdiff(names(Ind_All), names(Data_All))
      Ind_All_to_merge = Ind_All %>% select('code', all_of(col_to_merge))
      
      Data_All_to_merge = unique(Data_All[order(-date)], by = c('code','index_code'))
      my_data = merge(Data_All_to_merge, Ind_All_to_merge, by = 'code', all.x = T)
      # Ind_All  = merge( Ind_All, y [,.(code, market, date, sector, industry)], all.x = T, by = c('code'))
      # Ind_All$name = NULL
      # 
      # xData = xData[date == max(date)]
      compo_all_cw_list[[ifile]] = my_data
      GC_SILENT()
    }
    
  }
  compo_all_cw = rbindlist(compo_all_cw_list, fill = T)
  # compo_all_cw$code_mother = NULL
  # compo_all_cw = merge(compo_all_cw[,-c("code_mother")],
  #                   ins_ref[type=="IND",.(code_mother=code, index_name=gsub(" PR| CW| \\(VND\\)","",name))],
  #                   by="index_name",all.x=T)
  compo_all_cw[,code_mother:=gsub("PRVND","CWPRVND",index_code)]
  # data_mother_code = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_ind_mother.rds', ToKable = T, ToRestore = T)))
  # My.Kable(data_mother_code)
  
  # compo_all_cw = merge(compo_all_cw, data_mother_code[, .(index_code = code, index_compo)], by = 'index_code', all.x = T)
  # My.Kable(compo_all_cw[index_code %in% gsub('CW','',data_mother_code$index_compo)])
  
  compo_all_cw[, capibnvnd:=shares*close_unadj/1000000000,by="code_mother"]
  compo_all_cw[, sum_capi:=sum(capibnvnd, na.rm=T),by="code_mother"]
  compo_all_cw[, nb:=.N,by="code_mother"]
  compo_all_cw[, wgt_pc:=100*capibnvnd/sum_capi,by="code_mother"]
  compo_all_cw[, ticker:=gsub('STKVN','', code)]
  
  compo_all_ew = copy(compo_all_cw)
  compo_all_ew[,":="(code_mother=gsub("CWPRVND","EWPRVND",code_mother), wgt_pc=100/.N),by="code_mother"]
  
  COMPO_4INDEX_FINAL = rbind(compo_all_cw, compo_all_ew, fill = T)
  
  COMPO_4INDEX_FINAL = COMPO_4INDEX_FINAL %>% dplyr::select('code_mother', 'nb', 'ticker', 'code', 'name', 'date', 'shares', 
                                                            'close_unadj', 'capibnvnd', 'wgt_pc', everything())
  COMPO_4INDEX_FINAL$IFRC_rt=NULL
  
  
  # MaxDate = max(my_short_list$date)
  STKVN_CLEANED = copy(COMPO_4INDEX_FINAL[!is.na(capibnvnd) & !is.na(code_mother)][order(code_mother,code, -date)])
  
  dt_stkvn = DBL_CCPR_READRDS('S:/STKVN/PRICES/FINAL/', 'final_stkvn_4index_day.rds')[order(-capivnd)]
  
  dt_stkvn[, sum_capi:=sum(capivnd), by='date']
  dt_stkvn[, cum_capi:=cumsum(capivnd), by='date']
  dt_stkvn[, pc_capi:=100*cum_capi/sum_capi, by='date']
  dt_stkvn[pc_capi<=85, size:='LARGE']
  dt_stkvn[pc_capi>85 & pc_capi<=95, size:='MID']
  dt_stkvn[pc_capi>95, size:='SMALL']
  # dt_stkvn$pc_capi = NULL
  # dt_stkvn$cum_capi = NULL
  # dt_stkvn$sum_capi = NULL
  
  STKVN_CLEANED = merge(STKVN_CLEANED[,-c("size")], dt_stkvn[,.(code,size)], by=c("code"), all.x=T)
  
  STKVN_CLEANED = STKVN_CLEANED [order (-capibnvnd)]
  
  # COMPO_4INDEX_FINAL = merge (COMPO_4INDEX_FINAL,STKVN_CLEANED , all.x = T, by = 'code' )
  Data_Old = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_ind_compo.rds', ToKable = T, ToRestore = T)
  Data_Old = unique(rbind(Data_Old, STKVN_CLEANED, fill = T), by = c('index_code','code','code_mother'), fromLast = T)
  My.Kable(Data_Old)
  
  # # str(STKVN_CLEANED)
  if (ToSave ) {
    DBL_CCPR_SAVERDS(Data_Old,'S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_ind_compo.rds' , ToSummary=T, SaveOneDrive = T) 
  }
  # STEP 4: UPLOAD DATA
  if( nchar(pHost)>0 ) {
    # pHost = 'dashboard_live'
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = pHost, l.tablename= gsub ('.rds','', tolower('dbl_ind_compo.rds') ),
                                     l.filepath= tolower(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/',  'dbl_ind_compo.rds') ),
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
  }
}

# ==================================================================================================
TRAINEE_DBL_CHART = function(ToUpdated = T, ToUpload = T){
  # ------------------------------------------------------------------------------------------------
  
  if (ToUpdated){
    
    
    ind_all = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'ccpi_dashboard_indbeq_all_history.rds', ToRestore = T)))
    GC_SILENT()
    if (nrow(ind_all) > 0){
      try(TRAINEE_CREATE_PERIOD_FROM_HISTORY  (pData       = ind_all,
                                               Period      = 'YEAR', 
                                               File_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                               File_Name   = 'ccpi_dashboard_indbeq_all_history.rds',
                                               ToSave      = T))
      
      try(TRAINEE_CREATE_PERIOD_FROM_HISTORY  (pData       = ind_all,
                                               Period      = 'QUARTER', 
                                               File_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                               File_Name   = 'ccpi_dashboard_indbeq_all_history.rds',
                                               ToSave      = T))
      
      try(TRAINEE_CREATE_PERIOD_FROM_HISTORY  (pData       = ind_all,
                                               Period      = 'MONTH', 
                                               File_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                               File_Name   = 'ccpi_dashboard_indbeq_all_history.rds',
                                               ToSave      = T))
    }
    if (ToUpload){
      # try(UPLOAD_RDS_TO_SQL(Connexion = 'dashboard_live',  host = '', user = "", password = "", 
      #                       SQL_str = "", SQL_file = '',  pSleep = 1,ToReplace = F, PARAM_REPLACE = list('' ) ,
      #                       RDS_folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', RDS_file = 'ccpi_dashboard_indbeq_all_MONTH_history.rds',
      #                       SQL_table = 'dbl_ind_month_chart') )
      # 
      # try(UPLOAD_RDS_TO_SQL(Connexion = 'dashboard_live',  host = '', user = "", password = "", 
      #                       SQL_str = "", SQL_file = '',  pSleep = 1,ToReplace = F, PARAM_REPLACE = list('' ) ,
      #                       RDS_folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', RDS_file = 'ccpi_dashboard_indbeq_all_YEAR_history.rds',
      #                       SQL_table = 'dbl_ind_year_chart') )
      # 
      # try(UPLOAD_RDS_TO_SQL(Connexion = 'dashboard_live',  host = '', user = "", password = "", 
      #                       SQL_str = "", SQL_file = '',  pSleep = 1,ToReplace = F, PARAM_REPLACE = list('' ) ,
      #                       RDS_folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', RDS_file = 'ccpi_dashboard_indbeq_all_QUARTER_history.rds',
      #                       SQL_table = 'dbl_ind_quarter_chart') )
      
      try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_ind_month_chart',
                                       l.filepath= paste0('S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_MONTH_history.rds'),
                                       CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
      
      try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_ind_quarter_chart',
                                       l.filepath= paste0('S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_QUARTER_history.rds'),
                                       CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
      
      try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'dbl_ind_year_chart',
                                       l.filepath= paste0('S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_YEAR_history.rds'),
                                       CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
    }
  }
}

# ==================================================================================================
TRAINEE_DBL_PERFORMANCE = function(FromFolder="S:/CCPR/DATA/DASHBOARD_LIVE/", FromFile="ccpi_dashboard_indbeq_all_history.rds",
                                   List_Codes = list(),
                                   Excess_List=c("INDVNINDEX","INDSPX","INDNDX"),
                                   SaveFolder="S:/CCPR/DATA/DASHBOARD_LIVE/",
                                   SaveFile="dbl_ind_performance.rds", ToUpload=T){
  # ------------------------------------------------------------------------------------------------
  ind_history = DBL_CCPR_READRDS(FromFolder,FromFile)
  GC_SILENT()
  dt_perf = data.table()
  
  if (length(List_Codes) > 0){
    ind_all = ind_history[code %in% List_Codes]
  } else {
    ind_all = ind_history
  }
  
  if ( !'close_adj' %in% colnames(ind_all) ) { ind_all[, close_adj := close]}
  ind_all[,":="(change=close_adj-shift(close_adj), varpc=100*(close_adj/shift(close_adj) -1)),by="code"]
  # My.Kable(unique(ind_all[order(code,-date)],by="code")[order(date)])
  
  if (any(grepl('CRYPTO', ind_all$name) | grepl('CURCRY', ind_all$code))){
    dt_perf =  try(TRAINEE_CALCULATE_FILE_PERFORMANCE_NEW  (pData = ind_all, pFolder = '', pFile   = '', EndDate  = SYSDATETIME(1),ToAddRef = F, Remove_MAXABSRT = F) )
  } else {
    dt_perf =  try(TRAINEE_CALCULATE_FILE_PERFORMANCE_NEW  (pData = ind_all, pFolder = '', pFile   = '', EndDate  = SYSDATETIME(1),ToAddRef = F ) )
  }
  GC_SILENT()
  if (all(class(dt_perf)!='try-error')){
    if (nrow(dt_perf) > 0){
      My.Kable(dt_perf)
      dt_perf[,excess:=0]
      dt_perf[code %in% Excess_List,excess:=1]
      dt_perf[order(excess)]
      
      codemother = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","dbl_ind_mother.rds")
      dt_perf = merge(dt_perf[,-c("name","short_name","wgtg","version","cur","coverage","category","base_date")],
                      codemother[,.(code,name,short_name,wgtg,version,cur,coverage,category,base_date)], by="code",all.x=T)
      
      # dt_excess = dt_perf[code %in% c("INDVNINDEX","INDSPX")]
      # dt_excess[code=="INDVNINDEX",coverage:="VIETNAM"]
      # dt_excess[code=="INDSPX",coverage:="INTERNATIONAL"]
      # dt_excess = select(dt_excess,"coverage",everything())
      # colnames(dt_excess) = c("coverage",paste0(colnames(dt_excess[,-c("coverage")]),"_bcm"))
      # 
      # bcm_cols = c("coverage",grep("M|Q|Y",names(dt_excess),value = T))
      # dt_perf = merge(dt_perf,dt_excess[,..bcm_cols],by="coverage",all.x=T)
      # 
      # dt_perf[,":="(Y6_cum=100*((1+Y6/100)*(1+Y5/100)*(1+Y4/100)*(1+Y3/100)*(1+Y2/100)*(1+Y1/100) - 1),
      #               M1_excess = M1-M1_bcm, M2_excess = M2-M2_bcm, M3_excess = M3-M3_bcm, M4_excess = M4-M4_bcm,
      #               M5_excess = M5-M5_bcm, M6_excess = M6-M6_bcm, Q1_excess = Q1-Q1_bcm, Q2_excess = Q2-Q2_bcm,
      #               Q3_excess = Q3-Q3_bcm, Q4_excess = Q4-Q4_bcm, Q5_excess = Q5-Q5_bcm, Q6_excess = Q6-Q6_bcm,
      #               Y1_excess = Y1-Y1_bcm, Y2_excess = Y2-Y2_bcm, Y3_excess = Y3-Y3_bcm, Y4_excess = Y4-Y4_bcm,
      #               Y5_excess = Y5-Y5_bcm, Y6_excess = Y6-Y6_bcm)]
      # 
      # dt_perf = select(dt_perf,-contains(grep("_bcm",names(dt_excess),value = T)))
      if ("Y6" %in% names(dt_perf))
      {
        dt_perf[,":="(Y6_cum=100*((1+Y6/100)*(1+Y5/100)*(1+Y4/100)*(1+Y3/100)*(1+Y2/100)*(1+Y1/100) - 1))]
      }
      if (nchar(SaveFile) > 0){
        DBL_CCPR_SAVERDS(dt_perf, SaveFolder, SaveFile)
      }
      if (ToUpload)
      {
        try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",SaveFile),
                                         l.filepath= paste0(SaveFolder,SaveFile),
                                         CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
      }
    }
    
  } else {
    CATln_Border('DATA IN FILE HAS REMOVED FROM FN CACULATE PERFORMANCE')
  }
  
  return(dt_perf)
}

# 
# DBL_UPDATE_UPLOAD_PERF_CHART_BY_FILE_OLD = function(  pOption = "PERFORMANCE" ,Folder_Index = UData, File_Index = "download_ifrc_indetf_history.rds",
#                                                       Perf_Index = "performance_ifrc_indetf_history.rds",
#                                                       Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
#                                                       File_Compare = "dbl_ind_performance.rds",
#                                                       ToUpdate = T, ToUpload = T, ToForce=F){
#   # Folder_Index = "S:/CCPR/DATA/"
#   # File_Index = "IFRC_CCPR_INDWCEOIN_ALL_HISTORY.rds"
#   # Perf_Index = "performance_ifrc_ind_history.rds"
#   if (ToForce || ToUpdate)
#   {
#     summary_index = setDT(fread(paste0(Folder_Index,gsub(".rds","_summary.txt",File_Index))))
#     
#     switch (pOption,
#             "PERFORMANCE" = {
#               # Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
#               # File_Compare = "dbl_ind_performance.rds"
#               WRITE_SUMMARY_FULLPATH(paste0(Folder_Compare,File_Compare))
#               summary_compare = setDT(fread(paste0(Folder_Compare,gsub(".rds","_summary.txt",File_Compare))))
#               
#               in_compare = summary_compare[code %in% summary_index$code]
#               if (ToForce || (max(summary_index$date) > max(in_compare$date)))
#               {
#                 perf_ind = TRAINEE_DBL_PERFORMANCE(FromFolder=Folder_Index, FromFile=File_Index,
#                                                    Excess_List=c("INDVNINDEX","INDSPX","INDNDX"),
#                                                    SaveFolder=Folder_Index,
#                                                    SaveFile=Perf_Index, ToUpload=F)
#                 perf_all = DBL_CCPR_READRDS(Folder_Compare, File_Compare)
#                 perf_final = unique(rbind(perf_all[!code %in% perf_ind$code], perf_ind, fill=T),by="code")
#                 
#                 codemother = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","dbl_ind_mother.rds")
#                 perf_final = merge(perf_final[,-c("name","short_name","wgtg","version","cur","coverage","category","base_date")],
#                                    codemother[,.(code,name,short_name,wgtg,version,cur,coverage,category,base_date)], by="code",all.x=T)
#                 
#                 My.Kable.Min(perf_final[code %in% perf_ind$code][order(date)])
#                 DBL_CCPR_SAVERDS(perf_final, Folder_Compare, File_Compare, ToSummary = T, SaveOneDrive = T)
#                 if (ToForce || ToUpload)
#                 {
#                   try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",File_Compare),
#                                                    l.filepath= paste0(Folder_Compare,File_Compare),
#                                                    CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
#                 }
#               }else{ CATln_Border("DATA ALREADY UPDATED!")}
#               
#             },
#             "CHART" = {
#               # Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
#               # File_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_month_history.rds"
#               # S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_month_history.rds
#               
#               PERIOD = ifelse(grepl('month', tolower(File_Compare)), 'MONTH', ifelse(grepl('quarter', tolower(File_Compare)), 'QUARTER', 'YEAR'))
#               WRITE_SUMMARY_FULLPATH(paste0(Folder_Compare,File_Compare))
#               summary_compare = setDT(fread(paste0(Folder_Compare,gsub(".rds","_summary.txt",File_Compare))))
#               
#               in_compare = summary_compare[code %in% summary_index$code]
#               if (ToForce || max(summary_index$date) > max(in_compare$date))
#               {
#                 perf_ind = try(TRAINEE_CREATE_PERIOD_FROM_HISTORY  (pData       = data.table(),
#                                                                     Period      = PERIOD, 
#                                                                     File_Folder = Folder_Index,
#                                                                     File_Name   = File_Index,
#                                                                     ToSave      = F))
#                 perf_all = DBL_CCPR_READRDS(Folder_Compare, File_Compare)
#                 perf_final = rbind(perf_all[!code %in% perf_ind$code], perf_ind, fill=T)
#                 
#                 codemother = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","dbl_ind_mother.rds")
#                 perf_final = merge(perf_final[,-c("name","short_name","wgtg","version","cur","coverage","category","base_date")],
#                                    codemother[,.(code,name,short_name,wgtg,version,cur,coverage,category,base_date)], by="code",all.x=T)
#                 
#                 My.Kable.Min(perf_final[code %in% perf_ind$code][order(date)])
#                 DBL_CCPR_SAVERDS(perf_final, Folder_Compare, File_Compare, ToSummary = T, SaveOneDrive = T)
#                 if (ToForce || ToUpload)
#                 {
#                   try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",paste0("dbl_ind_",tolower(PERIOD),"_chart")),
#                                                    l.filepath= paste0(Folder_Compare,File_Compare),
#                                                    CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
#                 }
#               }
#               
#             },
#             'RISK' = {
#               # Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
#               # File_Compare = "dbl_ind_risk.rds"
#               WRITE_SUMMARY_FULLPATH(paste0(Folder_Compare,File_Compare))
#               summary_compare = setDT(fread(paste0(Folder_Compare,gsub(".rds","_summary.txt",File_Compare))))
#               
#               in_compare = summary_compare[code %in% summary_index$code]
#               if (ToForce || (max(summary_index$date) > max(in_compare$date)))
#               {
#                 # perf_ind = TRAINEE_DBL_PERFORMANCE(FromFolder=Folder_Index, FromFile=File_Index,
#                 #                                    Excess_List=c("INDVNINDEX","INDSPX","INDNDX"),
#                 #                                    SaveFolder=Folder_Index,
#                 #                                    SaveFile=Perf_Index, ToUpload=F)
#                 
#                 risk_ind = TRAINEE_CALCULATE_FILE_RISK_NEW (pData = data.table(), pFolder = Folder_Index,
#                                                             pFile = File_Index, EndDate  = SYSDATETIME(hour(Sys.time())), ToUpload = F, ToSave = F)
#                 
#                 risk_all = DBL_CCPR_READRDS(Folder_Compare, File_Compare)
#                 risk_final = rbind(risk_all[!code %in% risk_ind$code], risk_ind, fill=T)
#                 
#                 codemother = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","dbl_ind_mother.rds")
#                 risk_final = merge(risk_final[,-c("name","short_name","wgtg","version","cur","coverage","category","base_date")],
#                                    codemother[,.(code,name,short_name,wgtg,version,cur,coverage,category,base_date)], by="code",all.x=T)
#                 
#                 My.Kable.Min(risk_final[code %in% risk_ind$code][order(date)])
#                 DBL_CCPR_SAVERDS(risk_final, Folder_Compare, File_Compare, ToSummary = T, SaveOneDrive = T)
#                 if (ToForce || ToUpload)
#                 {
#                   try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",File_Compare),
#                                                    l.filepath= paste0(Folder_Compare,File_Compare),
#                                                    CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
#                 }
#               }else{ CATln_Border("DATA ALREADY UPDATED!")}
#             }
#     )
#   }else{
#     if (ToUpload)
#     {
#       switch (pOption,
#               "PERFORMANCE" = {
#                 try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",File_Compare),
#                                                  l.filepath= paste0(Folder_Compare,File_Compare),
#                                                  CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
#               },
#               "CHART" = {
#                 PERIOD = ifelse(grepl('month', tolower(File_Compare)), 'MONTH', ifelse(grepl('quarter', tolower(File_Compare)), 'QUARTER', 'YEAR'))
#                 
#                 try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",paste0("dbl_ind_",tolower(PERIOD),"_chart")),
#                                                  l.filepath= paste0(Folder_Compare,File_Compare),
#                                                  CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
#               },
#               'RISK' = {
#                 try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",File_Compare),
#                                                  l.filepath= paste0(Folder_Compare,File_Compare),
#                                                  CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
#               }
#       )
#       
#     }
#   }
# }
# # ==================================================================================================
# DBL_UPLOAD_UPDATE_LOOP_OLD = function(){
#   # ------------------------------------------------------------------------------------------------
#   DBL_RELOAD_INSREF()
#   list_file = setDT(fread('S:/CCPR/DATA/DASHBOARD_LIVE/list_files_beq_ind_history.txt'))
#   for (i in 1:nrow(list_file)){
#     # i = 10
#     Folder = list_file$folder[i]
#     File = list_file$filename[i]
#     
#     try(DBL_UPDATE_UPLOAD_PERF_CHART_BY_FILE(  pOption = "PERFORMANCE" ,Folder_Index = Folder, File_Index = File,
#                                                Perf_Index = "performance_ifrc_indetf_history.rds",
#                                                Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
#                                                File_Compare = "dbl_ind_performance.rds",
#                                                ToUpdate = T, ToUpload = T, ToForce=T))
#     
#     try(DBL_UPDATE_UPLOAD_PERF_CHART_BY_FILE(  pOption = "CHART" ,Folder_Index = Folder, File_Index = File,
#                                                Perf_Index = "performance_ifrc_indetf_history.rds",
#                                                Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
#                                                File_Compare = "ccpi_dashboard_indbeq_all_month_history.rds",
#                                                ToUpdate = T, ToUpload = T, ToForce=T))
#     
#     try(DBL_UPDATE_UPLOAD_PERF_CHART_BY_FILE(  pOption = "CHART" ,Folder_Index = Folder, File_Index = File,
#                                                Perf_Index = "performance_ifrc_indetf_history.rds",
#                                                Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
#                                                File_Compare = "ccpi_dashboard_indbeq_all_quarter_history.rds",
#                                                ToUpdate = T, ToUpload = T, ToForce=F))
#     
#     try(DBL_UPDATE_UPLOAD_PERF_CHART_BY_FILE(  pOption = "CHART" ,Folder_Index = Folder, File_Index = File,
#                                                Perf_Index = "performance_ifrc_indetf_history.rds",
#                                                Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
#                                                File_Compare = "ccpi_dashboard_indbeq_all_year_history.rds",
#                                                ToUpdate = T, ToUpload = T, ToForce=F))
#     
#     try(DBL_UPDATE_UPLOAD_PERF_CHART_BY_FILE(  pOption = "RISK" ,Folder_Index = Folder, File_Index = File,
#                                                Perf_Index = "risk_ifrc_indetf_history.rds",
#                                                Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
#                                                File_Compare = "dbl_ind_risk.rds",
#                                                ToUpdate = T, ToUpload = T, ToForce=T))
#   }
# }


# ==================================================================================================
DBL_UPDATE_UPLOAD_PERF_CHART_BY_FILE = function(  pOption = "PERFORMANCE" ,Folder_Index = UData, File_Index = "download_ifrc_indetf_history.rds",
                                                  Perf_Index = "performance_ifrc_indetf_history.rds",
                                                  Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
                                                  File_Compare = "dbl_ind_performance.rds",
                                                  Data_Compare = data.table(),
                                                  ToForce=F){
  # ------------------------------------------------------------------------------------------------
  # Folder_Index = "S:/CCPR/DATA/DASHBOARD_LIVE/"
  # File_Index = "dbl_source_ins_day_history.rds"
  # Perf_Index = "performance_ifrc_ind_history.rds"
  perf_ind = data.table()
  
  if (nrow(Data_Compare) > 0){
    perf_all = Data_Compare
  } else {
    perf_all = DBL_CCPR_READRDS(Folder_Compare, File_Compare, ToRestore = T)
  }
  
  if (file.exists(paste0(Folder_Index, File_Index)))
  {
    WRITE_SUMMARY_FULLPATH(paste0(Folder_Index,File_Index))
    
    summary_index = setDT(fread(paste0(Folder_Index,gsub(".rds","_summary.txt",File_Index))))
    
    CATln_Border(paste0("FILE UPDATING.....:",Folder_Index, File_Index))
    switch (pOption,
            "PERFORMANCE" = {
              # Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
              # File_Compare = "dbl_ind_performance.rds"
              WRITE_SUMMARY_FULLPATH(paste0(Folder_Compare,File_Compare))
              # WRITE_SUMMARY_FULLPATH(paste0(Folder_Index ,File_Index ))
              
              summary_compare = setDT(fread(paste0(Folder_Compare,gsub(".rds","_summary.txt",File_Compare))))
              
              in_compare = summary_compare[code %in% summary_index$code]
              
              if (nrow(in_compare) > 0){
                # max_date_df1 = in_compare %>%
                #   group_by(code) %>%
                #   summarize(max_date1 = max(as.Date(date)))
                
                max_date_df1 = unique(in_compare[, max_date1 := max(date), by = 'code'], by = 'code')[, .(code, max_date1,last1 = last)]
                # max_date_df2 = summary_index %>%
                #   group_by(code) %>%
                #   summarize(max_date2 = max(date))
                max_date_df2 = unique(summary_index[, max_date2 := max(date), by = 'code'], by = 'code')[, .(code, max_date2, last2 = last)]
                
                merged_df = merge(max_date_df1, max_date_df2, by = "code")
                
                list_code = merged_df[merged_df$max_date1 < merged_df$max_date2 | merged_df$last1 != merged_df$last2]
                
                all_equal = all(merged_df$max_date1 >= merged_df$max_date2)
                
                
                if (ToForce || (max(summary_index$date) > max(in_compare$date)) || !all_equal)
                {
                  if (ToForce){
                    perf_ind = try(TRAINEE_DBL_PERFORMANCE(FromFolder=Folder_Index, FromFile=File_Index,
                                                           Excess_List="",
                                                           SaveFolder=Folder_Index,
                                                           SaveFile=Perf_Index, ToUpload=F))
                  } else {
                    if (nrow(list_code) > 0){
                      perf_ind = try(TRAINEE_DBL_PERFORMANCE(FromFolder=Folder_Index, FromFile=File_Index,
                                                             Excess_List="", List_Codes = list_code$code,
                                                             SaveFolder=Folder_Index,
                                                             SaveFile=Perf_Index, ToUpload=F))
                    } 
                  }
                  My.Kable(perf_ind)
                }else{ CATln_Border("DATA ALREADY UPDATED!")}
              } else {
                CATln_Border("INDEXES ARE NOT IN FILE!")
                perf_ind = try(TRAINEE_DBL_PERFORMANCE(FromFolder=Folder_Index, FromFile=File_Index,
                                                       Excess_List="",
                                                       SaveFolder=Folder_Index,
                                                       SaveFile=Perf_Index, ToUpload=F))
                My.Kable(perf_ind)
                
              }
            },
            "CHART" = {
              # Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
              # File_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_month_history.rds"
              # S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_month_history.rds
              
              PERIOD = ifelse(grepl('month', tolower(File_Compare)), 'MONTH', ifelse(grepl('quarter', tolower(File_Compare)), 'QUARTER', 'YEAR'))
              WRITE_SUMMARY_FULLPATH(paste0(Folder_Compare,File_Compare))
              summary_compare = setDT(fread(paste0(Folder_Compare,gsub(".rds","_summary.txt",File_Compare))))
              
              in_compare = summary_compare[code %in% summary_index$code]
              
              if (nrow(in_compare) > 0){
                max_date_df1 = unique(in_compare[, max_date1 := max(date), by = 'code'], by = 'code')[, .(code, max_date1)]
                # max_date_df2 = summary_index %>%
                #   group_by(code) %>%
                #   summarize(max_date2 = max(date))
                max_date_df2 = unique(summary_index[, max_date2 := max(date), by = 'code'], by = 'code')[, .(code, max_date2)]
                
                merged_df = merge(max_date_df1, max_date_df2, by = "code")
                
                list_code = merged_df[merged_df$max_date1 < merged_df$max_date2]
                # any_equal = any(merged_df$max_date1 < merged_df$max_date2)
                
                all_equal = all(merged_df$max_date1 >= merged_df$max_date2)
                if (ToForce || max(summary_index$date) > max(in_compare$date) || !all_equal)
                {
                  data = DBL_CCPR_READRDS(Folder_Index, File_Index, ToKable = T, ToRestore = T)
                  data_to_do = data[code %in% list_code$code]
                  if (ToForce){
                    perf_ind = try(TRAINEE_CREATE_PERIOD_FROM_HISTORY  (pData       = data,
                                                                        Period      = PERIOD, 
                                                                        File_Folder = Folder_Index,
                                                                        File_Name   = File_Index,
                                                                        ToSave      = F))
                  } else {
                    perf_ind = try(TRAINEE_CREATE_PERIOD_FROM_HISTORY  (pData       = data_to_do,
                                                                        Period      = PERIOD, 
                                                                        File_Folder = Folder_Index,
                                                                        File_Name   = File_Index,
                                                                        ToSave      = F))
                  }
                  
                  My.Kable(perf_ind)
                  
                } else{ CATln_Border("DATA ALREADY UPDATED!")}
              } else {
                CATln_Border("INDEXES ARE NOT IN FILE!")
                perf_ind = try(TRAINEE_CREATE_PERIOD_FROM_HISTORY  (pData       = data.table(),
                                                                    Period      = PERIOD, 
                                                                    File_Folder = Folder_Index,
                                                                    File_Name   = File_Index,
                                                                    ToSave      = F))
                My.Kable(perf_ind)
                
              }
            },
            'RISK' = {
              # Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
              # File_Compare = "dbl_ind_risk.rds"
              WRITE_SUMMARY_FULLPATH(paste0(Folder_Compare,File_Compare))
              summary_compare = setDT(fread(paste0(Folder_Compare,gsub(".rds","_summary.txt",File_Compare))))
              
              in_compare = summary_compare[code %in% summary_index$code]
              if (nrow(in_compare) > 0){
                max_date_df1 = unique(in_compare[, max_date1 := max(date), by = 'code'], by = 'code')[, .(code, max_date1)]
                # max_date_df2 = summary_index %>%
                #   group_by(code) %>%
                #   summarize(max_date2 = max(date))
                max_date_df2 = unique(summary_index[, max_date2 := max(date), by = 'code'], by = 'code')[, .(code, max_date2)]
                
                merged_df = merge(max_date_df1, max_date_df2, by = "code")
                list_code = merged_df[merged_df$max_date1 < merged_df$max_date2]
                
                all_equal = all(merged_df$max_date1 >= merged_df$max_date2)
                if (ToForce || (max(summary_index$date) > max(in_compare$date)) || !all_equal)
                {
                  
                  data = DBL_CCPR_READRDS(Folder_Index, File_Index, ToKable = T, ToRestore = T)
                  data_to_do = data[code %in% list_code$code]
                  
                  if (ToForce){
                    perf_ind = try(TRAINEE_CALCULATE_FILE_RISK_NEW (pData = data, pFolder = Folder_Index,
                                                                    pFile = File_Index, EndDate  = SYSDATETIME(hour(Sys.time())), ToUpload = F, ToSave = F))
                    
                  } else {
                    perf_ind = try(TRAINEE_CALCULATE_FILE_RISK_NEW (pData = data_to_do, pFolder = Folder_Index,
                                                                    pFile = File_Index, EndDate  = SYSDATETIME(hour(Sys.time())), ToUpload = F, ToSave = F))
                  }
                  
                  My.Kable(perf_ind)
                  
                }else{ CATln_Border("DATA ALREADY UPDATED!")}
              }else {
                CATln_Border("INDEXES ARE NOT IN FILE!")
                perf_ind = try(TRAINEE_CALCULATE_FILE_RISK_NEW (pData = data.table(), pFolder = Folder_Index,
                                                                pFile = File_Index, EndDate  = SYSDATETIME(hour(Sys.time())), ToUpload = F, ToSave = F))
                My.Kable(perf_ind)
                
              }
            },
            'LIQUIDITY' = {
              # Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
              # File_Compare = "dbl_ind_risk.rds"
              WRITE_SUMMARY_FULLPATH(paste0(Folder_Compare,File_Compare))
              summary_compare = setDT(fread(paste0(Folder_Compare,gsub(".rds","_summary.txt",File_Compare))))
              
              in_compare = summary_compare[code %in% summary_index$code]
              if (nrow(in_compare) > 0){
                max_date_df1 = unique(in_compare[, max_date1 := max(date), by = 'code'], by = 'code')[, .(code, max_date1)]
                # max_date_df2 = summary_index %>%
                #   group_by(code) %>%
                #   summarize(max_date2 = max(date))
                max_date_df2 = unique(summary_index[, max_date2 := max(date), by = 'code'], by = 'code')[, .(code, max_date2)]
                
                merged_df = merge(max_date_df1, max_date_df2, by = "code")
                list_code = merged_df[merged_df$max_date1 < merged_df$max_date2]
                
                all_equal = all(merged_df$max_date1 >= merged_df$max_date2)
                if (ToForce || (max(summary_index$date) > max(in_compare$date)) || !all_equal)
                {
                  
                  data = DBL_CCPR_READRDS(Folder_Index, File_Index, ToKable = T, ToRestore = T)
                  data_to_do = data[code %in% list_code$code]
                  
                  if (ToForce){
                    perf_ind = try(TRAINEE_CALCULATE_STKVN_LIQUIDITY (pData = data, pFolder = Folder_Index,
                                                                      pFile = File_Index, EndDate  = SYSDATETIME(hour(Sys.time())), ToUpload = F, ToSave = F))
                    
                  } else {
                    perf_ind = try(TRAINEE_CALCULATE_STKVN_LIQUIDITY (pData = data_to_do, pFolder = Folder_Index,
                                                                      pFile = File_Index, EndDate  = SYSDATETIME(hour(Sys.time())), ToUpload = F, ToSave = F))
                  }
                  
                  My.Kable(perf_ind)
                  
                }else{ CATln_Border("DATA ALREADY UPDATED!")}
              }else {
                CATln_Border("STOCK ARE NOT IN FILE!")
                perf_ind = try(TRAINEE_CALCULATE_STKVN_LIQUIDITY (pData = data.table(), pFolder = Folder_Index,
                                                                  pFile = File_Index, EndDate  = SYSDATETIME(hour(Sys.time())), ToUpload = F, ToSave = F))
                My.Kable(perf_ind)
                
              }
            }
    )
  }else{
    CATln_Border("FILE NO EXISTS!")
  }
  return (perf_ind)
  
}

# ==================================================================================================
DBL_UPLOAD_UPDATE_LOOP = function(pList = data.table(),pOption = "CHART_MONTH", ToUpload = F, ToUpdate = F,
                                  Folder = '', File = ''){
  # ------------------------------------------------------------------------------------------------
  pMyPC = as.character(try(fread("C:/R/my_pc.txt", header = F)))
  DBL_RELOAD_INSREF()
  xList = list()
  
  if (nchar(Folder)*nchar(File) > 0){
    list_file = data.table(active    = 1,
                           folder    = Folder,
                           filename  = File,
                           filter    = as.character(NA),
                           toforce   = 1,
                           pc_to_run = as.character(NA),
                           country   = as.character(NA))
  } else {
    if (nrow(pList) > 0){
      list_file = pList
    } else {
      list_file = setDT(fread('S:/CCPR/DATA/DASHBOARD_LIVE/list_files_beq_ind_history.txt'))
    }
  }
  
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
  DataCompare = CHECK_CLASS(try(DBL_CCPR_READRDS(Folder_Compare, File_Compare, ToRestore = T)))
  if (grepl('CHART', pOption)){
    pOption = 'CHART'
  }
  for (i in 1:nrow(list_file)){
    # i = 7
    Folder = list_file$folder[i]
    File = list_file$filename[i]
    
    if (File == 'dbl_source_ins_all_day_history.rds'){
      special = DBL_CCPR_READRDS(Folder, File, ToRestore = T)[code == 'INDFSSTI']
      Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/'
      File   = 'FSSTI.rds'
      DBL_CCPR_SAVERDS(special,Folder, File, ToSummary = T, SaveOneDrive = T)
    }
    
    # Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/'
    # File = 'dbl_source_ins_day_history.rds'
    # 0 = TRUE, 1 = FALSE
    # summary_index = setDT(fread(paste0(Folder,gsub(".rds","_summary.txt",File))))
    # summary_compare = setDT(fread(paste0(Folder_Compare,gsub(".rds","_summary.txt",File_Compare))))
    # in_compare = summary_compare[code %in% summary_index$code]
    # 
    # if (('type' %in% names(in_compare) & all(in_compare$type == 'IND')) | all(substr(in_compare$code,1,3) == 'IND')){
    #   ind_mother = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_ind_mother.rds')
    #   in_compare = merge(in_compare, ind_mother[, .(code, code_mother = index_compo)], by = 'code', all.x = T)
    #   # in_compare[, in_compare_mother := substr(code, 1, nchar(code) - 3)]
    #   # summary_compare[grepl(in_compare$in_compare_mother, code)]
    #   # ind_mother[grepl(in_compare$in_compare_mother, code)]
    #   # i = 2
    #   # ind_mother[index_compo %in% in_compare$code_mother]
    #   # code_mother = ind_mother[grepl(in_compare$in_compare_mother[[i]], code)]
    #   if (nrow(ind_mother[index_compo %in% in_compare$code_mother]) > nrow(in_compare)){
    #     Folder = list_file$folder[i]
    #     File = paste0('FULL_CURRENCIES_',list_file$filename[i])
    #   }
    # }
    #   
    #   #
    #   # data_one = try(DBL_INDEXES_CONVERT_CURRENCIES_FROM_USD(IND_FOLDER    = Folder_Index, IND_FILENAME = File_Index,IND_PREFIX_NAME = '',
    #   #                                                        Save_Folder   = Folder_Index, Save_File     = File_Index,
    #   #                                                        IND_CURS_LIST = list('GBP', 'EUR', 'JPY', 'SGD', 'VND', 'AUD', 'CAD', 'KRW', 'HKD', 'CNY')))
    #   
    # }
    
    pc_run = unlist(strsplit(as.character(list_file$pc_to_run[i]), ","))
    
    if (ToUpdate){
      force = T
    } else {
      force = ifelse(list_file$toforce[i] == 1, T, F)
    }
    
    if (length(pc_run) == 0){
      mother_one = try(DBL_ADD_CODE_BY_FILE(pOption = "IND_MOTHER", Folder_Index = Folder, File_Index = File, 
                                            Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
                                            File_Compare = "dbl_ind_mother.rds",ToForce=force))
      
      
      # figs_one = try(DBL_ADD_CODE_BY_FILE(pOption = "FIGURES", Folder_Index = Folder, File_Index = File, 
      #                                     Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
      #                                     File_Compare = "dbl_ind_figures.rds",ToForce=F))
      
      data = try(DBL_UPDATE_UPLOAD_PERF_CHART_BY_FILE(  pOption = pOption ,Folder_Index = Folder, File_Index = File,
                                                        Perf_Index = "",
                                                        Folder_Compare = Folder_Compare,
                                                        File_Compare = File_Compare,
                                                        Data_Compare = DataCompare,
                                                        ToForce = force))  
      
      if (all(class(data)!='try-error')){
        if (nrow(data)>0)
        {
          if (all(class(mother_one)=='try-error')) {   mother_one = CHECK_CLASS(try(DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_ind_mother.rds", ToRestore = T))) }
          data = merge(data[,-c("name","short_name","wgtg","version","cur","coverage","category","base_date","group")],
                       mother_one[,.(code,name,short_name,wgtg,version,cur,coverage,category,base_date,group)], by="code",all.x=T)
          
          xList[[i]] = data
        }
      }
    } else {
      CATln_Border(paste0('ONLY PCs " ',list_file$pc_to_run[i],' " CAN RUN THIS FILE...:',Folder,File))
    }
    
    if (length(pc_run) != 0 & toupper(pMyPC) %in% pc_run){
      mother_one = try(DBL_ADD_CODE_BY_FILE(pOption = "IND_MOTHER", Folder_Index = Folder, File_Index = File, 
                                            Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
                                            File_Compare = "dbl_ind_mother.rds",ToForce=force))
      
      
      # figs_one = try(DBL_ADD_CODE_BY_FILE(pOption = "FIGURES", Folder_Index = Folder, File_Index = File, 
      #                                     Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
      #                                     File_Compare = "dbl_ind_figures.rds",ToForce=F))
      
      data = try(DBL_UPDATE_UPLOAD_PERF_CHART_BY_FILE(  pOption = pOption ,Folder_Index = Folder, File_Index = File,
                                                        Perf_Index = "performance_ifrc_indetf_history.rds",
                                                        Folder_Compare = Folder_Compare,
                                                        File_Compare = File_Compare,
                                                        Data_Compare = DataCompare,
                                                        ToForce = force))  
      
      if (all(class(data)!='try-error')){
        if (nrow(data)>0)
        {
          if (all(class(mother_one)=='try-error')) {   mother_one = CHECK_CLASS(try(DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_ind_mother.rds", ToRestore = T))) }
          data = merge(data[,-c("name","short_name","wgtg","version","cur","coverage","category","base_date","group")],
                       mother_one[,.(code,name,short_name,wgtg,version,cur,coverage,category,base_date,group)], by="code",all.x=T)
          xList[[i]] = data
        }
      }
    } 
    
    
  }
  
  perf_ind = rbindlist(xList, fill = T)
  
  # setdiff(perf_ind$code, perf_all$code)
  if (nrow(perf_ind) > 0){
    # SAVE BACKUP
    try(DBL_CCPR_SAVERDS(DataCompare, Folder_Compare, gsub('.rds','_old.rds', File_Compare), ToSummary = T, SaveOneDrive = T, Nb_repeat = 1))
    
    
    if (!'code_mother' %in% names(perf_ind)){
      mother_one = CHECK_CLASS(try(DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_ind_mother.rds", ToRestore = T)))
      perf_ind = merge(perf_ind, mother_one[, .(code, code_mother = index_compo)], by = 'code', all.x = T)
    } else {
      if (nrow(perf_ind[is.na(code_mother)]) > 0){
        x = merge(perf_ind[is.na(code_mother)][, -c('code_mother')], mother_one[, .(code, code_mother = index_compo)], by = 'code', all.x = T)
        perf_ind = rbind(perf_ind[!code %in% x$code], x, fill = T)
      }
    }
    
    perf_all = DataCompare
    # codemother = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","dbl_ind_mother.rds")
    
    if (!grepl("CHART",pOption))
    {
      perf_final = unique(rbind(perf_all[!code %in% perf_ind$code], perf_ind, fill=T)[order(code,-date)],by=c("code"))
      perf_final = merge(perf_final[,-c("name","short_name","wgtg","version","cur","coverage","category","base_date","group")],
                         mother_one[,.(code,name,short_name,wgtg,version,cur,coverage,category,base_date,group)], by="code",all.x=T)
      excess_code = c("INDVNINDEX","INDSPX","INDNDX","CMDGOLD")
      excess_dt = perf_final[code %in% excess_code]
      excess_dt = merge(excess_dt[,-c("name","short_name")],ins_ref[,.(code,name,short_name)],by="code",all.x=T)
      perf_final = rbind(perf_final[!code %in% excess_dt$code],excess_dt[,excess:=1])
      
      if(pOption == 'PERFORMANCE'){
        perf_final = perf_final[!(is.na(M1) & is.na(M2) & is.na(M3) & is.na(M4) & is.na(M5) & is.na(M6))]
        
        dt_ann = DBL_CALCULATE_PERFORMANCE_BY_NUMBER_PERIOD(pFolder="S:/CCPR/DATA/DASHBOARD_LIVE/",
                                                            pFile=paste0("ccpi_dashboard_indbeq_all_year_history.rds"),
                                                            pType="IND",pPeriod = "YEAR", nb_per = 6)
        
        dt_ann = setDT(spread(dt_ann[,-c("maxrt")], "nby", "rtx"))
        
        dt_ann = dt_ann %>%
          mutate(Y6_annualised = coalesce(Y6_annualised, Y5_annualised, Y4_annualised, Y3_annualised, Y2_annualised, Y1_annualised))
        
        perf_final = merge(perf_final[, -c('Y6_annualised')], dt_ann[, .(code, Y6_annualised)], by = 'code', all.x = T)
        
        cols_structrue = setdiff(names(DataCompare)[grepl("[1-9]", names(DataCompare))], list("t1","t2"))
        
        perf_final = perf_final[, c("code", "date", "close", "close_adj", "rt", "change", "varpc", "country", "continent", "type", "prtr", "var", "yyyy", "yyyymm", "yyyyqn", "mtd", "qtd", "ytd",
                                    "last", "excess", "name_mother", "provider", "scat", "isin", "base_value", "code_mother", "name", "short_name", "wgtg", "version", "cur", "coverage", "category",
                                    "base_date", "group", ..cols_structrue)]
        
        perf_final = perf_final[mtd != 0 & ytd != 0 & !is.infinite(mtd) & !is.infinite(ytd)]
      }else{
        
        cols_structrue = setdiff(names(DataCompare)[grepl("[1-9]", names(DataCompare))], list("t1","t2"))
        
        perf_final = perf_final[, c("code", "date", "close", "close_adj", "rt", "change", "varpc", "country", "continent", "type", "prtr", "var", "yyyy", "yyyymm", "yyyyqn", "risk_mtd", "risk_qtd", "risk_ytd",
                                    "last", "excess", "name_mother", "provider", "scat", "isin", "base_value", "code_mother", "name", "short_name", "wgtg", "version", "cur", "coverage", "category",
                                    "base_date", "group", ..cols_structrue)]
        
        perf_final = perf_final[!(is.na(risk_M1) & is.na(risk_M2) & is.na(risk_M3) & is.na(risk_M4) & is.na(risk_M5) & is.na(risk_M6))]
      }
      perf_final = perf_final[month(perf_final$date) >= month(Sys.Date()) - 1 & year(date) == year(Sys.Date()) | grepl('INDVNXWMPR',code)]
      
      
    }else{
      perf_final = unique(rbind(perf_all[!code %in% perf_ind$code], perf_ind, fill=T),by=c("code","date"))
      perf_final = merge(perf_final[,-c("name","short_name","group")],
                         mother_one[,.(code,name,short_name,group)], by="code",all.x=T)
    }
    
    # My.Kable.Min(perf_final[order(-date)])
    
    # My.Kable.Min(perf_final[order(-date)])
    # Data_Old = DBL_CCPR_READRDS(Folder_Compare, gsub('.rds','_old.rds', File_Compare), ToRestore = T)
    # cols = setdiff(names(Data_Old)[grepl('[1-9]', names(Data_Old))], list('iso2','iso3','t1','t2'))
    # 
    # cols_to_update = setdiff(names(Data_Old[, c("code", cols), with = FALSE]), "code")
    # 
    # # Perform the join and update missing data
    # # perf_final[Data_Old, on = "code", (cols_to_update) := mget(paste0("i.", cols_to_update))]
    # 
    # for (col in cols_to_update) {
    #   # Update only where `perf_final` has NA and `Data_Old` has valid data
    #   perf_final[is.na(get(col)) & !is.na(Data_Old[[col]]),
    #              (col) := Data_Old[.SD, on = "code", get(col), by=.EACHI]$V1]
    # }
    
    # Data_Old[, c("code", cols), with = FALSE]
    # perf_final[, c("code", cols), with = FALSE]
    
    perf_final = UPDATE_UPDATED(perf_final[substr(code,nchar(code)-2,nchar(code))!="LOC",-c("updated")])
    
    
    # SAVE NEW UPDATE
    # try(DBL_CCPR_SAVERDS(perf_final, Folder_Compare, gsub('.rds','_old.rds', File_Compare), ToSummary = T, SaveOneDrive = T))    # SAVE NEW UPDATE
    try(DBL_CCPR_SAVERDS(perf_final, Folder_Compare,  File_Compare, ToSummary = T, SaveOneDrive = T, Nb_repeat = 1))
    
    if (ToUpload)
    {
      if (grepl('CHART', pOption)){
        PERIOD = ifelse(grepl('month', tolower(File_Compare)), 'MONTH', ifelse(grepl('quarter', tolower(File_Compare)), 'QUARTER', 'YEAR'))
        
        try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",paste0("dbl_ind_",tolower(PERIOD),"_chart")),
                                         l.filepath= paste0(Folder_Compare,File_Compare),
                                         CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
      } else {
        
        try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",File_Compare),
                                         l.filepath= paste0(Folder_Compare,File_Compare),
                                         CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
      }
    }
  } else {
    if (ToUpload)
    {
      if (grepl('CHART', pOption)){
        PERIOD = ifelse(grepl('month', tolower(File_Compare)), 'MONTH', ifelse(grepl('quarter', tolower(File_Compare)), 'QUARTER', 'YEAR'))
        
        try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",paste0("dbl_ind_",tolower(PERIOD),"_chart")),
                                         l.filepath= paste0(Folder_Compare,File_Compare),
                                         CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
      } else {
        perf_final = DataCompare
        perf_final = UPDATE_UPDATED(perf_final[substr(code,nchar(code)-2,nchar(code))!="LOC",-c("updated")])
        try(DBL_CCPR_SAVERDS(perf_final, Folder_Compare,  File_Compare, ToSummary = T, SaveOneDrive = T, Nb_repeat = 1))
        
        try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",File_Compare),
                                         l.filepath= paste0(Folder_Compare,File_Compare),
                                         CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
      }
    }
    CATln_Border("ALL DATA HAS ALREADY UPDATED!!!!!")
  }
  
  try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0('DASHBOARD_LIVE > UPDATE_IND_',pOption), pAction = "SAVE", NbSeconds = 900, ToPrint = F))
}

# # ==================================================================================================
# DBL_UPLOAD_UPDATE_LOOP = function(pData = data.table(),pOption = "CHART_MONTH", ToUpload = F, ToUpdate = F){
#   # ------------------------------------------------------------------------------------------------
#   pMyPC = as.character(try(fread("C:/R/my_pc.txt", header = F)))
#   DBL_RELOAD_INSREF()
#   xList = list()
#   if (nrow(pData) > 0){
#     list_file = pData
#   } else {
#     list_file = setDT(fread('S:/CCPR/DATA/DASHBOARD_LIVE/list_files_beq_ind_history.txt'))
#   }
#   
#   switch(pOption,
#          'PERFORMANCE' = {
#            Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
#            File_Compare = "dbl_ind_performance.rds"
#          },
#          'CHART_MONTH' = {
#            Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
#            File_Compare = "ccpi_dashboard_indbeq_all_month_history.rds"
#          },
#          'CHART_QUARTER' = {
#            Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
#            File_Compare = "ccpi_dashboard_indbeq_all_quarter_history.rds"
#          },
#          'CHART_YEAR' = {
#            Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
#            File_Compare = "ccpi_dashboard_indbeq_all_year_history.rds"
#          },
#          'RISK' = {
#            Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
#            File_Compare = "dbl_ind_risk.rds"
#          }
#   )
#   DataCompare = CHECK_CLASS(try(DBL_CCPR_READRDS(Folder_Compare, File_Compare, ToRestore = T)))
#   if (grepl('CHART', pOption)){
#     pOption = 'CHART'
#   }
#   for (i in 1:nrow(list_file)){
#     # i = 7
#     Folder = list_file$folder[i]
#     File = list_file$filename[i]
#     
#     if (File == 'dbl_source_ins_all_day_history.rds'){
#       special = DBL_CCPR_READRDS(Folder, File, ToRestore = T)[code == 'INDFSSTI']
#       Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/'
#       File   = 'FSSTI.rds'
#       DBL_CCPR_SAVERDS(special,Folder, File, ToSummary = T, SaveOneDrive = T)
#     }
#     
#     # Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/'
#     # File = 'dbl_source_ins_day_history.rds'
#     # 0 = TRUE, 1 = FALSE
#     # summary_index = setDT(fread(paste0(Folder,gsub(".rds","_summary.txt",File))))
#     # summary_compare = setDT(fread(paste0(Folder_Compare,gsub(".rds","_summary.txt",File_Compare))))
#     # in_compare = summary_compare[code %in% summary_index$code]
#     # 
#     # if (('type' %in% names(in_compare) & all(in_compare$type == 'IND')) | all(substr(in_compare$code,1,3) == 'IND')){
#     #   ind_mother = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_ind_mother.rds')
#     #   in_compare = merge(in_compare, ind_mother[, .(code, code_mother = index_compo)], by = 'code', all.x = T)
#     #   # in_compare[, in_compare_mother := substr(code, 1, nchar(code) - 3)]
#     #   # summary_compare[grepl(in_compare$in_compare_mother, code)]
#     #   # ind_mother[grepl(in_compare$in_compare_mother, code)]
#     #   # i = 2
#     #   # ind_mother[index_compo %in% in_compare$code_mother]
#     #   # code_mother = ind_mother[grepl(in_compare$in_compare_mother[[i]], code)]
#     #   if (nrow(ind_mother[index_compo %in% in_compare$code_mother]) > nrow(in_compare)){
#     #     Folder = list_file$folder[i]
#     #     File = paste0('FULL_CURRENCIES_',list_file$filename[i])
#     #   }
#     # }
#     #   
#     #   #
#     #   # data_one = try(DBL_INDEXES_CONVERT_CURRENCIES_FROM_USD(IND_FOLDER    = Folder_Index, IND_FILENAME = File_Index,IND_PREFIX_NAME = '',
#     #   #                                                        Save_Folder   = Folder_Index, Save_File     = File_Index,
#     #   #                                                        IND_CURS_LIST = list('GBP', 'EUR', 'JPY', 'SGD', 'VND', 'AUD', 'CAD', 'KRW', 'HKD', 'CNY')))
#     #   
#     # }
#     
#     pc_run = unlist(strsplit(as.character(list_file$pc_to_run[i]), ","))
#     
#     if (ToUpdate){
#       force = T
#     } else {
#       force = ifelse(list_file$toforce[i] == 1, T, F)
#     }
#     
#     if (length(pc_run) == 0){
#       mother_one = try(DBL_ADD_CODE_BY_FILE(pOption = "IND_MOTHER", Folder_Index = Folder, File_Index = File, 
#                                             Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
#                                             File_Compare = "dbl_ind_mother.rds",ToForce=force))
#       
#       
#       # figs_one = try(DBL_ADD_CODE_BY_FILE(pOption = "FIGURES", Folder_Index = Folder, File_Index = File, 
#       #                                     Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
#       #                                     File_Compare = "dbl_ind_figures.rds",ToForce=F))
#       
#       data = try(DBL_UPDATE_UPLOAD_PERF_CHART_BY_FILE(  pOption = pOption ,Folder_Index = Folder, File_Index = File,
#                                                         Perf_Index = "performance_ifrc_indetf_history.rds",
#                                                         Folder_Compare = Folder_Compare,
#                                                         File_Compare = File_Compare,
#                                                         Data_Compare = DataCompare,
#                                                         ToForce = force))  
#       
#       if (all(class(data)!='try-error')){
#         if (nrow(data)>0)
#         {
#           if (all(class(mother_one)=='try-error')) {   mother_one = CHECK_CLASS(try(DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_ind_mother.rds", ToRestore = T))) }
#           data = merge(data[,-c("name","short_name","wgtg","version","cur","coverage","category","base_date","group")],
#                        mother_one[,.(code,name,short_name,wgtg,version,cur,coverage,category,base_date,group)], by="code",all.x=T)
#           
#           xList[[i]] = data
#         }
#       }
#     } else {
#       CATln_Border(paste0('ONLY PCs " ',list_file$pc_to_run[i],' " CAN RUN THIS FILE...:',Folder,File))
#     }
#     
#     if (length(pc_run) != 0 & toupper(pMyPC) %in% pc_run){
#       mother_one = try(DBL_ADD_CODE_BY_FILE(pOption = "IND_MOTHER", Folder_Index = Folder, File_Index = File, 
#                                             Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
#                                             File_Compare = "dbl_ind_mother.rds",ToForce=force))
#       
#       
#       # figs_one = try(DBL_ADD_CODE_BY_FILE(pOption = "FIGURES", Folder_Index = Folder, File_Index = File, 
#       #                                     Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
#       #                                     File_Compare = "dbl_ind_figures.rds",ToForce=F))
#       
#       data = try(DBL_UPDATE_UPLOAD_PERF_CHART_BY_FILE(  pOption = pOption ,Folder_Index = Folder, File_Index = File,
#                                                         Perf_Index = "performance_ifrc_indetf_history.rds",
#                                                         Folder_Compare = Folder_Compare,
#                                                         File_Compare = File_Compare,
#                                                         Data_Compare = DataCompare,
#                                                         ToForce = force))  
#       
#       if (all(class(data)!='try-error')){
#         if (nrow(data)>0)
#         {
#           if (all(class(mother_one)=='try-error')) {   mother_one = CHECK_CLASS(try(DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_ind_mother.rds", ToRestore = T))) }
#           data = merge(data[,-c("name","short_name","wgtg","version","cur","coverage","category","base_date","group")],
#                        mother_one[,.(code,name,short_name,wgtg,version,cur,coverage,category,base_date,group)], by="code",all.x=T)
#           xList[[i]] = data
#         }
#       }
#     } 
#     
#     
#   }
#   
#   perf_ind = rbindlist(xList, fill = T)
#   
#   # setdiff(perf_ind$code, perf_all$code)
#   if (nrow(perf_ind) > 0){
#     # SAVE BACKUP
#     try(DBL_CCPR_SAVERDS(DataCompare, Folder_Compare, gsub('.rds','_old.rds', File_Compare), ToSummary = T, SaveOneDrive = T))
#     
#     
#     if (!'code_mother' %in% names(perf_ind)){
#       mother_one = CHECK_CLASS(try(DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_ind_mother.rds", ToRestore = T)))
#       perf_ind = merge(perf_ind, mother_one[, .(code, code_mother = index_compo)], by = 'code', all.x = T)
#     } else {
#       if (nrow(perf_ind[is.na(code_mother)]) > 0){
#         x = merge(perf_ind[is.na(code_mother)][, -c('code_mother')], mother_one[, .(code, code_mother = index_compo)], by = 'code', all.x = T)
#         perf_ind = rbind(perf_ind[!code %in% x$code], x, fill = T)
#       }
#     }
#     
#     perf_all = DataCompare
#     # codemother = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","dbl_ind_mother.rds")
#     
#     if (!grepl("CHART",pOption))
#     {
#       perf_final = unique(rbind(perf_all[!code %in% perf_ind$code], perf_ind, fill=T)[order(code,-date)],by=c("code"))
#       perf_final = merge(perf_final[,-c("name","short_name","wgtg","version","cur","coverage","category","base_date","group")],
#                          mother_one[,.(code,name,short_name,wgtg,version,cur,coverage,category,base_date,group)], by="code",all.x=T)
#       excess_code = c("INDVNINDEX","INDSPX","INDNDX","CMDGOLD")
#       excess_dt = perf_final[code %in% excess_code]
#       excess_dt = merge(excess_dt[,-c("name","short_name")],ins_ref[,.(code,name,short_name)],by="code",all.x=T)
#       perf_final = rbind(perf_final[!code %in% excess_dt$code],excess_dt[,excess:=1])
#       
#       if(pOption == 'PERFORMANCE'){
#         perf_final = perf_final[!(is.na(M1) & is.na(M2) & is.na(M3) & is.na(M4) & is.na(M5) & is.na(M6))]
#         
#         dt_ann = DBL_CALCULATE_PERFORMANCE_BY_NUMBER_PERIOD(pFolder="S:/CCPR/DATA/DASHBOARD_LIVE/",
#                                                             pFile=paste0("ccpi_dashboard_indbeq_all_year_history.rds"),
#                                                             pType="IND",pPeriod = "YEAR", nb_per = 6)
#         
#         dt_ann = setDT(spread(dt_ann[,-c("maxrt")], "nby", "rtx"))
#         
#         perf_final = merge(perf_final[, -c('Y6_annualised')], dt_ann[, .(code, Y6_annualised)], by = 'code', all.x = T)
#         
#         
#       }else{
#         perf_final = perf_final[!(is.na(risk_M1) & is.na(risk_M2) & is.na(risk_M3) & is.na(risk_M4) & is.na(risk_M5) & is.na(risk_M6))]
#       }
#       perf_final = perf_final[month(perf_final$date) >= month(Sys.Date()) & year(date) == year(Sys.Date()) | grepl('INDVNXWMPR',code)]
#       
#       
#     }else{
#       perf_final = unique(rbind(perf_all[!code %in% perf_ind$code], perf_ind, fill=T),by=c("code","date"))
#       perf_final = merge(perf_final[,-c("name","short_name","group")],
#                          mother_one[,.(code,name,short_name,group)], by="code",all.x=T)
#     }
#     
#     # My.Kable.Min(perf_final[order(-date)])
#     
#     perf_final = UPDATE_UPDATED(perf_final[substr(code,nchar(code)-2,nchar(code))!="LOC",-c("updated")])
#     # SAVE NEW UPDATE
#     try(DBL_CCPR_SAVERDS(perf_final, Folder_Compare,  File_Compare, ToSummary = T, SaveOneDrive = T))
#     
#     if (ToUpload)
#     {
#       if (grepl('CHART', pOption)){
#         PERIOD = ifelse(grepl('month', tolower(File_Compare)), 'MONTH', ifelse(grepl('quarter', tolower(File_Compare)), 'QUARTER', 'YEAR'))
#         
#         try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",paste0("dbl_ind_",tolower(PERIOD),"_chart")),
#                                          l.filepath= paste0(Folder_Compare,File_Compare),
#                                          CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
#       } else {
#         
#         try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",File_Compare),
#                                          l.filepath= paste0(Folder_Compare,File_Compare),
#                                          CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
#       }
#     }
#   } else {
#     if (ToUpload)
#     {
#       if (grepl('CHART', pOption)){
#         PERIOD = ifelse(grepl('month', tolower(File_Compare)), 'MONTH', ifelse(grepl('quarter', tolower(File_Compare)), 'QUARTER', 'YEAR'))
#         
#         try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",paste0("dbl_ind_",tolower(PERIOD),"_chart")),
#                                          l.filepath= paste0(Folder_Compare,File_Compare),
#                                          CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
#       } else {
#         perf_final = DataCompare
#         perf_final = UPDATE_UPDATED(perf_final[substr(code,nchar(code)-2,nchar(code))!="LOC",-c("updated")])
#         try(DBL_CCPR_SAVERDS(perf_final, Folder_Compare,  File_Compare, ToSummary = T, SaveOneDrive = T))
#         
#         try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",File_Compare),
#                                          l.filepath= paste0(Folder_Compare,File_Compare),
#                                          CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
#       }
#     }
#     CATln_Border("ALL DATA HAS ALREADY UPDATED!!!!!")
#   }
#   
#   try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0('DASHBOARD_LIVE > UPDATE_IND_',pOption), pAction = "SAVE", NbSeconds = 900, ToPrint = F))
# }


# ==================================================================================================
DBL_CALCULATE_PERFORMANCE_BY_NUMBER_PERIOD = function(pData=data.table(), pFolder, pFile, pType="IND", pPeriod = "YEAR",nb_per = 6){
  # ------------------------------------------------------------------------------------------------
  
  # pPeriod = "YEAR"
  # nb_per = 6
  if (nrow(pData) > 0){
    indall_year = pData
  } else {
    indall_year = DBL_CCPR_READRDS(pFolder,pFile)[order(code,-date)]
  }
  # indall_year = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/",paste0("ccpi_dashboard_indbeq_",tolower(pPeriod),"_history.rds"))[order(code,-date)]
  indall_year[,nby:=as.numeric(seq(1,.N)),by="code"]
  
  codemother = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/",paste0("dbl_",tolower(pType),"_mother.rds"))
  indall_year = merge(indall_year[,-c("coverage")], codemother[,.(code,coverage)], by="code",all.x=T)
  
  indcode_dt = indall_year[nby<=nb_per+1][order(code,date)]
  if (pType!="STK") { indcode_dt[,close_adj:=close] }
  indcode_dt[, rty:=close_adj/shift(close_adj) -1,by="code"]
  switch (pPeriod,
          "MONTH" = { setnames(indcode_dt,"yyyymm","colx"); indcode_dt[,nbdays:=30] },
          "QUARTER" = { setnames(indcode_dt,"yyyyqn","colx"); indcode_dt[,nbdays:=90] },
          "YEAR" = { setnames(indcode_dt,"yyyy","colx"); indcode_dt[,nbdays:=360] }
  )
  
  excess_dt = rbind(indcode_dt[code=="INDVNINDEX",.(coverage="VIETNAM",colx,excess_close=close_adj,excess_rt=rty)],
                    indcode_dt[code=="INDSPX",.(coverage="INTERNATIONAL",colx,excess_close=close_adj,excess_rt=rty)])
  
  indcode_dt = merge(indcode_dt[!code %in% excess_dt$code], excess_dt,by=c("colx","coverage"),all.x=T)
  indcode_dt[,rty_excess:=rty-excess_rt]
  indcode_dt[,":="(indcol=close_adj[.N], excesscol=excess_close[.N]),by="code"]
  indcode_dt[,":="(rty_cum=indcol/close_adj -1, excess_cum=excesscol/excess_close -1),by="code"]
  indcode_dt[,rty_cum_excess:=rty_cum-excess_cum]
  
  indcode_dt[,":="(rty_cum=shift(rty_cum), excess_cum=shift(excess_cum), rty_cum_excess=shift(rty_cum_excess)),by="code"]
  indcode_dt[,rty_cum_excess:=rty_cum-excess_cum]
  
  indcode_dt[,pdate:=shift(date),by="code"]
  indcode_dt[nby==1, nbdays:=as.numeric(date-pdate)]
  indcode_dt = indcode_dt[order(code,-date)]
  indcode_dt[,cumsum_nbd:=cumsum(nbdays),by="code"]
  
  indcode_dt = indcode_dt[order(code,date)]
  indcode_dt[,":="(rty_ann=(1+rty_cum)^(1/((cumsum_nbd/360))) -1),by="code"]
  
  indcode_dt[,":="(rty_ann=(1+rty_cum)^(1/((cumsum_nbd/360))) -1, excess_ann=(1+excess_cum)^(1/((cumsum_nbd/360))) -1),by="code"]
  indcode_dt[,rty_ann_excess:=rty_ann-excess_ann]
  
  perf_dt = rbind(indcode_dt[nby<max(nby), .(code,nby=paste0(substr(pPeriod,1,1),nby,"_annualised"),rtx=rty_ann*100)],
                  indcode_dt[nby<max(nby), .(code,nby=paste0(substr(pPeriod,1,1),nby,"_excess_annualised"),rtx=rty_ann_excess*100)])
  # perf_dt = setDT(spread(perf_dt, "nby", "rtx"))
  return(perf_dt)
}

# ==================================================================================================
DBL_CREATE_DATA_SCREENER = function(){
  # ------------------------------------------------------------------------------------------------
  list_period = c("MONTH","QUARTER","YEAR")
  ind_per = list()
  for (iper in 1:length(list_period)){
    ind_per[[iper]] = DBL_CALCULATE_PERFORMANCE_BY_NUMBER_PERIOD(pFolder="S:/CCPR/DATA/DASHBOARD_LIVE/",
                                                                 pFile=paste0("ccpi_dashboard_indbeq_all_",tolower(list_period[iper]),"_history.rds"),
                                                                 pType="IND",pPeriod = list_period[iper], nb_per = 6)
  }
  ind_per = rbindlist(ind_per)
  ind_per[!grepl("excess",nby),maxrt:=max(abs(rtx),na.rm = T),by="code"]
  ind_per = ind_per[maxrt<2000]
  ind_per = setDT(spread(ind_per[,-c("maxrt")], "nby", "rtx"))
  
  ind_perf = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","dbl_ind_performance.rds")
  ind_perf[,type:=substr(code,1,3)]
  colx = names(ind_per[,-c("code")])
  ind_perf = merge(ind_perf[,!..colx], ind_per, by="code",all.x=T)
  ind_perf = ind_perf[!(is.na(M1_annualised) & is.na(M2_annualised) & is.na(M3_annualised) & is.na(M4_annualised)
                        & is.na(M5_annualised) & is.na(M6_annualised))]
  
  indmother = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/",paste0("dbl_ind_mother.rds"))
  ind_perf = merge(ind_perf[,-c("name","short_name","wgtg","version","cur","coverage","category","base_date","group")],
                   indmother[,.(code,name,short_name,wgtg,version,cur,coverage,category,base_date,group)], by="code",all.x=T)  
  ind_perf = ind_perf[(coverage=="VIETNAM" & cur=="VND" & version=="PR" & wgtg=="CW")
                      | (coverage=="INTERNATIONAL" & cur=="USD" & version=="PR" & wgtg=="CW")]
  
  
  stk_per = list()
  for (iper in 1:length(list_period)){
    # pData = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", paste0("ccpi_dashboard_stk_all_",tolower(list_period[iper]),"_history.rds"))
    
    stk_per[[iper]] = DBL_CALCULATE_PERFORMANCE_BY_NUMBER_PERIOD(pFolder="S:/CCPR/DATA/DASHBOARD_LIVE/",
                                                                 pFile=paste0("ccpi_dashboard_stk_all_",tolower(list_period[iper]),"_history.rds"),
                                                                 pType="STK",pPeriod = list_period[iper], nb_per = 6)
  }
  stk_per = rbindlist(stk_per)
  stk_per[!grepl("excess",nby),maxrt:=max(abs(rtx),na.rm = T),by="code"]
  stk_per = stk_per[maxrt<1000]
  stk_per = setDT(spread(stk_per[,-c("maxrt")], "nby", "rtx"))
  
  stk_perf = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","dbl_stk_performance.rds")
  stk_perf[,type:=substr(code,1,3)]
  stk_perf[coverage=="VIETNAM",":="(iso2="VN",country="VIETNAM",continent="AISA")]
  colx = names(stk_per[,-c("code")])
  stk_perf = merge(stk_perf[,!..colx], stk_per, by="code",all.x=T)
  stk_perf = stk_perf[!(is.na(M1_annualised) & is.na(M2_annualised) & is.na(M3_annualised) & is.na(M4_annualised)
                        & is.na(M5_annualised) & is.na(M6_annualised))]
  
  stkmother = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/",paste0("dbl_stk_mother.rds"))
  stk_perf = merge(stk_perf[,-c("ticker","market","category","name","short_name","coverage","country","continent","sector","industry")],
                   stkmother[,.(code,ticker,market,category,name,short_name,coverage,country,continent,sector,industry)], by="code",all.x=T)  
  
  dt_all = unique(rbind(ind_perf[type=="IND",type:="INDEX"], stk_perf[type=="STK",type:="STOCK"],fill=T),by=c("code","type"))
  dt_all[!type %in% c("INDEX","STOCK"), type:=as.character(NA)]
  unique(dt_all$type)
  # DBL_CCPR_SAVERDS(dt_all, "S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_screener_performance.rds")
  
  data_index = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_ind_risk.rds", ToRestore = T)
  
  risk_cols = c("risk_Y1","risk_Y2","risk_Y3","risk_Y4","risk_Y5","risk_Y6",
                "risk_Q1","risk_Q2","risk_Q3","risk_Q4","risk_Q5","risk_Q6",
                "risk_M1","risk_M2","risk_M3","risk_M4","risk_M5","risk_M6")
  
  risk_cols_level = c("code","risk_Y1_level","risk_Y2_level","risk_Y3_level","risk_Y4_level","risk_Y5_level","risk_Y6_level",
                      "risk_Q1_level","risk_Q2_level","risk_Q3_level","risk_Q4_level","risk_Q5_level","risk_Q6_level",
                      "risk_M1_level","risk_M2_level","risk_M3_level","risk_M4_level","risk_M5_level","risk_M6_level")
  # dt = Res_Month
  # data_index[, paste0(risk_cols, "_level") := lapply(.SD, function(x) {
  #   ifelse(x > 0 & x <= 15, "LOW", 
  #          ifelse(x > 15 & x <= 30, "MEDIUM", 
  #                 ifelse(x > 30, "HIGH", NA)))
  # }), .SDcols = risk_cols]
  
  data_stk = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_stk_risk.rds", ToRestore = T)
  # data_stk[, paste0(risk_cols, "_level") := lapply(.SD, function(x) {
  #   ifelse(x > 0 & x <= 15, "LOW", 
  #          ifelse(x > 15 & x <= 30, "MEDIUM", 
  #                 ifelse(x > 30, "HIGH", NA)))
  # }), .SDcols = risk_cols]
  
  data = rbind(data_index[substr(code,1,3)=="IND"], data_stk[substr(code,1,3)=="STK"], fill = T)
  
  liquid = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_stk_liquidity.rds", ToRestore = T)
  liquid_cols = c("liquid_Y1","liquid_Y2","liquid_Y3","liquid_Y4","liquid_Y5","liquid_Y6",
                  "liquid_Q1","liquid_Q2","liquid_Q3","liquid_Q4","liquid_Q5","liquid_Q6",
                  "liquid_M1","liquid_M2","liquid_M3","liquid_M4","liquid_M5","liquid_M6")
  
  liquid_cols_color = c("liquid_Y1_color","liquid_Y2_color","liquid_Y3_color","liquid_Y4_color","liquid_Y5_color","liquid_Y6_color",
                        "liquid_Q1_color","liquid_Q2_color","liquid_Q3_color","liquid_Q4_color","liquid_Q5_color","liquid_Q6_color",
                        "liquid_M1_color","liquid_M2_color","liquid_M3_color","liquid_M4_color","liquid_M5_color","liquid_M6_color")
  
  
  liquid_cols_level = c("code","liquid_Y1_level","liquid_Y2_level","liquid_Y3_level","liquid_Y4_level","liquid_Y5_level","liquid_Y6_level",
                        "liquid_Q1_level","liquid_Q2_level","liquid_Q3_level","liquid_Q4_level","liquid_Q5_level","liquid_Q6_level",
                        "liquid_M1_level","liquid_M2_level","liquid_M3_level","liquid_M4_level","liquid_M5_level","liquid_M6_level")
  
  liquid[, paste0(liquid_cols, "_level") := lapply(.SD, function(x) {
    ifelse(x == '357A38', "HIGH", 
           ifelse(x == 'FDA124', "MEDIUM", 
                  ifelse(x == 'EF4444', "LOW", NA)))
  }), .SDcols = liquid_cols_color]
  
  
  # screener = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_screener_performance.rds")
  dt_all = merge(dt_all, data[, ..risk_cols_level], by = 'code', all.x = T)
  
  dt_all = merge(dt_all, liquid[, ..liquid_cols_level], by = 'code', all.x = T)
  
  dt_all[, (risk_cols_level) := lapply(.SD, as.character), .SDcols = risk_cols_level]
  dt_all[, (liquid_cols_level) := lapply(.SD, as.character), .SDcols = liquid_cols_level]
  
  DBL_CCPR_SAVERDS(dt_all, "S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_screener_performance.rds", ToSummary = T, SaveOneDrive = T)
  
  
  # library(data.table)
  # library(DBI)
  # library(RMySQL)
  # library(RMariaDB)
  # library(DT)
  # # user = "dashboard_user_login", password = "admin@beqholdings", host = '27.71.235.71', dbname = "ccpi_dashboard_login")
  # connection <- dbConnect(
  #   dbDriver("MySQL"),
  #   host = "27.71.235.71",
  #   port = 3306,
  #   user = "dashboard_user_login",
  #   password = "admin@beqholdings",
  #   db = "ccpi_dashboard_login"
  # )
  # # connection <- dbConnect(
  # #   dbDriver("MySQL"),
  # #   host = "27.71.235.71",
  # #   port = 3306,
  # #   user = "dashboard_user_login_data",
  # #   password = "admin@beqholdings",
  # #   db = "dashboard_user_login_dev"
  # # )
  # My.Kable(screener)
  # 
  # result = dbWriteTable(connection, 'dbl_screener_performance', screener, row.names = FALSE, overwrite = TRUE)
  # print(result)
  # dbDisconnect(connection)
  
  try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = 'dashboard_live', l.tablename= "dbl_screener_performance",
                                   l.filepath= tolower(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/' , 'dbl_screener_performance.rds')),
                                   CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
}


# # ==================================================================================================
# DBL_CREATE_IND_MOTHER = function (FromFolder = "S:/CCPR/DATA/DASHBOARD_LIVE/", FromFile = "ccpi_dashboard_indbeq_all_history.rds",
#                                   SaveFolder = "S:/CCPR/DATA/DASHBOARD_LIVE/", SaveFile = "dbl_ind_mother.rds")  {
#   # ------------------------------------------------------------------------------------------------
#   DBL_RELOAD_INSREF()
#   DATA_ALL = DBL_CCPR_READRDS(FromFolder,FromFile)[order(code,date)]
#   GC_SILENT()
#   if (!"short_name" %in% names(DATA_ALL)) { DATA_ALL[,short_name:=as.character(NA)] }
#   if (!"name" %in% names(DATA_ALL)) { DATA_ALL[,name:=as.character(NA)] }
#   
#   DATA_FINAL = unique (DATA_ALL[,.(name,short_name,base_date=min(date),base_value=close[1],
#                                    history=min(date),enddate=max(date),date=Sys.Date()),by="code"][order(code)], by = 'code' )
#   GC_SILENT()
#   DATA_FINAL = merge(DATA_FINAL, ins_ref[type=="IND",.(code,new_name=name)],by="code",all.x=T)
#   DATA_FINAL[is.na(name) & !is.na(new_name),name:=new_name]
#   DATA_FINAL$new_name = NULL
#   
#   DATA_NANAME = DATA_FINAL[is.na(name) & substr(code,nchar(code)-2,nchar(code)) %in% c("AUD", "CAD", "CNY", "EUR", "GBP", "HKD", "JPY", "KRW", "SGD", "USD", "VND") ][,codex:=paste0(substr(code,1,nchar(code)-3),"VND")]
#   DATA_NANAME = merge(DATA_NANAME[,-c("name","short_name")],
#                       DATA_FINAL[code %in% DATA_NANAME$codex,.(codex=code,name,short_name)],
#                       by = "codex", all.x=T)
#   DATA_NANAME[,":="(name=gsub("\\(VND\\)",paste0("\\(",substr(code,nchar(code)-2,nchar(code)),"\\)"),name),
#                     short_name=gsub("\\(VND\\)",paste0("\\(",substr(code,nchar(code)-2,nchar(code)),"\\)"),short_name))]
#   
#   DATA_FINAL = rbind(DATA_FINAL[!code %in% DATA_NANAME$code], DATA_NANAME[,-c("codex")])
#   
#   DATA_FINAL[is.na(name)]
#   DATA_FINAL[code=="INDVNXSECRSTEWTRVND"]
#   
#   DATA_FINAL[,c3:=substr(code,nchar(code)-2,nchar(code))]
#   DATA_FINAL[!c3 %in% c("AUD", "CAD", "CNY", "EUR", "GBP", "HKD", "JPY", "KRW", "SGD", "USD", "VND"), c3:=as.character(NA)]
#   DATA_FINAL[substr(name, nchar(name), nchar(name)) == ')' & substr(code, nchar(code) - 2, nchar(code)) == 'LOC', c3 := substr(word(name, 2, 2, '[(]'),1,3)]
#   DATA_FINAL[grepl("CRYPTO",name) & is.na(c3),c3:="USD"]
#   unique(DATA_FINAL$c3)
#   DATA_FINAL[is.na(c3)]
#   
#   DATA_FINAL = DATA_FINAL[substr(code,1,3)=="IND"]
#   # DATA_FINAL = DATA_FINAL[nchar(code)>12]
#   
#   DATA_FINAL[,c2:=substr(code,nchar(code)-4,nchar(code)-3)]
#   DATA_FINAL[!c2 %in% c("PR","TR"), c2:=as.character(NA)]
#   DATA_FINAL[grepl("CRYPTO",name),c2:="PR"]
#   unique(DATA_FINAL$c2)
#   DATA_FINAL[is.na(c2)]
#   
#   DATA_FINAL[,c1:=substr(code,nchar(code)-6,nchar(code)-5)]
#   DATA_FINAL[!c1 %in% c("CW","EW","FW"), c1:=as.character(NA)]
#   DATA_FINAL[grepl("CAPITALISATION WEIGHTED| CW ",name),c1:="CW"]
#   DATA_FINAL[grepl("EQUAL WEIGHTED| EW ",name),c1:="EW"]
#   DATA_FINAL[grepl("VNX",code) & is.na(c1) & !grepl("CW|EW",name), c1:="CW"]
#   unique(DATA_FINAL$c1)
#   DATA_FINAL[is.na(c1)]
#   
#   DATA_FINAL[,cur:=c3]
#   DATA_FINAL[,version:=c2]
#   DATA_FINAL[,wgtg:=c1]
#   
#   DATA_FINAL[,c4:=substr(code,nchar(code)-12,nchar(code)-7)]
#   
#   DATA_FINAL[c4=="SECENY" & is.na(name), name:=paste("IFRC/BEQ HOLDINGS VNX SECTOR ENERGY", version, wgtg, paste0("(",cur,")"))]
#   DATA_FINAL[c4=="SECENY"]
#   
#   DATA_FINAL[c4=="GRPFPT" & is.na(name), name:=paste("IFRC VNX GROUP FPT", version, wgtg, paste0("(",cur,")"))]
#   DATA_FINAL[c4=="GRPFPT"]
#   
#   DATA_FINAL[c4=="SECFIN" & is.na(name), name:=paste("IFRC/BEQ HOLDINGS VNX SECTOR FINANCE", version, wgtg, paste0("(",cur,")"))]
#   DATA_FINAL[c4=="SECFIN"]
#   
#   DATA_FINAL[c4=="SECRST" & is.na(name), name:=paste("IFRC/BEQ HOLDINGS VNX SECTOR REAL ESTATE", version, wgtg, paste0("(",cur,")"))]
#   DATA_FINAL[c4=="SECRST"]
#   
#   DATA_FINAL[c4=="GRPVTL" & is.na(name), name:=paste("IFRC VNX GROUP VIETTEL", version, wgtg, paste0("(",cur,")"))]
#   DATA_FINAL[c4=="GRPVTL"]
#   
#   DATA_FINAL[c4=="GRPHPT" & is.na(name), name:=paste("IFRC VNX GROUP HOA PHAT", version, wgtg, paste0("(",cur,")"))]
#   DATA_FINAL[c4=="GRPHPT"]
#   
#   DATA_FINAL[c4=="GRPVIN" & is.na(name), name:=paste("IFRC VNX GROUP VINGROUP", version, wgtg, paste0("(",cur,")"))]
#   DATA_FINAL[c4=="GRPVIN"]
#   
#   DATA_FINAL[c4=="GRPMAS" & is.na(name), name:=paste("IFRC VNX GROUP MASSAN", version, wgtg, paste0("(",cur,")"))]
#   DATA_FINAL[c4=="GRPMAS"]
#   
#   
#   DATA_FINAL[,name:=gsub("^IFRC/BEQ HOLDINGS IFRC/BEQ HOLDINGS ","IFRC/BEQ HOLDINGS ",name)]
#   DATA_FINAL[,name:=gsub("^IFRC/BEQ VNX ","IFRC/BEQ HOLDINGS VNX ",name)]
#   DATA_FINAL[is.na(short_name),short_name:=name]
#   DATA_FINAL[,short_name:=gsub("^IFRC |^IFRC/BEQ HOLDINGS |^IFRC/BEQ ","",short_name)]
#   
#   My.Kable(DATA_FINAL[grepl("GROUP",name)])
#   My.Kable(DATA_FINAL[is.na(name)])
#   
#   DATA_FINAL[,index_group:=c4]
#   
#   DATA_FINAL[,c5:=substr(code,1,nchar(code)-7)]
#   DATA_FINAL[,index_doc:=c5]
#   
#   DATA_FINAL[, index_compo:=paste0(index_doc,wgtg, "PRVND")]
#   
#   DATA_FINAL = TRAINEE_MERGE_COL_FROM_REF (pData = DATA_FINAL[,-c('category', 'coverage')],  Folder_Fr = "",File_Fr =  "" , 
#                                            ToSave = F ,   Folder_To = '' , File_To = '' ,
#                                            List_Fields  = c('category', 'coverage'))
#   
#   DATA_FINAL[grepl("ETF INDEX", name), ':=' (coverage = "INTERNATIONAL", category = 'TRADABLE')]
#   
#   DATA_FINAL[grepl("WOMEN", name) & grepl('VIETNAM|VNX', name) & !grepl('TOP', name), ':=' (coverage = "VIETNAM", category = 'BENCHMARK')]
#   DATA_FINAL[grepl("WOMEN", name) & grepl('VIETNAM|VNX', name) & grepl('TOP', name), ':=' (coverage = "VIETNAM", category = 'TRADABLE')]
#   
#   DATA_FINAL[grepl("WOMEN", name) & !grepl('VIETNAM|VNX', name) & !grepl('TOP', name), ':=' (coverage = "INTERNATIONAL", category = 'BENCHMARK')]
#   DATA_FINAL[grepl("WOMEN", name) & !grepl('VIETNAM|VNX', name) & grepl('TOP', name), ':=' (coverage = "INTERNATIONAL", category = 'TRADABLE')]
#   
#   DATA_FINAL[grepl("VNX ", name) & !grepl('TOP', name) & is.na(coverage) & is.na(category), ':=' (coverage = "VIETNAM", category = 'BENCHMARK')]
#   DATA_FINAL[grepl("VNX ", name) & grepl('TOP', name)  & is.na(coverage) & is.na(category), ':=' (coverage = "VIETNAM", category = 'TRADABLE')]
#   
#   DATA_FINAL[grepl("CRYPTO",name), ':=' (coverage = "INTERNATIONAL")]
#   DATA_FINAL[grepl("CRYPTO",name) & grepl("TOP",name), ':=' (category = "TRADABLE")]
#   DATA_FINAL[grepl("CRYPTO",name) & grepl("ALLSHARE",name), ':=' (category = "BENCHMARK")]
#   
#   DATA_FINAL[grepl("RESEARCH",name), ":="(category="BENCHMARK",coverage="VIETNAM")]
#   
#   DATA_FINAL[grepl("LARGE|MID|SMALL|SIZE",name), sub_category:="SIZE"]
#   DATA_FINAL[grepl("SECTOR|GICS|ICB|IFRC VNX ALLSHARE",name), sub_category:="SECTOR"]
#   DATA_FINAL[grepl("WOMEN CEO",name), sub_category:="WOMEN CEO"]
#   DATA_FINAL[grepl("CRYPTO",name), sub_category:="CRYPTO"]
#   DATA_FINAL[grepl("ETF",name), sub_category:="ETF INDEX"]
#   DATA_FINAL[grepl("THEMATIC",name), sub_category:="THEMATIC"]
#   DATA_FINAL[grepl("REGIONAL",name), sub_category:="REGIONAL"]
#   DATA_FINAL[grepl("PROVINCIAL",name), sub_category:="PROVINCIAL"]
#   DATA_FINAL[grepl("VNX GROUP",name), sub_category:="GROUP"]
#   DATA_FINAL[grepl("HSX|HNX|UPC|MARKET",name), sub_category:="MARKET"]
#   DATA_FINAL[name %in% c("IFRC VNX ALLSHARE","IFRC VNX GENERAL"), sub_category:="MARKET"]
#   DATA_FINAL[grepl("RESEARCH",name), sub_category:="RESEARCH"]
#   
#   DATA_FINAL[grepl("IFRC VNX ",name) & !grepl("TOP",name) & is.na(sub_category), sub_category:="SECTOR"]
#   DATA_FINAL[grepl("IFRC VNX ",name) & grepl("TOP",name) & is.na(sub_category), sub_category:="BLUECHIPS"]
#   
#   DATA_FINAL[grepl("IFRC/BEQ|RESEARCH|CRYPTO",name), provider:="IFRC/BEQ"]
#   
#   
#   DATA_FINAL[coverage=="VIETNAM",benchmark:="VNI"]
#   DATA_FINAL[coverage=="INTERNATIONAL",benchmark:="SP500"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("VNX",name), benchmark:="VNI"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("AUSTRALIA",name), benchmark:="ASX"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("SINGAPORE",name), benchmark:="STI"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("THAILAND",name), benchmark:="SET"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("BELGIUM",name), benchmark:="BEL"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("CHINA",name), benchmark:="CSI"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("GERMANY",name), benchmark:="DAX"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("FRANCE",name), benchmark:="CAC"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("INDONESIA",name), benchmark:="IDX"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("JAPAN",name), benchmark:="NIKKEI225"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("MALAYSIA",name), benchmark:="KLCI"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("NETHERLANDS",name), benchmark:="AEX"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("NORWAY",name), benchmark:="OSE"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("USA",name), benchmark:="SP500"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("PORTUGAL",name), benchmark:="PSI"]
#   DATA_FINAL[sub_category=="WOMEN CEO" & grepl("TAIWAN",name), benchmark:="TWII"]
#   
#   DATA_FINAL[, ":="(name=gsub("CAPITALISATION WEIGHTED|CAPITALISATION WEIGTED","CW",name),
#                     short_name=gsub("CAPITALISATION WEIGHTED|CAPITALISATION WEIGTED","CW",short_name))]
#   DATA_FINAL[, ":="(name=gsub("EQUAL WEIGHTED|EQUAL WEIGTED","EW",name),
#                     short_name=gsub("EQUAL WEIGHTED|EQUAL WEIGTED","EW",short_name))]
#   
#   DATA_FINAL[grepl("SECBRK",code),dbl:="SECBRK"]
#   DATA_FINAL[grepl("SECENY",code),dbl:="SECENY"]
#   DATA_FINAL[grepl("SECLOG",code),dbl:="SECLOG"]
#   DATA_FINAL[grepl("SECFIN",code),dbl:="SECFIN"]
#   DATA_FINAL[grepl("SECARI",code),dbl:="SECARI"]
#   DATA_FINAL[grepl("SECBNK",code),dbl:="SECBNK"]
#   DATA_FINAL[grepl("SECRST",code),dbl:="SECRST"]
#   DATA_FINAL[grepl("SECRTL",code),dbl:="SECRTL"]
#   DATA_FINAL[grepl("SECHLC",code),dbl:="SECHLC"]
#   
#   DATA_FINAL[grepl("CRYPTO CURRENCY",name),dbl:="CRYPTO"]
#   
#   DATA_FINAL[grepl("ETF INDEX",name),dbl:="ETF"]
#   
#   DATA_FINAL[grepl("GRP",code) | grepl("GROUP",name),dbl:="GRP"]
#   
#   DATA_FINAL[grepl("RESEARCH|PORTFOLIO",name),dbl:="RESEARCH"]
#   
#   DATA_FINAL[grepl("WOMEN",name) & coverage=="VIETNAM",dbl:="WOMEN_VN"]
#   DATA_FINAL[grepl("WOMEN",name) & coverage!="VIETNAM",dbl:="WOMEN_IN"]
#   
#   DATA_FINAL[grepl("GICS",name),dbl:="GICS"]
#   DATA_FINAL[grepl("ICB",name),dbl:="ICB"]
#   
#   DATA_FINAL[grepl("REGIONAL|PROVINCIAL",name),dbl:="PROVINCIAL"]
#   
#   DATA_FINAL[grepl("SIZE|MARKET|LARGE|MID|SMALL",name),dbl:="SIZE_MARKET"]
#   
#   DATA_FINAL[is.na(dbl) & grepl("VNX",name),dbl:="VNX"]
#   DATA_FINAL[is.na(dbl)]
#   
#   DATA_FINAL [grepl('REAL ESTATE',name)& grepl('GI|SEC',code), group:= 'REALESTATE']
#   DATA_FINAL [grepl('FINANC',name)& grepl('GI|SEC',code), group:= 'FINANCE']
#   DATA_FINAL [grepl('ENERGY',name)& grepl('GI|SEC',code), group:= 'ENERGY']
#   
#   DATA_FINAL = DATA_FINAL[!code %in% c("INDMSCIWORLDUSD","INDTUNINDEX20")]
#   
#   DATA_FINAL[,":="(c1=NULL,c2=NULL,c3=NULL,c4=NULL,c5=NULL)]
#   My.Kable(DATA_FINAL)
#   unique(DATA_FINAL$wgtg)
#   unique(DATA_FINAL$version)
#   unique(DATA_FINAL$cur)
#   # DATA_FINAL[grep("WOMEN",name)]
#   # DATA_FINAL = DATA_FINAL[,.(code,name,short_name,wgtg,version,cur,index_group,index_doc,index_compo,
#   #                            base_date,base_value,history)]
#   
#   check_files = c("dbl_ind_specifications.rds","dbl_ind_figures.rds","dbl_ind_performance.rds",
#                   "ccpi_dashboard_indbeq_all_month_history.rds","dbl_ind_compo.rds")
#   for (ifile in 1:length(check_files)) {
#     dt_file = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", check_files[ifile])
#     if ("updated" %in% names(dt_file))
#     {
#       if (grepl("compo",check_files[ifile]))
#       {
#         # dt_file[,code_mother:=gsub("PRVND|CWPRVND","CWPRVND",index_code)]
#         DATA_FINAL = merge(DATA_FINAL, unique(dt_file[,.(index_compo=code_mother, colx=as.Date(updated))],by="index_compo"),
#                            by = "index_compo", all.x=T)
#       }else{
#         if ("index_code" %in% names(dt_file)) { dt_file[,code:=index_code] }
#         DATA_FINAL = merge(DATA_FINAL, unique(dt_file[,.(code, colx=as.Date(updated))],by="code"),
#                            by = "code", all.x=T)
#       }
#       setnames(DATA_FINAL,"colx",gsub("dbl_ind_|.rds|ccpi_dashboard_indbeq_all_","",check_files[ifile]))
#     }
#   }
#   
#   DATA_FINAL = UPDATE_UPDATED(DATA_FINAL[,-c("updated")])
#   if (nchar(SaveFolder)>0 & nchar(SaveFile)>0)
#   {
#     DBL_CCPR_SAVERDS(DATA_FINAL, SaveFolder ,SaveFile)
#   }
#   # try (write.xlsx(DATA_FINAL, paste0(SaveFolder, gsub(".rds",".xlsx",SaveFile)) ) )
#   # try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = 'dashboard_live', l.tablename= gsub(".rds","",SaveFile),
#   #                                  l.filepath= tolower(paste0(SaveFolder, SaveFile ) ),
#   #                                  CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
#   
#   My.Kable.TB(DATA_FINAL)
#   return(DATA_FINAL)
# }

# ==================================================================================================
DBL_CREATE_IND_MOTHER = function (pData = data.table(), FromFolder = "S:/CCPR/DATA/DASHBOARD_LIVE/", FromFile = "dbl_source_ins_day_history.rds",
                                  SaveFolder = "S:/CCPR/DATA/DASHBOARD_LIVE/", SaveFile = "dbl_ind_mother.rds")  {
  # ------------------------------------------------------------------------------------------------
  DBL_RELOAD_INSREF()
  if (nrow(pData) > 0){
    DATA_ALL = pData
  } else {
    DATA_ALL = DBL_CCPR_READRDS(FromFolder,FromFile)[order(code,date)]
  }
  GC_SILENT()
  if (nrow(DATA_ALL) > 0){
    if (!"short_name" %in% names(DATA_ALL)) { DATA_ALL[,short_name:=as.character(NA)] }
    if (!"name" %in% names(DATA_ALL)) { DATA_ALL[,name:=as.character(NA)] }
    
    DATA_FINAL = unique (DATA_ALL[,.(name,short_name,base_date=min(date),base_value=close[1],
                                     history=min(date,na.rm = T),enddate=max(date,na.rm = T),
                                     date=max(date,na.rm = T)),by="code"][order(code)], by = 'code' )
    GC_SILENT()
    DATA_FINAL = merge(DATA_FINAL, ins_ref[type=="IND",.(code,new_name=name)],by="code",all.x=T)
    DATA_FINAL[is.na(name) & !is.na(new_name),name:=new_name]
    DATA_FINAL$new_name = NULL
    
    if (nrow(DATA_FINAL[is.na(name)])>0)
    {
      curloc = DATA_FINAL[!is.na(name),.(cur=substr(code,nchar(code)-2,nchar(code)))][1]$cur
      DATA_NANAME = DATA_FINAL[is.na(name) & substr(code,nchar(code)-2,nchar(code)) %in% c("AUD", "CAD", "CNY", "EUR", "GBP", "HKD", "JPY", "KRW", "SGD", "USD", "VND") ][,codex:=paste0(substr(code,1,nchar(code)-3),curloc)]
      DATA_NANAME = merge(DATA_NANAME[,-c("name","short_name")],
                          DATA_FINAL[code %in% DATA_NANAME$codex,.(codex=code,name,short_name)],
                          by = "codex", all.x=T)
      DATA_NANAME[,":="(name=gsub("\\(.*\\)",paste0("\\(",substr(code,nchar(code)-2,nchar(code)),"\\)"),name),
                        short_name=gsub("\\(.*\\)",paste0("\\(",substr(code,nchar(code)-2,nchar(code)),"\\)"),short_name)),by="code"]
      
      DATA_FINAL = rbind(DATA_FINAL[!code %in% DATA_NANAME$code], DATA_NANAME[,-c("codex")])
    }else{ curloc = NA }
    
    DATA_FINAL[is.na(name)]
    DATA_FINAL[code=="INDVNXSECRSTEWTRVND"]
    
    DATA_FINAL[,c3:=substr(code,nchar(code)-2,nchar(code))]
    DATA_FINAL[!c3 %in% c("AUD", "CAD", "CNY", "EUR", "GBP", "HKD", "JPY", "KRW", "SGD", "USD", "VND"), c3:=as.character(NA)]
    DATA_FINAL[substr(name, nchar(name), nchar(name)) == ')' & substr(code, nchar(code) - 2, nchar(code)) == 'LOC', c3 := substr(word(name, 2, 2, '[(]'),1,3)]
    DATA_FINAL[grepl("CRYPTO",name) & is.na(c3),c3:="USD"]
    unique(DATA_FINAL$c3)
    DATA_FINAL[is.na(c3)]
    
    DATA_FINAL = DATA_FINAL[substr(code,1,3)=="IND"]
    # DATA_FINAL = DATA_FINAL[nchar(code)>12]
    if (nrow(DATA_FINAL) > 0){
      DATA_FINAL[,c2:=substr(code,nchar(code)-4,nchar(code)-3)]
      DATA_FINAL[!c2 %in% c("PR","TR"), c2:=as.character(NA)]
      DATA_FINAL[grepl("CRYPTO",name),c2:="PR"]
      unique(DATA_FINAL$c2)
      DATA_FINAL[is.na(c2)]
      
      DATA_FINAL[,c1:=substr(code,nchar(code)-6,nchar(code)-5)]
      DATA_FINAL[!c1 %in% c("CW","EW","FW","VW"), c1:=as.character(NA)]
      DATA_FINAL[grepl("CAPITALISATION WEIGHTED| CW ",name),c1:="CW"]
      DATA_FINAL[grepl("EQUAL WEIGHTED| EW ",name),c1:="EW"]
      DATA_FINAL[grepl("VNX",code) & is.na(c1) & !grepl("CW|EW",name), c1:="CW"]
      DATA_FINAL[grepl("VNX",code) & is.na(c1) & grepl("EW",name), c1:="EW"]
      unique(DATA_FINAL$c1)
      DATA_FINAL[is.na(c1)]
      
      DATA_FINAL[,cur:=c3]
      DATA_FINAL[,version:=c2]
      DATA_FINAL[,wgtg:=c1]
      
      DATA_FINAL[,c4:=substr(code,4,nchar(code)-ifelse(grepl("CW|EW|FW|VW",code),7,ifelse(grepl("EPR|ETR",code) & wgtg=="EW",6,5)))]
      
      DATA_FINAL[c4=="VNXSECENY" & is.na(name), name:=paste("IFRC/BEQ HOLDINGS VNX SECTOR ENERGY", version, wgtg, paste0("(",cur,")"))]
      DATA_FINAL[c4=="VNXSECENY"]
      
      DATA_FINAL[c4=="VNGRPFPT" & is.na(name), name:=paste("IFRC VNX GROUP FPT", version, wgtg, paste0("(",cur,")"))]
      DATA_FINAL[c4=="VNGRPFPT"]
      
      DATA_FINAL[c4=="VNXSECFIN" & is.na(name), name:=paste("IFRC/BEQ HOLDINGS VNX SECTOR FINANCE", version, wgtg, paste0("(",cur,")"))]
      DATA_FINAL[c4=="VNXSECFIN"]
      
      DATA_FINAL[c4=="VNXSECRST" & is.na(name), name:=paste("IFRC/BEQ HOLDINGS VNX SECTOR REAL ESTATE", version, wgtg, paste0("(",cur,")"))]
      DATA_FINAL[c4=="VNXSECRST"]
      
      DATA_FINAL[c4=="VNGRPVTL" & is.na(name), name:=paste("IFRC VNX GROUP VIETTEL", version, wgtg, paste0("(",cur,")"))]
      DATA_FINAL[c4=="VNGRPVTL"]
      
      DATA_FINAL[c4=="VNGRPHPT" & is.na(name), name:=paste("IFRC VNX GROUP HOA PHAT", version, wgtg, paste0("(",cur,")"))]
      DATA_FINAL[c4=="VNGRPHPT"]
      
      DATA_FINAL[c4=="VNGRPVIN" & is.na(name), name:=paste("IFRC VNX GROUP VINGROUP", version, wgtg, paste0("(",cur,")"))]
      DATA_FINAL[c4=="VNGRPVIN"]
      
      DATA_FINAL[c4=="VNGRPMAS" & is.na(name), name:=paste("IFRC VNX GROUP MASSAN", version, wgtg, paste0("(",cur,")"))]
      DATA_FINAL[c4=="VNGRPMAS"]
      
      
      DATA_FINAL[,name:=gsub("^IFRC/BEQ HOLDINGS IFRC/BEQ HOLDINGS ","IFRC/BEQ HOLDINGS ",name)]
      DATA_FINAL[,name:=gsub("^IFRC/BEQ VNX ","IFRC/BEQ HOLDINGS VNX ",name)]
      DATA_FINAL[is.na(short_name),short_name:=name]
      DATA_FINAL[,short_name:=gsub("^IFRC |^IFRC/BEQ HOLDINGS |^IFRC/BEQ ","",short_name)]
      
      My.Kable(DATA_FINAL[grepl("GROUP",name)])
      My.Kable(DATA_FINAL[is.na(name)])
      
      DATA_FINAL[,index_group:=c4]
      
      DATA_FINAL = TRAINEE_MERGE_COL_FROM_REF (pData = DATA_FINAL[,-c('category', 'coverage','iso2')],  Folder_Fr = "",File_Fr =  "" , 
                                               ToSave = F ,   Folder_To = '' , File_To = '' ,
                                               List_Fields  = c('category', 'coverage', 'iso2'))
      
      DATA_FINAL[iso2 == 'VN', coverage := 'VIETNAM']
      DATA_FINAL[iso2 != 'VN' & !is.na(iso2), coverage := 'INTERNATIONAL']
      
      DATA_FINAL[(!grepl('[0-9]',code)), category := 'BENCHMARK']
      
      
      DATA_FINAL[grepl("ETF INDEX", name), ':=' (coverage = "INTERNATIONAL", category = 'TRADABLE')]
      
      DATA_FINAL[grepl("WOMEN", name) & grepl('VIETNAM|VNX', name) & !grepl('TOP', name), ':=' (coverage = "VIETNAM", category = 'BENCHMARK')]
      DATA_FINAL[grepl("WOMEN", name) & grepl('VIETNAM|VNX', name) & grepl('TOP', name), ':=' (coverage = "VIETNAM", category = 'TRADABLE')]
      
      DATA_FINAL[grepl("WOMEN", name) & !grepl('VIETNAM|VNX', name) & !grepl('TOP', name), ':=' (coverage = "INTERNATIONAL", category = 'BENCHMARK')]
      DATA_FINAL[grepl("WOMEN", name) & !grepl('VIETNAM|VNX', name) & grepl('TOP', name), ':=' (coverage = "INTERNATIONAL", category = 'TRADABLE')]
      
      DATA_FINAL[grepl("VNX ", name) & !grepl('TOP', name) & is.na(coverage) & is.na(category), ':=' (coverage = "VIETNAM", category = 'BENCHMARK')]
      DATA_FINAL[grepl("VNX ", name) & grepl('TOP', name)  & is.na(coverage) & is.na(category), ':=' (coverage = "VIETNAM", category = 'TRADABLE')]
      
      DATA_FINAL[grepl("CRYPTO",name), ':=' (coverage = "INTERNATIONAL")]
      DATA_FINAL[grepl("CRYPTO",name) & grepl("TOP",name), ':=' (category = "TRADABLE")]
      DATA_FINAL[grepl("CRYPTO",name) & grepl("ALLSHARE",name), ':=' (category = "BENCHMARK")]
      
      DATA_FINAL[grepl("RESEARCH",name), ":="(category="BENCHMARK",coverage="VIETNAM")]
      
      DATA_FINAL[grepl("LARGE|MID|SMALL|SIZE",name), sub_category:="SIZE"]
      DATA_FINAL[grepl("SECTOR|GICS|ICB|IFRC VNX ALLSHARE",name), sub_category:="SECTOR"]
      DATA_FINAL[grepl("WOMEN CEO",name), sub_category:="WOMEN CEO"]
      DATA_FINAL[grepl("CRYPTO",name), sub_category:="CRYPTO"]
      DATA_FINAL[grepl("ETF",name), sub_category:="ETF INDEX"]
      DATA_FINAL[grepl("THEMATIC",name), sub_category:="THEMATIC"]
      DATA_FINAL[grepl("REGIONAL",name), sub_category:="REGIONAL"]
      DATA_FINAL[grepl("PROVINCIAL",name), sub_category:="PROVINCIAL"]
      DATA_FINAL[grepl("VNX GROUP",name), sub_category:="GROUP"]
      DATA_FINAL[grepl("HSX|HNX|UPC|MARKET",name), sub_category:="MARKET"]
      DATA_FINAL[name %in% c("IFRC VNX ALLSHARE","IFRC VNX GENERAL"), sub_category:="MARKET"]
      DATA_FINAL[grepl("RESEARCH",name), sub_category:="RESEARCH"]
      
      DATA_FINAL[grepl("IFRC VNX ",name) & !grepl("TOP",name) & is.na(sub_category), sub_category:="SECTOR"]
      DATA_FINAL[grepl("IFRC VNX ",name) & grepl("TOP",name) & is.na(sub_category), sub_category:="BLUECHIPS"]
      
      DATA_FINAL[grepl("IFRC/BEQ|RESEARCH|CRYPTO",name), provider:="IFRC/BEQ"]
      
      
      DATA_FINAL[coverage=="VIETNAM",benchmark:="VNI"]
      DATA_FINAL[coverage=="INTERNATIONAL",benchmark:="SP500"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("VNX",name), benchmark:="VNI"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("AUSTRALIA",name), benchmark:="ASX"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("SINGAPORE",name), benchmark:="STI"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("THAILAND",name), benchmark:="SET"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("BELGIUM",name), benchmark:="BEL"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("CHINA",name), benchmark:="CSI"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("GERMANY",name), benchmark:="DAX"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("FRANCE",name), benchmark:="CAC"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("INDONESIA",name), benchmark:="IDX"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("JAPAN",name), benchmark:="NIKKEI225"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("MALAYSIA",name), benchmark:="KLCI"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("NETHERLANDS",name), benchmark:="AEX"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("NORWAY",name), benchmark:="OSE"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("USA",name), benchmark:="SP500"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("PORTUGAL",name), benchmark:="PSI"]
      DATA_FINAL[sub_category=="WOMEN CEO" & grepl("TAIWAN",name), benchmark:="TWII"]
      
      DATA_FINAL[, ":="(name=gsub("CAPITALISATION WEIGHTED|CAPITALISATION WEIGTED","CW",name),
                        short_name=gsub("CAPITALISATION WEIGHTED|CAPITALISATION WEIGTED","CW",short_name))]
      DATA_FINAL[, ":="(name=gsub("EQUAL WEIGHTED|EQUAL WEIGTED","EW",name),
                        short_name=gsub("EQUAL WEIGHTED|EQUAL WEIGTED","EW",short_name))]
      
      DATA_FINAL[grepl("SECBRK",code),dbl:="SECBRK"]
      DATA_FINAL[grepl("SECENY",code),dbl:="SECENY"]
      DATA_FINAL[grepl("SECLOG",code),dbl:="SECLOG"]
      DATA_FINAL[grepl("SECFIN",code),dbl:="SECFIN"]
      DATA_FINAL[grepl("SECARI",code),dbl:="SECARI"]
      DATA_FINAL[grepl("SECBNK",code),dbl:="SECBNK"]
      DATA_FINAL[grepl("SECRST",code),dbl:="SECRST"]
      DATA_FINAL[grepl("SECRTL",code),dbl:="SECRTL"]
      DATA_FINAL[grepl("SECHLC",code),dbl:="SECHLC"]
      
      DATA_FINAL[grepl("CRYPTO CURRENCY",name),dbl:="CRYPTO"]
      
      DATA_FINAL[grepl("ETF INDEX",name),dbl:="ETF"]
      
      DATA_FINAL[grepl("GRP",code) | grepl("GROUP",name),dbl:="GRP"]
      
      DATA_FINAL[grepl("RESEARCH|PORTFOLIO",name),dbl:="RESEARCH"]
      
      DATA_FINAL[grepl("WOMEN",name) & coverage=="VIETNAM",dbl:="WOMEN_VN"]
      DATA_FINAL[grepl("WOMEN",name) & coverage!="VIETNAM",dbl:="WOMEN_IN"]
      
      DATA_FINAL[grepl("GICS",name),dbl:="GICS"]
      DATA_FINAL[grepl("ICB",name),dbl:="ICB"]
      
      DATA_FINAL[grepl("REGIONAL|PROVINCIAL",name),dbl:="PROVINCIAL"]
      
      DATA_FINAL[grepl("SIZE|LARGE|MID|SMALL",name),dbl:="SIZE"]
      
      DATA_FINAL[grepl("MARKET",name),dbl:="MARKET"]
      
      DATA_FINAL[is.na(dbl) & grepl("VNX",name),dbl:="VNX"]
      DATA_FINAL[is.na(dbl)]
      
      DATA_FINAL [grepl('REAL ESTATE',name)& grepl('GI|SEC',code), group:= 'REAL ESTATE']
      DATA_FINAL [grepl('FINANC',name)& grepl('GI|SEC',code), group:= 'FINANCE']
      DATA_FINAL [grepl('ENERGY',name)& grepl('GI|SEC',code), group:= 'ENERGY']
      
      # DATA_FINAL = DATA_FINAL[!code %in% c("INDMSCIWORLDUSD","INDTUNINDEX20")]
      
      DATA_FINAL[,c5:=substr(code,1,nchar(code)-ifelse(grepl("CW|EW|FW|VW",code),7,ifelse(grepl("EPR|ETR",code) & wgtg=="EW",6,5)))]
      DATA_FINAL[,index_doc:=c5]
      
      # DATA_FINAL[, index_compo:=paste0(index_doc,wgtg, "PR",ifelse(coverage=="VIETNAM","VND","USD"))]
      DATA_FINAL[, index_compo:=paste0(index_doc,ifelse(grepl("CW|EW|FW|VW",code),wgtg,
                                                        ifelse(grepl("EPR|ETR",code) & wgtg=="EW","E","")),
                                       "PR",ifelse(coverage=="VIETNAM","VND","USD"))]
      
      DATA_FINAL[,c6:=substr(code,nchar(code)-2,nchar(code))]
      DATA_FINAL[,c7:=substr(code,nchar(code)-4,nchar(code)-3)]
      
      DATA_FINAL[grepl('CRYPTO CURRENCY',name) & !c6 %in% c("AUD", "CAD", "CNY", "EUR", "GBP", "HKD", "JPY", "KRW", "SGD", "USD", "VND") & !c7 %in% c('PR','TR'), 
                 ':=' (index_compo = paste0(code,'PR','USD'), index_doc = gsub(wgtg,'',code), index_group = gsub('IND','',gsub(wgtg,'',code)))]
      
      DATA_FINAL[,":="(c1=NULL,c2=NULL,c3=NULL,c4=NULL,c5=NULL,c6=NULL,c7=NULL)]
      
      My.Kable(DATA_FINAL)
      unique(DATA_FINAL$wgtg)
      unique(DATA_FINAL$version)
      unique(DATA_FINAL$cur)
      # DATA_FINAL[grep("WOMEN",name)]
      # DATA_FINAL = DATA_FINAL[,.(code,name,short_name,wgtg,version,cur,index_group,index_doc,index_compo,
      #                            base_date,base_value,history)]
      
      check_files = c("dbl_ind_specifications.rds","dbl_ind_figures.rds","dbl_ind_performance.rds",
                      "ccpi_dashboard_indbeq_all_month_history.rds","dbl_ind_compo.rds")
      for (ifile in 1:length(check_files)) {
        dt_file = CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", check_files[ifile])
        if ("updated" %in% names(dt_file))
        {
          if (grepl("compo",check_files[ifile]))
          {
            # dt_file[,code_mother:=gsub("PRVND|CWPRVND","CWPRVND",index_code)]
            DATA_FINAL = merge(DATA_FINAL, unique(dt_file[,.(index_compo=code_mother, colx=as.Date(updated))],by="index_compo"),
                               by = "index_compo", all.x=T)
          }else{
            if ("index_code" %in% names(dt_file)) { dt_file[,code:=index_code] }
            DATA_FINAL = merge(DATA_FINAL, unique(dt_file[,.(code, colx=as.Date(updated))],by="code"),
                               by = "code", all.x=T)
          }
          setnames(DATA_FINAL,"colx",gsub("dbl_ind_|.rds|ccpi_dashboard_indbeq_all_","",check_files[ifile]))
        }
      }
      
      DATA_FINAL = UPDATE_UPDATED(DATA_FINAL[,-c("updated")])
      if (nchar(SaveFolder)>0 & nchar(SaveFile)>0)
      {
        DBL_CCPR_SAVERDS(DATA_FINAL, SaveFolder ,SaveFile)
      }
      # try (write.xlsx(DATA_FINAL, paste0(SaveFolder, gsub(".rds",".xlsx",SaveFile)) ) )
      # try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = 'dashboard_live', l.tablename= gsub(".rds","",SaveFile),
      #                                  l.filepath= tolower(paste0(SaveFolder, SaveFile ) ),
      #                                  CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
      
      My.Kable.TB(DATA_FINAL)
    } else {DATA_FINAL = data.table()}
  } else {DATA_FINAL = data.table()}
  return(DATA_FINAL)
}

# ==================================================================================================
DBL_ADD_CODE_BY_FILE = function(pOption, Folder_Index, File_Index, Folder_Compare, File_Compare, ToForce){
  # ------------------------------------------------------------------------------------------------
  
  # pOption = "IND_MOTHER"
  # Folder_Index = "U:/EFRC/DATA/IFRC_INDEXES_2023/"
  # File_Index = "IFRC_CCPR_IND_SECTORS_ICB_ALL.rds"
  # Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
  # File_Compare = "dbl_ind_mother.rds"
  summary_index = try(setDT(fread(paste0(Folder_Index,gsub(".rds","_summary.txt",File_Index)))))
  
  if (all(class(summary_index)!="try-error"))
  {
    WRITE_SUMMARY_FULLPATH(paste0(Folder_Compare,File_Compare))
    # WRITE_SUMMARY_FULLPATH(paste0(Folder_Index ,File_Index ))
    
    summary_compare = setDT(fread(paste0(Folder_Compare,gsub(".rds","_summary.txt",File_Compare))))
    
    new_codes = summary_index[!code %in% summary_compare$code]
    
    if (ToForce || nrow(new_codes) > 0){
      switch (pOption,
              "IND_MOTHER" = {
                pData = DBL_CCPR_READRDS(Folder_Index, File_Index, ToRestore = T)
                if (!ToForce){
                  pData = pData[code %in% new_codes$code]
                }
                dt_one = try(DBL_CREATE_IND_MOTHER(pData, FromFolder = Folder_Index, FromFile = File_Index,
                                                   SaveFolder = "", SaveFile = ""))
                dt_all = DBL_CCPR_READRDS(Folder_Compare,File_Compare)
                
                if (nrow(dt_one) > 0){
                  # dt_all[,":="(base_date=as.Date(base_date), history=as.Date(history), enddate=as.Date(enddate),
                  #              date=as.Date(date), specifications=as.Date(specifications), figures=as.Date(figures),
                  #              performance=as.Date(performance), month_chart=as.Date(month_chart),
                  #              compo=as.Date(compo))]
                  dt_all = unique(rbind(dt_all[!code %in% dt_one$code], dt_one, fill = T),by="code")
                  # dt_all[!is.na(close)]
                  # dt_all[, ':=' (codesource = NULL, source = NULL, close = NULL)]
                  DBL_CCPR_SAVERDS(dt_all, Folder_Compare, File_Compare, ToSummary = T, SaveOneDrive = T)
                  try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = 'dashboard_live', l.tablename= gsub(".rds","",File_Compare),
                                                   l.filepath= tolower(paste0(Folder_Compare,File_Compare ) ),
                                                   CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
                  
                  # CHART_CODE_LABEL
                  label_all = CCPR_READRDS(Folder_Compare,"dbl_chart_code_label.rds")
                  label_all = unique(rbind(label_all[!code %in% dt_all$code], dt_all[,.(code,label=short_name,updated)]),by="code")
                  
                  label_add = CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","ifrc_ccpr_investment_history.rds")
                  label_all = unique(rbind(label_all, unique(label_add[!code %in% label_all$code,.(code,label=name)],by="code"),fill=T),by="code")
                  
                  CCPR_SAVERDS(label_all, Folder_Compare, "dbl_chart_code_label.rds")
                  try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = 'dashboard_live', l.tablename= "dbl_chart_code_label",
                                                   l.filepath= tolower(paste0(Folder_Compare,"dbl_chart_code_label.rds" ) ),
                                                   CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
                }
              },
              "FIGURES" = {
                dt_all = try(DBL_FIGURES_BY_FILE(  pFolder = Folder_Index, pFile = File_Index ,
                                                   ListCodes = "",CompoFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                   CompoFile = "dbl_ind_compo.rds",
                                                   ToFolder = "", ToFile  = '',
                                                   FolderMerge = Folder_Compare, FileMerge = File_Compare,
                                                   ToSave = F, ToIntergration = T,ToUpload = T,pHost = "dashboard_live"))
              }
      )
      
      My.Kable.Min(dt_all)
    }
    else {
      dt_all = DBL_CCPR_READRDS(Folder_Compare,File_Compare)
    }
  }else{
    dt_all = DBL_CCPR_READRDS(Folder_Compare,File_Compare)
  }
  return(dt_all)
}

# ==================================================================================================
DBL_CONVERT_CURRENCY = function(pData = data.table(),pOption = 'PERFORMANCE', ToForce = F){
  # ------------------------------------------------------------------------------------------------
  DBL_RELOAD_INSREF()
  # INDEXES_DATA_CLEAN_OPTIMISE(Option = 'FILL_ALL_DAYS', Folder = UData, FileName = 'IFRC_CUR_4INDEX.rds', Source = '')
  if (nrow(pData) > 0){
    list_file = pData
  } else {
    list_file = setDT(fread('S:/CCPR/DATA/DASHBOARD_LIVE/list_files_beq_ind_history.txt'))
  }
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
  # dt_rep = DBL_REPORT_DATA_UPDATE_LIST_FILE(Chart_Type = 'CHART_MONTH')
  list_file = list_file[filename != 'ifrc_ccpr_investment_history.rds']
  list_file = list_file[filename != 'dbl_source_ins_all_day_history.rds']
  
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
          # if (nrow(CUR_VND) > 0 & nrow(CUR_USD) > 0 & max(CUR_USD$date) < max(CUR_VND$date)){
          #   TO_DO_VND = T
          # } else {
          #   TO_DO_VND = F
          # }
          
          if (nrow(CUR_VND) > 0){
            if (nrow(CUR_USD) > 0){
              if (max(CUR_USD$date) < max(CUR_VND$date)){
                TO_DO_VND = T
              } else {TO_DO_VND = F}
            } else {TO_DO_VND = T }
          } else {TO_DO_VND = F}
          
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
                                                             Save_Folder     = Folder_Index, Save_File     = paste0(File_Index),
                                                             CUR_FILE_NAME   = 'IFRC_CUR_4INDEX.rds',
                                                             IND_CURS_LIST   = list('USD', 'GBP', 'EUR', 'JPY', 'SGD', 'AUD', 'CAD', 'KRW', 'HKD', 'CNY'),
                                                             IncludeOnly_VND = T))
              } else {
                data_one = try(DBL_INDEXES_CONVERT_CURRENCIES_FROM_USD(IND_FOLDER    = Folder_Index, IND_FILENAME = File_Index,IND_PREFIX_NAME = '',
                                                                       Save_Folder   = Folder_Index, Save_File     = paste0(File_Index),
                                                                       CUR_FOLDER = UData,
                                                                       CUR_FILENAME = 'IFRC_CUR_4INDEX.rds',
                                                                       IND_CURS_LIST = list('GBP', 'EUR', 'JPY', 'SGD', 'VND', 'AUD', 'CAD', 'KRW', 'HKD', 'CNY')))
              }
            } else {
              CATln_Border("ALL DATA HAS ALREADY UPDATED!!!!!")
            }
          } else {
            if (TO_DO_VND){
              try(DBL_INDEXES_CONVERT_CURRENCIES_FROM_VND (IND_FOLDER      = Folder_Index, IND_FILENAME  = File_Index,
                                                           Save_Folder     = Folder_Index, Save_File     = paste0(File_Index),
                                                           CUR_FILE_NAME   = 'IFRC_CUR_4INDEX.rds',
                                                           IND_CURS_LIST   = list('USD', 'GBP', 'EUR', 'JPY', 'SGD', 'AUD', 'CAD', 'KRW', 'HKD', 'CNY'),
                                                           IncludeOnly_VND = T))
            } else {
              data_one = try(DBL_INDEXES_CONVERT_CURRENCIES_FROM_USD(IND_FOLDER    = Folder_Index, IND_FILENAME = File_Index,IND_PREFIX_NAME = '',
                                                                     Save_Folder   = Folder_Index, Save_File     = paste0(File_Index),
                                                                     CUR_FOLDER = UData,
                                                                     CUR_FILENAME = 'IFRC_CUR_4INDEX.rds',
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
DBL_FIGURES_BY_FILE = function (  pFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', pFile = paste0( 'ccpi_dashboard_indbeq_all_history.rds' ),
                                  ListCodes = c("INDVNXSECLOGCWPRVND"),CompoFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                  CompoFile = "dbl_ind_compo.rds",
                                  ToFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', ToFile  = paste0('all_figures.rds'),
                                  FolderMerge = 'S:/CCPR/DATA/DASHBOARD_LIVE/', FileMerge = 'dbl_ind_figures.rds',
                                  ToSave = T, ToIntergration = T,ToUpload = T,pHost = "dashboard_live")  {
  # ------------------------------------------------------------------------------------------------
  
  # pFolder = 'S:/STKVN/INDEX_2024/'; pFile = paste0( 'beq_indlogals_history.rds' )
  # pFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/'; pFile = paste0( 'ifrc_ccpr_investment_history.rds' )
  
  # ListCodes = c(); CompoFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/'; CompoFile = "dbl_ind_compo.rds"
  # ToFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/'; ToFile  = paste0('all_figures.rds')
  # FolderMerge = 'S:/CCPR/DATA/DASHBOARD_LIVE/'; FileMerge = 'dbl_ind_figures.rds'
  # ToSave = T; ToIntergration = T; ToUpload = T ; pDate = Sys.Date() ; pHost = "dashboard_live"
  # FIGURES ........................................................................................
  IND_HISTORY = CCPR_READRDS(pFolder,pFile , ToKable=T, ToRestore = T)[order(code,-date)]
  GC_SILENT()
  ind_bcm = CCPR_READRDS("U:/EFRC/DATA/","efrc_indhome_history.rds")
  # IND_HISTORY = unique(rbind(IND_HISTORY, ind_bcm[code %in% c("INDVNINDEX","INDSPX")],fill=T),by=c("code","date"))
  
  IND_HISTORY = unique(rbind(IND_HISTORY, ind_bcm[code %in% c("INDSPX")],fill=T),by=c("code","date"))
  
  VNI = CCPR_READRDS( 'S:/STKVN/INDEX_2024/','DOWNLOAD_SOURCES_INDVN_HISTORY.rds')
  VNI[code == 'INDVNINDEX']
  
  IND_HISTORY = unique(rbind(IND_HISTORY,  VNI[code == 'INDVNINDEX'],fill=T),by=c("code","date"))
  
  codemother = CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","dbl_ind_mother.rds")
  IND_HISTORY = merge(IND_HISTORY[,-c("coverage")], codemother[,.(code,coverage)],by="code",all.x=T)
  IND_HISTORY[coverage=="VIETNAM", codebm:="INDVNINDEX"]
  IND_HISTORY[coverage=="INTERNATIONAL", codebm:="INDSPX"]
  IND_HISTORY[,maxdate:=max(date),by="code"]
  IND_HISTORY = IND_HISTORY[date<=min(IND_HISTORY[code %in% c("INDVNINDEX","INDSPX")]$maxdate)]
  IND_HISTORY[,nbd:=seq(1,.N),by="code"]
  
  IND_VNI     = IND_HISTORY[code %in% c("INDVNINDEX","INDSPX")][order(code,date)]
  
  if (any(ListCodes!="")) { IND_HISTORY = IND_HISTORY[code %in% ListCodes] }
  IND_2023    = IND_HISTORY[nbd<=250][order(code,date)]
  
  # DBL_CCPR_SAVERDS(IND_VNI,('S:/CCPR/DATA/DASHBOARD/'), 'CCPR_INDVNI_HISTORY.rds' )
  IND_VNI     = IND_VNI[order(date)]
  IND_VNI[, lnrt:=log(close/shift(close)), by='code']
  
  IND_2023[, rt:=close/shift(close)-1, by='code']
  IND_2023[, lnrt:=log(close/shift(close)), by='code']
  
  IND_VNI[, rt:=close/shift(close)-1, by='code']
  IND_VNI[, lnrt:=log(close/shift(close)), by='code']
  
  IND_2023    = merge(IND_2023[, -c('benchmark')], IND_VNI[, .(codebm=code, date, benchmark=lnrt)], all.x=T, by=c('codebm','date'))
  My.Kable(IND_2023)
  IND_2023[, dlnrt:=lnrt-benchmark]
  
  
  # IND_HISTORY = DBL_CCPR_READRDS(paste0('R:/CCPR/DATA/DASHBOARD/'), 'ccpi_dashboard_indvn_history.rds', ToKable=T)
  DB_TEMPLATE = setDT(fread(paste0('S:/LIST/', 'template_figures.txt')))
  DB_TEMPLATE_EMPTY = DB_TEMPLATE
  DB_TEMPLATE_EMPTY$value=NULL
  DB_TEMPLATE_EMPTY$value=as.character('')
  str(DB_TEMPLATE_EMPTY)
  # 
  # IND_HISTORY = IND_HISTORY[order(code, date)][grepl('^IND', code)]
  # IND_2023    = IND_HISTORY[, lnrt:=log(close/shift(close)), by='code'][year(date)==2023]
  # IND_2023    = merge(IND_2023[, -c('benchmark')], IND_VNI[, .(date, benchmark=lnrt)], all.x=T, by='date')
  # My.Kable(IND_2023)
  # IND_2023[, dlnrt:=lnrt-benchmark]
  IND_2023_STATS = IND_2023[, .(
    volatility=sqrt(250)*100*sqrt(var(lnrt, na.rm=T)),
    tracking_error=100*sqrt(var(dlnrt, na.rm=T))
  ), by='code']
  IND_2023_STATS = IND_2023_STATS[!is.na(volatility)]
  My.Kable(IND_2023_STATS)
  
  IND_List     = unique(IND_HISTORY[grepl('^IND', code) & !code %in% c("INDVNINDEX","INDSPX")]$code)
  IND_HISTORY_STATS = IND_HISTORY[,.(
    min_close=round(min(close, na.rm=T),2), 
    avg_close=round(mean(close, na.rm=T),2), 
    max_close=round(max(close, na.rm=T),2)
  ) , by=c('code')]
  
  temp_min_close    = IND_HISTORY[, .SD[which.min(close)], by = code]
  IND_HISTORY_STATS = merge(IND_HISTORY_STATS, temp_min_close[, .(code, min_close_date = as.character(date))], by = "code", all.x = T)
  temp_max_close    = IND_HISTORY[, .SD[which.max(close)], by = code]
  IND_HISTORY_STATS = merge(IND_HISTORY_STATS, temp_max_close[, .(code, max_close_date = as.character(date))], by = "code", all.x = T)
  
  
  IND_HISTORY_STATS = merge(IND_HISTORY_STATS[, -c('volatility', 'tracking_error')], IND_2023_STATS, all.x=T, by='code')
  
  IND_HISTORY_STATS = IND_HISTORY_STATS[!code %in% c("INDVNINDEX","INDSPX")]
  IND_COMPO = CCPR_READRDS(CompoFolder, CompoFile , ToKable=T, ToRestore = T)
  # IND_COMPO[code_mother == 'INDVNXSECENYCWPRVND']
  # IND_COMPO[code_mother == 'INDVNXSECENYEWPRVND']
  
  ind_mother = CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_ind_mother.rds', ToRestore = T)
  
  BEQ_CUR_4INDEX = setDT(fread(paste0(UData, 'IFRC_CUR_4INDEX_summary.txt')))
  
  # BEQ_CUR_4INDEX[code == 'CURAUDVND']
  # BEQ_CUR_4INDEX = setDT(readRDS(paste0(UData, 'IFRC_CUR_4INDEX.rds')))
  
  FIGURES_List = list()
  for (icode in 1:length(IND_List))
  {
    # icode = 44
    
    # dt_compo = unique(IND_COMPO[gsub("CW.*|EW.*","",code_mother)==gsub("CW.*|EW.*","",IND_List[icode])],by="code")
    dt_compo = IND_COMPO[code_mother == ind_mother[code == IND_List[icode]]$index_compo]
    
    cur = ind_mother[code == IND_List[icode]]$cur
    if (cur != 'VND' & !is.na(cur)){
      cur_value = BEQ_CUR_4INDEX[code == paste0('CUR',cur,'VND')]$last
    } else {cur_value = 1}
    
    pData = IND_2023[code==IND_List[icode]][!is.na(lnrt) & !is.na(benchmark)]
    # pData = IND_2023[code=='INDGI2BBA05CWPRVND'][!is.na(lnrt) & !is.na(benchmark)]
    if ( nrow (pData) > 1) {
      fit <- lm(lnrt ~ benchmark, data=pData)
      
      beta  = fit$coefficients[2] # This prints out the beta
      alpha = fit$coefficients[1] # This prints out the beta
      
      
      DB_TEMPLATE_ONE = copy(DB_TEMPLATE_EMPTY)
      DB_TEMPLATE_ONE[, figure:=trimws(as.character(figure))]
      DB_TEMPLATE_ONE[, index_code:=IND_List[[icode]]]
      DB_TEMPLATE_ONE[, code:=IND_List[[icode]]]
      
      DB_TEMPLATE_ONE[figure=='Lowest level',          value:=paste(as.character(format(IND_HISTORY_STATS[icode]$min_close,big.mark=",",scientific=FALSE)))]
      DB_TEMPLATE_ONE[figure=='Highest level',         value:=paste(as.character(format(IND_HISTORY_STATS[icode]$max_close,big.mark=",",scientific=FALSE)))]
      
      DB_TEMPLATE_ONE[figure=='Lowest level Date',          value:=as.character(IND_HISTORY_STATS[icode]$min_close_date)]
      DB_TEMPLATE_ONE[figure=='Highest level Date',         value:=as.character(IND_HISTORY_STATS[icode]$max_close_date)]
      
      DB_TEMPLATE_ONE[figure=='Volatility',            value:=paste(as.character(round(IND_HISTORY_STATS[icode]$volatility,2)), '%')]
      DB_TEMPLATE_ONE[figure=='Tracking error',        value:=paste(as.character(round(IND_HISTORY_STATS[icode]$tracking_error,2)), '%')]
      DB_TEMPLATE_ONE[figure=='Alpha',                 value:=as.character(round(alpha,6))]
      DB_TEMPLATE_ONE[figure=='Beta',                  value:=as.character(round(beta,6))]
      if (nrow(dt_compo)>0)
      {
        DB_TEMPLATE_ONE[figure=='Index Capitalisation*', value:=paste(as.character(format(round(sum(dt_compo$capi_stk ,na.rm = T)/cur_value, 2),big.mark=",",scientific=FALSE)))]
        DB_TEMPLATE_ONE[figure=='Capitalisation* Min.',  value:=paste(as.character(format(round(min(dt_compo$capi_stk ,na.rm = T)/cur_value, 2),big.mark=",",scientific=FALSE)))]
        DB_TEMPLATE_ONE[figure=='Capitalisation* Max.',  value:=paste(as.character(format(round(max(dt_compo$capi_stk ,na.rm = T)/cur_value, 2),big.mark=",",scientific=FALSE)))]
        DB_TEMPLATE_ONE[figure=='Capitalisation* Mean',  value:=paste(as.character(format(round(mean(dt_compo$capi_stk,na.rm = T)/cur_value, 2),big.mark=",",scientific=FALSE)))]
      }
      
      My.Kable.All(DB_TEMPLATE_ONE)
      FIGURES_List[[icode]] = DB_TEMPLATE_ONE } else {  FIGURES_List[[icode]] = data.table ()}
  }
  FIGURES_ALL = rbindlist(FIGURES_List)
  My.Kable(FIGURES_ALL)
  
  
  if (ToSave ) { CCPR_SAVERDS (FIGURES_ALL, ToFolder , ToFile) }
  # saveRDS(FIGURES_ALL, paste0('R:/CCPR/DATA/DASHBOARD/', 'FIGURES_ALL.rds'))
  My_Data = FIGURES_ALL
  if (ToIntergration) { 
    if (file.exists (paste0(FolderMerge , FileMerge)) ) {
      Data_Old = CCPR_READRDS( FolderMerge , FileMerge, ToRestore = T)
    } else {Data_Old = data.table () }
    # SPECIFICATIONS_ALL
    My_Data = unique ( rbind (Data_Old, FIGURES_ALL, fill =T), fromLast = T, by = c ('figure', 'index_code') )
    # pDate = '2024-01-12'
    My_Data = UPDATE_UPDATED(My_Data)
    CCPR_SAVERDS(My_Data, FolderMerge , FileMerge )
  }
  
  if (ToUpload) {
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = pHost, l.tablename= gsub(".rds","",FileMerge),
                                     l.filepath= paste0(FolderMerge,  FileMerge ) ,
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
  }
  
  return (My_Data)
}

# ==================================================================================================
TRAINEE_REPORT_CUR_DBL = function (Folder_name = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                   File_name = 'ccpi_dashboard_indbeq_all_history_summary.txt',
                                   Chart_Type = 'CHART_MONTH'){
  # ------------------------------------------------------------------------------------------------
  
  # Folder_name = 'U:/EFRC/DATA/'
  # File_name = 'download_ifrc_INDALS_history_summary.txt'
  
  indmother = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","dbl_ind_mother.rds")
  dt_ind = setDT(fread(paste0(Folder_name, File_name)))
  dt_ind = merge(dt_ind, indmother[,.(code,dbl)],by="code",all.x=T)
  
  dt_ind_vnd   = dt_ind[grepl("VND$",code)]
  dt_ind_usd   = dt_ind[grepl("USD$",code)]
  dt_ind_other = dt_ind[!grepl("VND$",code) & !grepl("USD$",code)]
  
  summary = merge(dt_ind_vnd[,.(vnd_history=max(date)),by="dbl"],
                  dt_ind_usd[,.(usd_history=max(date)),by="dbl"],by="dbl",all.x=T)
  summary = merge(summary, dt_ind_other[,.(other_history=max(date)),by="dbl"],by="dbl",all.x=T)
  
  max_date_his = unique(dt_ind[, max_date_1 := max(date), by = 'code'], by = 'code')[, .(dbl, code, max_date_1)]
  
  summary[vnd_history != usd_history| vnd_history<SYSDATETIME(23) ,history:="***"]
  
  
  My.Kable.All(summary)
  
  dbl_ind_performance       = readRDS("S:/CCPR/DATA/DASHBOARD_LIVE/dbl_ind_performance.rds")
  dbl_ind_performance       =  merge(dbl_ind_performance, indmother[,.(code,dbl)],by="code",all.x=T)
  dbl_ind_performance       =  dbl_ind_performance[!is.na(dbl)]
  
  max_date_per = unique(dbl_ind_performance[, max_date_2 := max(date), by = 'code'], by = 'code')[, .(dbl, code, max_date_2)]
  
  dbl_ind_performance_vnd   = dbl_ind_performance[grepl("VND$",code)][,.(vnd_performance=max(date)),by="dbl"]
  dbl_ind_performance_usd   = dbl_ind_performance[grepl("USD$",code)][,.(usd_performance=max(date)),by="dbl"]
  dbl_ind_performance_other = dbl_ind_performance[!grepl("USD$",code) & !grepl("VND$",code)][,.(other_performance=max(date)),by="dbl"]
  
  report_performance                    = merge(dbl_ind_performance_vnd, dbl_ind_performance_usd ,by="dbl",all.x=T)
  report_performance                    = merge(report_performance, dbl_ind_performance_other ,by="dbl",all.x=T)
  
  report_performance[vnd_performance!=usd_performance | vnd_performance<SYSDATETIME(23),performance:="***"]
  My.Kable.All(report_performance)
  
  switch (Chart_Type,
          'CHART_MONTH' = {
            if (file.exists("S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_MONTH_history_summary.txt")){
              dbl_chart = setDT(fread("S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_MONTH_history_summary.txt"))
            } else {
              WRITE_SUMMARY_FULLPATH(paste0("S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_MONTH_history.rds"))
              dbl_chart = setDT(fread("S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_MONTH_history_summary.txt"))
            }
            
            dbl_chart = merge(dbl_chart, indmother[,.(code,dbl)],by="code",all.x=T)
            
            dbl_chart = dbl_chart [!is.na(dbl)]
            
            max_date_chart = unique(dbl_chart[, max_date_3 := max(date), by = 'code'], by = 'code')[, .(dbl, code, max_date_3)]
            
            dbl_chart_vnd   = dbl_chart[grepl("VND$",code)][,.(vnd_chart=max(date)),by="dbl"]
            dbl_chart_usd = dbl_chart[grepl("USD$",code)][,.(usd_chart=max(date)),by="dbl"]
            dbl_chart_other = dbl_chart[!grepl("USD$",code) & !grepl("VND$",code)][,.(other_chart=max(date)),by="dbl"]
            
            report_chart                    = merge(dbl_chart_vnd, dbl_chart_usd ,by="dbl",all.x=T)
            report_chart                    = merge(report_chart, dbl_chart_other ,by="dbl",all.x=T)
            report_chart[vnd_chart!=usd_chart | vnd_chart<SYSDATETIME(23),chart:="***"]
            My.Kable.All(report_chart)
          },
          'CHART_QUARTER' = {
            WRITE_SUMMARY_FULLPATH(paste0("S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_QUARTER_history.rds"))
            
            dbl_chart = setDT(fread("S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_QUARTER_history_summary.txt"))
            dbl_chart = merge(dbl_chart, indmother[,.(code,dbl)],by="code",all.x=T)
            
            dbl_chart = dbl_chart [!is.na(dbl)]
            max_date_chart = unique(dbl_chart[, max_date_3 := max(date), by = 'code'], by = 'code')[, .(dbl, code, max_date_3)]
            
            dbl_chart_vnd   = dbl_chart[grepl("VND$",code)][,.(vnd_chart=max(date)),by="dbl"]
            dbl_chart_usd = dbl_chart[grepl("USD$",code)][,.(usd_chart=max(date)),by="dbl"]
            dbl_chart_other = dbl_chart[!grepl("USD$",code) & !grepl("VND$",code)][,.(other_chart=max(date)),by="dbl"]
            
            report_chart                    = merge(dbl_chart_vnd, dbl_chart_usd ,by="dbl",all.x=T)
            report_chart                    = merge(report_chart, dbl_chart_other ,by="dbl",all.x=T)
            report_chart[vnd_chart!=usd_chart | vnd_chart<SYSDATETIME(23),chart:="***"]
            My.Kable.All(report_chart)
          },
          'CHART_YEAR' = {
            WRITE_SUMMARY_FULLPATH(paste0("S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_YEAR_history.rds"))
            
            dbl_chart = setDT(fread("S:/CCPR/DATA/DASHBOARD_LIVE/ccpi_dashboard_indbeq_all_YEAR_history_summary.txt"))
            dbl_chart = merge(dbl_chart, indmother[,.(code,dbl)],by="code",all.x=T)
            
            dbl_chart = dbl_chart [!is.na(dbl)]
            max_date_chart = unique(dbl_chart[, max_date_3 := max(date), by = 'code'], by = 'code')[, .(dbl, code, max_date_3)]
            
            dbl_chart_vnd   = dbl_chart[grepl("VND$",code)][,.(vnd_chart=max(date)),by="dbl"]
            dbl_chart_usd = dbl_chart[grepl("USD$",code)][,.(usd_chart=max(date)),by="dbl"]
            dbl_chart_other = dbl_chart[!grepl("USD$",code) & !grepl("VND$",code)][,.(other_chart=max(date)),by="dbl"]
            
            report_chart                    = merge(dbl_chart_vnd, dbl_chart_usd ,by="dbl",all.x=T)
            report_chart                    = merge(report_chart, dbl_chart_other ,by="dbl",all.x=T)
            report_chart[vnd_chart!=usd_chart | vnd_chart<SYSDATETIME(23),chart:="***"]
            My.Kable.All(report_chart)
          }
  )
  
  report = merge(summary, report_performance, by = 'dbl', all.x = T)
  report = merge(report, report_chart, by = 'dbl', all.x = T)
  
  report_by_code = merge(max_date_his, max_date_per[code %in% max_date_his$code], by = c("code","dbl"))
  report_by_code = merge(report_by_code, max_date_chart[code %in% report_by_code$code], by = c("code","dbl"))
  if (any(report_by_code$max_date_1 > report_by_code$max_date_2)){
    report[, top_per := '***']
  } else {
    report[, top_per := as.character(NA)]
  }
  
  if (any(report_by_code$max_date_1 > report_by_code$max_date_3)){
    report[, top_chart := '***']
  } else {
    report[, top_chart := as.character(NA)]
  }
  
  My.Kable.All(report)
  report[!is.na(history), summary := "HIS" ]
  report[is.na(history) & !is.na(performance) & !is.na(chart), summary := "PER/CHART"]
  report[is.na(history) & is.na(performance) & !is.na(chart), summary := "CHART"]
  report[is.na(history) & is.na(performance) & is.na(chart), summary := "PER"]
  My.Kable.All(report)
  return(report)
}


# ==================================================================================================
DBL_REPORT_DATA_UPDATE_LIST_FILE = function(Chart_Type = 'CHART_MONTH', ToSave = T){
  # ------------------------------------------------------------------------------------------------
  
  list_file = setDT(fread('S:/CCPR/DATA/DASHBOARD_LIVE/list_files_beq_ind_history.txt'))
  
  dt_rep = list()
  for (ifile in 1:nrow(list_file)) {
    x = try(TRAINEE_REPORT_CUR_DBL  (Folder_name = list_file[ifile]$folder,
                                     File_name = gsub(".rds","_summary.txt",list_file[ifile]$filename),
                                     Chart_Type = Chart_Type))
    if (all(class(x)!="try-error"))
    {
      dt_rep[[ifile]] = x[,":="(folder=list_file[ifile]$folder,filename=list_file[ifile]$filename,
                                toforce=list_file[ifile]$toforce,pc_to_run=list_file[ifile]$pc_to_run,
                                active=list_file[ifile]$active)]
    }
  }
  dt_rep = rbindlist(dt_rep, fill = T)
  # dt_rep = unique(dt_rep[order(performance)],by=c("filename","folder"))
  dt_rep = rbind(dt_rep, list_file[!paste(folder,filename) %in% paste(dt_rep$folder,dt_rep$filename)],fill=T)
  
  if (ToSave){
    CCPR_SAVERDS(dt_rep, 's:/CCPR/DATA/DASHBOARD_LIVE/','top_file_to_do.rds')
  }
  return(dt_rep)
}


# ==================================================================================================
DBL_STK_UPLOAD_UPDATE_LOOP = function(pData = data.table(),pOption = "LIQUIDITY", ToUpload = F, ToUpdate = F){
  # ------------------------------------------------------------------------------------------------
  pMyPC = toupper(as.character(try(fread("C:/R/my_pc.txt", header = F))))
  DBL_RELOAD_INSREF()
  xList = list()
  if (nrow(pData) > 0){
    list_file = pData
  } else {
    list_file = setDT(fread('S:/CCPR/DATA/DASHBOARD_LIVE/list_files_beq_stk_history.txt'))
    list_file[,pc_to_run:=as.character(pc_to_run)]
    list_file[is.na(pc_to_run),pc_to_run:=""]
  }
  
  switch(pOption,
         'PERFORMANCE' = {
           Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
           File_Compare = "dbl_stk_performance.rds"
         },
         'CHART_MONTH' = {
           Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
           File_Compare = "ccpi_dashboard_stk_all_month_history.rds"
         },
         'CHART_QUARTER' = {
           Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
           File_Compare = "ccpi_dashboard_stk_all_quarter_history.rds"
         },
         'CHART_YEAR' = {
           Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
           File_Compare = "ccpi_dashboard_stk_all_year_history.rds"
         },
         'RISK' = {
           Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
           File_Compare = "dbl_stk_risk.rds"
         },
         'LIQUIDITY' = {
           Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
           File_Compare = "dbl_stk_liquidity.rds"
         }
  )
  DataCompare = CHECK_CLASS(try(DBL_CCPR_READRDS(Folder_Compare, File_Compare, ToRestore = T)))
  if (grepl('CHART', pOption)){
    pOption = 'CHART'
  }
  
  if (pOption == 'LIQUIDITY'){
    list_file = list_file[active == 1]
  }
  
  for (i in 1:nrow(list_file)){
    # i = 34
    Folder = list_file$folder[i]
    File = list_file$filename[i]
    # Folder = 'S:/STKVN/PRICES/FINAL/'
    # File = 'download_yah_stkin_sp500_history.rds'
    # 0 = TRUE, 1 = FALSE
    pc_run = unlist(strsplit(as.character(list_file$pc_to_run[i]), ","))
    
    if (ToUpdate){
      force = T
    } else {
      force = ifelse(list_file$toforce[i] == 1, T, F)
    }
    
    if (length(pc_run) == 0){
      mother_one = try(DBL_STK_ADD_CODE_BY_FILE(pOption = "STK_MOTHER", Folder_Stk = Folder, File_Stk = File,
                                                Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
                                                File_Compare = "dbl_stk_mother.rds",ToForce = F))
      
      
      # figs_one = try(DBL_ADD_CODE_BY_FILE(pOption = "FIGURES", Folder_Index = Folder, File_Index = File, 
      #                                     Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
      #                                     File_Compare = "dbl_ind_figures.rds"))
      
      data = try(DBL_UPDATE_UPLOAD_PERF_CHART_BY_FILE(  pOption = pOption ,Folder_Index = Folder, File_Index = File,
                                                        Perf_Index = "",
                                                        Folder_Compare = Folder_Compare,
                                                        File_Compare = File_Compare,
                                                        Data_Compare = DataCompare,
                                                        ToForce = force))  
      
      if (all(class(data)!='try-error')){
        if (nrow(data) > 0)
        {
          if (all(class(mother_one)=='try-error')) {   mother_one = CHECK_CLASS(try(DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_stk_mother.rds", ToRestore = T))) }
          
          # data = merge(data[,-c("name","short_name","wgtg","version","cur","coverage","category","base_date")],
          #              mother_one[,.(code,name,short_name,wgtg,version,cur,coverage,category,base_date)], by="code",all.x=T)
          xList[[i]] = data
        }
      }
    } else {
      CATln_Border(paste0('ONLY PCs " ',list_file$pc_to_run[i],' " CAN RUN THIS FILE...:',Folder,File))
    }
    
    
    if (length(pc_run) != 0 & toupper(pMyPC) %in% pc_run){
      mother_one = try(DBL_STK_ADD_CODE_BY_FILE(pOption = "STK_MOTHER", Folder_Stk = Folder, File_Stk = File,
                                                Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
                                                File_Compare = "dbl_stk_mother.rds",ToForce = force))
      
      
      # figs_one = try(DBL_ADD_CODE_BY_FILE(pOption = "FIGURES", Folder_Index = Folder, File_Index = File, 
      #                                     Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/",
      #                                     File_Compare = "dbl_ind_figures.rds"))
      
      data = try(DBL_UPDATE_UPLOAD_PERF_CHART_BY_FILE(  pOption = pOption ,Folder_Index = Folder, File_Index = File,
                                                        Perf_Index = "performance_ifrc_stk_history.rds",
                                                        Folder_Compare = Folder_Compare,
                                                        File_Compare = File_Compare,
                                                        Data_Compare = DataCompare,
                                                        ToForce = force))  
      
      if (all(class(data)!='try-error')){
        if (nrow(data) > 0)
        {
          if (all(class(mother_one)=='try-error')) {   mother_one = CHECK_CLASS(try(DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_stk_mother.rds", ToRestore = T))) }
          
          # data = merge(data[,-c("name","short_name","wgtg","version","cur","coverage","category","base_date")],
          #              mother_one[,.(code,name,short_name,wgtg,version,cur,coverage,category,base_date)], by="code",all.x=T)
          xList[[i]] = data
        }
      }
    } 
    
  }
  
  perf_ind = rbindlist(xList, fill = T)
  # setdiff(perf_ind$code, perf_all$code)
  if (nrow(perf_ind) > 0){
    # SAVE BACKUP
    try(DBL_CCPR_SAVERDS(DataCompare, Folder_Compare, gsub('.rds','_old.rds', File_Compare), ToSummary = T, SaveOneDrive = T))
    
    if ('timestamp' %in% names(perf_ind)){
      perf_ind[, timestamp := as.character(timestamp)]
    }
    perf_all = DataCompare
    
    perf_all[, timestamp := as.character(timestamp)]
    perf_all[, timestamp_vn := as.character(timestamp_vn)]
    perf_ind[, timestamp := as.character(timestamp)]
    perf_ind[, timestamp_vn := as.character(timestamp_vn)]
    # codemother = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","dbl_ind_mother.rds")
    
    if (!grepl("CHART",pOption))
    {
      perf_final = unique(rbind(perf_all[!code %in% perf_ind$code], perf_ind, fill=T)[order(code,-date)],by=c("code"))
      
      if(pOption == 'PERFORMANCE'){
        perf_final = perf_final[!(is.na(M1) & is.na(M2) & is.na(M3) & is.na(M4) & is.na(M5) & is.na(M6))]
        
        
        dt_ann = DBL_CALCULATE_PERFORMANCE_BY_NUMBER_PERIOD(pFolder="S:/CCPR/DATA/DASHBOARD_LIVE/",
                                                            pFile=paste0("ccpi_dashboard_stk_all_year_history.rds"),
                                                            pType="STK",pPeriod = "YEAR", nb_per = 6)
        
        dt_ann = setDT(spread(dt_ann[,-c("maxrt")], "nby", "rtx"))
        
        # perf_final = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_stk_performance.rds')
        
        perf_final = merge(perf_final[, -c('Y6_annualised')], dt_ann[, .(code, Y6_annualised)], by = 'code', all.x = T)
        # perf_final[!is.na(Y6_annualised)]
        
      }
      if(pOption == 'RISK'){
        perf_final = perf_final[!(is.na(risk_M1) & is.na(risk_M2) & is.na(risk_M3) & is.na(risk_M4) & is.na(risk_M5) & is.na(risk_M6))]
      }
      if(pOption == 'LIQUIDITY'){
        perf_final = perf_final[!(is.na(liquid_M1) & is.na(liquid_M2) & is.na(liquid_M3) & is.na(liquid_M4) & is.na(liquid_M5) & is.na(liquid_M6))]
      }
      perf_final = perf_final[month(perf_final$date) >= month(Sys.Date()) - 1 & year(date) == year(Sys.Date())]
      
    }else{
      perf_final = unique(rbind(perf_all[!code %in% perf_ind$code], perf_ind, fill=T)[order(code,-date)],by=c("code","date"))
    }
    perf_final = merge(perf_final[,-c("ticker","name","short_name","cur","coverage","country","continent","category","sector","industry",'market')],
                       mother_one[,.(ticker,code,name,short_name,cur,coverage,country,continent,category,sector,industry,market)], by="code",all.x=T)
    
    excess_code = c("INDVNINDEX","INDSPX","INDNDX","CMDGOLD")
    
    if (pOption != 'LIQUIDITY'){
      excess_dt = DBL_CCPR_READRDS(Folder_Compare, gsub("stk", ifelse(pOption == 'CHART', "indbeq", "ind"), File_Compare))
      perf_cols = intersect(names(perf_final), names(excess_dt))
      excess_dt = excess_dt[code %in% excess_code,..perf_cols]
      excess_dt = merge(excess_dt[,-c("name")],ins_ref[,.(code,name)],by="code",all.x=T)
      perf_final = rbind(perf_final[!code %in% excess_dt$code],excess_dt[,excess:=1],fill=T)
      
    }
    
    # My.Kable.Min(perf_final[order(-date)])
    perf_final = perf_final[!is.na(close) & !is.na(rt)]
    perf_final = UPDATE_UPDATED(perf_final[!is.na(code) | code %in% excess_code,-c("updated")])
    # SAVE NEW UPDATE
    try(DBL_CCPR_SAVERDS(perf_final, Folder_Compare,  File_Compare, ToSummary = T, SaveOneDrive = T))
    
    if (ToUpload)
    {
      if (grepl('CHART', pOption)){
        PERIOD = ifelse(grepl('month', tolower(File_Compare)), 'MONTH', ifelse(grepl('quarter', tolower(File_Compare)), 'QUARTER', 'YEAR'))
        
        try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",paste0("dbl_stk_",tolower(PERIOD),"_chart")),
                                         l.filepath= paste0(Folder_Compare,File_Compare),
                                         CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
      } else {
        
        try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",File_Compare),
                                         l.filepath= paste0(Folder_Compare,File_Compare),
                                         CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
      }
    }
  } else {
    if (ToUpload)
    {
      if (grepl('CHART', pOption)){
        PERIOD = ifelse(grepl('month', tolower(File_Compare)), 'MONTH', ifelse(grepl('quarter', tolower(File_Compare)), 'QUARTER', 'YEAR'))
        
        try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",paste0("dbl_stk_",tolower(PERIOD),"_chart")),
                                         l.filepath= paste0(Folder_Compare,File_Compare),
                                         CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
      } else {
        perf_final = DataCompare
        perf_final = UPDATE_UPDATED(perf_final[,-c("updated")])
        try(DBL_CCPR_SAVERDS(perf_final, Folder_Compare,  File_Compare, ToSummary = T, SaveOneDrive = T))
        
        try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= gsub(".rds","",File_Compare),
                                         l.filepath= paste0(Folder_Compare,File_Compare),
                                         CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
      }
    }
    CATln_Border("ALL DATA HAS ALREADY UPDATED!!!!!")
  }
  
  try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption=paste0('DASHBOARD_LIVE > UPDATE_STK_', pOption), pAction = "SAVE", NbSeconds = 900, ToPrint = F))
}

# ==================================================================================================
DBL_INDEXES_STATS = function(Action='IFRC/BEQ'){
  # ------------------------------------------------------------------------------------------------
  
  STATS_NUMBER = data.table()
  switch(Action,
         'IFRC/BEQ' = {
           
           
           DBL_RELOAD_INSREF(ToForce=T)
           x = ins_ref[type=='IND' & grepl('IFRC', name)]
           
           x[grepl(' TOP ', name) , ':='(category='TRADABLE', coverage='VIETNAM', subcategory='BLUE CHIPS')]
           
           x[grepl('VOLATILITY', name), ':='(category='VOLATILITY', coverage='INTERNATIONAL')]
           x[grepl('FEAR GREED|FEAR & GREED', name), ':='(category='FEAR & GREED', coverage='INTERNATIONAL')]
           x[grepl('ETF INDEX', name), ':='(category='TRADABLE', coverage='INTERNATIONAL', subcategory='ETF')]
           x[grepl('INDEX OF INDEXES', name), ':='(category='TRADABLE', coverage='INTERNATIONAL', subcategory='INDEXES')]
           x[grepl('BLUE CHIP', name), ':='(category='TRADABLE', coverage='INTERNATIONAL')]
           x[grepl('GLOBAL', name), ':='(category='TRADABLE', coverage='INTERNATIONAL')]
           x[grepl('CURRENCY', name), ':='(category='TRADABLE', coverage='INTERNATIONAL', subcategory='CURRENCY')]
           x[grepl('VNX PVNPLUS 10', name), ':='(category='TRADABLE', coverage='VIETNAM', subcategory='GROUP')]
           x[grepl('IFRC SENTIMENT HSX', name), ':='(category='SENTIMENT', coverage='VIETNAM')]
           x[grepl('IFRC SENTIMENT VNX', name), ':='(category='SENTIMENT', coverage='VIETNAM')]
           x[grepl('RESEARCH VIETNAM PORTFOLIO', name), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='RESEARCH')]
           x[grepl('VNX TOP ', name), ':='(category='TRADABLE', coverage='VIETNAM', subcategory='BLUE CHIPS')]
           x[grepl('VNX 300', name), ':='(category='TRADABLE', coverage='VIETNAM', subcategory='BLUE CHIPS')]
           x[grepl('VNX ALLSHARE ', name) & nchar(name)>nchar('IFRC VNX ALLSHARE '), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX AIRLINE', name) , ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX BANKS', name) , ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX BASIC MATERIALS', name) , ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX BROKERAGE', name) , ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX BUILDING MATERIALS EX CEMENT', name) , ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX BUILDING MATERIALS', name) , ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX CONSUMER SERVICES', name) , ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX CEMENT', name) , ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX CHEAP 10', name) , ':='(category='TRADABLE', coverage='VIETNAM', subcategory='SIZE')]
           x[grepl('IFRC VNX CHEAP 20', name) , ':='(category='TRADABLE', coverage='VIETNAM', subcategory='SIZE')]
           x[grepl('IFRC VNX CHEAP 30', name) , ':='(category='TRADABLE', coverage='VIETNAM', subcategory='SIZE')]
           
           x[grepl('IFRC VNX LARGE 50', name) , ':='(category='TRADABLE', coverage='VIETNAM', subcategory='SIZE')]
           x[grepl('IFRC VNX LARGE & MID 150', name) , ':='(category='TRADABLE', coverage='VIETNAM', subcategory='SIZE')]
           x[grepl('IFRC VNX MID 100', name) , ':='(category='TRADABLE', coverage='VIETNAM', subcategory='SIZE')]
           x[grepl('IFRC VNX MID & SMALL 250', name) , ':='(category='TRADABLE', coverage='VIETNAM', subcategory='SIZE')]
           x[grepl('IFRC VNX SMALL 150', name) , ':='(category='TRADABLE', coverage='VIETNAM', subcategory='SIZE')]
           
           x[grepl('IFRC VNX LEADERS 10', name) , ':='(category='TRADABLE', coverage='VIETNAM', subcategory='SECTOR')]
           
           # x[grepl(' TOP ', name) , ':='(category='TRADABLE', coverage='VIETNAM', subcategory='BLUE CHIPS')]
           
           x[grepl('IFRC VNX ENERGY', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX FINANCE', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX GENERAL', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='MARKET')]
           
           x[grepl('IFRC VNX GROUP ELECTRICITY', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='GROUP')]
           x[grepl('IFRC VNX GROUP OIL', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='GROUP')]
           x[grepl('IFRC VNX GROUP TELECOM', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='GROUP')]
           x[grepl('IFRC VNX HIGH QUALITY GOODS', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='GROUP')]
           
           x[grepl('IFRC VNX CONSUMER GOODS', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX GROUP COAL & MINERAL', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX HEALTHCARE', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX INDUSTRIALS', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX OIL & GAS', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX PHARMACEUTICALS', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX REAL ESTATE', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX STEEL', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX TECHNOLOGY', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC VNX UTILITIES', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           x[grepl('IFRC/IREEDS VIETNAM AI', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           
           x[grepl('IFRC/BEQ HOLDINGS VNX SECTOR', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='SECTOR')]
           
           x[grepl('IFRC VNX HNX', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='MARKET')]
           x[grepl('IFRC VNX HSX', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='MARKET')]
           x[grepl('IFRC VNX TOTAL MARKET', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='MARKET')]
           
           x[grepl('IFRC VNX PROVINCIAL', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='PROVINCIAL')]
           x[grepl('IFRC VNX REGIONAL', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='PROVINCIAL')]
           
           x[grepl('IFRC VNX WOMEN CEO', name) & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='WOMEN')]
           x[name == 'IFRC VNX ALLSHARE' & is.na(category), ':='(category='BENCHMARK', coverage='VIETNAM', subcategory='MARKET')]
           
           My.Kable.All(x[, .(n=.N), by=c('coverage', 'category')][order(coverage, category)])
           
           My.Kable.All(x[, .(n=.N), by=c('coverage')][order(coverage)])
           My.Kable(x[is.na(coverage) | is.na(category)][, .(code, name, coverage, category, subcategory, code_underlying)], Nb=6)
           
           STATS_NUMBER = x[, .(stat = 'IFRC/BEQ INDEXES', n=.N), by=c('coverage')][order(coverage)]
           STATS_NUMBER = rbind(STATS_NUMBER, data.table(coverage='TOTAL', stat='IFRC/BEQ INDEXES', n=sum(STATS_NUMBER$n)), fill=T)
           My.Kable(STATS_NUMBER)
           My.Kable(x[, .(code, name, coverage, category, subcategory, code_underlying)], Nb=6)
         }
  )
  return(STATS_NUMBER)
}


# # ==================================================================================================
# DBL_CREATE_STK_MOTHER = function (pData = data.table(),FromFolder = "S:/STKVN/PRICES/FINAL/", FromFile = "download_yah_stkin_sp500_history.rds",
#                                   SaveFolder = "S:/CCPR/DATA/DASHBOARD_LIVE/", SaveFile = "dbl_stk_mother.rds")  {
#   # ------------------------------------------------------------------------------------------------
#   DBL_RELOAD_INSREF()
#   if (nrow(pData) > 0){
#     DATA_ALL = pData
#   } else {
#     DATA_ALL = DBL_CCPR_READRDS(FromFolder,FromFile)[order(code,date)]
#   }
#   
#   if (nrow(DATA_ALL) > 0){
#     GC_SILENT()
#     if (grepl("stkin|stkhome",tolower(FromFile)))
#     {
#       dt_sec = DBL_CCPR_READRDS('S:/STKVN/', 'download_yah_stk_sector_history.rds')[order(iso2,-capiusd)]
#       dt_sec[, sum_capi:=sum(capiusd, na.rm = T), by=c('date',"iso2")]
#       dt_sec[!is.na(capiusd), cum_capi:=cumsum(capiusd), by=c('date',"iso2")]
#       dt_sec[, pc_capi:=100*cum_capi/sum_capi, by=c('date',"iso2")]
#       dt_sec[!is.na(sum_capi), size := ifelse(pc_capi<=85, 'LARGE', ifelse(pc_capi>85 & pc_capi<=95, 'MID', 'SMALL'))]
#       dt_sec[is.na(sum_capi), size := NA]  # Set SIZE to NA when sum_capi is NA
#       DATA_ALL = merge(DATA_ALL[,-c("size","sector","country","industry","cur",'name')], dt_sec[,.(code,size,country,sector,industry,cur,name)], by = 'code', all.x = T)
#       
#       if ("capiusd" %in% names(DATA_ALL)){
#         DATA_ALL[, sum_capi:=sum(capiusd, na.rm = T), by=c('date',"iso2")]
#         DATA_ALL[!is.na(capiusd), cum_capi:=cumsum(capiusd), by=c('date',"iso2")]
#         DATA_ALL[, pc_capi:=100*cum_capi/sum_capi, by=c('date',"iso2")]
#         DATA_ALL[!is.na(sum_capi), size := ifelse(pc_capi<=85, 'LARGE', ifelse(pc_capi>85 & pc_capi<=95, 'MID', 'SMALL'))]
#         DATA_ALL[is.na(sum_capi), size := NA]  # Set SIZE to NA when sum_capi is NA
#       }
#     }
#     if (!"name" %in% names(DATA_ALL)) { DATA_ALL[,name:=as.character(NA)] }
#     if (!"market" %in% names(DATA_ALL)) { DATA_ALL[,market:=as.character(NA)] }
#     if (!"country" %in% names(DATA_ALL)) { DATA_ALL[,country:=as.character(NA)] }
#     if (!"continent" %in% names(DATA_ALL)) { DATA_ALL[,continent:=as.character(NA)] }
#     if (!"codesource" %in% names(DATA_ALL)) { DATA_ALL[,codesource:=as.character(NA)] }
#     if (!"size" %in% names(DATA_ALL)) { DATA_ALL[,size:=as.character(NA)] }
#     if (!"cur" %in% names(DATA_ALL)) { DATA_ALL[,cur:=as.character(NA)] }
#     
#     DATA_FINAL = unique (DATA_ALL[order(code,-date)][,.(ticker=substr(code,6,nchar(code)),codesource=as.character(codesource),name,market,
#                                                         category=size,cur,country,continent,sector=toupper(sector),industry=toupper(industry),
#                                                         date=max(date)),by="code"], by = 'code' )
#     GC_SILENT()
#     DATA_FINAL = DATA_FINAL[!code %in% DATA_FINAL[substr(code,1,5)=="STKVN" & date!=max(date)]$code]
#     DATA_FINAL = merge(DATA_FINAL, ins_ref[type=="STK",.(code,new_codesource=codesource,new_name=name,
#                                                          new_country=country,new_continent=continent)],by="code",all.x=T)
#     DATA_FINAL[is.na(name) & !is.na(new_name),name:=new_name]
#     DATA_FINAL$new_name = NULL
#     DATA_FINAL[is.na(name)]
#     DATA_FINAL[,short_name:=name]
#     
#     DATA_FINAL[is.na(codesource) & !is.na(new_codesource),codesource:=new_codesource]
#     DATA_FINAL$new_codesource = NULL
#     
#     DATA_FINAL[is.na(country) & !is.na(new_country),country:=new_country]
#     DATA_FINAL$new_country = NULL
#     
#     DATA_FINAL[is.na(continent) & !is.na(new_continent),continent:=new_continent]
#     DATA_FINAL$new_continent = NULL
#     
#     DATA_FINAL[(is.na(market) | nchar(market)==0) & grepl("\\.",codesource),market:=gsub(".*\\.","",codesource)]
#     DATA_FINAL[is.na(ticker) | nchar(ticker)==0,ticker:=gsub("\\..*","",codesource)]
#     
#     DATA_FINAL[substr(code,1,5)=="STKVN", ':=' (coverage = "VIETNAM",cur="VND")]
#     DATA_FINAL[substr(code,1,5)!="STKVN", ':=' (coverage = "INTERNATIONAL")]
#     
#     DATA_FINAL[coverage=="VIETNAM",benchmark:="VNI"]
#     DATA_FINAL[coverage=="INTERNATIONAL",benchmark:="SP500"]
#     
#     DATA_FINAL[code=="STKVNDSE",":="(name="DNSE SECURITIES JSC",country="VIETNAM",continent="ASIA",
#                                      short_name = "DNSE SECURITIES JSC",industry="FINANCIAL SERVICES",
#                                      sector="FINANCIALS")]
#     DATA_FINAL[code=="STKVNSBG",":="(name=toupper("Siba High-Tech Mechanical Group JSC"),country="VIETNAM",continent="ASIA",
#                                      short_name = toupper("Siba High-Tech Mechanical Group JSC"), sector = toupper('INDUSTRIALS'), industry = toupper('CAPITAL GOODS'))]
#     DATA_FINAL[code=="STKVNQNP",":="(name=toupper("Quy Nhon Port JSC"),country="VIETNAM",continent="ASIA",
#                                      short_name = toupper("Quy Nhon Port JSC"), sector = toupper('INDUSTRIALS'), industry = toupper('TRANSPORTATION'))]
#     # DATA_FINAL[grep("WOMEN",name)]
#     # DATA_FINAL = DATA_FINAL[,.(code,name,short_name,wgtg,version,cur,index_group,index_doc,index_compo,
#     #                            base_date,base_value,history)]
#     
#     check_files = c("dbl_stk_performance.rds", "ccpi_dashboard_stk_all_month_history.rds")
#     for (ifile in 1:length(check_files)) {
#       dt_file = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", check_files[ifile])
#       if ("updated" %in% names(dt_file))
#       {
#         DATA_FINAL = merge(DATA_FINAL, unique(dt_file[,.(code, colx=as.Date(updated))],by="code"),
#                            by = "code", all.x=T)
#         setnames(DATA_FINAL,"colx",gsub("dbl_stk_|.rds|ccpi_dashboard_stk_all_","",check_files[ifile]))
#       }
#     }
#     
#     DATA_FINAL = UPDATE_UPDATED(DATA_FINAL[,-c("updated")])
#     if (nchar(SaveFolder)>0 & nchar(SaveFile)>0)
#     {
#       DBL_CCPR_SAVERDS(DATA_FINAL, SaveFolder ,SaveFile)
#     }
#     # try (write.xlsx(DATA_FINAL, paste0(SaveFolder, gsub(".rds",".xlsx",SaveFile)) ) )
#     # try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = 'dashboard_live', l.tablename= gsub(".rds","",SaveFile),
#     #                                  l.filepath= tolower(paste0(SaveFolder, SaveFile ) ),
#     #                                  CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
#     
#     My.Kable.TB(DATA_FINAL)
#   } else {DATA_FINAL = data.table()}
#   
#   return(DATA_FINAL)
# }

# ==================================================================================================
DBL_CREATE_STK_MOTHER = function (pData = data.table(),FromFolder = "S:/STKVN/PRICES/FINAL/", FromFile = "download_yah_stkin_sp500_history.rds",
                                  SaveFolder = "S:/CCPR/DATA/DASHBOARD_LIVE/", SaveFile = "dbl_stk_mother.rds")  {
  # ------------------------------------------------------------------------------------------------
  DBL_RELOAD_INSREF()
  if (nrow(pData) > 0){
    DATA_ALL = pData
  } else {
    DATA_ALL = DBL_CCPR_READRDS(FromFolder,FromFile)[order(code,date)]
  }
  
  DATA_NA_SIZE = DATA_ALL[is.na(size)]
  if (nrow(DATA_ALL) > 0){
    GC_SILENT()
    if (grepl("stkin|stkhome",tolower(FromFile)))
    {
      dt_sec = DBL_CCPR_READRDS('S:/STKVN/', 'download_yah_stk_sector_history.rds')[order(iso2,-capiusd)]
      
      DATA_ALL = merge(DATA_ALL[,-c("sector","country","industry","cur",'name')], dt_sec[,.(code,country,sector,industry,cur,name)], by = 'code', all.x = T)
      
      if ((("capiusd" %in% names(DATA_ALL)) | ("capi" %in% names(DATA_ALL))) & (nrow(DATA_NA_SIZE) > 0)){
        dt_sec = DBL_CCPR_READRDS('S:/STKVN/', 'download_yah_stk_sector_history.rds')[order(iso2,-capiusd)]
        dt_sec[, sum_capi:=sum(capiusd, na.rm = T), by=c('date',"iso2")]
        dt_sec[!is.na(capiusd), cum_capi:=cumsum(capiusd), by=c('date',"iso2")]
        dt_sec[, pc_capi:=100*cum_capi/sum_capi, by=c('date',"iso2")]
        dt_sec[!is.na(sum_capi), size := ifelse(pc_capi<=85, 'LARGE', ifelse(pc_capi>85 & pc_capi<=95, 'MID', 'SMALL'))]
        dt_sec[is.na(sum_capi), size := NA]  # Set SIZE to NA when sum_capi is NA
        DATA_NA_SIZE = merge(DATA_NA_SIZE[,-c("size","sector","country","industry","cur",'name')], dt_sec[,.(code,size,country,sector,industry,cur,name)], by = 'code', all.x = T)
        DATA_ALL = rbind(DATA_ALL[!codesource %in% DATA_NA_SIZE$codesource], DATA_NA_SIZE, fill = T)
      }
    }
    if (!"name" %in% names(DATA_ALL)) { DATA_ALL[,name:=as.character(NA)] }
    if (!"market" %in% names(DATA_ALL)) { DATA_ALL[,market:=as.character(NA)] }
    if (!"country" %in% names(DATA_ALL)) { DATA_ALL[,country:=as.character(NA)] }
    if (!"continent" %in% names(DATA_ALL)) { DATA_ALL[,continent:=as.character(NA)] }
    if (!"codesource" %in% names(DATA_ALL)) { DATA_ALL[,codesource:=as.character(NA)] }
    if (!"size" %in% names(DATA_ALL)) { DATA_ALL[,size:=as.character(NA)] }
    if (!"cur" %in% names(DATA_ALL)) { DATA_ALL[,cur:=as.character(NA)] }
    
    DATA_FINAL = unique (DATA_ALL[order(code,-date)][,.(ticker=substr(code,6,nchar(code)),codesource=as.character(codesource),name,market,
                                                        category=size,cur,country,continent,sector=toupper(sector),industry=toupper(industry),
                                                        date=max(date)),by="code"], by = 'code' )
    GC_SILENT()
    DATA_FINAL = DATA_FINAL[!code %in% DATA_FINAL[substr(code,1,5)=="STKVN" & date!=max(date)]$code]
    DATA_FINAL = merge(DATA_FINAL, ins_ref[type=="STK",.(code,new_codesource=codesource,new_name=name,
                                                         new_country=country,new_continent=continent)],by="code",all.x=T)
    DATA_FINAL[is.na(name) & !is.na(new_name),name:=new_name]
    DATA_FINAL$new_name = NULL
    DATA_FINAL[is.na(name)]
    DATA_FINAL[,short_name:=name]
    
    DATA_FINAL[is.na(codesource) & !is.na(new_codesource),codesource:=new_codesource]
    DATA_FINAL$new_codesource = NULL
    
    DATA_FINAL[is.na(country) & !is.na(new_country),country:=new_country]
    DATA_FINAL$new_country = NULL
    
    DATA_FINAL[is.na(continent) & !is.na(new_continent),continent:=new_continent]
    DATA_FINAL$new_continent = NULL
    
    DATA_FINAL[(is.na(market) | nchar(market)==0) & grepl("\\.",codesource),market:=gsub(".*\\.","",codesource)]
    DATA_FINAL[is.na(ticker) | nchar(ticker)==0,ticker:=gsub("\\..*","",codesource)]
    
    DATA_FINAL[substr(code,1,5)=="STKVN", ':=' (coverage = "VIETNAM",cur="VND")]
    DATA_FINAL[substr(code,1,5)!="STKVN", ':=' (coverage = "INTERNATIONAL")]
    
    DATA_FINAL[coverage=="VIETNAM",benchmark:="VNI"]
    DATA_FINAL[coverage=="INTERNATIONAL",benchmark:="SP500"]
    
    DATA_FINAL[code=="STKVNDSE",":="(name="DNSE SECURITIES JSC",country="VIETNAM",continent="ASIA",
                                     short_name = "DNSE SECURITIES JSC",industry="FINANCIAL SERVICES",
                                     sector="FINANCIALS")]
    DATA_FINAL[code=="STKVNSBG",":="(name=toupper("Siba High-Tech Mechanical Group JSC"),country="VIETNAM",continent="ASIA",
                                     short_name = toupper("Siba High-Tech Mechanical Group JSC"), sector = toupper('INDUSTRIALS'), industry = toupper('CAPITAL GOODS'))]
    DATA_FINAL[code=="STKVNQNP",":="(name=toupper("Quy Nhon Port JSC"),country="VIETNAM",continent="ASIA",
                                     short_name = toupper("Quy Nhon Port JSC"), sector = toupper('INDUSTRIALS'), industry = toupper('TRANSPORTATION'))]
    # DATA_FINAL[grep("WOMEN",name)]
    # DATA_FINAL = DATA_FINAL[,.(code,name,short_name,wgtg,version,cur,index_group,index_doc,index_compo,
    #                            base_date,base_value,history)]
    
    check_files = c("dbl_stk_performance.rds", "ccpi_dashboard_stk_all_month_history.rds")
    for (ifile in 1:length(check_files)) {
      dt_file = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", check_files[ifile])
      if ("updated" %in% names(dt_file))
      {
        DATA_FINAL = merge(DATA_FINAL, unique(dt_file[,.(code, colx=as.Date(updated))],by="code"),
                           by = "code", all.x=T)
        setnames(DATA_FINAL,"colx",gsub("dbl_stk_|.rds|ccpi_dashboard_stk_all_","",check_files[ifile]))
      }
    }
    
    DATA_FINAL = UPDATE_UPDATED(DATA_FINAL[,-c("updated")])
    if (nchar(SaveFolder)>0 & nchar(SaveFile)>0)
    {
      DBL_CCPR_SAVERDS(DATA_FINAL, SaveFolder ,SaveFile)
    }
    # try (write.xlsx(DATA_FINAL, paste0(SaveFolder, gsub(".rds",".xlsx",SaveFile)) ) )
    # try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = 'dashboard_live', l.tablename= gsub(".rds","",SaveFile),
    #                                  l.filepath= tolower(paste0(SaveFolder, SaveFile ) ),
    #                                  CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
    
    My.Kable.TB(DATA_FINAL)
  } else {DATA_FINAL = data.table()}
  
  return(DATA_FINAL)
}

# ==================================================================================================
DBL_STK_ADD_CODE_BY_FILE = function(pOption, Folder_Stk, File_Stk, Folder_Compare, File_Compare, ToForce){
  # ------------------------------------------------------------------------------------------------
  
  # pOption = "STK_MOTHER"
  # Folder_Stk = "S:/STKVN/PRICES/FINAL/"
  # File_Stk = "download_yah_stkin_sp500_history.rds"
  # Folder_Compare = "S:/CCPR/DATA/DASHBOARD_LIVE/"
  # File_Compare = "dbl_stk_mother.rds"
  summary_index = try(setDT(fread(paste0(Folder_Stk,gsub(".rds","_summary.txt",File_Stk)))))
  
  if (all(class(summary_index)!="try-error"))
  {
    WRITE_SUMMARY_FULLPATH(paste0(Folder_Compare,File_Compare))
    # WRITE_SUMMARY_FULLPATH(paste0(Folder_Stk ,File_Stk ))
    
    summary_compare = setDT(fread(paste0(Folder_Compare,gsub(".rds","_summary.txt",File_Compare))))
    
    new_codes = summary_index[!code %in% summary_compare$code]
    
    if (ToForce || nrow(new_codes) > 0){
      switch (pOption,
              "STK_MOTHER" = {
                pData = DBL_CCPR_READRDS(Folder_Stk, File_Stk, ToRestore = T)
                if (!ToForce){
                  pData = pData[code %in% new_codes$code]
                }                
                dt_one = try(DBL_CREATE_STK_MOTHER(pData, FromFolder = Folder_Stk, FromFile = File_Stk,
                                                   SaveFolder = "", SaveFile = ""))
                
                if (nrow(dt_one) > 0){
                  dt_all = DBL_CCPR_READRDS(Folder_Compare,File_Compare)
                  # dt_all[,":="(base_date=as.Date(base_date), history=as.Date(history), enddate=as.Date(enddate),
                  #              date=as.Date(date), specifications=as.Date(specifications), figures=as.Date(figures),
                  #              performance=as.Date(performance), month_chart=as.Date(month_chart),
                  #              compo=as.Date(compo))]
                  dt_all = unique(rbind(dt_all[!code %in% dt_one$code], dt_one, fill = T),by="code")
                  
                  DBL_CCPR_SAVERDS(dt_all, Folder_Compare, File_Compare)
                  try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = 'dashboard_live', l.tablename= gsub(".rds","",File_Compare),
                                                   l.filepath= tolower(paste0(Folder_Compare,File_Compare ) ),
                                                   CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
                  
                  # CHART_CODE_LABEL
                  label_all = CCPR_READRDS(Folder_Compare,"dbl_chart_code_label.rds")
                  label_all = unique(rbind(label_all[!code %in% dt_all$code], dt_all[,.(code,label=short_name,updated)]),by="code")
                  
                  label_add = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","ifrc_ccpr_investment_history.rds")
                  label_all = unique(rbind(label_all, unique(label_add[!code %in% label_all$code,.(code,label=name)],by="code"),fill=T),by="code")
                  
                  CCPR_SAVERDS(label_all, Folder_Compare, "dbl_chart_code_label.rds")
                  try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = 'dashboard_live', l.tablename= "dbl_chart_code_label",
                                                   l.filepath= tolower(paste0(Folder_Compare,"dbl_chart_code_label.rds" ) ),
                                                   CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
                }
                
              },
              "FIGURES" = {
                
              }
      )
      
    }
    else {
      dt_all = DBL_CCPR_READRDS(Folder_Compare,File_Compare)
    }
  }else{
    dt_all = DBL_CCPR_READRDS(Folder_Compare,File_Compare)
  }
  return(dt_all)
}


# ==================================================================================================
SPLIT_DATA_TABLE = function(dt, rows_per_split = 20) {
  # ------------------------------------------------------------------------------------------------
  split_list = split(dt, (seq(nrow(dt)) - 1) %/% rows_per_split)
  return(split_list)
}


# ==================================================================================================
DBL_LOOP_FOR_INDEXES = function(Updated = F){
  # ------------------------------------------------------------------------------------------------
  list_options = list("PERFORMANCE", "CHART_MONTH", "CHART_QUARTER", "CHART_YEAR", "RISK")
  pMyPC = toupper(as.character(try(fread("C:/R/my_pc.txt", header = F))))
  
  TODO_TOP_FILE =  try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption = 'DO_TOP_FILE', pAction = "COMPARE", NbSeconds = 3600, ToPrint = F))
  
  if (TODO_TOP_FILE){
    TODO_TOP_FILE =  try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption = 'DO_TOP_FILE', pAction = "SAVE", NbSeconds = 3600, ToPrint = F))
    
    dt_rep = DBL_REPORT_DATA_UPDATE_LIST_FILE(Chart_Type = 'CHART_MONTH', ToSave = T)
  } else {
    dt_rep = CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/','top_file_to_do.rds')
  }
  
  dt_perf = unique(dt_rep[order(top_per,performance,tolower(filename),folder)],by=c("folder","filename"))
  dt_chart = unique(dt_rep[order(top_chart, chart,tolower(filename),folder)],by=c("folder","filename"))
  
  for (option in sample(list_options)){
    # option = 'PERFORMANCE'
    CATln_Border(paste0(pMyPC,' ------------- ',option))
    
    TO_DO_TOP = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption = paste0('UPDATE_TOP_',option), pAction = "COMPARE", NbSeconds = 900, ToPrint = F))
    
    # TO_DO = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption = paste0('UPDATE_',option), pAction = "COMPARE", NbSeconds = 900, ToPrint = F)
    if (TO_DO_TOP){
      TO_DO_TOP = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption = paste0('UPDATE_TOP_',option), pAction = "SAVE", NbSeconds = 1200, ToPrint = F))
      if (grepl('CHART', option)){
        if (nrow(dt_chart[!is.na(top_chart)]) > 0){
          list_chart = SPLIT_DATA_TABLE(dt_chart[!is.na(chart) | !is.na(top_chart)][order(top_chart, chart)])
          for (i in 1:length(list_chart)){
            # i = 1
            data_to_do = setDT(list_chart[[i]])
            # try(DBL_UPLOAD_UPDATE_LOOP(pData = list_perf[!is.na(top_per)],pOption = option, ToUpload = T))
            try(DBL_UPLOAD_UPDATE_LOOP(pList = data_to_do, pOption = option, ToUpload = T, ToUpdate = Updated))
          }
          # try(DBL_UPLOAD_UPDATE_LOOP(pData = list_chart[!is.na(top_chart)],pOption = option, ToUpload = T))
        }
      } else {
        if (nrow(dt_perf[!is.na(top_per)]) > 0){
          list_perf = SPLIT_DATA_TABLE(dt_perf[!is.na(performance) | !is.na(top_per)][order(top_per, performance)])
          for (i in 1:length(list_perf)){
            # i = 1
            data_to_do = setDT(list_perf[[i]])
            # try(DBL_UPLOAD_UPDATE_LOOP(pData = list_perf[!is.na(top_per)],pOption = option, ToUpload = T))
            try(DBL_UPLOAD_UPDATE_LOOP(pList = data_to_do, pOption = option, ToUpload = T, ToUpdate = Updated))
          }
        }
      }
    }
  }
  
  for (option in sample(list_options)){
    TO_DO_NONTOP = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption = paste0('UPDATE_NONTOP_',option), pAction = "COMPARE", NbSeconds = 900, ToPrint = F))
    
    if (TO_DO_NONTOP){
      TO_DO_NONTOP = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption = paste0('UPDATE_NONTOP_',option), pAction = "SAVE", NbSeconds = 1200, ToPrint = F))
      if (grepl('CHART', option)){
        if (nrow(dt_chart[is.na(top_chart)]) > 0){
          # try(DBL_UPLOAD_UPDATE_LOOP(pData = list_chart[!is.na(top_chart)],pOption = option, ToUpload = T))
          list_chart = SPLIT_DATA_TABLE(dt_chart[is.na(chart) | is.na(top_chart)][order(top_chart, chart)])
          for (i in 1:length(list_chart)){
            # i = 1
            data_to_do = setDT(list_chart[[i]])
            # try(DBL_UPLOAD_UPDATE_LOOP(pData = list_perf[!is.na(top_per)],pOption = option, ToUpload = T))
            try(DBL_UPLOAD_UPDATE_LOOP(pList = data_to_do, pOption = option, ToUpload = T, ToUpdate = Updated))
          }
        }
      } else {
        if (nrow(dt_perf[is.na(top_per)]) > 0){
          # try(DBL_UPLOAD_UPDATE_LOOP(pData = list_perf[!is.na(top_per)],pOption = option, ToUpload = T))
          list_perf = SPLIT_DATA_TABLE(dt_perf[is.na(performance) & is.na(top_per)])
          for (i in 1:length(list_perf)){
            # i = 1
            data_to_do = setDT(list_perf[[i]])
            # try(DBL_UPLOAD_UPDATE_LOOP(pData = list_perf[!is.na(top_per)],pOption = option, ToUpload = T))
            try(DBL_UPLOAD_UPDATE_LOOP(pList = data_to_do, pOption = option, ToUpload = T, ToUpdate = Updated))
          }
        }
      }
    }
  }
}

# ==================================================================================================
DBL_LOOP_FOR_STOCK = function(){
  # ------------------------------------------------------------------------------------------------
  list_options = list("PERFORMANCE", "CHART_MONTH", "RISK", 'CHART_QUARTER', 'CHART_YEAR', 'LIQUIDITY')
  pMyPC = toupper(as.character(try(fread("C:/R/my_pc.txt", header = F))))
  
  for (option in sample(list_options)){
    # option = 'CHART_MONTH'
    CATln_Border(paste0(pMyPC,' ------------- ',option))
    
    TO_DO = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption = paste0('UPDATE_STOCK_',option), pAction = "COMPARE", NbSeconds = 900, ToPrint = F))
    
    # TO_DO = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption = paste0('UPDATE_',option), pAction = "COMPARE", NbSeconds = 900, ToPrint = F)
    if (TO_DO){
      TO_DO = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption = paste0('UPDATE_STOCK_',option), pAction = "SAVE", NbSeconds = 1200, ToPrint = F))
      try(DBL_STK_UPLOAD_UPDATE_LOOP(pOption = option, ToUpload = T))
    } else {
      CATln_Border('WAITING FOR THE NEXT LOOP')
    }
  }
}

#===================================================================================================
TRAINEE_MERGE_COL_FROM_SOURCE = function(pSource      = 'INS_REF',pData     = data.table(), 
                                         Folder_Fr    = '',       File_Fr   = '' , 
                                         ToSave       = F ,       Folder_To = '' ,       File_To = '' ,
                                         List_Fields  = c('name','cur','wgtg','prtr', 'category', 'size')) {
  # ------------------------------------------------------------------------------------------------
  switch(pSource,
         'INS_REF' = {
           DBL_RELOAD_INSREF(ToForce = T)
           merged_data = ins_ref
         },
         'DBL_IND_MOTHER' = {
           merged_data = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',"dbl_ind_mother.rds")
         },
         'DBL_STK_MOTHER' = {
           merged_data = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/',"dbl_stk_mother.rds")
         })
  
  if (nrow (pData) > 0 )
  {
    data_table = pData
  } else {
    data_table = DBL_CCPR_READRDS ( Folder_Fr,File_Fr, ToRestore = T, ToKable = T )
  }
  
  for (col in List_Fields) 
  {
    if (col %in% names(merged_data)) 
    {
      data_table = merge(data_table[, -c('new')], merged_data[, .(code, new = get(col))], all.x = TRUE, by = 'code')
      if (col %in% names(data_table)) 
      {
        data_table[is.na(get(col)) & !is.na(new), (col) := new]
        data_table$new = NULL
      } else {
        data_table[, (col) := new]
        data_table$new = NULL
      }
    }
  }
  
  My.Kable (data_table)
  if (ToSave) { DBL_CCPR_SAVERDS (data_table, Folder_To  ,  File_To, ToSummary = T   )}
  return(data_table)
  
}

#===================================================================================================
DBL_SHOWROOM_INDEXS = function(){
  # ------------------------------------------------------------------------------------------------
  
  dt_host = CCPR_LOAD_SQL_HOST(lhost = 'dashboard_live', pTableSQL = 'ccpi_dashboard_indcustomer_history', ToDisconnect=T)[order(code,date)]
  dt_host[,":="(change=close-shift(close), varpc=100*(close/shift(close) -1)),by="code"]
  dt_host =  try(TRAINEE_CALCULATE_FILE_PERFORMANCE  (pData = dt_host, pFolder = '', pFile   = '', EndDate  = SYSDATETIME(1),
                                                      ToAddRef = F, Remove_MAXABSRT = T) )
  if ("Y6" %in% names(dt_host))
  {
    dt_host[,":="(Y6_cum=100*((1+Y6/100)*(1+Y5/100)*(1+Y4/100)*(1+Y3/100)*(1+Y2/100)*(1+Y1/100) - 1))]
  }
  
  # dt_local = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/","dbl_indcustomer_performance.rds")
  # 
  # if (nrow(dt_host[!paste(code,user_id) %in% paste(dt_local$code,dt_local$user_id)])>0)
  # {
  dt_host = UPDATE_UPDATED(dt_host[,-c("updated")])
  DBL_CCPR_SAVERDS(dt_host[,-c("id")], "S:/CCPR/DATA/DASHBOARD_LIVE/","dbl_indcustomer_performance.rds")
  try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = 'dashboard_live', l.tablename= "dbl_indcustomer_performance",
                                   l.filepath= tolower(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/','dbl_indcustomer_performance.rds')),
                                   CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
  # }
}

#===================================================================================================
BATCH_FOR_UPDATE_DBL = function(){
  # ------------------------------------------------------------------------------------------------
  
  # ================================================================================================
  # QUOTE SLIDER
  # ================================================================================================
  
  DATACENTER_LINE_BORDER_BEGINEND(pOption="DASHBOARD_LIVE > QUOTE SLIDER", pMethod="BEGIN", AddText='' )
  
  try (MAINTENANCE_DASHBOARD (Action = 'UPDATE_UPLOAD_CCPR_MARKET_LAST',  Minutes=30))
  
  DATACENTER_LINE_BORDER_BEGINEND(pOption="DASHBOARD_LIVE > QUOTE SLIDER", pMethod="END", AddText='' )
  IFRC_SLEEP(10)
  
  # ================================================================================================
  # MARKET
  # ================================================================================================
  DATACENTER_LINE_BORDER_BEGINEND(pOption="DASHBOARD_LIVE > MARKETS", pMethod="BEGIN", AddText='' )
  
  try (MAINTENANCE_DASHBOARD (Action = 'DASHBOARD_CCPI_MARKET_CHARTS',  Minutes= 30))
  try (MAINTENANCE_DASHBOARD (Action = 'UPDATE_UPLOAD_CCPR_MARKET_TABS',  Minutes=30))
  
  DATACENTER_LINE_BORDER_BEGINEND(pOption="DASHBOARD_LIVE > MARKETS", pMethod="END", AddText='' )
  IFRC_SLEEP(10)
  
  # try(TRAINEE_DOWNLOAD_MARKET_CUSTOM (pOption = 'IPO', ToDownload = T, ToSave = T, ToUpload = T, pHost = 'dashboard_live'))
  
  # ================================================================================================
  # WHERE TO INVEST
  # ================================================================================================
  
  # try(IFRC_CCPR_INVESTMENT_HISTORY (MaxDate=Sys.Date()))
  TO_DO_WTI = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DASHBOARD_LIVE > WHERE_TO_INVEST', pAction = "COMPARE", NbSeconds = 900, ToPrint = F))
  
  if (TO_DO_WTI){
    TO_DO_WTI = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DASHBOARD_LIVE > WHERE_TO_INVEST', pAction = "SAVE", NbSeconds = 900, ToPrint = F))
    
    DATACENTER_LINE_BORDER_BEGINEND(pOption="DASHBOARD_LIVE > WHERE_TO_INVEST", pMethod="BEGIN", AddText='' )
    
    # try( TRAINEE_DASHBOARD_CALCULATE_STATS_BLOCK  (pOption = 'WHERE_TO_INVEST', Fr_Data = data.table(),  Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
    #                                                Fr_File = 'ifrc_ccpr_investment_history.rds',
    #                                                To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'ccpi_dashboard_wti.rds',
    #                                                DateLimit = Sys.Date() ,Remove_MAXABSRT = F,
    #                                                ToAddBenchmark = F, REF_LIST = list ('INDSPX', 'INDVNINDEX', 'CMDGOLD'),
    #                                                ToSave = T, ToUpload = T , pHost = 'dashboard_live') )
    wti = try( TRAINEE_DASHBOARD_CALCULATE_STATS_BLOCK  (pOption = 'WHERE_TO_INVEST', Fr_Data = data.table(),  Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                         Fr_File = 'ifrc_ccpr_investment_history.rds',
                                                         To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'ccpi_dashboard_wti.rds',
                                                         DateLimit = Sys.Date() ,Remove_MAXABSRT = F,
                                                         ToAddBenchmark = F, REF_LIST = list ('INDSPX', 'INDVNINDEX', 'CMDGOLD'),
                                                         ToSave = F, ToUpload = F , pHost = 'dashboard_live') )
    
    risk = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/','dbl_ind_risk.rds', ToRestore = T)
    
    wti = merge(wti[, -c('risk_mtd', 'risk_M1', 'risk_M2', 'risk_M3', 'risk_M4', 'risk_M5', 'risk_M6',
                         'risk_qtd', 'risk_Q1', 'risk_Q2', 'risk_Q3', 'risk_Q4', 'risk_Q5', 'risk_Q6',
                         'risk_ytd', 'risk_Y1', 'risk_Y2', 'risk_Y3', 'risk_Y4', 'risk_Y5', 'risk_Y6')], 
                risk[, .(code, risk_mtd, risk_M1, risk_M2, risk_M3, risk_M4, risk_M5, risk_M6,
                         risk_qtd, risk_Q1, risk_Q2, risk_Q3, risk_Q4, risk_Q5, risk_Q6,
                         risk_ytd, risk_Y1, risk_Y2, risk_Y3, risk_Y4, risk_Y5, risk_Y6)], by = 'code', all.x = T)
    
    dt_ann = DBL_CALCULATE_PERFORMANCE_BY_NUMBER_PERIOD(pFolder="S:/CCPR/DATA/DASHBOARD_LIVE/",
                                                        pFile=paste0("ccpi_dashboard_indbeq_all_year_history.rds"),
                                                        pType="IND",pPeriod = "YEAR", nb_per = 6)
    dt_ann = setDT(spread(dt_ann[,-c("maxrt")], "nby", "rtx"))
    
    wti = merge(wti[, -c('Y6_annualised')], dt_ann[, .(code, Y6_annualised)], by = 'code', all.x = T)
    
    DBL_CCPR_SAVERDS(wti, 'S:/CCPR/DATA/DASHBOARD_LIVE/','ccpi_dashboard_wti.rds', ToSummary = T, SaveOneDrive = T)
    
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = "dashboard_live", l.tablename= 'ccpi_dashboard_wti',
                                     l.filepath= paste0('S:/CCPR/DATA/DASHBOARD_LIVE/','ccpi_dashboard_wti.rds'),
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=T))
    
    
    DATACENTER_LINE_BORDER_BEGINEND(pOption="DASHBOARD_LIVE > WHERE_TO_INVEST", pMethod="END", AddText='' )
    IFRC_SLEEP(10)
  }
  
  # ================================================================================================
  # WORLD EQUITIES INDEXES
  # ================================================================================================
  TO_DO_WEI = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DASHBOARD_LIVE > WORLD_EQUITIES_INDEX', pAction = "COMPARE", NbSeconds = 900, ToPrint = F))
  
  if (TO_DO_WEI){
    TO_DO_WEI = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DASHBOARD_LIVE > WORLD_EQUITIES_INDEX', pAction = "SAVE", NbSeconds = 900, ToPrint = F))
    
    DATACENTER_LINE_BORDER_BEGINEND(pOption="DASHBOARD_LIVE > WORLD_EQUITIES_INDEX", pMethod="BEGIN", AddText='' )
    
    data = try( TRAINEE_DASHBOARD_CALCULATE_STATS_BLOCK  (pOption = 'WORLD_EQUITIES_INDEX', Fr_Data = data.table(),  Fr_Folder = 'S:/CCPR/DATA/',
                                                          Fr_File = 'CCPR_WORLDINDEXES_HISTORY.rds',
                                                          To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_indworld_performance.rds',
                                                          DateLimit = Sys.Date() ,
                                                          ToAddBenchmark = T, REF_LIST = list ('INDSPX', 'INDVNINDEX','CMDGOLD'),
                                                          ToSave = F, ToUpload = F , pHost = 'dashboard_live') )
    
    data_new = try( TRAINEE_DASHBOARD_CALCULATE_STATS_BLOCK  (pOption = 'WORLD_EQUITIES_INDEX', Fr_Data = data.table(),  Fr_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                                              Fr_File = 'dbl_source_ins_day_history.rds',
                                                              To_Folder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', To_File = 'dbl_indworld_performance.rds',
                                                              DateLimit = Sys.Date() ,
                                                              ToAddBenchmark = T, REF_LIST = list ('INDSPX', 'INDVNINDEX','CMDGOLD'),
                                                              ToSave = F, ToUpload = F , pHost = 'dashboard_live') )
    
    DATA_OLD = CHECK_CLASS (try (DBL_CCPR_READRDS ('S:/CCPR/DATA/DASHBOARD_LIVE/',  'dbl_indworld_performance.rds')))
    data_new = data_new[type == 'IND']
    common_cols_data = intersect(names(DATA_OLD), names(data))
    common_cols_data_new = intersect(names(DATA_OLD), names(data_new))
    
    MY_DATA = unique( rbind (DATA_OLD, data[order(-date) & !is.na(close) & !is.na(rt)][,..common_cols_data], 
                             data_new[order(-date) & !is.na(close) & !is.na(rt)& type == 'IND' & !is.na(country)][,..common_cols_data_new], fill = T), fromLast = T, by = 'code')
    
    dt_ann = DBL_CALCULATE_PERFORMANCE_BY_NUMBER_PERIOD(pFolder="S:/CCPR/DATA/DASHBOARD_LIVE/",
                                                        pFile=paste0("ccpi_dashboard_indbeq_all_year_history.rds"),
                                                        pType="IND",pPeriod = "YEAR", nb_per = 6)
    
    dt_ann = setDT(spread(dt_ann[,-c("maxrt")], "nby", "rtx"))
    
    MY_DATA = merge(MY_DATA[, -c('Y6_annualised')], dt_ann[, .(code, Y6_annualised)], by = 'code', all.x = T)
    
    MY_DATA [is.na(varpc), varpc:= rt*100]
    MY_DATA [is.na(change), change:= rt*close]
    MY_DATA = MY_DATA[!is.na(close) & !is.na(rt)]
    MY_DATA = MY_DATA[year(date) == year(Sys.Date()) & month(MY_DATA$date) >= month(Sys.Date())]
    MY_DATA = MY_DATA[,..STRUCTURE_WORLD_INDEX]
    DBL_CCPR_SAVERDS(MY_DATA[order(-date)],'S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_indworld_performance.rds', ToSummary = T, SaveOneDrive = T)
    
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = 'dashboard_live', l.tablename= gsub(".rds","",'dbl_indworld_performance.rds'),
                                     l.filepath= paste0('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_indworld_performance.rds' ) ,
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=T, AutoUpload = F))
    
    
    DATACENTER_LINE_BORDER_BEGINEND(pOption="DASHBOARD_LIVE > WORLD_EQUITIES_INDEX", pMethod="END", AddText='' )
  }
  
  # ================================================================================================
  # VIETNAM AND INTERNATIONAL STOCKS
  # ================================================================================================
  UPDATE_STOCK_DIV = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='UPDATE_STOCK_DIV', pAction = "COMPARE", NbSeconds = 7200, ToPrint = F))
  
  if (UPDATE_STOCK_DIV){
    UPDATE_STOCK_DIV = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='UPDATE_STOCK_DIV', pAction = "SAVE", NbSeconds = 3600, ToPrint = F))
    
    if (CHECK_TIMEBETWEEN('15:30' , '23:00'))
    {
      
      try(TRAINEE_STKVN_FOR_DASHBOARD())
      # try(TRAINEE_CALCULATE_STKVN_LIQUIDITY  (pData = data.table(), pFolder = 'S:/STKVN/PRICES/FINAL/',
      #                                         pFile   = 'IFRC_CCPR_STKVN_FOR_DASHBOARD.rds', EndDate  = SYSDATETIME(hour(Sys.time())), ToUpload = T, ToSave = T))
      GC_SILENT()
      # TO_DO_RISK_COMPO = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='UPDATE_RISK_COMPO', pAction = "SAVE", NbSeconds = 1, ToPrint = F))
    }
    IFRC_SLEEP(10)
    
    
    
  }
  
  try(DBL_LOOP_FOR_STOCK())
  IFRC_SLEEP(10)
  
  # ================================================================================================
  # INDEXES
  # ================================================================================================  
  
  try(DBL_LOOP_FOR_INDEXES())
  
  
  if (CHECK_TIMEBETWEEN('12:00','14:00') |  CHECK_TIMEBETWEEN('00:00','06:00'))
  {
    if (pMyPC %in% list('IFRC-IREEDS', 'IFRC-3630', 'BEQ-RD5', 'BEQ-INDEX')){
      try(DBL_MERGE_DATA_IND_BY_LIST(Index_Folder = '', Index_File = '', IndFolder="S:/CCPR/DATA/DASHBOARD_LIVE/",
                                     IndFile="ccpi_dashboard_indbeq_all_history.rds",
                                     ToFilter = F, Check_Empty = T,min_nb_files = 2000,
                                     SaveFolder="S:/CCPR/DATA/DASHBOARD_LIVE/", SaveFile="ccpi_dashboard_indbeq_all_history.rds"))
      
      STKVN = DBL_CCPR_READRDS ('S:/STKVN/PRICES/FINAL/', 'IFRC_CCPR_STKVN_FOR_DASHBOARD.rds')
      DBL_RELOAD_INSREF()
      # ins_ref [code %in% STKVN$code]
      # str (x)
      x = merge (STKVN [, -'name'], ins_ref [code %in% STKVN$code] [,.(code, name)], all.x = T, by = 'code')
      y = DBL_CCPR_READRDS (UData, 'ifrc_beq_stkin_history.rds', ToRestore = T)
      z = DBL_CCPR_READRDS (UData, 'download_yah_stkhome_history.rds', ToRestore = T)
      t = DBL_CCPR_READRDS ('S:/STKVN/PRICES/FINAL/','download_yah_stkin_sp500_history.rds', ToRestore = T)
      t = unique(t, by = c('code','date'))
      mother = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/','dbl_stk_mother.rds', ToRestore = T)
      # mother[, n:= (1:.N), by ='ticker']
      # unique(mother[n==2], by = 'code')
      # 
      # mother[ticker == 'SHL']
      mother = unique(mother, by = 'code')
      t = merge(t[, -c('sector','industry')], mother[,.(code, sector, industry)], all.x = T, by = 'code')
      
      t[, sum_capi:=sum(capiusd), by='date']
      t[, cum_capi:=cumsum(capiusd), by='date']
      t[, pc_capi:=100*cum_capi/sum_capi, by='date']
      t[!is.na(sum_capi), size := ifelse(pc_capi<=85, 'LARGE', ifelse(pc_capi>85 & pc_capi<=95, 'MID', 'SMALL'))]
      
      MERGED_STK = rbind ( x [,.(code, name,date,symbol = ticker, ticker,market, shares, close, close_unadj,
                                 close_adj,change, capi = capibvnd,rt, varpc,sector, industry, size, volume)],
                           y [,.(code, name = company_name, date, symbol, ticker, market, shares ,close=close_adj,
                                 close_unadj=close, close_adj,change, capi = capiusd/1000000000,rt, varpc,sector,
                                 industry, size =as.character (NA), volume )],
                           z [,.(code, name = company_name, date, symbol, ticker, market, shares ,close=close_adj,
                                 close_unadj=close, close_adj,change, capi = capiusd/1000000000,rt, varpc,sector,
                                 industry, size =as.character (NA), volume )],
                           t [,.(code, name, date, symbol = ticker, ticker, market, shares = share_outstanding, close, 
                                 close_adj, close_unadj = close, change, capi = capiusd/1000000000, rt, varpc, sector, industry,
                                 size =as.character (NA), volume)], fill = T)
      
      MERGED_STK = unique (MERGED_STK , by = c('code', 'date') )
      STK_DAY = merge (MERGED_STK [, -c('name')], mother [code %in% MERGED_STK$code] [,.(code, name)], all.x = T, by = 'code')
      # STK_DAY = merge (MERGED_STK [, -c('name')], ins_ref [code %in% MERGED_STK$code] [,.(code, name)], all.x = T, by = 'code')
      STK_DAY [is.na (ticker), ticker := symbol]
      STK_DAY [is.na (symbol), symbol := ticker]
      STK_DAY = STK_DAY[!is.na(ticker)]
      STK_DAY[,capixrt:=capi*rt]
      STK_DAY[substr(code,1,5)=="STKVN",coverage:="VIETNAM"]
      STK_DAY[substr(code,1,5)!="STKVN",coverage:="INTERNATIONAL"]
      DBL_CCPR_SAVERDS (STK_DAY[order(-capi)],'S:/CCPR/DATA/DASHBOARD_LIVE/','ccpi_dashboard_stk_all_day_history.rds' )
      
      TRAINEE.IFRC.UPLOAD.RDS.2023 (l.host = "dashboard_live", l.tablename = 'dbl_stk_day_chart',
                                    l.filepath = paste0('S:/CCPR/DATA/DASHBOARD_LIVE/','ccpi_dashboard_stk_all_day_history.rds'), CheckField=T, ToPrint = T,
                                    ToForceUpload=T, ToForceStructure=F, AutoUpload= T)
      
    }
  }
  
  # ================================================================================================
  # COMPO
  # ================================================================================================    
  
  if (CHECK_TIMEBETWEEN('00:01' , '08:00') | CHECK_TIMEBETWEEN('18:00' , '23:59'))
  {
    TO_DO_COMPO = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='UPDATE_COMPO', pAction = "COMPARE", NbSeconds = 3*60*60, ToPrint = F))
    if (TO_DO_COMPO){
      try(TRAINEE_DBL_IND_COMPO  (pOPTION = 'COMPO', StartDate = '2023-07-01', EndDate = Sys.Date (),
                                  ToSave = T, ToIntegrate = F , ToUpload = T) )
      GC_SILENT()
      IFRC_SLEEP(10)
      
      
      try(TRAINEE_DBL_IND_COMPO  (pOPTION = 'TOP_COMPO', StartDate = '2023-07-01', EndDate = Sys.Date (),
                                  ToSave = T, ToIntegrate = F , ToUpload = T) )
      
      GC_SILENT()
      IFRC_SLEEP(10)
      TO_DO_COMPO = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='UPDATE_COMPO', pAction = "SAVE", NbSeconds = 1, ToPrint = F))
    }
  }
  
  # ================================================================================================
  # FIGURES
  # ================================================================================================ 
  if (CHECK_TIMEBETWEEN('00:01' , '08:00') | CHECK_TIMEBETWEEN('18:00' , '23:59'))
  {
    TO_DO_FIGURES = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='UPDATE_FIGURES', pAction = "COMPARE", NbSeconds = 3*60*60, ToPrint = F))
    if (TO_DO_FIGURES){
      GC_SILENT()
      IFRC_SLEEP(10)
      list_file = setDT(fread('S:/CCPR/DATA/DASHBOARD_LIVE/list_files_beq_ind_history.txt'))
      
      for (i in 1:nrow(list_file)){
        if (list_file$filename[i] != 'ifrc_ccpr_investment_history.rds'){
          try(DBL_FIGURES_BY_FILE(  pFolder = list_file$folder[i], pFile = list_file$filename[i],
                                    ListCodes = c(),CompoFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/',
                                    CompoFile = "dbl_ind_compo.rds",
                                    ToFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', ToFile  = paste0('all_figures.rds'),
                                    FolderMerge = 'S:/CCPR/DATA/DASHBOARD_LIVE/', FileMerge = 'dbl_ind_figures.rds',
                                    ToSave = T, ToIntergration = T,ToUpload = T,pHost = "dashboard_live"))
        }
      }
      
      
      try(DBL_STK_FIGURES_BY_FILE  (  pFolder = 'S:/STKVN/PRICES/FINAL/', pFile = paste0('IFRC_CCPR_STKVN_FOR_DASHBOARD.rds'),
                                      ListCodes = c(),
                                      ToFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', ToFile  = paste0('all_figures.rds'),
                                      FolderMerge = 'S:/CCPR/DATA/DASHBOARD_LIVE/', FileMerge = 'dbl_stk_figures.rds',
                                      ToSave = T, ToIntergration = T,ToUpload = T,pHost = "dashboard_live"))
      
      try(DBL_STK_FIGURES_BY_FILE  (  pFolder = UData, pFile = paste0('download_yah_stkhome_history.rds'),
                                      ListCodes = c(),
                                      ToFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', ToFile  = paste0('all_figures.rds'),
                                      FolderMerge = 'S:/CCPR/DATA/DASHBOARD_LIVE/', FileMerge = 'dbl_stk_figures.rds',
                                      ToSave = T, ToIntergration = T,ToUpload = T,pHost = "dashboard_live")) 
      
      try(DBL_STK_FIGURES_BY_FILE  (  pFolder = 'S:/STKVN/PRICES/FINAL/', pFile = paste0('download_yah_stkin_sp500_history.rds'),
                                      ListCodes = c(),
                                      ToFolder = 'S:/CCPR/DATA/DASHBOARD_LIVE/', ToFile  = paste0('all_figures.rds'),
                                      FolderMerge = 'S:/CCPR/DATA/DASHBOARD_LIVE/', FileMerge = 'dbl_stk_figures.rds',
                                      ToSave = T, ToIntergration = T,ToUpload = T,pHost = "dashboard_live"))    
      
      GC_SILENT()
      IFRC_SLEEP(10)
      TO_DO_COMPO = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='UPDATE_FIGURES', pAction = "SAVE", NbSeconds = 1, ToPrint = F))
    }
  }
  IFRC_SLEEP(10)
  
  
  
  # ================================================================================================
  # SCREENER
  # ================================================================================================ 
  TO_DO_SCREENER = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DASHBOARD_LIVE > UPDATE_SCREENER', pAction = "COMPARE", NbSeconds = 3600, ToPrint = F))
  
  if (TO_DO_SCREENER){
    try(DBL_CREATE_DATA_SCREENER())
    TO_DO_SCREENER = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DASHBOARD_LIVE > UPDATE_SCREENER', pAction = "SAVE", NbSeconds = 3600, ToPrint = F))
  }
  
  # ================================================================================================
  # PREPARE DATA FOR BACKTESTING
  # ================================================================================================ 
  if (CHECK_TIMEBETWEEN('17:00' , '23:59')){
    TO_DO_PREPARE = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DASHBOARD_LIVE >  PREPARE_DATA_FOR_BACKTESTING', pAction = "COMPARE", NbSeconds = 3600, ToPrint = F))
    if(TO_DO_PREPARE){
      try(SMOOTH_DATA_COMPARE_BACKTESTING(ToSave =T, ToUpload = T))
      TO_DO_PREPARE = try(TRAINEE_MONITOR_EXECUTION_SUMMARY(pOption='DASHBOARD_LIVE >  PREPARE_DATA_FOR_BACKTESTING', pAction = "SAVE", NbSeconds = 3600, ToPrint = F))
      
    }
  }
  
}

# ==================================================================================================
SMOOTH_DATA_COMPARE_BACKTESTING = function(ToSave =T, ToUpload = T){
  # ------------------------------------------------------------------------------------------------
  x = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", "dbl_source_ins_day_history.rds")
  wti = DBL_CCPR_READRDS("S:/CCPR/DATA/DASHBOARD_LIVE/", "ifrc_ccpr_investment_history.rds")
  wti[code == 'CURCRYBTC', code := 'CURBTCUSD']
  x = x[code %in% c("CMDGOLD","CURBTCUSD","INDNDX","INDSPX","INDVNINDEX")]
  wti = wti[code %in% c("CMDGOLD","CURBTCUSD","INDNDX","INDSPX","INDVNINDEX")]
  
  x = unique(rbind(x,wti,fill = T), by = c('code','date'))
  
  x[,close:=format(round(close,2),nsmall = 2)]
  # x = UPDATE_UPDATED(x[,.(code,date,name,close)])
  name = unique(x, by = 'code')
  name[code == 'CMDGOLD', name := 'GOLD']
  name[code == 'CURBTCUSD', name := 'BITCOIN']
  dt = x[,.(code,date,name,close)][order(code,date)]
  end_date = Sys.Date() 
  complete_dt = dt[, .(date = seq.Date(min(date), end_date, by = "day")), by = code]
  merged_dt = merge(complete_dt, dt, by = c("code", "date"), all.x = TRUE)
  merged_dt[, close := na.locf(close, na.rm = FALSE), by = code]
  data_backtesting = merge(merged_dt[,-c('name')], name[,.(code,name)], all.x = T, by = 'code')
  data_backtesting = UPDATE_UPDATED(data_backtesting[,-c('updated')])
  
  if (ToSave){
    DBL_CCPR_SAVERDS(data_backtesting, 'S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_backtesting_compare.rds', ToSummary = T, SaveOneDrive = T)
  }
  if (ToUpload){
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = 'dashboard_live', l.tablename= "dbl_backtesting_compare",
                                     l.filepath= tolower(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_backtesting_compare.rds') ),
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F))
    
    try(TRAINEE.IFRC.UPLOAD.RDS.2023(l.host = 'dashboard_live_dev', l.tablename= "dbl_backtesting_compare",
                                     l.filepath= tolower(paste0('S:/CCPR/DATA/DASHBOARD_LIVE/', 'dbl_backtesting_compare.rds') ),
                                     CheckField=T, ToPrint = T, ToForceUpload=T, ToForceStructure=F, AutoUpload = F))
  }
}


# ==================================================================================================
DEV_IFRC_CCPR_INVESTMENT_HISTORY = function(MaxDate=Sys.Date()) {
  # ------------------------------------------------------------------------------------------------
  
  # try(IFRC_CCPR_INVESTMENT_HISTORY())
  SCCPRData = 'S:/CCPR/DATA/'
  
  # Bonds ..........................................................................................
  y = download.file(paste0('https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1318&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=DGS10&scale=left&cosd=1900-01-01&coed=',Sys.Date(),'&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Daily&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=',Sys.Date(),'&revision_date=',Sys.Date(),'&nd=1962-01-02'), 
                    'C:/temp/fed.csv')
  
  data = setDT(fread('C:/temp/fed.csv'))
  My.Kable(data)
  y = data[, .(code = 'BNDGUS10Y', name = 'U.S. 10Y BOND YIELD', date = as.Date(DATE), close = as.numeric(DGS10))]
  y[, close := na.locf(close)]
  My.Kable(y[is.na(close)])
  
  xc  = DBL_CCPR_READRDS(UData, 'DOWNLOAD_INV_BND_HISTORY.rds', ToKable = T, ToRestore = T)
  My.Kable(xc)
  
  BND = unique(xc[code=='BNDGUS10Y'][order(date)], by=c('code', 'date'))
  BND = unique(rbind(BND[!is.na(close)], y, fill = T), by = c('code', 'date'),  fromLast = T)
  BND[, rtd:=(close/100)/365]
  My.Kable(BND[, .(code, date, rtd, close)])
  BND[, value:=100]
  LastValue = 100
  
  cumulative_rtd = cumprod(1 + BND$rtd)
  BND$value      = c(LastValue, LastValue * cumulative_rtd[-nrow(BND)])
  
  BND = BND[, .(code, date, rtd, close=value, value)]
  BND = MERGE_DATASTD_BYCODE(BND, pOption = 'FINAL')
  My.Kable(BND[order(date)])
  
  # Crypto ..........................................................................................
  xc = DBL_CCPR_READRDS(UData, 'DOWNLOAD_CNM_CURCRY_HISTORY.rds', ToKable = T, ToRestore = T)
  x1 = xc[grepl('BTC', code)]
  BTC = x1[code=='CURCRYBTC']
  My.Kable.Min(BTC[order(date)])
  
  y = DOWNLOAD_YAH_INS_PRICES_UNADJ (p_nbdays_back = 3600, tickers = 'BTC-USD', freq.data = 'daily', p_saveto="", NbTry=10) 
  y[, code := 'CURCRYBTC']
  z = unique(rbind(BTC, y, fill = T), by = c("code", "date"), fromLast = T)[, .(code, date, close)]
  My.Kable(z[order(date)][, .(code, date, close)])
  BTC = z
  
  xc = DBL_CCPR_READRDS(UData, 'DOWNLOAD_YAH_CMD_HISTORY.rds', ToKable = T, ToRestore = T)
  x1 = xc[grepl('GOLD', code)]
  GOLD = x1[code=='CMDGOLD'][, close := close_adj]
  My.Kable.Min(GOLD[order(date)])
  
  y = DOWNLOAD_YAH_INS_PRICES_UNADJ (p_nbdays_back = 3600, tickers = 'GC=F', freq.data = 'daily', p_saveto="", NbTry=10) 
  z = unique(rbind(GOLD, y, fill = T), by = c("code", "date"), fromLast = T)[, .(code, date, close)]
  My.Kable(z[order(date)][, .(code, date, close)])
  GOLD = z
  
  x = DBL_CCPR_READRDS(UData, 'EFRC_INDHOME_HISTORY.rds', ToKable = T, ToRestore = T)
  x1 = x[grepl('VNI', code)]
  VNIx1 = x1[code =='INDVNINDEX']
  
  y = DBL_CCPR_READRDS(UData, 'download_caf_indvn_prices_history.rds', ToKable = T, ToRestore = T)
  y1 = y[grepl('VNI', code)]
  VNIy1 = y1[code=='INDVNINDEX']
  z = unique(rbind(VNIx1, VNIy1, fill = T), by = c("code", "date"), fromLast = T)
  My.Kable(z[order(date)][, .(code, date, close)])
  VNI = z
  
  x1 = x[grepl('500', name)]
  SPX = x1[code=='INDSPX']
  My.Kable(SPX[, .(code, date, close)])
  y = DOWNLOAD_YAH_INS_PRICES_UNADJ (p_nbdays_back = 3600, tickers = '^GSPC', freq.data = 'daily', p_saveto="", NbTry=10) 
  z = unique(rbind(SPX, y, fill = T), by = c("code", "date"), fromLast = T)[, .(code, date, close)]
  My.Kable(z[order(date)][, .(code, date, close)])
  SPX = z
  
  x1 = x[grepl('NASDAQ', name)]
  NDX = x1[code=='INDNDX']
  My.Kable(NDX[, .(code, date, close)])
  y = DOWNLOAD_YAH_INS_PRICES_UNADJ (p_nbdays_back = 3600, tickers = '^NDX', freq.data = 'daily', p_saveto="", NbTry=10) 
  z = unique(rbind(NDX, y, fill = T), by = c("code", "date"), fromLast = T)[, .(code, date, close)]
  My.Kable(z[order(date)][, .(code, date, close)])
  NDX = z
  
  # N225 = x1[code=='INDN225']
  
  IPO = DOWNLOAD_YAH_INS_PRICES_UNADJ (p_nbdays_back = 3600, tickers = 'IPO', freq.data = 'daily', p_saveto="", NbTry=10) 
  # x1 = MERGE_DATASTD_BYCODE(IPO)[order(code, date)]
  # x2 = MERGE_DATASRC_CODE_BYCODESOURCE('YAH', IPO)
  IPO = MERGE_DATASTD_BYCODE(IPO, pOption = 'FINAL')
  IPO[, ':=' (code = 'FNDINIPOIPO', name = 'RENAISSANCE IPO ETF', type = 'FND', fcat = 'FND', scat = 'EQUITY', isin = 'US7599372049')]
  
  # a = DOWNLOAD_YAH_INS_PRICES_UNADJ (p_nbdays_back = 3600, tickers = '^990100-USD-STRD', freq.data = 'daily', p_saveto="", NbTry=10) 
  # # x1 = MERGE_DATASTD_BYCODE(IPO)[order(code, date)]
  # # x2 = MERGE_DATASRC_CODE_BYCODESOURCE('YAH', IPO)
  # MSCI = MERGE_DATASTD_BYCODE(MSCI, pOption = 'FINAL')
  # MSCI[, ':=' (code = 'INDMSCIWORLD', name = 'MSCI WORLD PR (USD)', type = 'FND', fcat = 'INDOTH', scat = 'EQUITY')]
  x = DBL_CCPR_READRDS(UData, 'EFRC_INDHOME_HISTORY.rds', ToKable = T, ToRestore = T)
  # y = CCPR_READRDS(UData, 'DOWNLOAD_YAH_INDHOME_HISTORY.rds', ToKable = T, ToRestore = T)
  MSCIz = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'MSCI_WORLD.rds', ToKable = T, ToRestore = T)
  # MSCI_YAH = DOWNLOAD_YAH_INS_PRICES_UNADJ (p_nbdays_back = 3600, tickers = '^990100-USD-STRD', freq.data = 'daily', p_saveto="", NbTry=10) 
  MSCI = x[grepl('INDMSCIWORLD', code)][, code := gsub('USD','',code)]
  # MSCI[, codesource := '^990100-USD-STRD']  
  # MSCI = merge(unique(MSCI[, -c('date','close')], by = 'codesource'), MSCI_YAH[, .(codesource, date, close)], all.y = T, by = 'codesource')
  MSCI = unique(rbind(MSCI, MSCIz, fill = T), fromLast = T, by = c('code','date'))
  
  ASEAN40 = x[grepl('ASEAN40', code)]
  # y = CCPR_READRDS(UData, 'DOWNLOAD_RTS_INDHOME_HISTORY.rds', ToKable = T, ToRestore = T)
  ASEAN40z = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'ASEAN_40.rds', ToKable = T, ToRestore = T)
  ASEAN40z[, code := 'INDFTSEASEAN40']
  ASEAN40 = unique(rbind(ASEAN40, ASEAN40z, fill = T), fromLast = T, by = c('code','date'))
  
  LOG_HISTORY = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/STKVN/INDEX_2024/', 'beq_indlogals_history.rds', ToRestore = T)))
  LOG = LOG_HISTORY[code == 'INDVNXSECLOGCWPRVND']
  
  FIN_HISTORY = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/STKVN/INDEX_2024/', 'beq_indfinals_history.rds', ToRestore = T)))
  FIN = FIN_HISTORY[code == 'INDVNXSECFINCWPRVND']
  
  HLC_HISTORY = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/STKVN/INDEX_2024/', 'beq_indhlcals_history.rds', ToRestore = T)))
  HLC = HLC_HISTORY[code == 'INDVNXSECHLCCWPRVND']
  
  ENY_HISTORY = CHECK_CLASS(try(DBL_CCPR_READRDS('S:/STKVN/INDEX_2024/', 'beq_indenyals_history.rds', ToRestore = T)))
  ENY = ENY_HISTORY[code == 'INDVNXSECENYCWPRVND']
  
  xALL = rbind(VNI, NDX, SPX, BTC, GOLD, BND, LOG, FIN, HLC, ENY, IPO, MSCI, ASEAN40, fill=T)[, .(code, name, date, close)]
  
  # x = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/','dbl_source_ins_day_history.rds')
  # 
  # common_cols = intersect(names(x), names(xALL))
  # 
  # xALL = rbind(xALL, x[code %in% xALL$code, ..common_cols])
  # 
  xALL = unique(xALL, by=c('code', 'date'))
  xALL = MERGE_DATASTD_BYCODE(xALL, pOption='FINAL')[order(code, date)]
  xALL = xALL[date<=MaxDate]
  
  xALL = xALL[order(code, date)]
  xALL[, rt := close/shift(close), by = 'code']
  
  xALL[, sub_name := name]
  
  xALL[code == 'INDVNXSECLOGCWPRVND', sub_name := 'BEQ LOGISTICS SECTOR']
  xALL[code == 'INDVNXSECFINCWPRVND', sub_name := 'BEQ FINANCE SECTOR']
  xALL[code == 'INDVNXSECENYCWPRVND', sub_name := 'BEQ ENERGY SECTOR']
  xALL[code == 'INDVNXSECHLCCWPRVND', sub_name := 'BEQ HEALTH CARE SECTOR']
  xALL[code == 'INDMSCIWORLD'       , sub_name := 'MSCI WORLD PR (USD)']
  xALL[code == 'INDFTSEASEAN40'     , sub_name := 'FTSE/ASEAN40']
  xALL[code == 'INDFTSEASEAN40'     , name := 'FTSE/ASEAN40']
  
  # xALL[, sub_name := name]
  # xALL[, name := NULL]
  My.Kable.Min(xALL)
  My.Kable.All(unique(xALL[order(code, -date)], by='code'))
  
  Data_Old = DBL_CCPR_READRDS('S:/CCPR/DATA/DASHBOARD_LIVE/', 'ifrc_ccpr_investment_history.rds', ToRestore = T)
  if (nrow(Data_Old) > 0){
    xAll = unique(rbind(Data_Old, xAll, fill = T), by = c('code', 'date'))
    dir.create(paste0(CCPRData, 'INDEX/'))
    dir.create(paste0(SCCPRData, 'INDEX/'))
    
    try(DBL_CCPR_SAVERDS(xALL, paste0(CCPRData, 'INDEX/'), 'ifrc_ccpr_investment_history.rds', ToSummary = T, SaveOneDrive = T))
    try(DBL_CCPR_SAVERDS(xALL, paste0(CCPRData, 'DASHBOARD/'), 'ifrc_ccpr_investment_history.rds', ToSummary = T, SaveOneDrive = T))
    try(DBL_CCPR_SAVERDS(xALL, paste0(SCCPRData, 'INDEX/'), 'ifrc_ccpr_investment_history.rds', ToSummary = T, SaveOneDrive = T))
    try(DBL_CCPR_SAVERDS(xALL, paste0(SCCPRData, 'DASHBOARD/'), 'ifrc_ccpr_investment_history.rds', ToSummary = T, SaveOneDrive = T))
    try(DBL_CCPR_SAVERDS(xALL, paste0('S:/SHINY/INDEX/DASHBOARD/'), 'ifrc_ccpr_investment_history.rds', ToSummary = T, SaveOneDrive = T))
    try(DBL_CCPR_SAVERDS(xALL, paste0('S:/CCPR/DATA/DASHBOARD_LIVE/'), 'ifrc_ccpr_investment_history.rds', ToSummary = T, SaveOneDrive = T))
  }
}


# ==================================================================================================
TRAINEE_CALCULATE_FILE_PERFORMANCE_NEW = function (pData = data.table(), pFolder = 'S:/CCPR/DATA/DASHBOARD/',
                                                   pFile   = 'CCPI_DASHBOARD_INDIFRCCURCRY_HISTORY.rds', EndDate  = '2024-05-06', 
                                                   ToAddRef = T, Remove_MAXABSRT = T )  {
  # ------------------------------------------------------------------------------------------------
  #updated: 2024-06-04 18:55
  
  if (nrow(pData ) > 1 ) 
  { 
    x = pData
    if ( !'close_adj' %in% colnames(pData) ) { x [, close_adj := close]}
    x [is.na(close_adj), close_adj := close]
    if ( !'code' %in% colnames(pData) ) { x [, code := codesource]}
  } else {
    if (pFile == 'ifrc_ccpr_investment_history.rds') {
      investment = CCPR_READRDS(pFolder, pFile) [order (-date)]
      ipo = CCPR_READRDS ('S:/CCPR/DATA/DASHBOARD_LIVE/', 'ETFIPO.rds')
      investment = rbind (investment, ipo, fill=  T)
      investment [, rt:=((close/shift(close))-1), by='code']
      investment [, ':=' (cur = NA, close_adj = close)]
      x = investment
    } else { 
      x = CCPR_READRDS(pFolder, pFile)
      My.Kable(x[order(date)])
      if ( !'close_adj' %in% colnames(x) ) { x [, close_adj := close]}
      if ( !'code' %in% colnames(x) ) { x [, code := codesource]}
    }
  }
  
  # STEP 1: READ FILE PRICES HISTORY
  My.Kable(x)
  colnames(x)
  # cleanse raw data
  x = unique(x, by = c('code', 'date'))
  x = x [!is.na(close_adj)]
  
  if ( nrow(x [substr(code,1,3) == 'STK']) == nrow (x)) { pType = 'STK' } else { 
    if ( nrow(x [substr(code,1,3) == 'STK']) == 0) {pType = 'OTHER'} else {pType = ''}
  }
  if (Remove_MAXABSRT) { 
    source(paste0(SDLibrary, "TRAINEE_INTRO.R"),    echo=F)
    
    print (paste(pType,STK_MAXABSRT,IND_MAXABSRT) )
    switch (pType,
            'STK' = { 
              ToExclude = unique (x [abs(rt) > STK_MAXABSRT ], by = c('code','date'))
              if (length (ToExclude) >0) { x = x [!ToExclude, on =.(code, date) ]}
            },
            'OTHER' = {
              # ToExclude = unique (x [abs(rt) > IND_MAXABSRT & !grepl('VIX',code)]$date)
              ToExclude = unique (x [abs(rt) > STK_MAXABSRT & !grepl('VIX',code)], by = c('code','date'))
              
              if (length (ToExclude) >0) { x = x [!ToExclude, on =.(code, date) ]}
            },
            {
              ToExclude = unique (x [abs(rt) > STK_MAXABSRT & !grepl('VIX',code)], by = c('code','date'))
              if (length (ToExclude) >0) { x = x [!ToExclude, on =.(code, date) ]}
            })
  } 
  # rm(pType)
  
  
  # if (grepl ('STK',pFile)) { x [rt < STK_MAXABSRT]} else { x [rt <= IND_MAXABSRT]}
  
  # STEP 2: RE-CALCULATE DATA
  # Limit data maxdate
  exclude = try (setDT (fread ('S:/CCPR/DATA/DASHBOARD_LIVE/dbl_list_ind_to_exclude.txt') ) )
  if (all(class(exclude)!='try-error')) {
    Ind_History = x  [date <= EndDate] [! code %in% exclude [active == 1]$code] 
  }
  
  My.Kable(Ind_History[, .(code, date, close)][order(date)])
  str(Ind_History)
  
  #Calculate necessary column: change, varpc, yyyy, yyyymm, yyyyqn
  # Ind_History = Ind_History[order(code, date)] 
  Ind_History = CALCULATE_CHANGE_RT_VARPC(Ind_History)
  if (all (! c('change', 'varpc') %in% names (Ind_History))) {
    Ind_History[, ':='(change=close_adj-shift(close_adj), varpc=100*(close_adj/shift(close_adj)-1)), by='code']
  }
  
  # if (pType != 'STK'){
  #   Ind_History = CHECK_LAST_CHANGE_ZERO(Ind_History)
  # }
  Ind_History[,":="(yyyy=year(date),yyyymm=year(date)*100+month(date),yyyyqn=year(date)*100+floor((month(date)-1)/3)+1),]
  
  #Calculate rtm
  Ind_Month = unique(Ind_History[order(code, -date)], by=c('code', 'yyyymm'))[order(code, date)]
  Ind_Month[, rtm:=(close_adj/shift(close_adj))-1, by='code']
  
  My.Kable(Ind_History[, .(code, date, close, yyyy, yyyymm, yyyyqn)])
  #Calculate rtq
  Ind_Quarter = unique(Ind_History[order(code, -date)], by=c('code', 'yyyyqn'))[order(code, date)]
  Ind_Quarter[, rtq:=(close_adj/shift(close_adj))-1, by='code']
  
  #Calculate rty
  Ind_Year  = unique(Ind_History[order(code, -date)], by=c('code', 'yyyy'))[order(code, date)]
  Ind_Year[, rty:=(close_adj/shift(close_adj))-1, by='code']
  
  #Merge data: month, quarter, year
  Ind_Overview = unique(Ind_History[order(code, -date)], by=c('code')) #[, .(code, name, date, last=close_adj, last_change, last_varpc)]
  Ind_Overview = merge(Ind_Overview, Ind_Month[, .(code, date, mtd=round(100*rtm,12))], all.x = T, by=c('code', 'date'))
  Ind_Overview = merge(Ind_Overview, Ind_Quarter[, .(code, date, qtd=round(100*rtq,12))], all.x = T, by=c('code', 'date'))
  Ind_Overview = merge(Ind_Overview, Ind_Year[, .(code, date, ytd=round(100*rty,12))], all.x = T, by=c('code', 'date'))
  My.Kable.All(Ind_Overview[,.(code, date, close, yyyy, yyyymm, yyyyqn, mtd, qtd, ytd)])
  
  
  Ind_Month6M = Ind_Month[order(code, -date)][, nr:=seq.int(1, .N), by='code'][nr<=6][, .(code, date, rtm, nr)]
  My.Kable(setDT(spread(Ind_Month6M[, .(code, nr = paste0('M',nr), rtm=round(100*rtm,12))], key='nr', value='rtm'))[order(-M1)])
  Res_Month = setDT(spread(Ind_Month6M[, .(code, nr = paste0('M',nr), rtm=round(100*rtm,12))], key='nr', value='rtm'))[order(-M1)]
  
  Ind_Quarter6Y = Ind_Quarter[order(code, -date)][, nr:=seq.int(1, .N), by='code'][nr<=6][, .(code, date, rtq, nr)]
  Res_Quarter   = setDT(spread(Ind_Quarter6Y[, .(code, nr = paste0('Q',nr), rtq=round(100*rtq,12))], key='nr', value='rtq'))[order(-Q1)]
  My.Kable(Res_Quarter)
  
  Ind_Year6Y = Ind_Year[order(code, -date)][, nr:=seq.int(1, .N), by='code'][nr<=6][, .(code, date, rty, nr)]
  Res_Year   = setDT(spread(Ind_Year6Y[, .(code, nr = paste0('Y',nr), rty=round(100*rty,12))], key='nr', value='rty'))[order(-Y1)]
  My.Kable(Res_Year)
  
  
  
  Ind_All = merge(Ind_Overview,Res_Month, all.x = T, by = 'code' )
  Ind_All = merge(Ind_All,Res_Quarter, all.x = T, by = 'code' )
  Ind_All = merge(Ind_All,Res_Year, all.x = T, by = 'code' )
  My.Kable(Ind_All)
  
  # my_add   =  ins_ref [ code %in% Ind_All$code]
  # Ind_Data = merge (Ind_All, my_add [,.(code,  iso2, country, continent)], all.x = T, by = 'code')
  if (ToAddRef) {
    Ind_Data = TRAINEE_MERGE_COL_FROM_REF (pData = Ind_All, 
                                           List_Fields = c('name','cur','wgtg','prtr', 'category', 'size'))
  } else { Ind_Data = Ind_All}
  
  # if (!'name' %in% names (Ind_All)) {
  #   Ind_Data = merge (Ind_All, my_add [,.(code, name)], all.x = T, by = 'code')  
  # } else { Ind_Data = Ind_All}
  
  if (!'last' %in% names (Ind_Data)) {
    Ind_Data [, last:= close]
  }
  Ind_Data [, updated := SYS.TIME()]
  
  return(Ind_Data)
}