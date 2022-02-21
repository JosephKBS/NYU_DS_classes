#==================================================
# Title : NYU Data Services Class data processing
# Author: Joseph Shim
#==================================================

library(tidyverse)
library(lubridate)
library(stringr)
library(qdap)

############################################################

#               1. Meeting INFORMATION
   
############################################################

#=============== meeting information extraction ===============
mtinfo_extract <- function(file){
  if (!file.size( paste0("./zoom_attendance/", file ) ) == 0) {
    df <- read.csv( paste0("./zoom_attendance/", file) ) 
    if ( dim(df)[1] == 0 ) { print("File has 0 row") }
    } 
  else {print("File is NULL or damaged") 
  }    
  
  if("X" %in% colnames(df)) { 
  mt_info <- df[1, ] %>% select(-X) 
  } else{ mt_info <- df[1, ]}
  
  mt_info <-  mt_info %>%
    mutate(Date = as.POSIXct(Start.Time, format = "%m/%d/%Y %H"),
          Year = year(Date),
          Month = month(Date),
          date = day(Date),
          hour = format(Date, format= "%H"),
          source = file)
  colnames(mt_info) <- c("MeetingID", "Topic", "Start_Time", "End_Time", "User_Email", "Duration",
                         "Participants","Date","Year", "Month","Day","Hour","source")
  mt_info     
}

# ================ Meeting Infromation Extraction  ================
count = 0
success = 0
mt_total <- data.frame()
er_total <- data.frame()
total = length(list.files(path="./zoom_attendance" ) )

for (file in list.files(path="./zoom_attendance")) {
  tryCatch( 
    expr = {
     # extract meeting information
     mt_extract <- mtinfo_extract(file)
     mt_total = rbind(mt_total, mt_extract)
     message("Successfully processed ", file, " ", "\n") 
     success = success + 1
     },
    error = function(e){
      cat('# Error! : ', file , "\n") 
    },
    finally = { message('All done', "\n")  }
  )
}
paste("---All process completed!---", "\n", "Success :", success, "Error :", total- success)


# creating error file list
`%!in%` <- Negate(`%in%`)
for (file in list.files(path="./zoom_attendance")) {
  if(file %!in% mt_total$source){
    er_total = rbind(er_total, file)
    colnames(er_total)<-c("error_file_list")
  } 
}

# extract potential MeetingID
er_total$MeetingID <- NA
end_file_num <- length(str_extract_all(er_total$error_file_list,"\\(?[0-9,.]+\\)?"))
for( j in 1: end_file_num){
  num_list <- gsub("[[:punct:]]", "", str_extract_all(er_total$error_file_list,"\\(?[0-9,.]+\\)?")[[j]] )
  for (w in num_list) {
    if( nchar(w)==11){
      er_total$MeetingID[j] <- w 
    } 
  }
}

# meeting ID extractor function
mtid_extractor <- function(file){
  end_file_num <- length(str_extract_all(file,"\\(?[0-9,.]+\\)?"))
  for( j in 1: end_file_num){
    word_list <- gsub("[[:punct:]]", "", str_extract_all(file, "\\(?[0-9,.]+\\)?")[[j]] )
    for (w in word_list) {
      if( nchar(w)==11){
        return ( as.numeric(w) ) 
      }  
     } 
   }
}

# Meeting Info with Missing data
mt_info_missing <- function( file ){ 
  # warning sign for data size
  if (!file.size( paste0("./zoom_attendance/", file ) ) < 80) {  # smaller than 100 byte
    df <- read.csv( paste0("./zoom_attendance/", file) ) 
    # warning for 0 rows
      if ( dim(df)[1] == 0 ) { 
        print("WARNING: File has 0 row") }
  } else {
      print("WARNING: File size is smaller than 80 byte; File is NULL or damaged") 
  }
  # warning sign for column size
  if (dim(df)[2] == 5 | dim(df)[2] == 6 ) {
    #print("looks good")
  } else{
    print("WARNING: File format is not right; number of columns are not 5 nor 6.")
  }
  # flexible data format
  if ("Guest" %in% colnames(df) ){
    colnames(df) <-c("name", "email", "jointime","leavetime", "duration", "guest")
  } else {
    colnames(df) <-c("name", "email", "jointime","leavetime", "duration")
  }
  
  mt_info <-  df %>%
    mutate( MeetingID = mtid_extractor(file), 
            Topic = NA,
            starttime = jointime,  endtime = leavetime,
            us = NA ,dur = NA,    Part = NA, 
            Date = as.POSIXct(jointime, format = "%m/%d/%Y %H"),
            Year = year(Date), Month = month(Date), dayy = day(Date),
           hour = format(Date, format= "%H"),
           source = file) %>% 
    select( c("MeetingID", "Topic","starttime","endtime","us","dur","Part",
              "Date","Year","Month","dayy","hour","source") )
  colnames(mt_info) <- c("MeetingID", "Topic", "Start_Time", "End_Time", "User_Email", "Duration",
                         "Participants","Date","Year", "Month","Day","Hour","source")
  mt_info[1,] 
}


# ================ Meeting Information Extraction (Missing file ONLY)  ================
success_2nd = 0
#mt_extract_missing <- data.frame()
total_in_error = nrow(er_total)

for (file in er_total$error_file_list ) {
  tryCatch( 
    expr = {
     # extract meeting information
     #mt_extract_missing <- mt_info_missing(file)
     mt_total = rbind(mt_total, mt_info_missing(file))
     message("Successfully processed ", file, " ", "\n") 
     success_2nd = success_2nd + 1
     },
    error = function(e){
      cat('# Error! : ', file , "\n") 
    },
    finally = { message('File has processed', "\n")  }
  )
}
paste("---All process completed!---", "\n", "Success :", success_2nd, "Error :", total_in_error - success_2nd)

mt_total

# ======================= summary report =======================
print(paste(
  "In first process, ", total, "files are processed; ",
  "Success :", success, "Error :", total - success ,
  
  "In second process, ", total_in_error, "files are processed; ",
  "Success :", success_2nd, "Error :", total_in_error - success_2nd
))
print(paste(
  "In total, ",
   total-(total_in_error - success_2nd), "files were extracted successfully, ",
   total_in_error - success_2nd, "files have error" 
))

# save meeting information
write.csv(mt_total,"mt_info_total.csv", row.names = FALSE)



############################################################

#               2. USER INFORMATION
   
############################################################

# User information extraction

user_info_extract <- function(file){
  # warning sign for data size
  if ( isTRUE(!file.size( paste0("./zoom_attendance/", file ) ) < 80)==T ) {  # smaller than 100 byte
    df <- read.csv( paste0("./zoom_attendance/", file) ) 
    # warning for 0 rows
      if ( dim(df)[1] == 0 ) {  
      print("WARNING: File has 0 row")
      }
  } else { 
  print("WARNING: File size is smaller than 80 byte; File is NULL or damaged")  
  }
  
  # format filtering
  if( "Meeting.ID" %!in% colnames(df) | "Topic" %!in% colnames(df) ){
    print("File format is incorrect. Stop")
    break 
    }
  
  if("X" %in% colnames(df)) {  
  mt_info <- df[1, ] %>% select(-X) 
  } 
  else{  mt_info <- df[1, ]
  }  
  
  df_new <- df[, 1:6]
  colnames(df_new) <- paste( gsub(" ", "_", df_new[2,],) )
  df_new <- df_new[3:dim(df)[1], ]
  
  # here two different version of processing
  `%!in%` <- Negate(`%in%`)
  
  if ("Join_Time" %!in% colnames(df_new) | "Join.Time" %!in% colnames(df_new) ){
  print("WARNING: No Join/Leave time records; Processing with form2")
  colnames(df_new ) <- c("name1","email1","dur1","guest1")
  
  df_new <- df_new[, 1:4] %>% 
    mutate( name = name1, email = email1,
            jointime = NA,  leavetime = NA, dur = dur1,  guest = guest1,
            Date = NA, Year = NA, Month = NA, day = NA,
            MeetingID = paste(mt_info[1]) , Topic = paste(mt_info[2]) ,
            source = file) %>%
    select(c("name","email","jointime","leavetime","dur","guest","Date",
             "Year","Month","day","MeetingID","Topic","source"))
    colnames(df_new) <- c("Name_(Original_Name)","User_Email","Join_Time","Leave_Time", "Duration_(Minutes)","Guest",
                      "Date","Year","Month","date","MeetingID" ,"Topic", "Source")
  } else {   
      df_new <- df_new %>% 
        mutate( Date = as.POSIXct(Join_Time, format = "%m/%d/%Y"),
                              Year = year(Date),
                              Month = month(Date),
                              date = day(Date),
                              MeetingID = paste(mt_info[1]) ,
                              Topic = paste(mt_info[2]) ,
                              source = file)  }
   colnames(df_new) <- c("Name_(Original_Name)", "User_Email", "Join_Time","Leave_Time","Duration_(Minutes)","Guest",
                      "Date","Year","Month","date","MeetingID" ,"Topic", "Source") 
   df_new
}

# ================ User Information Extraction  ================
count = 0
success = 0
user_total <- data.frame()
total = length(list.files(path="./zoom_attendance" ) )

for (file in list.files(path="./zoom_attendance")) {
  tryCatch( 
    expr = {
     user_extract <- user_info_extract(file)
     user_total = rbind(user_total, user_extract)
     message("Successfully processed ", file, " ", "\n") 
     success = success + 1  
     },
    error = function(e){  
    cat('# Error! : ', file , "\n")  
    },
    finally = { message('All done', "\n")  
    }
  ) 
  }

paste("---All process completed!---", "\n", "Success :", success, "Error :", total - success)

# ================ ERROR file management  ================
er_total_user <- data.frame()

# creating error file list
`%!in%` <- Negate(`%in%`)
for (file in list.files(path="./zoom_attendance")) {
  if(file %!in% user_total$Source){
    er_total_user = rbind(er_total_user, file)
    colnames(er_total_user)<-c("error_file_list")
   } 
  }

# meeting ID extractor function
mtid_extractor <- function(file){
  end_file_num <- length(str_extract_all(file,"\\(?[0-9,.]+\\)?"))
  for( j in 1: end_file_num) {
    word_list <- gsub("[[:punct:]]", "", str_extract_all(file, "\\(?[0-9,.]+\\)?")[[j]] )
    for (w in word_list) {
      if( nchar(w)==11){
        return ( as.numeric(w) ) 
      }  
     } 
   }
}

er_total_user

user_info_missing <- function( file ) {
  # warning sign for data size
  if ( isTRUE(!file.size( paste0("./zoom_attendance/", file ) ) < 80)==T ) {  # smaller than 100 byte
    df <- read.csv( paste0("./zoom_attendance/", file) ) 
    # warning for 0 rows
      if ( dim(df)[1] == 0 ) {  
      print("WARNING: File has 0 row") 
      }
  } else { 
  print("WARNING: File size is smaller than 80 byte; File is NULL or damaged")  
  }
  
  # warning sign for column size
  if (dim(df)[2] == 4 | dim(df)[2] == 5 ){
    #print("looks good")
  } else { 
  print("WARNING: error in dimension; dimensions are not 4 nor 5.")
  }
  
  if( isTRUE( nchar(mtid_extractor(file) )==11 ) == F  ){
    print("WARNING: No Meeting ID information") 
   }
  
  # here two different version of processing
  `%!in%` <- Negate(`%in%`)
  df_new <- df
  colnames(df_new) <- paste( gsub(" ", "_", colnames(df) ) )
  #print(colnames(df_new))

  if ("Join.Time" %!in% colnames(df_new) & "Guest" %in% colnames(df_new) & dim(df_new)[2]==4 ) {
  #print("WARNING: No Join/Leave time records; Processing with form2")
    
  colnames(df_new ) <- c("name1","email1","dur1","guest1")
  df_new <- df_new %>% 
    mutate( name = name1, 
            email = email1,
            jointime = NA,  leavetime = NA, dur = dur1,  guest = guest1,
            Date = NA, Year = NA, Month = NA, day = NA,
            MeetingID = ifelse( isTRUE(nchar(mtid_extractor(file))==11)==T , mtid_extractor(file), NA), 
            Topic = NA ,
            source = file) %>%
    select(c("name","email","jointime","leavetime","dur","guest","Date",
             "Year","Month","day","MeetingID","Topic","source"))
    colnames(df_new) <- c("Name_(Original_Name)","User_Email","Join_Time","Leave_Time", "Duration_(Minutes)", "Guest",
                      "Date","Year","Month","date","MeetingID" ,"Topic", "Source")

  } else if ("Join.Time" %in% colnames(df_new) & "Guest" %!in% colnames(df_new) & dim(df_new)[2]==5) {
  #print("WARNING: No Guest records; Processing with form3")
    df_new <- df_new %>% 
        mutate( Guest = NA, Date = as.POSIXct(Join.Time, format = "%m/%d/%Y"),
                              Year = year(Date),
                              Month = month(Date),
                              date = day(Date),
                              MeetingID = ifelse( isTRUE(nchar(mtid_extractor(file))==11)==T  , mtid_extractor(file), NA), 
                              Topic = NA ,
                              source = file)
    colnames(df_new) <- c("Name_(Original_Name)", "User_Email", "Join_Time","Leave_Time","Duration_(Minutes)","Guest",
                      "Date","Year","Month","date","MeetingID" ,"Topic", "Source") 

  }  else {   df_new <- df_new %>% 
        mutate( Date = as.POSIXct(Join.Time, format = "%m/%d/%Y"),
                              Year = year(Date), Month = month(Date), date = day(Date),
                              MeetingID = ifelse( isTRUE(nchar(mtid_extractor(file))==11)==T, mtid_extractor(file), NA), 
                              Topic = NA,
                              source = file)  
  }
   colnames(df_new) <- c("Name_(Original_Name)", "User_Email", "Join_Time","Leave_Time","Duration_(Minutes)","Guest",
                      "Date","Year","Month","date","MeetingID" ,"Topic", "Source") 
   df_new
}

# ================ User information extraction from ERROR FILES only  ================
success_2nd = 0
#user_miss_total <- data.frame()
total_in_error <-  nrow( er_total_user )

for (file in er_total_user$error_file_list ) {
  tryCatch(  expr = {
     # extract meeting information
     #user_extract_missing <- user_info_missing(file)
     user_total  = rbind(user_total , #user_miss_total , 
                              user_info_missing(file))
     message("Successfully processed ", file, " ", "\n") 
     success_2nd = success_2nd + 1
     },
    error = function(e){  
    cat('# Error! : ', file , "\n") 
    },
    finally = { 
    message('File has processed', "\n") 
    }
  )
}

# ======================= summary report =======================

print(paste(
  "In first process, ", total, "files are processed; ",
  "Success :", success, "Error :", total - success ,
  "In second process, ", total_in_error, "files are processed; ",
  "Success :", success_2nd, "Error :", total_in_error - success_2nd
))
print(paste(
  "In total, ",
   total-(total_in_error - success_2nd), "files were extracted successfully, ",
   total_in_error - success_2nd, "files has error" 
))

# save meeting information
write.csv(user_total,"user_info_total.csv", row.names = FALSE)
