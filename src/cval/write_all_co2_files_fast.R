# convert CO2 to fst
# read in co2 data

TotalTime <- Sys.time()
setwd("N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyCO2-H2O/z_archive/")

fileList<- list.files("N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyCO2-H2O/z_archive/",
                      pattern="*.csv")

i <- fileList[1]

for(i in fileList){
  fstFileName <- substr(i,1,nchar(i)-3)
  ingest <- data.table::fread(paste0("N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyCO2-H2O/z_archive/",i))
  ingest$dateID <- lubridate::ymd(ingest$dateID)
  write.fst(ingest, paste0("C:/1_Data/Co2Emails/",fstFileName,"fst"))
}


write_fast_co2 <- function(){
  # Grab just G2131 Files
  file_names <- list.files(path = "C:/1_Data/Co2Emails/",
                           pattern = "*.fst")
  # How many number
  message(length(file_names))
  # Read in all the files into a list
  files = lapply(paste0("C:/1_Data/Co2Emails/",file_names), fst::read.fst)
  # Bind all the files together
  co2Files = rbindlist(files)
  
  # Arrange the data for indexing in preparation for saving it
  co2FilesArr <- co2Files %>%
    tidytable::dt_distinct() 
  write.fst(co2FilesArr, paste0("C:/Users/kstyers/Dropbox/SwiftData/fst/co2Fast.fst"))
}
write_fast_co2()
