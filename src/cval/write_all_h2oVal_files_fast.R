# convert CO2 to fst
# read in co2 data

TotalTime <- Sys.time()
setwd("N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyH2OPicarro/z_archive/")

fileList<- list.files("N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyH2OPicarro/z_archive/",
                      pattern="*.csv")

i <- fileList[1]

for(i in fileList){
  fstFileName <- substr(i,1,nchar(i)-3)
  ingest <- data.table::fread(paste0("N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyH2OPicarro/z_archive/",i))
  ingest$`Time Code` <- lubridate::ymd_hms(ingest$`Time Code`)
  if(nrow(ingest)>1){
  write.fst(ingest, paste0("C:/1_Data/CalibrationData/fst/H2OValidations/",fstFileName,"fst"))
  } else {
    message(paste0(i," has no data..."))
  }
}


write_fast_h2oCal <- function(){
  # Grab just G2131 Files
  file_names <- list.files(path = "C:/1_Data/CalibrationData/fst/H2OValidations/",
                           pattern = "*.fst")
  # How many number
  message(length(file_names))
  # Read in all the files into a list
  files = lapply(paste0("C:/1_Data/CalibrationData/fst/H2OValidations/",file_names), fst::read.fst)
  # Bind all the files together
  h2oValFiles = rbindlist(files, fill = TRUE)
  
  # Arrange the data for indexing in preparation for saving it
  h2oValFilesArr <- h2oValFiles %>%
    tidytable::dt_distinct() 
  write.fst(h2oValFilesArr, paste0("C:/Users/kstyers/Dropbox/SwiftData/fst/h2oValFast.fst"))
}
write_fast_h2oCal()
