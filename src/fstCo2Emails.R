# MASTER <- readRDS("C:/Users/kstyers/Dropbox/SwiftData/co2MASTERFull.rds")
# fst::write.fst(x = MASTER,path = "C:/GitHub/fieldscience_collab/TIS/IS3Rmarkdown/1_fstPursley/data/co2.fst")
# fst::read.fst(path = "C:/GitHub/fieldscience_collab/TIS/IS3Rmarkdown/1_fstPursley/data/co2.fst")

## Testing with Data.table/readr to read/bind and write
TotalTime <- Sys.time()

# List all new files in the Co2 tidy folder
fileList <- base::list.files(path = "N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyCO2-H2O/",
                             pattern="*.csv",full.names = TRUE)

# Import Data -- Assign 'files' to the product of reading though all of the csv's in 'file_names'
files = base::lapply(fileList, data.table::fread, header=T, stringsAsFactors = F)
files = data.table::rbindlist(files, fill = TRUE)

# Move to Archive
filesstrings::file.move(fileList, "N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyCO2-H2O/z_archive", overwrite = TRUE)

# If statement to avoid overwriting files
if(length(fileList) == 0){
  print("NO FILES IN FOLDER, CHECK WITH MIKE'S EMAILS")
} else {
  names(files) = c("siteID", "dateID","sensorID","partNumber", "streamID","maxRange","minRange", "units","readings","eeprom")
  
  # Remove any blanks
  files <- files %>%
    tidytable::dt_filter(readings != "") %>%
    dplyr::distinct(siteID,dateID,sensorID,partNumber,streamID,readings,eeprom,.keep_all = TRUE)
  
  files$dateID <- lubridate::ymd(files$dateID)
  files$sensorID <- as.character(files$sensorID) 
  files$sensorID[files$sensorID == "EX2DWind:ML:1"]   <- "2DWind:ML:1"
  files$sensorID[files$sensorID == "EX2DWind:ML:2"]   <- "2DWind:ML:2"
  files$sensorID[files$sensorID == "EX2DWind:ML:3"]   <- "2DWind:ML:3"
  files$sensorID[files$sensorID == "EX2DWind:ML:4"]   <- "2DWind:ML:4"
  files$sensorID[files$sensorID == "EX2DWind:ML:5"]   <- "2DWind:ML:5"
  files$sensorID[files$sensorID == "EX2DWind:ML:6"]   <- "2DWind:ML:6"
  files$sensorID[files$sensorID == "EX2DWind:ML:7"]   <- "2DWind:ML:7"
  files$sensorID[files$sensorID == "EX2DWind:ML:8"]   <- "2DWind:ML:8"
  files$siteID[files$siteID == "CLBJ_2"]   <- "CLBJ"
  files$siteID[files$siteID == "WREF_2"]   <- "WREF"
  files$siteID[files$siteID == "PUUM_2"]   <- "PUUM"
  files$siteID[files$siteID == "TALL_2"]   <- "TALL"
  
  # Load
  fstMaster <- fst::read.fst(path = paste0(repoDir, "data/co2.fst")) %>%
    tidytable::dt_filter(readings != "") %>%
    dplyr::distinct(siteID,dateID,sensorID,partNumber,streamID,readings,eeprom,.keep_all = TRUE) # Remove duplicates
  
  # Bind
  # New data rows
  fstRowIngest <- nrow(files)
  message("New Co2 File Row = ", fstRowIngest)
  # Old data rows
  fstRowMaster <- nrow(fstMaster)
  message("Master Co2 File Row = ", fstRowMaster)
  
  co2List <- list(fstMaster,files)
  newFstMaster <- data.table::rbindlist(co2List)
  
  # New Joined size
  fstRowNewMaster <- nrow(newFstMaster)
  message("New Master Co2 File Row = ", fstRowNewMaster)
  # newFstMaster <- tidytable::dt_distinct(newFstMaster)
  fst::write.fst(x = newFstMaster, compress = 100,
                 path = paste0(repoDir, "data/co2.fst"))
  
  co2NoNan <- newFstMaster %>%
    tidytable::dt_filter(readings != "NaN" & readings != "" & readings != "1970-01-01 00:00:00")
  fst::write.fst(x = co2NoNan, compress = 100,
                 path = paste0(repoDir, "data/co2NoNan.fst"))
  
  
  # Gas measurements and diagnostics with no NaNs limited to last 180 days.
  co2SensorOnlyNoNan180 <- co2NoNan %>%
    tidytable::dt_filter(!sensorID %in% c("G2131-I H2O","LI840 H2O","G2131-I CO2","LI840 CO2")) %>%
    tidytable::dt_filter(dateID > Sys.Date() - 180)
  
  fst::write.fst(x = co2SensorOnlyNoNan180, path = paste0(repoDir, "data/co2NoNanSGasAnalyzerOnly.fst"), compress = 100)
  
  co2CVAL <- newFstMaster %>%
    tidytable::dt_filter(sensorID %in% c("LI7200 CO2","LI7200 EX CO2","LI7200 H2O","LI7200 EX H2O","G2131iLastValidationTime", "Li7200LastValidationTime", "Li840LastCalibrationTime", "Li840LastValidationTime",
                           "LI7200 DIAG","LI7200 EX DIAG","AMRS Pitch","AMRS Roll","LI7200 MFC Flow","LI7200 DPress","LI7200 DIAG","LI7200 EX DIAG",
                           "LI840 MFC Flow","LI840 MFC Mass Flow Rate","LI840 Cell Pressure","G2131-I Stat","L2130-I Stat")
    )
  
  # saveRDS(co2CVAL, "C:/Users/kstyers/Dropbox/SwiftData/co2CVAL.rds")
  fst::write.fst(x = co2CVAL, compress = 100,
                 path = paste0(repoDir, "data/co2Cval.fst"))
  
}


# aaaand TIME!
TotalTime <- (Sys.time()-TotalTime)
print(TotalTime)

