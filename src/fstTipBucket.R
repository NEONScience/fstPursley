# MasterTipper <- readRDS("C:/Users/kstyers/Dropbox/SwiftData/tipData.rds")
# fst::write.fst(MasterTipper, compress = 100,
#                path = "C:/GitHub/fieldscience_collab/TIS/IS3Rmarkdown/1_fstPursley/data/tipBucket.fst")


# Grab file names from the set directory
fileList <- base::list.files(path = "N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyTips/",
                         pattern="*.csv",full.names = TRUE)

# Import Data -- Assign 'files' to the product of reading though all of the csv's in 'fileList'
files = base::lapply(fileList, fread, header=F, stringsAsFactors = F)
# Import Data -- Bind all of the csv object together
fstIngest = data.table::rbindlist(files, fill = T)

paste0("Sites in new TipBucket files: ")
print(unique(fstIngest$SiteID))
print(length(unique(fstIngest$SiteID)))

filesstrings::file.move(fileList, "N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyTips/z_archive", overwrite = TRUE)

if(length(fileList) == 0){
  print("NO FILES IN FOLDER, CHECK WITH MIKE'S EMAILS")
} else {

  names(fstIngest) = c("SiteID", "Date","StreamName","Value")
  fstIngest$Date <- lubridate::ymd(fstIngest$Date)
  fstIngest$Week <- lubridate::week(fstIngest$Date)
  fstIngest <- fstIngest %>%
    dplyr::distinct(SiteID,Date,StreamName,Value,Week)
  
  # Load in Master
  MasterTipper <- fst::read.fst(paste0(repoDir, "data/tipBucket.fst"))
  
  # New data rows
  fstRowIngest <- nrow(fstIngest)
  message("New Span Gas File Row = ", fstRowIngest)
  # Old data rows
  fstRowMaster <- nrow(MasterTipper)
  message("Master Span Gas File Row = ", fstRowMaster)
  
  
  MasterDataList <- list(MasterTipper,fstIngest)
  newFstMaster <- data.table::rbindlist(MasterDataList)%>%
    dplyr::distinct(SiteID,Date,StreamName,Value,Week)
  
  # New Joined size
  fstRowNewMaster <- nrow(newFstMaster)
  message("New Master Span Gas File Row = ", fstRowNewMaster)
  
  newFstMaster$StreamName[newFstMaster$StreamName == "SP1TBID"]   <- "SP1"
  newFstMaster$StreamName[newFstMaster$StreamName == "SP2TBID"]   <- "SP2"
  newFstMaster$StreamName[newFstMaster$StreamName == "SP3TBID"]   <- "SP3"
  newFstMaster$StreamName[newFstMaster$StreamName == "SP4TBID"]   <- "SP4"
  newFstMaster$StreamName[newFstMaster$StreamName == "SP5TBID"]   <- "SP5"
  newFstMaster$StreamName[newFstMaster$StreamName == "MLTSecPrecipID"]   <- "SecondaryPrecip"
  
  
  fst::write.fst(newFstMaster, compress = 100,
                 path = paste0(repoDir, "data/tipBucket.fst"))               
}