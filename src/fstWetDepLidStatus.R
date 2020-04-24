# fstMaster <- readRDS("C:/Users/kstyers/Dropbox/SwiftData/lidData.rds")
# fst::write.fst(x = fstMaster,path = "C:/GitHub/fieldscience_collab/TIS/IS3Rmarkdown/1_fstPursley/data/wetDepLidStatus.fst")

# Grab file names from the set directory
fileList <- base::list.files(path = "N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zWetDepLid/",
                               pattern="*.csv",full.names = TRUE)

# Import Data -- Assign 'files' to the product of reading though all of the csv's in 'fileList'
files = base::lapply(fileList, fread, header=F, stringsAsFactors = F)
# Import Data -- Bind all of the csv object together
fstIngest = data.table::rbindlist(files, fill = T)
names(fstIngest) = c("SiteID", "Date","EEPROM","Value")

fstIngest <- fstIngest %>% 
  tidytable::dt_filter(EEPROM != "NA" | Value != "") 

fstIngest$Value[is.na(fstIngest$Value)] <- 0

paste0("Sites with tips in new WetDepLid files: ")
print(unique(fstIngest$SiteID))
paste0((round(length(unique(fstIngest$Value))/37, 2)*100),"% Experienced Tips")

file.move(fileList, "N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zWetDepLid/z_archive", overwrite = TRUE)

if(length(fileList) == 0){
  print("NO FILES IN FOLDER, CHECK WITH MIKE'S EMAILS")
} else {

  fstIngest$Date <- lubridate::ymd(fstIngest$Date)
  fstIngest$Week <- lubridate::week(fstIngest$Date)
  # NewfstMaster <- fstIngest
  
  fstIngest <- fstIngest %>%
    dplyr::distinct(SiteID,Date,EEPROM,Value, Week)
  
  fstMaster <- fst::read.fst(paste0(repoDir, "data/wetDepLidStatus.fst")) %>%
    dplyr::distinct(SiteID,Date,EEPROM,Value, Week)
  
  # Row Counting
  # New data rows
  fstRowIngest <- nrow(fstIngest)
  message("New WetDep File Row = ", fstRowIngest)
  # Old data rows
  fstRowMaster <- nrow(fstMaster)
  message("Master WetDep File Row = ", fstRowMaster)
  
  MasterDataList <- list(fstMaster,fstIngest)
  NewfstMaster <- data.table::rbindlist(MasterDataList)%>%
    dplyr::distinct(SiteID,Date,EEPROM,Value, Week)
  # New Joined size
  fstRowNewMaster <- nrow(NewfstMaster)
  message("New Master WetDep File Row = ", fstRowNewMaster)
  
  fst::write.fst(x = NewfstMaster,compress = 100,
                 path = paste0(repoDir, "data/wetDepLidStatus.fst"))
}
