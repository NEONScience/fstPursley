# fstMaster <- fst::read.fst(path = "C:/GitHub/fieldscience_collab/TIS/IS3Rmarkdown/1_fstPursley/data/spanGasConc.fst")
# fstMaster$assetTag <- as.character(fstMaster$assetTag)


TotalTime <- Sys.time()
# Update Cylinder Assay Tables

# Grab archive CSVs
setwd("N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyCyl/")
fileList <- list.files(path = "N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyCyl/",
                       pattern="*.csv")

# Import Data -- Assign 'files' to the product of reading though all of the csv's in 'file_names'
files = lapply(fileList, read.csv, header=T, stringsAsFactors = F)
# Import Data -- Bind all of the csv object together
files = rbindlist(files)

# Move to Archive
file.move(fileList, "N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyCyl/z_archive", overwrite = TRUE)

# Format data
# If statement to avoid overwriting files

if(length(fileList) == 0){
  print("NO FILES IN FOLDER, CHECK WITH MIKE'S EMAILS")
} else {
  options(scipen = 999)
  files$name <- as.factor(files$name)
  files$siteID <- as.factor(files$siteID)
  files$date <- as.Date(files$date, format = "%Y-%m-%d")
  files$assetTag <- as.character(files$assetTag)
  # Remove any blank records.
  files <- files %>%
    tidytable::dt_filter(conc != "") %>%
    dplyr::distinct(date,siteID,name,conc,assetTag)
  fstRowIngest <- nrow(files)
  message("New Span Gas File Row = ", fstRowIngest)
  # Load Master, Rbind, Save
  fstMaster <- fst::read.fst(path = paste0(repoDir, "data/spanGasConc.fst"))

  fstRowMaster <- nrow(fstMaster)
  message("Master Span Gas File Row = ", fstRowMaster)
  
  # Make List and RBind
  fstList <- list(fstMaster, files)
  newFstMaster <- rbindlist(fstList)
  newFstMaster <- newFstMaster %>%
    tidytable::dt_filter(conc != "") %>%
    dplyr::distinct(date,siteID,name,conc,assetTag)
  
  fstRowNewMaster <- nrow(newFstMaster)
  message("New Master Span Gas File Row = ", fstRowNewMaster)
    
  fst::write.fst(newFstMaster, paste0(repoDir, "data/spanGasConc.fst"))
}


# Take the time
TotalTime <- (Sys.time()-TotalTime)
print(TotalTime)