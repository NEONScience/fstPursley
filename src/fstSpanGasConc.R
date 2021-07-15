# fstMaster <- fst::read.fst(path = "C:/GitHub/fieldscience_collab/TIS/IS3Rmarkdown/1_fstPursley/data/spanGasConc.fst")
# fstMaster$assetTag <- as.character(fstMaster$assetTag)
library(aws.s3)

secret_key = readRDS("C:/GitHub/fstPursley/secret_key.RDS")
Sys.setenv("AWS_ACCESS_KEY_ID"     = "research-eddy-inquiry",
           "AWS_SECRET_ACCESS_KEY" = secret_key,
           "AWS_S3_ENDPOINT"       = "neonscience.org",
           "AWS_DEFAULT_REGION"    = "s3.data")
TotalTime <- Sys.time()
# Update Cylinder Assay Tables

# Grab archive CSVs
setwd("N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyCyl/")
fileList <- list.files(path = "N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyCyl/",
                       pattern="*.csv")

# Import Data -- Assign 'files' to the product of reading though all of the csv's in 'file_names'
files = lapply(fileList, data.table::fread)
# Import Data -- Bind all of the csv object together
files.in = data.table::rbindlist(files, fill = TRUE)

# Move to Archive
file.move(fileList, "N:/Common/CVL/Field_Calibration/Field_Calibration_Data/TIS/tidy_data/zDailyCyl/z_archive", overwrite = TRUE)

# Format data
# If statement to avoid overwriting files

if(length(fileList) == 0){
  print("NO FILES IN FOLDER, CHECK WITH MIKE'S EMAILS")
} else {
  options(scipen = 999)
  
  names(files.in) = c("date", "siteID", "name", "CertificateNumber", "conc", "concDELTA", "concCH4", "assetTag", "aTag")
  
  files.in$name <- as.factor(files.in$name)
  files.in$siteID <- as.factor(files.in$siteID)
  files.in$date <- as.Date(files.in$date, format = "%Y-%m-%d")
  files.in$assetTag <- as.character(files.in$assetTag)
  # Remove any blank records.
  files.distinct <- files.in %>%
    dplyr::filter(conc != "") %>%
    dplyr::distinct(date,siteID,name,conc,assetTag, .keep_all = TRUE) 
  
  fstRowIngest <- nrow(files.distinct)
  message("New Span Gas File Row = ", fstRowIngest)
  # Load Master, Rbind, Save
  fstMaster <- fst::read.fst(path = paste0(repoDir, "data/spanGasConc.fst"))

  fstRowMaster <- nrow(fstMaster)
  message("Master Span Gas File Row = ", fstRowMaster)
  
  # bind all data together
  newFstMaster <- data.table::rbindlist(l = list(fstMaster, files.distinct), fill = TRUE)
  newFstMaster <- newFstMaster %>%
    dplyr::filter(conc != "") %>%
    dplyr::distinct(date,siteID,name,conc,assetTag, .keep_all = TRUE)
  
  fstRowNewMaster <- nrow(newFstMaster)
  message("New Master Span Gas File Row = ", fstRowNewMaster)
    
  fst::write.fst(newFstMaster, paste0(repoDir, "data/spanGasConc.fst"))
  
  aws.s3::put_object(
    file = paste0(repoDir, "data/spanGasConc.fst"),
    object = "lookup/spanGasConc.fst",
    bucket = "research-eddy-inquiry"
  )
}


# Take the time
TotalTime <- (Sys.time()-TotalTime)
print(TotalTime)