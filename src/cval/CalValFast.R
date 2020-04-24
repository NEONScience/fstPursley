# Function to read from the CalVal Archive and write out fast files
# Dependencies
library(fst)
library(data.table)
library(tidytable)
library(dplyr)

write_fast_cval <- function(sensorPattern){
  # Grab just G2131 Files
  file_names <- list.files(path = "C:/1_Data/CalibrationData/fst/",
                           pattern = paste0(sensorPattern))
  # How many number
  message(length(file_names))
  # Read in all the files into a list
  files = lapply(paste0("C:/1_Data/CalibrationData/fst/",file_names), fst::read.fst)
  # Bind all the files together
  calFiles = rbindlist(files)
  # Name the columns
  names(calFiles) = c("siteID", "calID","TimeStamp","CO2","H2O","Temp","Pressure")
  
  # Arrange the data for indexing in preparation for saving it
  calFilesArr <- calFiles %>%
    tidytable::dt_arrange(dplyr::desc(TimeStamp))%>%
    tidytable::dt_distinct() %>%
    tidytable::dt_mutate(date = as.Date(TimeStamp, format = "%Y-%m-%d"))%>%
    tidytable::dt_filter(is.na(TimeStamp) == FALSE) %>%
    tidyr::unite("siteCal", c("siteID","calID","date"), remove = FALSE)
  write.fst(calFilesArr, paste0("C:/Users/kstyers/Dropbox/SwiftData/fst/",sensorPattern,".fst"))
}
sensorPattern <- "Li7200LastValidationTime"
ecseAnalyzers <- c("Li7200LastValidationTime","G2131iLastValidationTime","Li840LastCalibrationTime","Li840LastValidationTime")
for(i in ecseAnalyzers){
  start <- Sys.time()
  message(paste0("Starting ",i))
  write_fast_cval(sensorPattern = i)
  end <- Sys.time()
  message(paste0("Finished in ", end-start))
}
smallFileNames <- list.files(path = "C:/Users/kstyers/Dropbox/SwiftData/fst/")
i <- smallFileNames[1]
for(i in smallFileNames){
  fstFileName <- substr(i,1,nchar(i)-3)
  ingest <- fst::read.fst(paste0("C:/Users/kstyers/Dropbox/SwiftData/fst/",i),from = 1, to = 3000000, as.data.table = TRUE)
  write.fst(ingest, paste0("C:/Users/kstyers/Dropbox/SwiftData/fst/small",fstFileName,"fst"),compress = 100)
}
read.fst()