# Convert all archive files to fst
library(tidyverse)
library(tidytable)
library(lubridate)
library(fst) # v0.9.2

# Daily Ingest Direc 
file_names <- list.files(path = "C:/GitHub/fstPursley/data/cval/csvH2OVal/", pattern = "IsoWater",
                         full.names = FALSE)
i <- file_names[4]

if(length(file_names) > 0){
  
  # Read in CSV's and convert them to fst
  for(i in file_names){
    fstFileName <- substr(i,1,nchar(i)-3)
    ingest <- data.table::fread(paste0("C:/GitHub/fstPursley/data/cval/archive/",i))
    
    if(nrow(ingest) > 0){
    # Rename columns
    if(length(names(ingest)) == 40){
    names(ingest) = c("siteID", "line","Analysis","TimeStamp","Port","Injection",
                           "Mean_18_16_Isotope","Mean_D_H_Isotope","Mean_H2O","Ignore","Good","rm4","rm5","rm6","ID1","ID2","Gas", "rm1",
                           "SD_18_16_Isotope", "SD_D_H_Isotope","SD_H2O",
                           "SI_18_16_Isotope","SI_D_H_Isotope","SI_H2O",
                           "baseline_shift","slope_shift","residuals",
                           "baseline_curvature","interval","ch4_ppm", "rm2","rm3",
                           "n2_flag",
                           "DAS_Temp",
                           "Tray","Sample","Job","Method","ErrorCode",
                           "Pulse_Good")
    ingest <- ingest %>%
      dplyr::select(-dplyr::starts_with("rm"))
    
    } else if(length(names(ingest)) == 36){
      names(ingest) = c("siteID", "line","Analysis","TimeStamp","Port","Injection",
                        "Mean_18_16_Isotope","Mean_D_H_Isotope","Mean_H2O","Ignore","Good","ID1","ID2","Gas", "rm1",
                        "SD_18_16_Isotope", "SD_D_H_Isotope","SD_H2O",
                        "SI_18_16_Isotope","SI_D_H_Isotope","SI_H2O",
                        "baseline_shift","slope_shift","residuals",
                        "baseline_curvature","interval","ch4_ppm", "rm2","rm3",
                        "n2_flag",
                        "DAS_Temp",
                        "Tray","Sample","Job","Method","ErrorCode")
      ingest <- ingest %>%
        dplyr::select(-dplyr::starts_with("rm"))
    }
    
    # Format Dates
    ingest$TimeStamp <- lubridate::ymd_hms(ingest$TimeStamp)
    ingest$date <- base::as.Date(ingest$TimeStamp, format="%Y-%m-%d")
    
    message(paste0("This Sites Name Is ", unique(ingest$siteID)))
    message(paste0("The newest time is ", max(ingest$date, na.rm = TRUE)))
    
    base::message(paste0("Writing ", fstFileName, "fst"))
    fst::write.fst(ingest, paste0("C:/GitHub/fstPursley/data/cval/fstCvals/",unique(ingest$siteID),"_IsoData_",max(ingest$date, na.rm = TRUE),".fst"))
    } else {
      message(paste0("NO ROWS IN THE DATA"))
    }
  }
  
  # Move all files after Fasting them
  file_move <- list.files(path = "C:/GitHub/fstPursley/data/cval/csvCvals/", pattern = "Time",
                          full.names = TRUE)
  filesstrings::file.move(files = file_move, destinations = "C:/GitHub/fstPursley/data/cval/csvH2OVal/archive/",
                          overwrite = TRUE)
} else {
  message("Length of files in C:/GitHub/fstPursley/data/cval/csvCvals/ is = 0")
}
