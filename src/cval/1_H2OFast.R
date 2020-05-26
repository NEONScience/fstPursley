### Function to Site/Sensorify all H2O Validation Files
# Library dependecies
library(dplyr)
library(fst)
library(data.table)
library(lubridate)
library(ggplot2)
### Create master files for all sites
# Will have to treat L2130-I sites special ..... and here we are!

siteList <- list("HARV","SCBI","OSBS",
                           "GUAN","UNDE","KONZ","ORNL","TALL",
                           "WOOD","CPER","NIWO","CLBJ","YELL",
                           "ONAQ","SRER","WREF","SJER","TOOL",
                           "BARR","BONA","PUUM")

CvalFstReadDir <- paste0(repoDir,"data/cval/fstCvals/")
CvalFstWriteDir <- paste0(repoDir,"data/cval/fstCvalSite/")
CvalFstReadDirArchive <- paste0(repoDir,"data/cval/fstCvals/archive")
# Dev
# siteID <- "LAJA"
# cvalType <- "G2131iLastValidationTime"

# $$$
#   $$$$
#   $$$$ Important. This code must only grab the new fst files and aggregate them to be joined by the master file. 
#   $$$$ Preserve all the files in an archive. 
#   $$$$ This process currently will take 1.5 hours... so it is imperative that we only process what we have to
#   $$$$ Join the data will be interesting, check the swift Pipe to see if there are any lessons we can learn.
# $$$
# DEV
siteID = "BARR"
cvalType = "IsoData"

# Function for reading in all CVAL Files
CvalFstMaker <- function(siteID, cvalType){
  
  # List all Cval files in the CSV Directory
  ListOfCvalFiles <- base::list.files(path = CvalFstReadDir, pattern = base::paste0(siteID,"_",cvalType), full.names = TRUE)
  message(paste0("Starting ", siteID," ",cvalType," with ",base::length(ListOfCvalFiles)," files." ))
  # base::message(base::paste0(base::length(ListOfCvalFiles)," ",cvalType))
  # Check for more than 1 file
  if(base::length(ListOfCvalFiles) > 0){
    CvalFiles <- base::lapply(base::paste0(ListOfCvalFiles), fst::read.fst) 
    CvalFile = data.table::rbindlist(CvalFiles, fill = TRUE) %>%
      dplyr::filter(is.na(TimeStamp) == FALSE) %>%
      dplyr::filter(is.na(Mean_18_16_Isotope) == FALSE)
    # glimpse(CvalFile)
    # Join to master file
    # Read in master file
    # CvalMaster <- fst::read.fst(path = base::paste0(CvalFstWriteDir,siteID,"_",cvalType,".fst")) %>%
    #   dplyr::mutate(Co2 = round(Co2,3)) %>%
    #   dplyr::mutate(Co2 = round(Co2,3)) %>%
    #   dplyr::mutate(Co2 = round(Co2,3)) %>%
    #   dplyr::mutate(Co2 = round(Co2,3))
    
    
    # Join master and daily file ############################
    # newCvalMaster <- data.table::rbindlist(l = list(CvalFile,CvalMaster)) %>%
    #   dplyr::distinct(SiteID,CvalType,TimeStamp,Co2,H2o,Temp,Pressure)
    # Join master and daily file ############################
    
    # message(paste0("New ", siteID," ",cvalType, " has ", nrow(CvalFile)))
    # message(paste0("Master ", siteID," ",cvalType, " has ", nrow(CvalMaster)))
    # message(paste0("New Master", siteID," ",cvalType, " has ", nrow(newCvalMaster)))
    # message(paste0("Difference between new and old ",nrow(newCvalMaster)-nrow(CvalMaster), " rows"))
    # if((nrow(newCvalMaster)-nrow(CvalMaster)) == nrow(CvalFile)){
    #   message("FULL JOIN WITH NO DUPES!")
    # }
    
    fst::write.fst(x = CvalFile,
                   path = base::paste0(CvalFstWriteDir,siteID,"_",cvalType,".fst"),
                   compress = 100
    )
    
    # Move daily fst files to archive
    filesstrings::file.move(files = ListOfCvalFiles, destinations = CvalFstReadDirArchive, overwrite = TRUE)
    ## Plot for testing/verification
    # ggplot2::ggplot(CvalFile %>% filter(TimeStamp > "2020-01-07"), aes(x = TimeStamp, y = Co2))+
    #   geom_point() +
    #   theme(legend.position = "none")  
    
    # endTime <- base::Sys.time()
    # base::message(base::paste0("Completed ", siteID, " in ", 
    #                            round(difftime(endTime, startTime,units = "secs"),2),
    #                            " Seconds"
    # )
    # )
  } else {
    message(paste0("No Files for ", siteID," ", cvalType))
  }
}

siteID <- "BARR"
siteList[1]
for(siteID in siteList[1:1]){
  
  startTime <- base::Sys.time()
  
  CvalFstMaker(siteID = siteID ,cvalType = "IsoData")

  endTime <- base::Sys.time()
  
  base::message(base::paste0("Completed ", siteID, " in ", 
                             round(difftime(endTime, startTime,units = "secs"),2),
                             " Seconds"))
}

# CvalFstOrginMaker(siteID = "JORN", cvalType = "Li7200LastValidationTime")
# list.files(path = "C:/1_Data/fstCVALPipeline/AllCvalFst/",recursive = T,all.files = T,full.names = T, pattern = "*")


# for(i in DeleteList){
# file.remove(i)
# }