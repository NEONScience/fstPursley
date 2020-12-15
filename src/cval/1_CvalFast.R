### Function to Site/Sensorify all CVAL Files
### CVAL refers to Calibration and Validations 
# Library dependecies
library(dplyr)
library(fst)
library(data.table)
library(lubridate)
library(ggplot2)
### Create master files for all sites
# Will have to treat L2130-I sites special

siteList <- list("BART","HARV","BLAN","SCBI","SERC",
                 "JERC","OSBS","DSNY","GUAN","LAJA",
                 "STEI","UNDE","TREE","KONA","KONZ",
                 "UKFS","ORNL","GRSM","MLBS","TALL",
                 "LENO","DELA","WOOD","NOGP","DCFS",
                 "CPER","STER","NIWO","RMNP","OAES",
                 "CLBJ","YELL","MOAB","ONAQ","JORN",
                 "SRER","WREF","ABBY","TEAK","SOAP",
                 "SJER","BONA","HEAL","DEJU","BARR",
                 "TOOL","PUUM")

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

# Function for reading in all CVAL Files
CvalFstMaker <- function(siteID, cvalType){

  # List all Cval files in the CSV Directory
  ListOfCvalFiles <- base::list.files(path = CvalFstReadDir, pattern = base::paste0(siteID,"_",cvalType), full.names = TRUE)
  message(paste0("Starting ", siteID," ",cvalType," with ",base::length(ListOfCvalFiles)," files." ))
  # base::message(base::paste0(base::length(ListOfCvalFiles)," ",cvalType))
  # Check for more than 1 file
  if(base::length(ListOfCvalFiles) > 0){
    CvalFiles <- base::lapply(base::paste0(ListOfCvalFiles), fst::read.fst) 
    CvalFile = data.table::rbindlist(CvalFiles) %>%
      dplyr::filter(is.na(TimeStamp) == FALSE) 
      
    if(nrow(CvalFile) > 10){
    CvalFile <- CvalFile %>%  
      dplyr::group_by(TimeStamp = cut(TimeStamp, breaks="15 secs")) %>%
      dplyr::summarize(
        siteID = siteID[1],
        calID = cvalType[1],
        CO2 = mean(CO2, na.rm = TRUE),
        H2O = mean(H2O, na.rm = TRUE),
        Temp = mean(Temp, na.rm = TRUE),
        Pressure = mean(Pressure, na.rm = TRUE)
      ) %>% 
      dplyr::select(siteID,calID,TimeStamp,CO2,H2O,Temp,Pressure)
    
    names(CvalFile) <- c("SiteID","CvalType","TimeStamp","Co2","H2o","Temp","Pressure")
    CvalFile$TimeStamp <- lubridate::ymd_hms(CvalFile$TimeStamp)
    
    # Join to master file
    # Read in master file
    CvalMaster <- fst::read.fst(path = base::paste0(CvalFstWriteDir,siteID,"_",cvalType,".fst")) %>%
      dplyr::mutate(Co2 = round(Co2,3)) %>%
      dplyr::mutate(Co2 = round(Co2,3)) %>%
      dplyr::mutate(Co2 = round(Co2,3)) %>%
      dplyr::mutate(Co2 = round(Co2,3))
      
    
    # Join master and daily file
    newCvalMaster <- data.table::rbindlist(l = list(CvalFile,CvalMaster)) %>%
      dplyr::distinct(SiteID,CvalType,TimeStamp,Co2,H2o,Temp,Pressure)
    
    message(paste0("New ", siteID," ",cvalType, " has ", nrow(CvalFile)))
    message(paste0("Master ", siteID," ",cvalType, " has ", nrow(CvalMaster)))
    message(paste0("New Master", siteID," ",cvalType, " has ", nrow(newCvalMaster)))
    message(paste0("Difference between new and old ",nrow(newCvalMaster)-nrow(CvalMaster), " rows"))
    if((nrow(newCvalMaster)-nrow(CvalMaster)) == nrow(CvalFile)){
      message("FULL JOIN WITH NO DUPES!")
    }

    fst::write.fst(x = newCvalMaster,
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
      message(paste0(siteID,"'s data has too many NA's"))
    }
  } else {
    message(paste0("No Files for ", siteID," ", cvalType))
  }
}

for(siteID in siteList[1:47]){
  
  startTime <- base::Sys.time()
  
  CvalFstMaker(siteID = siteID ,cvalType = "Li840LastValidationTime")
  CvalFstMaker(siteID = siteID ,cvalType = "Li840LastCalibrationTime")
  CvalFstMaker(siteID = siteID ,cvalType = "Li7200LastValidationTime")
  CvalFstMaker(siteID = siteID ,cvalType = "G2131iLastValidationTime")
  
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