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

# Create Site Dirs DEPRECATED
# for(i in siteList){
#   if(base::dir.exists(base::paste0(repoDir,"data/cval/fstCvalSite/",i)) == FALSE){
#     base::dir.create(base::paste0(repoDir,"data/cval/fstCvalSite/",i))
#     message(paste0("Dir created for ", i))
#   } else {
#     message(paste0("Dir exists for ", i))
#   }
# }

# CSV Dir
# CvalCsvDir <- "C:/1_Data/fstCVALPipeline/AllCVALCSVs/"
          "C:\GitHub\fstPursley\data\cval\fstCvals"
CvalFstReadDir <- paste0(repoDir,"data/cval/fstCvals/")
CvalFstWriteDir <- paste0(repoDir,"data/cval/fstCvalSite/")
# Dev
siteID <- "JERC"
cvalType <- "Li7200LastValidationTime"

# Function for reading in all CVAL Files
CvalFstMaker <- function(siteID, cvalType){

  # List all Cval files in the CSV Directory
  ListOfCvalFiles <- base::list.files(path = CvalFstReadDir, pattern = base::paste0(siteID,"_",cvalType), full.names = TRUE)
  message(paste0("Starting ", siteID," ",cvalType," with ",base::length(ListOfCvalFiles)," files." ))
  # base::message(base::paste0(base::length(ListOfCvalFiles)," ",cvalType))
  # Check for more than 1 file
  if(base::length(ListOfCvalFiles) > 1){
    CvalFiles <- base::lapply(base::paste0(ListOfCvalFiles), fst::read.fst) 
    CvalFile = data.table::rbindlist(CvalFiles) %>% 
      dplyr::filter(is.na(TimeStamp) == FALSE) %>%
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

    fst::write.fst(x = CvalFile,
                   path = base::paste0(CvalFstWriteDir,siteID,"_",cvalType,".fst"),
                   compress = 100)
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

for(siteID in siteList[3:47]){
  
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
CvalFstOrginMaker(siteID = "JORN", cvalType = "Li7200LastValidationTime")
# list.files(path = "C:/1_Data/fstCVALPipeline/AllCvalFst/",recursive = T,all.files = T,full.names = T, pattern = "*")


# for(i in DeleteList){
  # file.remove(i)
# }