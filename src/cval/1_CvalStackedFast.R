# Fst Dir
CvalFstDir <- "C:/GitHub/fstPursley/data/cval/fstCvalSite/"

# Deve
siteID <- "HARV"
cvalType <- "Li7200LastValidationTime"
timestampCutoff = 120
# Function for reading in all CVAL Files
CvalFstOrginMaker <- function(siteID, cvalType,timestampCutoff){
  
  # List all Cval files in the CSV Directory
  ListOfCvalFiles <- base::list.files(path = CvalFstDir, pattern = base::paste0(siteID,"_",cvalType),full.names = TRUE)
  message(paste0("Starting ", siteID," ",cvalType," with ",base::length(ListOfCvalFiles)," files." ))
  # base::message(base::paste0(base::length(ListOfCvalFiles)," ",cvalType))
  # Check for more than 1 file
  if(base::length(ListOfCvalFiles) > 0){
    CvalFile <- fst::read.fst(base::paste0(ListOfCvalFiles)) 
    
    names(CvalFile) <- c("SiteID","CvalType","TimeStamp","Co2","H2o","Temp","Pressure")
    
    # Define Zeroing time period
    CvalFileTimed <- CvalFile %>%
      dplyr::select(SiteID,CvalType,TimeStamp,Co2) %>%
      tidytable::dt_mutate(TimeStamp = lubridate::ymd_hms(TimeStamp))%>%
      tidyr::separate(col = TimeStamp, into = c("Date","Time"), sep = " ", remove = FALSE) %>%
      dplyr::filter(Co2 > -50) %>%
      dplyr::filter(abs(Co2) < 600) %>%
      dplyr::mutate(ZeroStartTime = ifelse(Co2 < 50 & Co2 > - 50, "Yes","No"))

    # Select just 
    CvalZeroStarts <- CvalFileTimed %>%
      dplyr::filter(ZeroStartTime == "Yes") %>%
      dplyr::group_by(Date) %>%
      dplyr::mutate(Origin =  max(Time,na.rm = TRUE)) ######## Max or Min, pick your poisen
    CvalFileTimed2 <- CvalFileTimed %>%
      dplyr::filter(ZeroStartTime == "No")



    CvalFileTime3 <- rbindlist(list(CvalFileTimed2,CvalZeroStarts),fill = TRUE)
    ## If time is before Origin, remove these rows
    ## Can we mutate everything before Origin to be a "Bad" row?
    CvalOriginTime <- CvalFileTime3 %>%
      dplyr::group_by(Date) %>%
      dplyr::mutate(Origin2 = max(Origin,na.rm = TRUE))######## Max or Min, pick your poisen

    CvalPhaseShifted <- CvalOriginTime %>%
      dplyr::group_by(Date) %>%
      dplyr::filter(Time > Origin2) %>%
      dplyr::filter(TimeStamp < max(TimeStamp,na.rm = TRUE) - timestampCutoff) %>%
      dplyr::arrange(TimeStamp) %>%
      dplyr::mutate(TimeAugmented = seq(from = 0, to = (length(Time)*15)-15,by = 15))%>%
      dplyr::mutate(Month = as.Date(Date, "%Y-%m-%d")) %>%
      dplyr::mutate(Month2 = format(Month,"%B-%y")) %>%
      dplyr::select(SiteID,Date, TimeStamp ,CvalType, Co2,TimeAugmented,Month2)
    
    # plotly::ggplotly(
    #   ggplot2::ggplot(CvalPhaseShifted, aes(x = TimeAugmented, y = Co2, color = Month,a = TimeStamp))+
    #     geom_point(shape = 1, alpha = 0.5)+
    #     labs(title = paste0(CvalPhaseShifted$SiteID[1]," Li7200 Validations Since February"))+
    #     facet_grid(~Month)
    # )
    #   
    #   
    # glimpse(CvalPhaseShifted)


    
    
    fst::write.fst(x = CvalPhaseShifted,
                   path = base::paste0(CvalFstDir,"/",siteID,"_",cvalType,"CvalPhaseShifted.fst"),
                   compress = 100)
  }
  # endTime <- base::Sys.time()
  # base::message(base::paste0("Completed ", siteID, " in ", 
  #                            round(difftime(endTime, startTime,units = "secs"),2),
  #                            " Seconds"
  # )
  # )
}

### Function to Site/Sensorify all CVAL Files
library(dplyr)
library(fst)
library(data.table)
library(lubridate)
library(ggplot2)

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
for(siteID in siteList[1:47]){
  startTime <- base::Sys.time()
  
  # CvalFstOrginMaker(siteID = siteID ,cvalType = "Li840LastValidationTime", timestampCutoff = 120)
  # CvalFstOrginMaker(siteID = siteID ,cvalType = "Li840LastCalibrationTime")
  CvalFstOrginMaker(siteID = siteID ,cvalType = "Li7200LastValidationTime", timestampCutoff = 0)
  # CvalFstOrginMaker(siteID = siteID ,cvalType = "G2131iLastValidationTime")
  
  endTime <- base::Sys.time()
  
  base::message(base::paste0("Completed ", siteID, " in ", 
                             round(difftime(endTime, startTime,units = "secs"),2),
                             " Seconds"))
}

phaseFile <- read.fst("C:/GitHub/fstPursley/data/cval/fstCvalSite/ABBY_Li840LastValidationTimeCvalPhaseShifted.fst")
glimpse(phaseFile)
library(zoo)
phaseFile$Month2 <- as.yearmon(phaseFile$Month2, "%B-%y")
phaseFile$Month2 <- factor(phaseFile$Month2,
                                 levels = c("Nov 2019","Dec 2019","Jan 2020",
                                            "Feb 2020", "Mar 2020", "Apr 2020",
                                            "May 2020", "June 2020"))

plotly::ggplotly(
  ggplot2::ggplot(phaseFile, aes(x = TimeAugmented, y = Co2, color = Month2,a = TimeStamp))+
    geom_point(shape = 1, alpha = 0.5)+
    labs(title = paste0(phaseFile$SiteID[1]," Li7200 Validations Since February"))+
    facet_grid(~Month2)
)
vars(phaseFile$Date)
