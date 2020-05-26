# Convert all archive files to fst
library(tidyverse)
library(tidytable)
library(fst) # v0.9.2

# Daily Ingest Direc 
file_names <- list.files(path = "C:/GitHub/fstPursley/data/cval/csvCvals/", pattern = "Time",
                         full.names = FALSE)


if(length(file_names) > 0){
  
  # Read in CSV's and convert them to fst
  for(i in file_names){
    fstFileName <- substr(i,1,nchar(i)-3)
    ingest <- data.table::fread(paste0("C:/GitHub/fstPursley/data/cval/csvCvals/",i))
    names(ingest) = c("siteID", "calID","TimeStamp","CO2","H2O","Temp","Pressure")
    ingest$TimeStamp <- lubridate::ymd_hms(ingest$TimeStamp)
    write.fst(ingest, paste0("C:/GitHub/fstPursley/data/cval/fstCvals/",fstFileName,"fst"))
  }
  
  # Move all files after Fasting them
  file_move <- list.files(path = "C:/GitHub/fstPursley/data/cval/csvCvals/", pattern = "Time",
                           full.names = TRUE)
  filesstrings::file.move(files = file_move, destinations = "C:/GitHub/fstPursley/data/cval/csvCvals/csvArchive/",
                          overwrite = TRUE)
} else {
  message("Length of files in C:/GitHub/fstPursley/data/cval/csvCvals/ is = 0")
}
