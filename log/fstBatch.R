# Fast Batch #

# 2020-04-03
# Kevin Styers

# Mike's data usually hits the N:Drive before 6am, in that case let's have this batch script start at 6am
# Not too sure how long this batch will take but a safe time to have the dev server download the drop box files should be 7am

# Libraries
library(taskscheduleR) 
library(dplyr)
library(filesstrings)
library(knitr)
library(data.table)
library(phenocamapi)
library(ggplot2)
library(plotly)
library(RMySQL)
library(readr)
library(DT)
library(viridis)
library(tidyr)
library(lubridate)

# Repo location used in most (90%) of the src scripts.
repoDir <- "C:/GitHub/fstPursley/"

message("Fast CO2 Pursley Emails")
source(paste0(repoDir, "/src/fstCo2Emails.R"))
message(paste0("\b"))
message(paste0("\b"))
message(paste0("\b"))

message("Fst Span Gas Concentrations")
source(paste0(repoDir, "src/fstSpanGasConc.R"))
message(paste0("\b"))
message(paste0("\b"))
message(paste0("\b"))

message("Fst Tip Bucket Tips")
source(paste0(repoDir, "src/fstTipBucket.R"))
message(paste0("\b"))
message(paste0("\b"))
message(paste0("\b"))

message("Fst Wet Dep Lid Status")
source(paste0(repoDir, "src/fstWetDepLidStatus.R"))
message(paste0("\b"))
message(paste0("\b"))
message(paste0("\b"))

message("Run old cval file Ingest First!")
source("C:/Github/fieldscience_collab/TIS/IS3Rmarkdown/DataFlowR-Code/updateFieldCalValData.R")
message(paste0("\b"))
message(paste0("\b"))
message(paste0("\b"))

message("Run fst cval Ingest")
source(paste0(repoDir, "src/cval/write_all_cval_files_fast.R"))
message(paste0("\b"))
message(paste0("\b"))
message(paste0("\b"))

message("Run fst Site Cval Joins")
source(paste0(repoDir, "src/cval/1_CvalFast."))
message(paste0("\b"))
message(paste0("\b"))
message(paste0("\b"))

## Dropbox Upload
library(rdrop2)

# Token stuffs
local_KS_token_Path <- "C:/GitHub/swift/SwiftShinyDash/data/token.RDS"
rdrop2::drop_auth(rdstoken = local_KS_token_Path) # token is in C:/GitHub/fieldscience_collab/TIS/swift/shinydash/SwiftShinyDash/data

# Dropbox Uploads

# List of all Site Cvals
cvalSiteFileList <- base::list.files(path = "C:/GitHub/fstPursley/data/cval/fstCvalSite/",full.names = TRUE)
cvalSiteFileListSwift <- base::list.files(path = "C:/GitHub/fstPursley/data/cval/fstCvalSite/",full.names = FALSE)
# Saving this list to make it easy to download all of these files using a very similar for loop
base::saveRDS(object = cvalSiteFileListSwift,file = "C:/Github/fstPursley/data/cval/cvalSiteFileList.rds")

message(paste0("Uploading " ,length(cvalSiteFileList), " Cval Files."))

for(i in cvalSiteFileList){
  # upload each file in the list.
  rdrop2::drop_upload(i,"cval")
}


# Dropbox upload the span gases

rdrop2::drop_upload("C:/GitHub/fstPursley/data/spanGasConc.fst", "cval")



message("The End")