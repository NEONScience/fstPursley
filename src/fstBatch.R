# Fast Batch #

# 2020-04-03
# Kevin Styers

# Libraries
library(taskscheduleR)
# Set up task scheduleR
# myScript <- base::system.file("fstBatch","C:/Github/fieldscience_collab/TIS/IS3Rmarkdown/1_fstPursley/src/fstBatch.R", package = "taskscheduleR")
# 
# taskscheduleR::taskscheduler_create(taskname = "Run Fst Pursley Pipeline", rscript = myScript,
#                                     schedule = "ONCE", starttime = format(Sys.time() + 62, "%H:%M"))

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

message("Fast CO2 Pursley Emails")
source("C:/GitHub/fieldscience_collab/TIS/IS3Rmarkdown/1_fstPursley/src/fstCo2Emails.R")
message(paste0("\b"))
message(paste0("\b"))
message(paste0("\b"))

message("Fst Span Gas Concentrations")
source("C:/GitHub/fieldscience_collab/TIS/IS3Rmarkdown/1_fstPursley/src/fstSpanGasConc.R")
message(paste0("\b"))
message(paste0("\b"))
message(paste0("\b"))

message("Fst Tip Bucket Tips")
source("C:/GitHub/fieldscience_collab/TIS/IS3Rmarkdown/1_fstPursley/src/fstTipBucket.R")
message(paste0("\b"))
message(paste0("\b"))
message(paste0("\b"))

message("Fst Wet Dep Lid Status")
source("C:/GitHub/fieldscience_collab/TIS/IS3Rmarkdown/1_fstPursley/src/fstWetDepLidStatus.R")
message(paste0("\b"))
message(paste0("\b"))
message(paste0("\b"))


message("The End")