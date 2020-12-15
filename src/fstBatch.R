# Fast Batch #

# 2020-04-03
# Kevin Styers

# Mike's data usually hits the N:Drive before 6am, in that case let's have this batch script start at 6am
# Not too sure how long this batch will take but a safe time to have the dev server download the drop box files should be 7am

library(rdrop2)

# Token stuffs
local_KS_token_Path <- "C:/GitHub/swift/SwiftShinyDash/data/token.RDS"
rdrop2::drop_auth(rdstoken = local_KS_token_Path) # token is in C:/GitHub/fieldscience_collab/TIS/swift/shinydash/SwiftShinyDash/data

# Libraries
library(taskscheduleR) 
library(dplyr)
library(filesstrings)
library(knitr)
library(data.table)
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

message("Fst Span Gas Concentrations")
source(paste0(repoDir, "src/fstSpanGasConc.R"))
message(paste0("\b"))
message(paste0("\b"))
rdrop2::drop_upload("C:/GitHub/fstPursley/data/spanGasConc.fst", "cval")
message(paste0("\b"))

message("The End")