# script for quickly loading

options(conflicts.policy = list(warn = FALSE))
options(clustermq.scheduler = "LOCAL")

source("R/utils.R")
source("R/functions.R")

library(usethis)
library(devtools)
library(targets)
library(tarchetypes)

options(scipen = 9)

# ts <- as.list(targets::tar_manifest(fields = name)[["name"]])
# names(ts) <- ts

cnf <- config::get()
nms_orig <- names(cnf)
names(cnf) <- paste0("c_", names(cnf))
list2env(cnf, envir = .GlobalEnv)
names(cnf) <- names(nms_orig)
rm(nms_orig)

# source("_targets_packages.R")
