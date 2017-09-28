library(tidyverse)
library(assertthat)
library(rvest)
library(httr)
library(purrr)

prefix <- ifelse(Sys.getenv("LOGNAME") == "NateM", file.path("data"), 
                     ifelse(Sys.getenv("LOGNAME") == "nami1114", file.path("data"), 
                            file.path("../data")))
climate_prefix <- file.path(prefix, "climate")
vpd_prefix <- file.path(climate_prefix, "vpd")
pdsi_prefix <- file.path(climate_prefix, "pdsi")
aet_prefix <- file.path(climate_prefix, "aet")
def_prefix <- file.path(climate_prefix, "def")

# Check if directory exists for all variable aggregate outputs, if not then create
var_dir <- list(prefix, climate_prefix, def_prefix, 
                vpd_prefix, aet_prefix, pdsi_prefix)

lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))

# Download elevation

vpd_nc <- file.path(vpd_prefix, 'vpd_19792016.nc')
if (!file.exists(vpd_nc)) {
  loc <- "http://nimbus.cos.uidaho.edu/abatz/DATA/vpd19792016.nc"
  dest <- paste0(vpd_prefix, "/vpd_19792016.nc")
  download.file(loc, dest)
  assert_that(file.exists(vpd_nc))
}

pdsi_nc <- file.path(pdsi_prefix, 'pdsi_19792016.nc')
if (!file.exists(pdsi_nc)) {
  loc <- "http://nimbus.cos.uidaho.edu/abatz/DATA/pdsi19792016.nc"
  dest <- paste0(pdsi_prefix, "/pdsi_19792016.nc")
  download.file(loc, dest)
  assert_that(file.exists(pdsi_nc))
}

aet_nc <- file.path(aet_prefix, 'aet_19792016.nc')
if (!file.exists(aet_nc)) {
  loc <- "http://nimbus.cos.uidaho.edu/abatz/DATA/aet19792016.nc"
  dest <- paste0(aet_prefix, "/aet_19792016.nc")
  download.file(loc, dest)
  assert_that(file.exists(aet_nc))
}

def_nc <- file.path(def_prefix, 'def_19792016.nc')
if (!file.exists(def_nc)) {
  loc <- "http://nimbus.cos.uidaho.edu/abatz/DATA/def19792016.nc"
  dest <- paste0(def_prefix, "/def_19792016.nc")
  download.file(loc, dest)
  assert_that(file.exists(def_nc))
}







