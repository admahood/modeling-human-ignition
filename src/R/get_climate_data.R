library(tidyverse)
library(assertthat)
library(rvest)
library(httr)
library(purrr)

prefix <- file.path("../data")
raw_prefix <- file.path(prefix, "raw")
climate_prefix <- file.path(raw_prefix, "climate")

# Check if directory exists for all variable aggregate outputs, if not then create
var_dir <- list(prefix, raw_prefix, climate_prefix)
lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))

# Download elevation

vpd_nc <- file.path(climate_prefix, 'vpd19792016.nc')
if (!file.exists(vpd_nc)) {
  loc <- "http://nimbus.cos.uidaho.edu/abatz/DATA/vpd19792016.nc"
  dest <- paste0(climate_prefix, "/vpd19792016.nc")
  download.file(loc, dest)
  assert_that(file.exists(vpd_nc))
}

pdsi_nc <- file.path(climate_prefix, 'pdsi19792016.nc')
if (!file.exists(pdsi_nc)) {
  loc <- "http://nimbus.cos.uidaho.edu/abatz/DATA/pdsi19792016.nc"
  dest <- paste0(climate_prefix, "/pdsi19792016.nc")
  download.file(loc, dest)
  assert_that(file.exists(pdsi_nc))
}

aet_nc <- file.path(climate_prefix, 'aet19792016.nc')
if (!file.exists(aet_nc)) {
  loc <- "http://nimbus.cos.uidaho.edu/abatz/DATA/aet19792016.nc"
  dest <- paste0(climate_prefix, "/aet19792016.nc")
  download.file(loc, dest)
  assert_that(file.exists(aet_nc))
}

def_nc <- file.path(climate_prefix, 'def19792016.nc')
if (!file.exists(def_nc)) {
  loc <- "http://nimbus.cos.uidaho.edu/abatz/DATA/def19792016.nc"
  dest <- paste0(climate_prefix, "/def19792016.nc")
  download.file(loc, dest)
  assert_that(file.exists(def_nc))
}

ffwi_nc <- file.path(climate_prefix, 'ffwi19792016.nc')
if (!file.exists(ffwi_nc)) {
  loc <- "nimbus.cos.uidaho.edu/abatz/DATA/ffwi_1979.nc"
  dest <- paste0(climate_prefix, "/ffwi19792016.nc")
  download.file(loc, dest)
  assert_that(file.exists(ffwi_nc))
}








