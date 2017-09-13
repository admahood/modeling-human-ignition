library(tidyverse)
library(assertthat)
library(rvest)
library(httr)
library(purrr)

raw_prefix <- file.path("data", "raw")
us_prefix <- file.path(raw_prefix, "cb_2016_us_state_20m")
ecoregion_prefix <- file.path(raw_prefix, "us_eco_l3")
roads_prefix <- file.path(raw_prefix, "tl_2016_us_primaryroads")
fpa_prefix <- file.path(raw_prefix, "fpa-fod")
rails_prefix <- file.path(raw_prefix, "tl_2016_us_rails")
lulc_prefix <- file.path(raw_prefix, "tl_2016_us_rails")
nlcd_prefix <- file.path(raw_prefix, "nlcd_2011_landcover_2011_edition_2014_10_10")
hd_prefix <- file.path(raw_prefix, 'us_pbg00')
iclus_shp <- file.path(raw_prefix, 'hd_iclus_bc')
elev_shp <- file.path(hd_prefix, 'us_pbg00_2007.nc')

# Check if directory exists for all variable aggregate outputs, if not then create
var_dir <- list(raw_prefix,
                us_prefix,
                ecoregion_prefix,
                roads_prefix,
                fpa_prefix,
                rails_prefix,
                lulc_prefix,
                nlcd_prefix,
                hd_prefix)

lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))

#Download the USA States layer

us_shp <- file.path(us_prefix, "cb_2016_us_state_20m.shp")
if (!file.exists(us_shp)) {
  loc <- "https://www2.census.gov/geo/tiger/GENZ2016/shp/cb_2016_us_state_20m.zip"
  dest <- paste0(us_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = us_prefix)
  unlink(dest)
  assert_that(file.exists(us_shp))
}

#Download the Level 3 Ecoregions

ecoregion_shp <- file.path(ecoregion_prefix, "us_eco_l3.shp")
if (!file.exists(ecoregion_shp)) {
  loc <- "ftp://newftp.epa.gov/EPADataCommons/ORD/Ecoregions/us/us_eco_l3.zip"
  dest <- paste0(ecoregion_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = ecoregion_prefix)
  unlink(dest)
  assert_that(file.exists(ecoregion_shp))
}

# Download the FPA-FOD data

fpa_gdb <- file.path(fpa_prefix, "Data", "FPA_FOD_20170508.gdb")
if (!file.exists(fpa_gdb)) {
  pg <- read_html("https://www.fs.usda.gov/rds/archive/Product/RDS-2013-0009.4/")
  fils <- html_nodes(pg, xpath=".//dd[@class='product']//li/a[contains(., 'zip') and contains(., 'GDB')]")
  dest <- paste0(fpa_prefix, ".zip")
  walk2(html_attr(fils, 'href'),  html_text(fils),
        ~GET(sprintf("https:%s", .x), write_disk(dest), progress()))
  unzip(dest, exdir = fpa_prefix)
  unlink(dest)
  assert_that(file.exists(fpa_gdb))
}

#Download the roads

roads_shp <- file.path(roads_prefix, 'tl_2016_us_primaryroads.shp')
if (!file.exists(roads_shp)) {
  loc <- "ftp://ftp2.census.gov/geo/tiger/TIGER2016/PRIMARYROADS/tl_2016_us_primaryroads.zip"
  dest <- paste0(roads_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = roads_prefix)
  unlink(dest)
  assert_that(file.exists(roads_shp))
}

#Download the railrods

rails_shp <- file.path(rails_prefix, 'tl_2016_us_rails.shp')
if (!file.exists(rails_shp)) {
  loc <- "ftp://ftp2.census.gov/geo/tiger/TIGER2016/RAILS/tl_2016_us_rails.zip"
  dest <- paste0(rails_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = rails_prefix)
  unlink(dest)
  assert_that(file.exists(rails_shp))
}

#Download the NLCD 2011

nlcd_shp <- file.path(nlcd_prefix, 'nlcd_2011_landcover_2011_edition_2014_10_10.tif')
if (!file.exists(nlcd_shp)) {
  loc <- "http://www.landfire.gov/bulk/downloadfile.php?TYPE=nlcd2011&FNAME=nlcd_2011_landcover_2011_edition_2014_10_10.zip"
  dest <- paste0(nlcd_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = nlcd_prefix)
  unlink(dest)
  assert_that(file.exists(nlcd_shp))
}

# Download housing density by census block from 1940-2030

hd_shp <- file.path(hd_prefix, 'us_pbg00_2007.gdb')
if (!file.exists(hd_shp)) {
  loc <- "http://silvis.forest.wisc.edu/sites/default/files/maps/pbg00_old/gis/us_pbg00.zip"
  dest <- paste0(hd_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = hd_prefix)
  unlink(dest)
  assert_that(file.exists(hd_shp))
}

# Download housing density baseline scenario

iclus_shp <- file.path(iclus_prefix, 'hd_iclus_bc.nc')
if (!file.exists(iclus_shp)) {
  loc <- "https://cida.usgs.gov/thredds/fileServer/ICLUS/files/housing_density/hd_iclus_bc.nc"
  dest <- paste0(iclus_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = iclus_prefix)
  unlink(dest)
  assert_that(file.exists(iclus_shp))
}

# Download elevation

elev_shp <- file.path(hd_prefix, 'us_pbg00_2007.nc')
if (!file.exists(elev_shp)) {
  loc <- "http://metdata.northwestknowledge.net/data/metdata_elevationdata.nc"
  dest <- paste0(hd_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = hd_prefix)
  unlink(dest)
  assert_that(file.exists(elev_shp))
}
