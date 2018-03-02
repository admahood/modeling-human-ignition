
x <- c("raster", "ncdf4", "tidyverse", "sf", "rasterVis", "gridExtra", "data.table", "assertthat", "rvest",
       "httr", "purrr", "rgdal", "maptools", "foreign", "purrr", "zoo", "lubridate", "magrittr", "snowfall")
lapply(x, library, character.only = TRUE, verbose = FALSE)

source('src/functions/helper_functions.R')
source("src/functions/download-data.R")

# Projections
p4string_ed <- "+proj=eqdc +lat_0=0 +lon_0=0 +lat_1=33 +lat_2=45 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"   #http://spatialreference.org/ref/esri/102005/
p4string_ea <- "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs"   #http://spatialreference.org/ref/sr-org/6903/

ncor <- parallel::detectCores()

prefix <- ("data")
raw_prefix <- file.path(prefix, "raw")

us_prefix <- file.path(raw_prefix, "cb_2016_us_state_20m")
ecoregion_prefix <- file.path(raw_prefix, "us_eco_l3")
fpa_prefix <- file.path(raw_prefix, "fpa-fod")
roads_prefix <- file.path(raw_prefix, "tlgdb_2015_a_us_roads")
rails_prefix <- file.path(raw_prefix, "tlgdb_2015_a_us_rails")
nlcd_prefix <- file.path(raw_prefix, "nlcd_2011_landcover_2011_edition_2014_10_10")
pd_prefix <- file.path(raw_prefix, "county_pop")
iclus_prefix <- file.path(raw_prefix, 'housing_den')
elev_prefix <- file.path(raw_prefix, 'metdata_elevationdata')
tl_prefix <- file.path(raw_prefix, 'Electric_Power_Transmission_Lines')
climate_prefix <- file.path(prefix, "climate")

ancillary_dir <- file.path(prefix, "ancillary")
summary_dir <- file.path(prefix, "summary")
summaries_dir <- file.path(summary_dir, "fpa_climate_summaries")
extraction_dir <- file.path(summary_dir, "fpa_climate_extraction")

processed_dir <- file.path(prefix, 'processed')
anthro_dir <- file.path(prefix, "anthro")

fishnet_path <- file.path(ancillary_dir, "fishnet")

s3_anc_prefix <- 's3://earthlab-modeling-human-ignitions/ancillary/'
s3_proc_prefix <- 's3://earthlab-modeling-human-ignitions/processed/'

# Check if directory exists for all variable aggregate outputs, if not then create
var_dir <- list(prefix, raw_prefix, us_prefix, ecoregion_prefix, roads_prefix, summary_dir,
                fpa_prefix, rails_prefix, pd_prefix, iclus_prefix, climate_prefix,
                nlcd_prefix, elev_prefix, tl_prefix, ancillary_dir, anthro_dir,
                fishnet_path, processed_dir, summaries_dir, extraction_dir)

lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))
