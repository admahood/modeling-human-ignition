x <- c("raster", "tidyverse", "sf", "rasterVis", "gridExtra", "data.table", "assertthat", "rvest",
       "httr", "purrr", "rgdal", "maptools", "foreign", "purrr", "zoo", "lubridate", "magrittr")
lapply(x, library, character.only = TRUE, verbose = FALSE)

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

climate_prefix <- file.path(raw_prefix, "climate")
ancillary_dir <- file.path(prefix, "ancillary")
anthro_dir <- file.path(ancillary_dir, "anthro")

fire_dir <- file.path(ancillary_dir, "fire")

# Check if directory exists for all variable aggregate outputs, if not then create
var_dir <- list(prefix, raw_prefix, us_prefix, ecoregion_prefix, roads_prefix,
                fpa_prefix, rails_prefix, pd_prefix, iclus_prefix, climate_prefix,
                nlcd_prefix, elev_prefix, tl_prefix, ancillary_dir, fire_dir, anthro_dir)

lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))

