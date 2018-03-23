
# load all ibraries
x <- c("raster", "ncdf4", "tidyverse", "sf", "rasterVis", "gridExtra", "data.table", "assertthat", "rvest", 'parallel', 'doParallel',
       'parallel', 'foreach', "httr", "purrr", "rgdal", "maptools", "foreign", "purrr", "zoo", "lubridate", "magrittr", "snowfall")
lapply(x, library, character.only = TRUE, verbose = FALSE)

# load all functions
source('src/functions/helper_functions.R')

# key rojections
p4string_ed <- "+proj=eqdc +lat_0=0 +lon_0=0 +lat_1=33 +lat_2=45 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"   #http://spatialreference.org/ref/esri/102005/
p4string_ea <- "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs"   #http://spatialreference.org/ref/sr-org/6903/

# define the amount of cores st_par runs on
ncore <- parallel::detectCores()

# create main directories
prefix <- ("data")
ancillary_dir <- file.path(prefix, "ancillary")
summary_dir <- file.path(prefix, "extractions")
processed_dir <- file.path(prefix, 'processed')

# create main raw folder and all subfolders to hold raw/unprocessed data
raw_prefix <- file.path(prefix, "raw")
us_prefix <- file.path(raw_prefix, "cb_2016_us_state_20m")
ecoregion_prefix <- file.path(raw_prefix, "us_eco_l3")
fpa_prefix <- file.path(raw_prefix, "fpa-fod")
roads_prefix <- file.path(raw_prefix, "tlgdb_2015_a_us_roads")
rails_prefix <- file.path(raw_prefix, "tlgdb_2015_a_us_rails")
nlcd_prefix <- file.path(raw_prefix, "nlcd_2011_landcover_2011_edition_2014_10_10")
nlcd92_prefix <- file.path(raw_prefix, "nlcd_1992")
nlcd01_prefix <- file.path(raw_prefix, "nlcd_2001_landcover_2011_edition_2014_10_10")
nlcd06_prefix <- file.path(raw_prefix, "nlcd_2006_landcover_2011_edition_2014_10_10")
nlcd_pdi_01_prefix <- file.path(raw_prefix, "nlcd_2001_impervious_2011_edition_2014_10_10")
nlcd_pdi_06_prefix <- file.path(raw_prefix, "nlcd_2006_impervious_2011_edition_2014_10_10")
nlcd_pdi_11_prefix <- file.path(raw_prefix, "nlcd_2011_impervious_2011_edition_2014_10_10")

pd_prefix <- file.path(raw_prefix, "county_pop")
iclus_prefix <- file.path(raw_prefix, 'housing_den')
elev_prefix <- file.path(raw_prefix, 'metdata_elevationdata')
tl_prefix <- file.path(raw_prefix, 'Electric_Power_Transmission_Lines')
climate_prefix <- file.path(prefix, "climate")

# create processed directories
terrain_dir <- file.path(processed_dir, 'terrain')
anthro_proc_dir <- file.path(processed_dir, 'anthro')
transportation_dir <- file.path(processed_dir, 'transportation')
transportation_density_dir <- file.path(transportation_dir, 'density')
transportation_processed_dir <- file.path(transportation_dir, 'processed')

# create direcotires to hold climate summary outputs
summaries_dir <- file.path(summary_dir, "climate_extractions")
summary_mean <- file.path(summaries_dir, "mean")
summary_95th <- file.path(summaries_dir, "95th")
summary_numdays95th <- file.path(summaries_dir, "numdays95th")
terrain_extract <- file.path(summary_dir, "terrain_extractions")
popden_extract <- file.path(summary_dir, "pop_den_extractions")

anthro_dir <- file.path(prefix, "anthro")
fishnet_path <- file.path(ancillary_dir, "fishnet")

# for pushing and pulling to s3 using the system function
s3_anc_prefix <- 's3://earthlab-modeling-human-ignitions/ancillary/'
s3_proc_prefix <- 's3://earthlab-modeling-human-ignitions/processed/'
s3_raw_prefix <- 's3://earthlab-modeling-human-ignitions/raw/'
s3_proc_extractions <- 's3://earthlab-modeling-human-ignitions/extractions/'


# Check if directory exists for all variable aggregate outputs, if not then create
var_dir <- list(prefix, raw_prefix, us_prefix, ecoregion_prefix, roads_prefix, summary_dir,
                fpa_prefix, rails_prefix, pd_prefix, iclus_prefix, climate_prefix,
                nlcd_prefix,nlcd92_prefix ,nlcd01_prefix ,nlcd06_prefix , elev_prefix,
                tl_prefix, ancillary_dir, anthro_dir, fishnet_path, processed_dir, summaries_dir,
                nlcd_pdi_01_prefix, nlcd_pdi_06_prefix, nlcd_pdi_11_prefix, summary_mean,
                summary_95th, summary_numdays95th, terrain_dir, transportation_dir, anthro_proc_dir,
                transportation_density_dir, transportation_processed_dir, anthro_dir,
                terrain_extract, popden_extract)

lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))
