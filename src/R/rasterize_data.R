library(raster)
library(tidyverse)
library(sf)
library(lubridate)
library(snowfall)


# Prepare all spatial data for analysis
raw_prefix <- file.path("../data", "raw")

# p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs" # Latlong
p4string_ed <- "+proj=eqdc +lat_0=0 +lon_0=0 +lat_1=33 +lat_2=45 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"   #http://spatialreference.org/ref/esri/102005/
p4string_ea <- "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs"   #http://spatialreference.org/ref/sr-org/6903/

# CONUS states
usa_shp <- st_read(dsn = file.path(raw_prefix, "cb_2016_us_state_20m"),
                   layer = "cb_2016_us_state_20m") %>%
  st_transform(p4string_ea) %>%
  filter(!STUSPS %in% c("HI", "AK", "PR")) %>%
  mutate(region = as.factor(ifelse(STUSPS %in% c("CO", "WA", "OR", "NV", "CA", "ID", "UT",
                                       "WY", "NM", "AZ", "MT"), 1, 2)))

# This will be the raster "template" for all shapefile to raster conversions
elevation <- raster(file.path(raw_prefix, "metdata_elevationdata", "metdata_elevationdata.nc")) %>%
  projectRaster(crs = p4string_ea, res = 4000) %>%
  crop(as(usa_shp, "Spatial")) %>%
  mask(as(usa_shp, "Spatial"))
elevation <- calc(elevation, fun = function(x){x[x < 0] <- NA; return(x)})

elevation.disaggregate <- disaggregate(elevation, fact = 20) %>%
  projectRaster(crs = p4string_ed, res = 200)
elevation.disaggregate <- calc(elevation.disaggregate, fun = function(x){x[x < 0] <- NA; return(x)})

# Rasterize all shapefiles
state <- rasterize(as(usa_shp, "Spatial"), elevation, "GEOID")
ecoregion <- rasterize(as(eco_reg, "Spatial"), elevation, "US_L3CODE")

fpa_counts <- rasterize(as(fpa_fire, "Spatial"), elevation, "IGNITION", fun = "count") %>%
  crop(as(usa_shp, "Spatial")) %>%
  mask(as(usa_shp, "Spatial"))
writeRaster(fpa_counts, filename = paste0("../data",  "/processed/", "fpa_counts", ".tif"),
            format = "GTiff")

fpa_density <- fpa_counts/4000 # Equiv to the fire counts / area of a 4km pixel (4k x 4k = 16k)
writeRaster(fpa_counts, filename = paste0("../data",  "/processed/", "fpa_density", ".tif"),
            format = "GTiff", overwrite=TRUE)

rm(fpa_fire)

ncor <- parallel::detectCores()

shp_rst <- function(y, x, lvl, j, k){
  # y = input shapefile
  # x = number of splits to iterate on in parallel
  # lvl = the shapefile attribute to rasterize
  # j = the larger underlying raster (4k)
  # k = the smaller underlying raster (200m)
  features <- 1:nrow(y[,])
  parts <- split(features, cut(features, ncor))

  outrst <- rasterize(as(y[parts[[x]],], "Spatial"), j, lvl) %>%
    disaggregate(., fact = 20) %>%
    projectRaster(k)
}

combine_rst <- function(y){
  # y = shp_rst output split rasters
  
  do.call(merge, y) %>%
    distance(.)  %>%
    aggregate(fact = 20, fun = mean) %>%
    projectRaster(elevation) %>%
    crop(as(usa_shp, "Spatial")) %>%
    mask(as(usa_shp, "Spatial"))
}

sfInit(parallel = TRUE, cpus = ncor)
sfLibrary(snowfall)
sfLibrary(raster)
sfLibrary(sf)
sfLibrary(tidyverse)

sfExportAll()
tl_rst <- sfLapply(1:ncor, shp_rst, y = tl, lvl = "bool_tl",
                   j = elevation, k = elevation.disaggregate)

rail_rst <- sfLapply(1:ncor, shp_rst, y = rail_rds, lvl = "bool_rrds",
                j = elevation, k = elevation.disaggregate)

prds_rst <- sfLapply(1:ncor, shp_rst, y = primary_rds, lvl = "bool_prds",
                     j = elevation, k = elevation.disaggregate)

srds_rst <- sfLapply(1:ncor, shp_rst, y = secondary_rds, lvl = "bool_srds",
                     j = elevation, k = elevation.disaggregate)

ards_rst <- sfLapply(1:ncor, shp_rst, y = all_rds, lvl = "bool_ards",
                j = elevation, k = elevation.disaggregate)
sfStop()

# Calculate distance to power lines
dis_transmission_lines <- combine_rst(tl_rst) 

# Calculate distance to railroads
dis_railroads <- combine_rst(rail_rst)

# Calculate distance to primary roads
dis_primary_rds <- combine_rst(prds_rst)

# Calculate distance to secondary roads
dis_secondary_rds <- combine_rst(srds_rst)

# Calculate distance to primary and secondary roads
dis_all_rds <- combine_rst(ards_rst)

writeRaster(dis_transmission_lines, filename = paste0("../data", "/ancillary/", "dis_transmission_lines", ".tif"),
            format = "GTiff", overwrite=TRUE)
writeRaster(dis_railroads, filename = paste0("../data", "/processed/", "dis_railroads", ".tif"),
            format = "GTiff", overwrite=TRUE)
writeRaster(dis_primary_rds, filename = paste0("../data", "/processed/", "dis_primary_rds", ".tif"),
            format = "GTiff", overwrite=TRUE)
writeRaster(dis_secondary_rds, filename = paste0("data", "/processed/", "dis_secondary_rds", ".tif"),
            format = "GTiff", overwrite=TRUE)
writeRaster(dis_all_rds, filename = paste0("../data", "/processed/", "dis_all_rds", ".tif"),
            format = "GTiff", overwrite=TRUE)
