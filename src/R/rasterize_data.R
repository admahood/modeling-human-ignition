library(raster)
library(tidyverse)
library(sf)
library(lubridate)
library(snowfall)

primary_rds <- st_read("../data/ancillary/primary_rds.gpkg") 
secondary_rds <- st_read("../data/ancillary/secondary_rds.gpkg") 
tl <- st_read("../data/ancillary/tranmission_lns.gpkg") 
rail_rds <- st_read("../data/ancillary/rail_rds.gpkg") 

psrds <- primary_rds %>%
  select(-starts_with("bool"))
ssrds <- secondary_rds  %>%
  select(-starts_with("bool"))

all_rds <- rbind(psrds, ssrds) %>%
  mutate(bool_ards = 1)

# Prepare all spatial data for analysis
raw_prefix <- ifelse(Sys.getenv("LOGNAME") == "NateM", file.path("data", "raw"), 
                     ifelse(Sys.getenv("LOGNAME") == "nami1114", file.path("data", "raw"), 
                            file.path("../data", "raw")))

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

shp_rst <- function(y, x, lvl, j){
  # y = input shapefile
  # x = number of splits to iterate on in parallel
  # lvl = the shapefile attribute to rasterize
  # j = the larger underlying raster (4k)
  # k = the smaller underlying raster (200m)
  features <- 1:nrow(y[,])
  parts <- split(features, cut(features, ncor))
  require(snowfall)
  require(raster)
  require(sf)
  require(tidyverse)

  outrst <- rasterize(as(y[parts[[x]],], "Spatial"), j, lvl) %>%
    projectRaster(j)
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

# Calculate distance to power lines
sfInit(parallel = TRUE, cpus = ncor)
sfExport(list = c("ncor", "usa_shp", "tl", "elevation"))
tl_rst <- sfLapply(1:ncor, shp_rst, y = tl, lvl = "bool_tl", j = elevation)
sfStop()
dis_transmission_lines <- combine_rst(tl_rst) 
writeRaster(dis_transmission_lines, filename = paste0("../data", "/processed/", "dis_transmission_lines", ".tif"),
            format = "GTiff", overwrite=TRUE)
#rm(list = c("tl_rst", "dis_transmission_lines"))

# Calculate distance to railroads
sfInit(parallel = TRUE, cpus = ncor)
sfExport(list = c("ncor", "usa_shp", "rail_rds", "elevation"))
rail_rst <- sfLapply(1:ncor, shp_rst, y = rail_rds, lvl = "bool_rrds", j = elevation)
sfStop()
dis_railroads <- combine_rst(rail_rst)
writeRaster(dis_railroads, filename = paste0("../data", "/processed/", "dis_railroads", ".tif"),
            format = "GTiff", overwrite=TRUE)
#rm(list = c("rail_rst", "dis_railroads"))

# Calculate distance to primary roads
sfInit(parallel = TRUE, cpus = ncor)
sfExport(list = c("ncor", "usa_shp", "primary_rds", "elevation"))
prds_rst <- sfLapply(1:ncor, shp_rst, y = primary_rds, lvl = "bool_prds", j = elevation)
sfStop()
dis_primary_rds <- combine_rst(prds_rst)
writeRaster(dis_primary_rds, filename = paste0("../data", "/processed/", "dis_primary_rds", ".tif"),
            format = "GTiff", overwrite=TRUE)
#rm(list = c("prds_rst", "dis_primary_rds"))

# Calculate distance to secondary roads
sfInit(parallel = TRUE, cpus = ncor)
sfExport(list = c("ncor", "usa_shp", "secondary_rds", "elevation"))
srds_rst <- sfLapply(1:ncor, shp_rst, y = secondary_rds, lvl = "bool_srds", j = elevation)
sfStop()
dis_secondary_rds <- combine_rst(srds_rst)
writeRaster(dis_secondary_rds, filename = paste0("../data", "/processed/", "dis_secondary_rds", ".tif"),
            format = "GTiff", overwrite=TRUE)
#rm(list = c("srds_rst", "dis_secondary_rds"))

# Calculate distance to primary and secondary roads
sfInit(parallel = TRUE, cpus = ncor)
sfExport(list = c("ncor", "usa_shp", "all_rds", "elevation"))
ards_rst <- sfLapply(1:ncor, shp_rst, y = all_rds, lvl = "bool_ards", j = elevation)
sfStop()
dis_all_rds <- combine_rst(ards_rst)
writeRaster(dis_all_rds, filename = paste0("../data", "/processed/", "dis_all_rds", ".tif"),
            format = "GTiff", overwrite=TRUE)
#rm(list = c("ards_rst", "dis_all_rds"))










