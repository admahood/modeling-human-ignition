library(raster)
library(tidyverse)
library(sf)
library(lubridate)

source("src/R/get_data.R")

# Prepare all spatial data for analysis
raw_prefix <- file.path("data", "raw")

p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"

# Clean the FPA database class
fpa_fire <- st_read(dsn = file.path(raw_prefix, "fpa-fod", "Data", "FPA_FOD_20170508.gdb"),
                    layer = "Fires", quiet= FALSE) %>%
  filter(!(STATE %in% c("Alaska", "Hawaii", "Puerto Rico") & FIRE_SIZE >= 0.1)) %>%  
  dplyr::select(FPA_ID, LATITUDE, LONGITUDE, ICS_209_INCIDENT_NUMBER, ICS_209_NAME, MTBS_ID, MTBS_FIRE_NAME,
                FIRE_YEAR, DISCOVERY_DATE, DISCOVERY_DOY, STAT_CAUSE_DESCR, FIRE_SIZE, STATE) %>%
  mutate(IGNITION = ifelse(STAT_CAUSE_DESCR == "Lightning", "Lightning", "Human"),
         FIRE_SIZE_m2 = FIRE_SIZE*4046.86,
         FIRE_SIZE_km2 = FIRE_SIZE_m2/1000000,
         FIRE_SIZE_ha = FIRE_SIZE_m2*10000,
         DISCOVERY_DAY = day(DISCOVERY_DATE),
         DISCOVERY_MONTH = month(DISCOVERY_DATE),
         DISCOVERY_YEAR = FIRE_YEAR) %>%
  st_transform(p4string)

# Read in the shapefiles
usa_shp <- st_read(dsn = file.path(raw_prefix, "cb_2016_us_state_20m"),
                   layer = "cb_2016_us_state_20m") %>% 
  st_transform(p4string) %>%
  filter(!STUSPS %in% c("HI", "AK", "PR"))

eco_reg <- st_read(dsn = file.path(raw_prefix, "us_eco_l3"),
                   layer = "us_eco_l3") %>% 
  st_transform(p4string)

tl <- st_read(dsn = file.path(raw_prefix, "Electric_Power_Transmission_Lines", 'Electric_Power_Transmission_Lines.shp')) %>%
  st_transform(p4string) %>%
  st_intersection(., usa_shp) %>%
  mutate(bool_tl = 1)

rails <- st_read(dsn = file.path(raw_prefix, "tlgdb_2015_a_us_rails", 'tlgdb_2015_a_us_rails.gdb'), layer = 'Rails') %>%
  st_transform(p4string) %>%
  st_intersection(., usa_shp) %>%
  mutate(bool_rail = 1)

rds <- st_read(dsn = file.path(raw_prefix, "tlgdb_2015_a_us_roads", "tlgdb_2015_a_us_roads.gdb"), layer = 'Roads') %>%
  st_transform(p4string) %>%
  st_intersection(., usa_shp) %>%
  mutate(bool_rds = 1)

pd <- st_read(dsn = file.path(raw_prefix, "county_pop", 'cofips_upp.shp')) %>%
  st_make_valid() %>%
  st_transform(p4string) %>%
  st_intersection(., usa_shp)

# This will be the raster "template" for all shapefile to raster conversions
elevation <- raster(file.path(raw_prefix, "metdata_elevationdata", "metdata_elevationdata.nc")) %>%
  mask(as(usa_shp, "Spatial"))
projection(elevation) <- CRS(p4string)

# Read in raster data
hd <- raster(file.path(raw_prefix, "housing_den", 'hd_iclus_bc.nc'))
hd <- flip(t(hd), direction = "x")
projection(hd) <- CRS(p4string)
hd <- crop(hd, elevation)
hd <- mask(hd, as(usa_shp, "Spatial"))



# Rasterize all shapefiles
state <- rasterize(as(usa_shp, "Spatial"), elevation)
ecoregion <- rasterize(as(eco_reg, "Spatial"), elevation)
transmission_lines <- rasterize(as(tl, "Spatial"), elevation, "bool_tl")
railroads <- rasterize(as(rails, "Spatial"), elevation, "bool_rail")
roads <- rasterize(as(rds, "Spatial"), elevation, "rds")
pop_den <- rasterize(as(pd, "Spatial"), elevation)
