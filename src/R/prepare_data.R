library(raster)
library(tidyverse)
library(sf)
library(lubridate)

#source("src/R/get_data.R")

# Prepare all spatial data for analysis
raw_prefix <- file.path("../data", "raw")

# p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs" # Latlong
p4string_ed <- "+proj=eqdc +lat_0=0 +lon_0=0 +lat_1=33 +lat_2=45 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"   #http://spatialreference.org/ref/esri/102005/
p4string_ea <- "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs"   #http://spatialreference.org/ref/sr-org/6903/

# CONUS states
usa_shp <- st_read(dsn = file.path(raw_prefix, "cb_2016_us_state_20m"),
                   layer = "cb_2016_us_state_20m") %>%
  st_transform(p4string_ea) %>%
  filter(!STUSPS %in% c("HI", "AK", "PR"))

# Level 3 ecoregions
eco_reg <- st_read(dsn = file.path(raw_prefix, "us_eco_l3"),
                   layer = "us_eco_l3") %>%
  st_transform(p4string_ea)

rds <- st_read(dsn = file.path(raw_prefix, "tlgdb_2015_a_us_roads", "tlgdb_2015_a_us_roads.gdb"), layer = 'Roads')

# Primary roads
primary_rds <- rds %>%
  filter(MTFCC == "S1100") %>%
  st_transform(p4string_ea) %>%
  st_intersection(., usa_shp) %>%
  mutate(bool_prds = 1)

# Secondary roads
secondary_rds <- rds %>%
  filter(MTFCC == "S1200") %>%
  st_transform(p4string_ea) %>%
  st_intersection(., usa_shp) %>%
  mutate(bool_srds = 1)

# # Tertiary roads
tertiary_rds <- rds %>%
  filter(MTFCC == "S1400") %>%
  st_transform(p4string_ea) %>%
  st_intersection(., usa_shp) %>%
  mutate(bool_trds = 1)

# All major roads
all_rds <- rds %>%
  filter(MTFCC == "S1400" | MTFCC == "S1200" | MTFCC == "S1200") %>%
  st_transform(p4string_ea) %>%
  st_intersection(., usa_shp) %>%
  mutate(bool_ards = 1)

st_write(primary_rds,
         "../data/processed/primary_rds.gpkg",
         driver = "GPKG",
         update=TRUE,
         delete_dsn=TRUE)
st_write(secondary_rds,
         "../data/processed/secondary_rds.gpkg",
         driver = "GPKG",
         update=TRUE,
         delete_dsn=TRUE)
st_write(tertiary_rds,
         "../data/processed/tertiary_rds.gpkg",
         driver = "GPKG",
         update=TRUE,
         delete_dsn=TRUE)
st_write(all_rds,
         "../data/processed/all_rds.gpkg",
         driver = "GPKG",
         update=TRUE,
         delete_dsn=TRUE)

# Railrods
rail_rds <- st_read(dsn = file.path(raw_prefix, "tlgdb_2015_a_us_rails",
                                    'tlgdb_2015_a_us_rails.gdb'), layer = 'Rails') %>%
  st_transform(p4string_ea) %>%
  st_intersection(., usa_shp) %>%
  mutate(bool_rrds = 1)

st_write(rail_rds,
         "../data/processed/rail_rds.gpkg",
         driver = "GPKG",
         update=TRUE,
         delete_dsn=TRUE)

# Power transmission lines
tl <- st_read(dsn = file.path(raw_prefix, "Electric_Power_Transmission_Lines",
                              'Electric_Power_Transmission_Lines.shp')) %>%
  st_transform(p4string_ea) %>%
  st_intersection(., usa_shp) %>%
  mutate(bool_tl = 1)
tl <- tl %>%
  filter(st_is(., c("LINESTRING")))

st_write(tl,
         "../data/processed/power_lines.gpkg",
         driver = "GPKG")

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
  filter(IGNITION == "Human") %>%
  st_transform(p4string_ea) %>%
  st_intersection(., usa_shp)

