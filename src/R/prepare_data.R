library(raster)
library(tidyverse)
library(sf)

# Prepare all spatial data for analysis
raw_prefix <- file.path("data", "raw")

p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"

# Read in the shapefiles
usa_shp <- st_read(dsn = file.path("data/raw/cb_2016_us_state_20m/"),
                   layer = "cb_2016_us_state_20m") %>% 
  st_transform(p4string) %>%
  filter(!STUSPS %in% c("HI", "AK", "PR"))

eco_reg <- st_read(dsn = file.path("data/raw/us_eco_l3/"),
                   layer = "us_eco_l3") %>% 
  st_transform(p4string)

tl <- st_read(dsn = file.path(raw_prefix, "Electric_Power_Transmission_Lines", 'Electric_Power_Transmission_Lines.shp')) %>%
  st_transform(p4string) %>%
  st_intersection(., usa_shp) %>%
  mutate(bool_tl = 1)

rails <- st_read(dsn = file.path(raw_prefix, "tl_2016_us_rails", 'tl_2016_us_rails.shp')) %>%
  st_transform(p4string) %>%
  st_intersection(., usa_shp)

rds <- st_read(dsn = file.path(raw_prefix, "tlgdb_2015_a_us_roads", "tlgdb_2015_a_us_roads.gdb"), layer = 'Roads') %>%
  st_transform(p4string) %>%
  st_intersection(., usa_shp) %>%
  mutate(rd_grp = 1)

pd <- st_read(dsn = file.path(raw_prefix, "county_pop", 'cofips_upp.shp')) %>%
  st_transform(p4string) %>%
  st_intersection(., usa_shp)

# Read in raster data
hd <- raster(file.path(raw_prefix, "housing_den", 'hd_iclus_bc.nc')) %>%
  mask(as(usa_shp, "Spatial"))

nlcd <- raster(file.path(nlcd_prefix, "nlcd_2011_landcover_2011_edition_2014_10_10", 'nlcd_2011_landcover_2011_edition_2014_10_10.img'))

# This will be the raster "template" for all shapefile to raster conversions
elevation <- raster(file.path(raw_prefix, "metdata_elevationdata", "metdata_elevationdata.nc")) %>%
  mask(as(usa_shp, "Spatial"))
projection(elevation) <- CRS(p4string)

# Rasterize all shapefiles
state <- rasterize(as(usa_shp, "Spatial"), elevation)
ecoregion <- rasterize(as(eco_reg, "Spatial"), elevation)
transmission_lines <- rasterize(as(tl, "Spatial"), elevation, "bool_tl")
railroads <- rasterize(as(rails, "Spatial"), elevation)


roads <- rasterize(as(rds, "Spatial"), elevation, "rd_grp")
plot(state)
plot(ecoregion)

plot(transmission_lines)