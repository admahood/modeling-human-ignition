
# Download and import CONUS states
# Download will only happen once as long as the file exists
if (!exists("usa_shp")){
  usa_shp <- load_data(url = "https://www2.census.gov/geo/tiger/GENZ2016/shp/cb_2016_us_state_20m.zip",
                       dir = us_prefix,
                       layer = "cb_2016_us_state_20m",
                       outname = "usa") %>%
    sf::st_transform(p4string_ea) %>%
    dplyr::filter(!STUSPS %in% c("HI", "AK", "PR"))
}

# Download and import the Level 3 Ecoregions data
# Download will only happen once as long as the file exists
if (!exists("ecoregions")){
  ecoregions <- load_data(url = "ftp://newftp.epa.gov/EPADataCommons/ORD/Ecoregions/us/us_eco_l3.zip",
                          dir = ecoregion_prefix, layer = "us_eco_l3", outname = "ecoregion") %>%
    sf::st_simplify(., preserveTopology = TRUE, dTolerance = 1000)  %>%
    sf::st_transform(st_crs(usa_shp)) %>%
    dplyr::mutate(NA_L3NAME = as.character(NA_L3NAME),
           NA_L3NAME = ifelse(NA_L3NAME == 'Chihuahuan Desert',
                              'Chihuahuan Deserts',
                              NA_L3NAME))
}

# Create raster mask
# 4k Fishnet
if (!exists("fishnet_4k")) {
  if (!file.exists(file.path(fishnet_path, "fishnet_4k.gpkg"))) {
    fishnet_4k <- sf::st_make_grid(usa_shp, cellsize = 4000, what = 'polygons') %>%
      sf::st_sf('geometry' = ., data.frame('fishid4k' = 1:length(.))) %>%
      sf::st_intersection(., st_union(usa_shp))

    sf::st_write(fishnet_4k,
             file.path(fishnet_path, "fishnet_4k.gpkg"),
             driver = "GPKG")

    system(paste0("aws s3 cp ",
                  fishnet_path, "/fishnet_4k.gpkg ",
                  s3_anc_prefix, "fishnet/fishnet_4k.gpkg"))
  }
}

# Load and process FPA-FOD wildfire iginition data
if (!exists("fpa_clean")) {
  if (!file.exists(file.path(processed_dir, "fpa_clean.gpkg"))){
    if (!exists("fpa")) {
      fpa <- sf::st_read(dsn = file.path(fpa_prefix, "Data", "FPA_FOD_20170508.gdb"),
                     layer = "Fires", quiet= FALSE) %>%
        sf::st_transform(st_crs(usa_shp)) %>%
        sf::st_intersection(., st_union(usa_shp))
    }

    fpa_clean <- fpa %>%
      dplyr::select(FPA_ID, LATITUDE, LONGITUDE, ICS_209_INCIDENT_NUMBER, ICS_209_NAME, MTBS_ID, MTBS_FIRE_NAME,
                    FIRE_YEAR, DISCOVERY_DATE, DISCOVERY_DOY, STAT_CAUSE_DESCR, FIRE_SIZE, STATE)  %>%
      dplyr::mutate(cause = ifelse(STAT_CAUSE_DESCR == "Lightning", "Lightning", "Human"),
             FIRE_SIZE_km2 = (FIRE_SIZE*4046.86)/1000000,
             doy = day(DISCOVERY_DATE),
             month = month(DISCOVERY_DATE),
             year = FIRE_YEAR,
             ym = as.yearmon(paste(year, sprintf("%02d", month),
                                   sep = "-"))) %>%
      dplyr::rename_all(tolower)

    sf::st_write(fpa_clean,
             file.path(processed_dir, "fpa_clean.gpkg"),
             driver = "GPKG")

    system(paste0("aws s3 cp ",
                  processed_dir, "/fpa_clean.gpkg",
                  s3_proc_prefix, "fpa_clean.gpkg"))
  }
}

# Import ancillary data
# Primary Roads
if (!exists("primary_rds")) {
  if (!file.exists(file.path(processed_dir, "primary_rds.gpkg"))) {
    if (!exists("rds")) {
      rds <- sf::st_read(dsn = file.path(roads_prefix, "tlgdb_2015_a_us_roads.gdb"), layer = 'Roads')
      }

    primary_rds <- rds %>%
      dplyr::filter(MTFCC == "S1100") %>%
      sf::st_transform(p4string_ea) %>%
      sf::st_intersection(., st_transform(usa_shp, p4string_ea)) %>%
      dplyr::mutate(bool_prds = 1)

    sf::st_write(primary_rds,
             file.path(processed_dir, "primary_rds.gpkg"),
             driver = "GPKG")
  } else {

    primary_rds <- sf::st_read(dsn = file.path(processed_dir, "primary_rds.gpkg"))
  }
}

# Secondary roads
if (!exists("secondary_rds")) {
  if (!file.exists(file.path(processed_dir, "secondary_rds.gpkg"))) {
    if (!exists("rds")) {
      rds <- sf::st_read(dsn = file.path(roads_prefix, "tlgdb_2015_a_us_roads.gdb"), layer = 'Roads')
      }

        secondary_rds <- rds %>%
          dplyr::filter(MTFCC == "S1200") %>%
          sf::st_transform(p4string_ea) %>%
          sf::st_intersection(., usa_shp) %>%
          dplyr::mutate(bool_srds = 1)


        sf::st_write(secondary_rds,
             file.path(processed_dir, "secondary_rds.gpkg"),
             driver = "GPKG")
  } else {

    secondary_rds <- sf::st_read(dsn = file.path(processed_dir, "secondary_rds.gpkg"))
  }
}

# Tertiary roads
if (!exists("tertiary_rds")) {
  if (!file.exists(file.path(processed_dir, "tertiary_rds.gpkg"))) {
    if (!exists("rds")) {
      rds <- sf::st_read(dsn = file.path(roads_prefix, "tlgdb_2015_a_us_roads.gdb"), layer = 'Roads')
      }

      tertiary_rds <- rds %>%
        dplyr::filter(MTFCC == "S1400") %>%
        sf::st_transform(p4string_ea) %>%
        sf::st_intersection(., usa_shp) %>%
        dplyr::mutate(bool_trds = 1)

      sf::st_write(secondary_rds,
        file.path(processed_dir, "tertiary_rds.gpkg"),
        driver = "GPKG")

      } else {

        tertiary_rds <- sf::st_read(dsn = file.path(processed_dir, "tertiary_rds.gpkg"))
      }
    }

# All major roads
if (!exists("all_rds")) {
  if (!file.exists(file.path(processed_dir, "all_rds.gpkg"))) {
    if (!exists("rds")) {
      rds <- sf::st_read(dsn = file.path(roads_prefix, "tlgdb_2015_a_us_roads.gdb"), layer = 'Roads')
      }

    all_rds <- rds %>%
      dplyr::filter(MTFCC == "S1200" | MTFCC == "S1200") %>%
      sf::st_transform(p4string_ea) %>%
      sf::st_intersection(., usa_shp) %>%
      dplyr::mutate(bool_ards = 1)

    sf::st_write(all_rds,
             file.path(processed_dir, "all_rds.gpkg"),
             driver = "GPKG")
  }
}

# Railrods
if (!exists("rail_rds")) {
  if (!file.exists(file.path(processed_dir, "rail_rds.gpkg"))) {
    rail_rds <- sf::st_read(dsn = file.path(rails_prefix, 'tlgdb_2015_a_us_rails.gdb'), layer = 'Rails') %>%
      sf::st_transform(p4string_ea) %>%
      sf::st_intersection(., usa_shp) %>%
      dplyr::mutate(bool_rrds = 1)

    sf::st_write(rail_rds,
             file.path(anthro_dir, "rail_rds.gpkg"),
             driver = "GPKG",
             update=TRUE,
             delete_dsn=TRUE)
  }
}


# Power transmission lines
if (!exists("tl")) {
  if (!file.exists(file.path(processed_dir, "power_lines.gpkg"))) {
    tl <- sf::st_read(dsn = file.path(tl_prefix, 'Electric_Power_Transmission_Lines.shp')) %>%
      sf::st_transform(p4string_ea) %>%
      sf::st_intersection(., usa_shp) %>%
      dplyr::mutate(bool_tl = 1) %>%
      dplyr::filter(st_is(., c("LINESTRING")))

    sf::st_write(tl,
             file.path(anthro_dir, "power_lines.gpkg"),
             driver = "GPKG")
  }
}

# Input terrain data
# Note this has to be manually downloaded from Earth Explorer unfortuantely
# Download elevation (https://lta.cr.usgs.gov/GTOPO30)

mosaic_rasters <- function(files){

  #Internal function to make a list of raster objects from list of files.
  list_rasters <- function(list_names) {
    raster_list <- list() # initialise the list of rasters
    for (i in 1:(length(list_names))){
      rst_name <- list_names[i] # list_names contains all the names of the images in .grd format
      raster_file <- raster::raster(rst_name)
    }
    raster_list <- append(raster_list, raster_file) # update raster_list at each iteration
  }

  #convert every raster path to a raster object and create list of the results
  raster_list <- sapply(list_names, FUN = list_rasters)

  # edit settings of the raster list for use in do.call and mosaic
  names(raster_list) <- NULL

  #run do call to implement mosaic over the list of raster objects.
  mos <- do.call(raster::mosaic, raster_list)

  #set crs of output
  crs(mos) <- crs(x = raster(list_names[1]))
  return(mos)
}

elev_files <- list.files(file.path(raw_prefix, 'gtopo30'),
                         pattern = '.tif',
                         recursive = TRUE,
                         full.names = TRUE))

if (!exists("elevation")) {
  if (!file.exists(file.path(processed_dir, 'elevation.tif'))) {

    elevation <- mosaic_rasters(elev_files) %>%
      raster::projetRaster(., res = 1000, crs = p4string_ea, method = 'bilinear') %>%
      raster::crop(sf::as_Spatial(usa_shp)) %>%
      raster::mask(sf::as_Spatial(usa_shp))

    raster::writeRaster(elevation, filename = file.path(processed_dir, "elevation.tif"), format = "GTiff")

    system(paste0("aws s3 sync ",
                  file.path(processed_dir, "elevation.tif"), " ",
                  s3_proc_prefix, "elevation.tif"))

    } else {

      elevation <- raster::raster(file.path(processed_dir, "elevation.tif"))
    }
}

# Create slope raster
if (!exists("slope")) {
  if (!file.exists(file.path(processed_dir, 'slope.tif'))) {

    slope <- raster::raster(file.path(processed_dir, "elevation.tif")) %>%
      raster::terrain(., opt = 'slope', unit = 'degrees')

    raster::writeRaster(slope, filename = file.path(processed_dir, "slope.tif"), format = "GTiff")

    system(paste0("aws s3 sync ",
                  file.path(processed_dir, "slope.tif"), " ",
                  s3_proc_prefix, "slope.tif"))

    } else {

      slope <- raster::raster(file.path(processed_dir, "slope.tif"))
    }
}

# Create terrain roughness
if (!exists("roughness")) {
  if (!file.exists(file.path(processed_dir, 'roughness.tif'))) {

    roughness <- raster::raster(file.path(processed_dir, "elevation.tif")) %>%
      raster::terrain(., opt = 'roughness')

    raster::writeRaster(roughness, filename = file.path(processed_dir, "roughness.tif"), format = "GTiff")

    system(paste0("aws s3 sync ",
                  file.path(processed_dir, "roughness.tif"), " ",
                  s3_proc_prefix, "roughness.tif"))

  } else {

    roughness <- raster::raster(file.path(processed_dir, "roughness.tif"))
  }
}

# Create aspect
if (!exists("aspect")) {
  if (!file.exists(file.path(processed_dir, 'aspect.tif'))) {

    aspect <- raster::raster(file.path(processed_dir, "elevation.tif")) %>%
      raster::terrain(., opt = 'aspect', unit = 'degrees')

    raster::writeRaster(aspect, filename = file.path(processed_dir, "aspect.tif"), format = "GTiff")

    system(paste0("aws s3 sync ",
                  file.path(processed_dir, "aspect.tif"), " ",
                  s3_proc_prefix, "aspect.tif"))

  } else {

    aspect <- raster::raster(file.path(processed_dir, "aspect.tif"))
  }
}
