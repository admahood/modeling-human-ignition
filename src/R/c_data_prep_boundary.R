# Download and import CONUS states
# Download will only happen once as long as the file exists
if (!exists("usa_shp")){
  usa_shp <- load_data(url = "https://www2.census.gov/geo/tiger/GENZ2016/shp/cb_2016_us_state_20m.zip",
                       dir = us_prefix,
                       layer = "cb_2016_us_state_20m",
                       outname = "usa") %>%
    sf::st_transform(p4string_ea) %>%
    dplyr::filter(!STUSPS %in% c("HI", "AK", "PR"))
  usa_shp$STUSPS <- droplevels(usa_shp$STUSPS)
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

    system(paste0("aws s3 sync ",
                  fishnet_path, " ",
                  s3_anc_prefix, "fishnet"))
  } else {
    fishnet_4k <- sf::st_read(file.path(fishnet_path, "fishnet_4k.gpkg"))
  }
}

# Create voxel
# 4k hexagonal fishnet
if (!exists("hexnet_4k")) {
  if (!file.exists(file.path(fishnet_path, "hexnet_4k.gpkg"))) {
    hex_points <- spsample(as(usa_shp, 'Spatial'), type = "hexagonal", cellsize = 4000)
    hex_grid <- HexPoints2SpatialPolygons(hex_points, dx = 4000)
    hexnet_4k <- st_as_sf(hex_grid) %>%
      mutate(hexid4k = row_number()) %>%
      st_intersection(., st_union(usa_shp)) %>%
      st_join(., usa_shp, join = st_intersects) %>%
      dplyr::select(hexid4k, STUSPS)

    sf::st_write(hexnet_4k,
                 file.path(fishnet_path, "hexnet_4k.gpkg"),
                 driver = "GPKG")

    system(paste0("aws s3 sync ",
                  fishnet_path, " ",
                  s3_anc_prefix, "fishnet"))
  } else {
    hexnet_4k <- sf::st_read(file.path(fishnet_path, "hexnet_4k.gpkg"))

  }
}
