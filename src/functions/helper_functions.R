
# Functions for `d_rasterize_anthro.R` ------------------------------------

# This function splits shapefile based on the number of cores for parallel rasterization
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

# A function to recombine the split data from `shp_rst`, calculate the distance and reproject to equal area
combine_rst <- function(y){
  # y = shp_rst output split rasters
  
  do.call(merge, y) %>%
    distance(.)  %>%
    aggregate(fact = 20, fun = mean) %>%
    projectRaster(elevation) %>%
    crop(as(usa_shp, "Spatial")) %>%
    mask(as(usa_shp, "Spatial"))
}



# Functions for `e_model_creation.R` ------------------------------------------------------------------------------

sfc_as_cols <- function(x, names = c("x","y")) {
  stopifnot(inherits(x,"sf") && inherits(sf::st_geometry(x),"sfc_POINT"))
  ret <- sf::st_coordinates(x)
  ret <- tibble::as_tibble(ret)
  stopifnot(length(names) == ncol(ret))
  x <- x[ , !names(x) %in% names]
  ret <- setNames(ret,names)
  dplyr::bind_cols(x,ret)
}
