
# functions for 4_fpa_climate_summaries.R -------------------------------

extract_one <- function(filename, shapefile_extractor) {
  # function to extract all climate time series based on shapefile input
  # this results in large list of all months/years within the raster climate data
  # each list is written out to a csv so this only needs to be run once.
  # inputs:
  # filename -> a list of all tif filenames with full path
  # shapefile_extractor -> the shapefile (point or polygon) to extract climate data

  out_name <- gsub('.tif', '.csv', filename)
  if (!file.exists(out_name)) {
    res <- raster::extract(raster::stack(filename), shapefile_extractor,
                           na.rm = TRUE, fun = 'mean', df = TRUE)
    write.csv(res, file = out_name)
  } else {
    res <- read.csv(out_name)
  }
  res
}

check_tifs <- function(j, i, ...) {
  # checks whether the statistic being evaluated has the variable data
  # if not then returns NULL which allows the larger loop below to skip the inputs

  tif <- tryCatch(list.files(file.path(climate_prefix, j),
                             pattern = i,
                             recursive = TRUE,
                             full.names = TRUE) %>%
                    Filter(function(x) grepl(".tif", x), .),
                  error = function(c) {
                    c$message <- paste0(c$message, " (in the", i, 'variable and ', j, 'statistic)')
                    warning(c)
                  }
  )

  # if the length of the tryCatch is greater than one then that indicates there
  # are data in the statistic/variable combination
  if (length(tif) > 1) {
    tif <- list.files(file.path(climate_prefix, j),
                      pattern = paste0(i, '_' , rep(1988:2015), "_", j, collapse = "|"),
                      recursive = TRUE,
                      full.names = TRUE) %>%
      Filter(function(x) grepl(".tif", x), .)
  } else {
    # if the tif length is 0 then that indicates there are no climate data for
    # that statistic/variable combination

    tif <- NULL
  }
}

get_climate_lags <- function(fpa_df, climate_df, start_date, time_lag) {

  # capture the variable name and statistic to be incorporated in the output column name
  variable <- paste0(climate_df$variable[1], '_', climate_df$statistic[1])

  # internal function to create a lagged date
  lag_date <- function(start_date, time_lag) {
    require(magrittr)
    require(tidyverse)
    require(lubridate)

    # breakup the start date into its components
    y <- year(start_date)
    m <- month(start_date)
    d <- day(start_date)

    # calculate the new lagged year
    y <- y + (m - time_lag - 1) %/% 12

    # calculate the new lagged month
    m <- ifelse(((m - time_lag) %% 12) == 0, 12, (m - time_lag) %% 12)

    # stitch the new lagged date together
    as.Date(paste0(y, "-", m, "-", d))
  }

  # remove the sf data - increases efficiency
  fpa_df <- fpa_df %>%
    as.data.frame() %>%
    select(-geom)

  # pair down to the climate_df to allow for easier left_join
  climate_df <- climate_df %>%
    select('FPA_ID', 'year_month_day', 'value')

  for (j in 0:time_lag) {
    require(magrittr)
    require(tidyverse)

    # create a lagged data year column that can be joined and extracted upon
    fpa_df[, paste0(variable, '_lag_', j)]  <- fpa_df %>%
      dplyr::mutate(ymd_lagged = lag_date(start_date, j)) %>%

      # the meat and potatoes.  This joins the fpa and climate data based on
      #climate data dates and lagged fpa dates.
      left_join(., climate_df, by = c('FPA_ID', 'ymd_lagged' = 'year_month_day')) %>%
      dplyr::select(value)
  }
  return(fpa_df)
}


# functions for c_prep_data.R ---------------------------------------------

mosaic_rasters <- function(files){

  # this function will take a list of raster data with full path names and:
  #  1. read in all rasters iteratively
  #  2. create a raster list of all created rasters
  #  3. mosaic all rasters, using mean if tiles overlap.
  #
  # the only input needed is the list of raster filenames

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
  raster_list <- sapply(files, FUN = list_rasters)

  # make all raster names null
  names(raster_list) <- NULL

  # take the mean of overlapping raster images
  raster_list$fun <- mean

  # mosaic all rasters in list
  mos <- do.call(raster::mosaic, raster_list)

  #set crs of output
  crs(mos) <- crs(x = raster(files[1]))
  return(mos)
}

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
