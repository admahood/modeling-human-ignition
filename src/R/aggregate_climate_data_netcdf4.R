library(raster)
library(lubridate)
library(rgdal)
library(tidyverse)
library(assertthat)
library(snowfall)
library(tools)

# Creat directories for state data
prefix <- file.path("../data")
raw_prefix <- file.path(prefix, "raw")
us_prefix <- file.path(raw_prefix, "cb_2016_us_state_20m")
climate_prefix <- file.path(raw_prefix, "climate")

# Check if directory exists for all variable aggregate outputs, if not then create
var_dir <- list(prefix, raw_prefix, us_prefix, climate_prefix)
lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))

us_shp <- file.path(raw_prefix, "cb_2016_us_state_20m", "cb_2016_us_state_20m.shp")
if (!file.exists(us_shp)) {
  loc <- "https://www2.census.gov/geo/tiger/GENZ2016/shp/cb_2016_us_state_20m.zip"
  dest <- paste0(us_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = us_prefix)
  unlink(dest)
}

usa_shp <- readOGR(dsn = file.path("../data/raw/cb_2016_us_state_20m/"), 
                   layer = "cb_2016_us_state_20m")
usa_shp <- spTransform(usa_shp, 
                       CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0"))

netcdf_import <- function(file, masks){
  masks = usa_shp
  file_split <- file %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- file_split[1]
  year <- substr(file_split[2], start = 1, stop = 4)
  endyear <- substr(file_split[2], start = 5, stop = 8)
  
  # Create dirctories
  prefix <- file.path("../data")
  clim_prefix <- file.path(prefix, "climate")
  var_prefix <- file.path(clim_prefix, var)
  
  # Check if directory exists for all variable aggregate outputs, if not then create
  var_dir <- list(prefix, clim_prefix, var_prefix)
  lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))
  
  start_date <- as.Date(paste(year, "01", "01", sep = "-"))
  end_date <- as.Date(paste(ifelse(year == endyear, year, endyear), "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 month")
  month_seq <- month(date_seq)
  
  out_name <- basename(file_path_sans_ext(file))
  
  nc <- nc_open(file)
  nc_att <- attributes(nc$var)$names
  ncvar <- ncvar_get(nc, nc_att)
  rm(nc)
  tvar <- aperm(ncvar, c(3,2,1))
  rm(ncvar)
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0 "
  
  raster <- brick(tvar, crs= proj)
  rm(tvar)
  extent(raster) <- c(-124.793, -67.043, 25.04186, 49.41686)
  names(raster) <- paste(var, year(date_seq),
                                     unique(month(date_seq, label = TRUE)),
                                     sep = "_")
  # Mean
  if(!file.exists(file.path(var_prefix, paste0(out_name, "_mean", ".tif")))) {
    monthly_mean <- mask(raster, masks)
    writeRaster(monthly_mean, filename = file.path(var_prefix, paste0(out_name, "_mean", ".tif")),
                format = "GTiff") 
    rm(monthly_mean)}

  # Monthly mean 90th percentile
  if(!file.exists(file.path(var_prefix, paste0(out_name, "_90th", ".tif")))){
    monthly_mean_90thpct <- calc(raster, fun = function(x){ quantile(x, probs = 0.90, na.rm =TRUE)})
    monthly_mean_90thpct <- mask(monthly_mean_90thpct, masks)
    writeRaster(monthly_mean_90thpct, filename = file.path(var_prefix, paste0(out_name, "_90th", ".tif")),
                format = "GTiff") 
    rm(monthly_mean_90thpct)}
  
  # Monthly mean 95th percentile
  if(!file.exists(file.path(var_prefix, paste0(out_name, "_95th", ".tif")))){
    monthly_mean_95thpct <- calc(raster, fun = function(x){ quantile(x, probs = 0.95, na.rm =TRUE)})
    monthly_mean_95thpct <- mask(monthly_mean_95thpct, masks)
    writeRaster(monthly_mean_95thpct, filename = file.path(var_prefix, paste0(out_name, "_95th", ".tif")),
                format = "GTiff") 
    rm(monthly_mean_95thpct)}
}

monthly_files <- list.files(climate_prefix, pattern = "nc", 
                          full.names = TRUE, recursive = TRUE)

sfInit(parallel = TRUE, cpus = parallel::detectCores())
sfLibrary(snowfall)
sfLibrary(tidyverse)
sfLibrary(raster)
sfLibrary(ncdf4)
sfLibrary(rgdal)
sfLibrary(lubridate)
sfLibrary(tools)

sfExportAll()

sfLapply(monthly_files,
         netcdf_import,
         masks = usa_shp)
sfStop()
