
library(raster)
library(lubridate)
library(rgdal)
library(tidyverse)
library(assertthat)
library(snowfall)

# Creat directories for state data
raw_prefix <- ifelse(Sys.getenv("LOGNAME") == "NateM", file.path("data", "raw"), 
                     ifelse(Sys.getenv("LOGNAME") == "nami1114", file.path("data", "raw"), 
                            file.path("../data", "raw")))
us_prefix <- file.path(raw_prefix, "cb_2016_us_state_20m")

# Check if directory exists for all variable aggregate outputs, if not then create
var_dir <- list(raw_prefix,
                us_prefix)

lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))

for(i in rep(1979:2016)) {
  if (!file.exists(file.path(".", paste0("/ffwi_", i, ".nc")))) {
    loc <- paste0("nimbus.cos.uidaho.edu/abatz/DATA/ffwi_", i, ".nc")
    dest <- file.path(".", paste0("/ffwi_", i, ".nc"))
    download.file(loc, dest)
    assert_that(file.exists(file.path(".", paste0("/ffwi_", i, ".nc"))))
  }
}

us_shp <- file.path("../data", "raw", "cb_2016_us_state_20m", "cb_2016_us_state_20m.shp")
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


daily_to_monthly <- function(file, mask){

  x <- c("lubridate", "rgdal", "ncdf4", "raster", "tidyverse", "snowfall")
  lapply(x, require, character.only = TRUE)
  
  file_split <- file %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- file_split[1]
  year <- substr(file_split[2], start = 1, stop = 4)
  
  # Check if directory exists for all variable aggregate outputs, if not then create
  data_pro <- file.path("../data",  "climate")
  data_var <- file.path(data_pro, var)
  dir_mean <- file.path(data_var, "monthly_mean")
  dir_std <- file.path(data_var, "monthly_std")
  dir_90th <- file.path(data_var, "monthly_mean_90thpct")
  dir_95th <- file.path(data_var, "monthly_mean_95thpct")
  dir_sum_90thpct <- file.path(data_var, "sum_days_90thpct")
  dir_sum_95thpct <- file.path(data_var, "sum_days_95thpct")
  
  var_dir <- list(data_pro, data_var, dir_mean, dir_std,
                  dir_sum_90thpct, dir_sum_95thpct)
  
  lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))
  
  if(var == "ffwi") {
    nc <- nc_open(file)
    array <- ncvar_get(nc, attributes(nc$var)$names)
    array <- aperm(array, c(3, 2, 1))
    raster <- brick(array, crs = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs",
                    xmn = -124.793, xmx = -67.043,  ymn = 25.04186, ymx = 49.41686)
  } else{ raster <- brick(file) }
  
  start_date <- as.Date(paste(year, "01", "01", sep = "-"))
  end_date <- as.Date(paste(year, "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day")
  date_seq <- date_seq[1:nlayers(raster)]
  month_seq <- month(date_seq)
  day_seq <- day(date_seq)
  
  # Mean
  if(!file.exists(file.path(dir_mean, paste0(var, "_", year, "_mean",".tif")))) {
    monthly_mean <- stackApply(raster, month_seq, fun = mean)
    monthly_mean <- flip(t(monthly_mean), direction = "x")
    names(monthly_mean) <- paste(var, year,
                                 unique(month(date_seq, label = TRUE)),
                                 sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(monthly_mean) <- CRS(p4string)
    monthly_mean <- mask(monthly_mean, mask)
    writeRaster(monthly_mean, filename = file.path(dir_mean, paste0(var, "_", year, "_mean",".tif")),
                format = "GTiff") 
    rm(monthly_mean) }
  
  # Standard deviation
  if(!file.exists(file.path(dir_std, paste0(var, "_", year, "_std",".tif")))){
    monthly_std <- stackApply(raster, month_seq, fun = sd)
    monthly_std <- flip(t(monthly_std), direction = "x")
    names(monthly_std) <- paste(var, year,
                                unique(month(date_seq, label = TRUE)),
                                sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(monthly_std) <- CRS(p4string)
    monthly_std <- mask(monthly_std, mask)
    writeRaster(monthly_std, filename = file.path(dir_std, paste0(var, "_", year, "_std",".tif")),
                format = "GTiff") 
    rm(monthly_std) }
  
  # Monthly mean above 90th percentile
  if(!file.exists(file.path(dir_90th, paste0(var, "_", year, "_90th",".tif")))){
    monthly_mean <- stackApply(raster, month_seq, fun = mean)
    
    mean_90thpct <- stack() # create an empty stack
    for (i in 1:nlayers(monthly_mean)){
      pctile <- calc(monthly_mean[[i]], 
                     fun = function(x) raster::quantile(x, probs = 0.90, na.rm = T))
      mean_90thpct <- stack(mean_90thpct, pctile)
    }
    
    mean_90thpct <- flip(t(mean_90thpct), direction = "x")
    names(mean_90thpct) <- paste(var, year,
                                 unique(month(date_seq, label = TRUE)),
                                 sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(mean_90thpct) <- CRS(p4string)
    mean_90thpct <- mask(mean_90thpct, mask)
    writeRaster(mean_90thpct, filename = file.path(dir_90th, paste0(var, "_", year, "_90th",".tif")),
                format = "GTiff") 
    rm(mean_90thpct) }
  
  # Monthly mean above 95th percentile
  if(!file.exists(file.path(dir_95th, paste0(var, "_", year, "_95th",".tif")))){
    monthly_mean <- stackApply(raster, month_seq, fun = mean)
    
    mean_95thpct <- stack() # create an empty stack
    for (i in 1:nlayers(monthly_mean)){
      pctile <- calc(monthly_mean[[i]], 
                     fun = function(x) raster::quantile(x, probs = 0.95, na.rm = T))
      mean_95thpct <- stack(mean_95thpct, pctile)
    }
    mean_95thpct <- flip(t(mean_95thpct), direction = "x")
    names(mean_95thpct) <- paste(var, year,
                                 unique(month(date_seq, label = TRUE)),
                                 sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(mean_95thpct) <- CRS(p4string)
    mean_95thpct <- mask(mean_95thpct, mask)
    writeRaster(mean_95thpct, filename = file.path(dir_95th, paste0(var, "_", year, "_95th",".tif")),
                format = "GTiff") 
    rm(mean_95thpct) }
  
  # Sum number of days above 90th percentile
  if(!file.exists(file.path(dir_sum_90thpct, paste0(var, "_", year, "_numdays90th",".tif")))){
    sum_days_90thpct <- calc(raster, fun = function(x){x > quantile(x, probs = 0.90, na.rm =TRUE)})
    sum_days_90thpct <- stackApply(sum_days_90thpct, month_seq, fun = mean)
    corrected_res_mean <- flip(t(sum_days_90thpct), direction = "x")
    names(sum_days_90thpct) <- paste(var, year,
                                     unique(month(date_seq, label = TRUE)),
                                     sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(sum_days_90thpct) <- CRS(p4string)
    sum_days_90thpct <- mask(sum_days_90thpct, mask)
    writeRaster(sum_days_90thpct, filename = file.path(dir_sum_90thpct, paste0(var, "_", year, "_numdays90th",".tif")),
                format = "GTiff") 
    rm(sum_days_90thpct) }
  
  # Sum number of days above 95th percentile
  if(!file.exists(file.path(dir_sum_95thpct, paste0(var, "_", year, "_numdays95th",".tif")))){
    sum_days_95thpct <- calc(raster, fun = function(x){x > quantile(x, probs = 0.95, na.rm =TRUE)})
    sum_days_95thpct <- stackApply(sum_days_95thpct, month_seq, fun = sum)
    sum_days_95thpct <- flip(t(sum_days_95thpct), direction = "x")
    names(sum_days_95thpct) <- paste(var, year,
                                     unique(month(date_seq, label = TRUE)),
                                     sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(sum_days_95thpct) <- CRS(p4string)
    masked_res_mean <- mask(sum_days_95thpct, mask)
    writeRaster(sum_days_95thpct, filename = file.path(dir_sum_95thpct, paste0(var, "_", year, "_numdays95th",".tif")),
                format = "GTiff") 
    rm(sum_days_95thpct) }
}

daily_files <- list.files(".", pattern = ".nc", full.names = TRUE)

sfInit(parallel = TRUE, cpus = parallel::detectCores())
sfExportAll()

sfLapply(daily_files, 
         daily_to_monthly,
         mask = usa_shp)
sfStop()
