atom

library(raster)
library(lubridate)
library(rgdal)
library(tidyverse)
library(assertthat)
library(snowfall)

# Creat directories for state data
raw_prefix <- file.path("data", "raw")
us_prefix <- file.path(raw_prefix, "cb_2016_us_state_20m")

# Check if directory exists for all variable aggregate outputs, if not then create
var_dir <- list(raw_prefix,
                us_prefix)

lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))

us_shp <- file.path("data", "raw", "cb_2016_us_state_20m", "cb_2016_us_state_20m.shp")
if (!file.exists(us_shp)) {
  loc <- "https://www2.census.gov/geo/tiger/GENZ2016/shp/cb_2016_us_state_20m.zip"
  dest <- paste0(us_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = us_prefix)
  unlink(dest)
}


usa_shp <- readOGR(dsn = file.path("data/raw/cb_2016_us_state_20m/"),
                   layer = "cb_2016_us_state_20m")

usa_shp <- spTransform(usa_shp,
                       CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0"))


daily_to_monthly <- function(file, mask){
  require(tidyverse)
  require(raster)
  require(rgdal)
  require(lubridate)
  file_split <- file %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- file_split[1]
  year <- substr(file_split[2], start = 1, stop = 4)
  
  out_name <- paste0("monthly_", var, "_", year, ".tif")
  
  raster <- brick(file)
  start_date <- as.Date(paste(year, "01", "01", sep = "-"))
  end_date <- as.Date(paste(year, "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day")
  date_seq <- date_seq[1:nlayers(raster)]
  month_seq <- month(date_seq)
  
  # Check if directory exists for all variable aggregate outputs, if not then create
  var_dir <- list(file.path("data",  "processed"),
                  file.path("data",  "processed", var),
                  file.path("data",  "processed", var, "monthly_mean"), 
                  file.path("data",  "processed", var, "monthly_std"),
                  file.path("data",  "processed", var, "mean_days_90thpct"),
                  file.path("data",  "processed", var, "mean_days_95thpct"),
                  file.path("data",  "processed", var, "sum_days_90thpct"),
                  file.path("data",  "processed", var, "sum_days_95thpct"))
  
  lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))
  
  # Mean
  if(!file.exists(file.path("data",  "processed", var, "monthly_mean", out_name))){
    monthly_mean <- stackApply(raster, month_seq, fun = mean)
    monthly_mean <- flip(t(monthly_mean), direction = "x")
    names(monthly_mean) <- paste(var, year,
                                       unique(month(date_seq, label = TRUE)),
                                       sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(monthly_mean) <- CRS(p4string)
    monthly_mean <- mask(monthly_mean, mask)
    writeRaster(monthly_mean, filename = file.path("data",  "processed", var, "monthly_mean", out_name),
                format = "GTiff") }

  # Standard deviation
  if(!file.exists(file.path("data",  "processed", var, "monthly_std", out_name))){
    monthly_std <- stackApply(raster, month_seq, fun = sd)
    monthly_std <- flip(t(monthly_std), direction = "x")
    names(monthly_std) <- paste(var, year,
                                       unique(month(date_seq, label = TRUE)),
                                       sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(monthly_std) <- CRS(p4string)
    monthly_std <- mask(monthly_std, mask)
    writeRaster(monthly_std, filename = file.path("data",  "processed", var, "monthly_std", out_name),
                format = "GTiff") }

  # Mean number of days above 90th percentile
  if(!file.exists(file.path("data",  "processed", var, "mean_days_90thpct", out_name))){
    mean_days_90thpct <- calc(raster, fun = function(x){x > quantile(x, probs = 0.90, na.rm =TRUE)})
    mean_days_90thpct <- stackApply(mean_days_90thpct, month_seq, fun = mean)
    mean_days_90thpct <- flip(t(mean_days_90thpct), direction = "x")
    names(mean_days_90thpct) <- paste(var, year,
                                       unique(month(date_seq, label = TRUE)),
                                       sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(mean_days_90thpct) <- CRS(p4string)
    mean_days_90thpct <- mask(mean_days_90thpct, mask)
    writeRaster(mean_days_90thpct, filename = file.path("data",  "processed", var, "mean_days_90thpct", out_name),
                format = "GTiff") }
  
  # Mean number of days above 95th percentile
  if(!file.exists(file.path("data",  "processed", var, "mean_days_95thpct", out_name))) {
      mean_days_95thpct <- calc(raster, fun = function(x){x > quantile(x, probs = 0.95, na.rm =TRUE)})
      mean_days_95thpct <- stackApply(mean_days_95thpct, month_seq, fun = mean)
      mean_days_95thpct <- flip(t(mean_days_95thpct), direction = "x")
      names(mean_days_95thpct) <- paste(var, year,
                                         unique(month(date_seq, label = TRUE)),
                                         sep = "_")
      p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
      projection(mean_days_95thpct) <- CRS(p4string)
      mean_days_95thpct <- mask(mean_days_95thpct, mask)
      writeRaster(mean_days_95thpct, filename = file.path("data",  "processed", var, "mean_days_95thpct", out_name),
                  format = "GTiff") }
  
  # Sum number of days above 90th percentile
  if(!file.exists(file.path("data",  "processed", var, "sum_days_90thpct", out_name))){
    sum_days_90thpct <- calc(raster, fun = function(x){x > quantile(x, probs = 0.90, na.rm =TRUE)})
    sum_days_90thpct <- stackApply(sum_days_90thpct, month_seq, fun = mean)
    corrected_res_mean <- flip(t(sum_days_90thpct), direction = "x")
    names(sum_days_90thpct) <- paste(var, year,
                                       unique(month(date_seq, label = TRUE)),
                                       sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(sum_days_90thpct) <- CRS(p4string)
    sum_days_90thpct <- mask(sum_days_90thpct, mask)
    writeRaster(sum_days_90thpct, filename = file.path("data",  "processed", var, "sum_days_90thpct", out_name),
                format = "GTiff") }
  
  # Sum number of days above 95th percentile
  if(!file.exists(file.path("data",  "processed", var, "sum_days_95thpct", out_name))){
    sum_days_95thpct <- calc(raster, fun = function(x){x > quantile(x, probs = 0.95, na.rm =TRUE)})
    sum_days_95thpct <- stackApply(sum_days_95thpct, month_seq, fun = sum)
    sum_days_95thpct <- flip(t(sum_days_95thpct), direction = "x")
    names(sum_days_95thpct) <- paste(var, year,
                                       unique(month(date_seq, label = TRUE)),
                                       sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(sum_days_95thpct) <- CRS(p4string)
    masked_res_mean <- mask(sum_days_95thpct, mask)
    writeRaster(sum_days_95thpct, filename = file.path("data",  "processed", var, "sum_days_95thpct", out_name),
                format = "GTiff") }
}

daily_files <- list.files("/Volumes/LaCie-2TB/data/climate/raw/historical_gridmet/raster", pattern = "nc", full.names = TRUE, recursive = TRUE)

sfInit(parallel = TRUE, cpus = parallel::detectCores())
sfLibrary(snowfall)

sfExportAll()
#sfExport(list = c("daily_to_monthly", "usa_shp", "daily_files"))

sfLapply(daily_files, 
         daily_to_monthly,
         mask = usa_shp)
sfStop()

# Run the daily_to_monthly function in parallel
# library(doParallel)
# library(foreach) 
# UseCores <- detectCores()
# cl <- makeCluster(UseCores)
# registerDoParallel(cl)
# 
# foreach(i = 1:length(daily_files)) %dopar% {
#   daily_to_monthly(daily_files[i], mask = usa_shp)}
# 
# stopCluster(cl)
