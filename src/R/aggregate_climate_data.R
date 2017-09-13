

library(raster)
library(lubridate)
library(rgdal)
library(tidyverse)
library(doParallel)
library(foreach) 

# Creat directories for state data
raw_prefix <- file.path("../data", "raw")
us_prefix <- file.path(raw_prefix, "cb_2016_us_state_20m")

# Check if directory exists for all variable aggregate outputs, if not then create
var_dir <- list(raw_prefix,
                us_prefix)

lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))

us_shp <- file.path("../data", "raw", "cb_2016_us_state_20m.shp")
if (!file.exists(us_shp)) {
  loc <- "https://www2.census.gov/geo/tiger/GENZ2016/shp/cb_2016_us_state_20m.zip"
  dest <- paste0(us_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = us_prefix)
  unlink(dest)
  assert_that(file.exists(us_shp))
}


usa_shp <- readOGR(dsn = file.path("../data/raw/cb_2016_us_state_20m/"),
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
  var_dir <- list(file.path("../data",  "processed"),
                  file.path("../data",  "processed", var),
                  file.path("../data",  "processed", var, "monthly_mean"), 
                  file.path("../data",  "processed", var, "monthly_std"),
                  file.path("../data",  "processed", var, "mean_days_90thpct"),
                  file.path("../data",  "processed", var, "mean_days_95thpct"),
                  file.path("../data",  "processed", var, "sum_days_90thpct"),
                  file.path("../data",  "processed", var, "sum_days_95thpct"))
  
  lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))
  
  # Mean
  if(!file.exists(file.path("../data",  "processed", var, "monthly_mean", out_name))){
    monthly_mean <- stackApply(raster, month_seq, fun = mean)
    corrected_res_mean <- flip(t(monthly_mean), direction = "x")
    names(corrected_res_mean) <- paste(var, year,
                                       unique(month(date_seq, label = TRUE)),
                                       sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(corrected_res_mean) <- CRS(p4string)
    masked_res_mean <- mask(corrected_res_mean, mask)
    writeRaster(masked_res_mean, filename = file.path("../data",  "processed", var, "monthly_mean", out_name),
                format = "GTiff") }

  # Standard deviation
  if(!file.exists(file.path("../data",  "processed", var, "monthly_std", out_name))){
    monthly_std <- stackApply(raster, month_seq, fun = sd)
    corrected_res_mean <- flip(t(monthly_std), direction = "x")
    names(corrected_res_mean) <- paste(var, year,
                                       unique(month(date_seq, label = TRUE)),
                                       sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(corrected_res_mean) <- CRS(p4string)
    masked_res_mean <- mask(corrected_res_mean, mask)
    writeRaster(masked_res_mean, filename = file.path("../data",  "processed", var, "monthly_std", out_name),
                format = "GTiff") }

  # Mean number of days above 90th percentile
  if(!file.exists(file.path("../data",  "processed", var, "mean_days_90thpct", out_name))){
    res <- calc(raster, fun = function(x){x > quantile(x, probs = 90, na.rm =TRUE)})
    mean_days_90thpct <- stackApply(res, month_seq, fun = mean)
    corrected_res_mean <- flip(t(mean_days_90thpct), direction = "x")
    names(corrected_res_mean) <- paste(var, year,
                                       unique(month(date_seq, label = TRUE)),
                                       sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(corrected_res_mean) <- CRS(p4string)
    masked_res_mean <- mask(corrected_res_mean, mask)
    writeRaster(masked_res_mean, filename = file.path("../data",  "processed", var, "mean_days_90thpct", out_name),
                format = "GTiff") }
  
  # Mean number of days above 95th percentile
  if(!file.exists(file.path("../data",  "processed", var, "mean_days_95thpct", out_name))){
    res <- calc(raster, fun = function(x){x > quantile(x, probs = 95, na.rm =TRUE)})
    mean_days_95thpct <- stackApply(res, month_seq, fun = mean)
    corrected_res_mean <- flip(t(mean_days_95thpct), direction = "x")
    names(corrected_res_mean) <- paste(var, year,
                                       unique(month(date_seq, label = TRUE)),
                                       sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(corrected_res_mean) <- CRS(p4string)
    masked_res_mean <- mask(corrected_res_mean, mask)
    writeRaster(masked_res_mean, filename = file.path("../data",  "processed", var, "mean_days_95thpct", out_name),
                format = "GTiff") }
  
  # Sum number of days above 90th percentile
  if(!file.exists(file.path("../data",  "processed", var, "sum_days_90thpct", out_name))){
    res <- calc(raster, fun = function(x){x > quantile(x, probs = 90, na.rm =TRUE)})
    sum_days_90thpct <- stackApply(res, month_seq, fun = mean)
    corrected_res_mean <- flip(t(sum_days_90thpct), direction = "x")
    names(corrected_res_mean) <- paste(var, year,
                                       unique(month(date_seq, label = TRUE)),
                                       sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(corrected_res_mean) <- CRS(p4string)
    masked_res_mean <- mask(corrected_res_mean, mask)
    writeRaster(masked_res_mean, filename = file.path("../data",  "processed", var, "sum_days_90thpct", out_name),
                format = "GTiff") }
  
  # Sum number of days above 95th percentile
  if(!file.exists(file.path("../data",  "processed", var, "sum_days_95thpct", out_name))){
    res <- calc(raster, fun = function(x){x > quantile(x, probs = 95, na.rm =TRUE)})
    sum_days_95thpct <- stackApply(res, month_seq, fun = sum)
    corrected_res_mean <- flip(t(sum_days_95thpct), direction = "x")
    names(corrected_res_mean) <- paste(var, year,
                                       unique(month(date_seq, label = TRUE)),
                                       sep = "_")
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(corrected_res_mean) <- CRS(p4string)
    masked_res_mean <- mask(corrected_res_mean, mask)
    writeRaster(masked_res_mean, filename = file.path("../data",  "processed", var, "sum_days_95thpct", out_name),
                format = "GTiff") }
}

daily_files <- list.files(".", pattern = "nc", full.names = TRUE)

# Run the daily_to_monthly function in parallel

UseCores <- detectCores()
cl <- makeCluster(UseCores)
registerDoParallel(cl)

foreach(i = 1:length(daily_files)) %dopar% {
  daily_to_monthly(daily_files[i], mask = usa_shp)}

stopCluster(cl)
