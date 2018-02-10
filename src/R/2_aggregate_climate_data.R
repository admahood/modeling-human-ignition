
source("src/R/a_make_dirs.R")

# The FFWI has is not freely downloadable from John's website
# These data must be downloaded upon John's request
if(!file.exists(file.path("data", "climate", "ffwi", "monthly_mean", "ffwi_2016_mean.tif"))) {
  for(i in rep(1979:2016)) {
  if (!file.exists(file.path(".", paste0("/ffwi_", i, ".nc")))) {
    loc <- paste0("nimbus.cos.uidaho.edu/abatz/DATA/ffwi_", i, ".nc")
    dest <- file.path(".", paste0("/ffwi_", i, ".nc"))
    download.file(loc, dest)
    assert_that(file.exists(file.path(".", paste0("/ffwi_", i, ".nc"))))
    }
  }
}

usa_shp <- st_read(dsn = file.path(raw_prefix, "cb_2016_us_state_20m/"), layer = "cb_2016_us_state_20m") %>%
                   st_transform("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")

usa_shp <- as(usa_shp, "Spatial")

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
  dir_75th <- file.path(data_var, "monthly_mean_75thpct")
  dir_90th <- file.path(data_var, "monthly_mean_90thpct")
  dir_95th <- file.path(data_var, "monthly_mean_95thpct")
  dir_sum_90thpct <- file.path(data_var, "sum_days_90thpct")
  dir_sum_95thpct <- file.path(data_var, "sum_days_95thpct")

  var_dir <- list(data_pro, data_var, dir_mean, dir_std, dir_75th,
                  dir_90th, dir_95th, dir_sum_90thpct, dir_sum_95thpct)

  lapply(var_dir, function(x) if(!dir.exists(x)) dir.create(x, showWarnings = FALSE))

  if(var == "ffwi") {
    nc <- nc_open(file)
    array <- ncvar_get(nc, attributes(nc$var)$names)
    array <- aperm(array, c(3, 2, 1))
    raster <- brick(array, crs = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs",
                    xmn = -124.793, xmx = -67.043,  ymn = 25.04186, ymx = 49.41686)
  } else{
    raster <- brick(file)
    p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
    projection(raster) <- CRS(p4string)
  }

  start_date <- as.Date(paste(year, "01", "01", sep = "-"))
  end_date <- as.Date(paste(year, "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day")
  date_seq <- date_seq[1:nlayers(raster)]
  month_seq <- month(date_seq)
  day_seq <- day(date_seq)

  # Mean
  if(!file.exists(file.path(dir_mean, paste0(var, "_", year, "_mean",".tif")))) {
    monthly_mean <- stackApply(raster, month_seq, fun = mean)
    if(var != "ffwi") {
      monthly_mean <- flip(t(monthly_mean), direction = "x")
    }
    names(monthly_mean) <- paste(var, year,
                                 unique(month(date_seq, label = TRUE)),
                                 sep = "_")
    monthly_mean <- mask(monthly_mean, mask)
    writeRaster(monthly_mean, filename = file.path(dir_mean, paste0(var, "_", year, "_mean",".tif")),
                format = "GTiff")
    rm(monthly_mean) }

  # Standard deviation
  if(!file.exists(file.path(dir_std, paste0(var, "_", year, "_std",".tif")))){
    monthly_std <- stackApply(raster, month_seq, fun = sd)
    if(var != "ffwi") {
      monthly_std <- flip(t(monthly_std), direction = "x")
    }
    names(monthly_std) <- paste(var, year,
                                unique(month(date_seq, label = TRUE)),
                                sep = "_")
    monthly_std <- mask(monthly_std, mask)
    writeRaster(monthly_std, filename = file.path(dir_std, paste0(var, "_", year, "_std",".tif")),
                format = "GTiff")
    rm(monthly_std) }

  # Monthly mean above 75th percentile
  if(var == "tmmx") {
    if(!file.exists(file.path(dir_75th, paste0(var, "_", year, "_75th",".tif")))){
    monthly_mean <- stackApply(raster, month_seq, fun = mean)

    mean_75thpct <- stack() # create an empty stack
    for (i in 1:nlayers(monthly_mean)){
      pctile <- calc(monthly_mean[[i]],
                     fun = function(x) raster::quantile(x, probs = 0.75, na.rm = T))
      mean_75thpct <- stack(mean_75thpct, pctile)
    }
    if(var != "ffwi") {
      mean_75thpct <- flip(t(mean_75thpct), direction = "x")
    }
    names(mean_75thpct) <- paste(var, year,
                                 unique(month(date_seq, label = TRUE)),
                                 sep = "_")
    mean_75thpct <- mask(mean_75thpct, mask)
    writeRaster(mean_75thpct, filename = file.path(dir_75th, paste0(var, "_", year, "_75th",".tif")),
                format = "GTiff")
    rm(mean_75thpct)
      }
    }

  # Monthly mean above 90th percentile
  if(!file.exists(file.path(dir_90th, paste0(var, "_", year, "_90th",".tif")))){
    monthly_mean <- stackApply(raster, month_seq, fun = mean)

    mean_90thpct <- stack() # create an empty stack
    for (i in 1:nlayers(monthly_mean)){
      pctile <- calc(monthly_mean[[i]],
                     fun = function(x) raster::quantile(x, probs = 0.90, na.rm = T))
      mean_90thpct <- stack(mean_90thpct, pctile)
    }
    if(var != "ffwi") {
      mean_90thpct <- flip(t(mean_90thpct), direction = "x")
    }
    names(mean_90thpct) <- paste(var, year,
                                 unique(month(date_seq, label = TRUE)),
                                 sep = "_")
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
    if(var != "ffwi") {
      mean_95thpct <- flip(t(mean_95thpct), direction = "x")
    }
    names(mean_95thpct) <- paste(var, year,
                                 unique(month(date_seq, label = TRUE)),
                                 sep = "_")
    mean_95thpct <- mask(mean_95thpct, mask)
    writeRaster(mean_95thpct, filename = file.path(dir_95th, paste0(var, "_", year, "_95th",".tif")),
                format = "GTiff")
    rm(mean_95thpct) }

  # Sum number of days above 90th percentile
  if(!file.exists(file.path(dir_sum_90thpct, paste0(var, "_", year, "_numdays90th",".tif")))){
    sum_days_90thpct <- calc(raster, fun = function(x){x > quantile(x, probs = 0.90, na.rm =TRUE)})
    sum_days_90thpct <- stackApply(sum_days_90thpct, month_seq, fun = mean)
    if(var != "ffwi") {
      sum_days_90thpct <- flip(t(sum_days_90thpct), direction = "x")
    }
    names(sum_days_90thpct) <- paste(var, year,
                                     unique(month(date_seq, label = TRUE)),
                                     sep = "_")
    sum_days_90thpct <- mask(sum_days_90thpct, mask)
    writeRaster(sum_days_90thpct, filename = file.path(dir_sum_90thpct, paste0(var, "_", year, "_numdays90th",".tif")),
                format = "GTiff")
    rm(sum_days_90thpct) }

  # Sum number of days above 95th percentile
  if(!file.exists(file.path(dir_sum_95thpct, paste0(var, "_", year, "_numdays95th",".tif")))){
    sum_days_95thpct <- calc(raster, fun = function(x){x > quantile(x, probs = 0.95, na.rm =TRUE)})
    sum_days_95thpct <- stackApply(sum_days_95thpct, month_seq, fun = sum)
    if(var != "ffwi") {
      sum_days_95thpct <- flip(t(sum_days_95thpct), direction = "x")
    }
    names(sum_days_95thpct) <- paste(var, year,
                                     unique(month(date_seq, label = TRUE)),
                                     sep = "_")
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
