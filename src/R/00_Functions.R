
# Functions ---------------------------------------------------------------
netcdf_nc_import <- function(y, mask) {
  ### A function to import NetCDF and create a raster brick of monthly climate data
  # input: 
  #  - y : a data list of all NetCDF ys
  #  - mask : CONUS shapefile projected in WGS84
  # output:
  #  - rbrck: a raster brick of all processed months
  require(dplyr)

  y_split <- y %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- y_split[1]
  year <- substr(y_split[2], start = 1, stop = 4)
  endyear <- substr(y_split[2], start = 5, stop = 8)
  
  require(lubridate)
  start_date <- as.Date(paste(year, "01", "01", sep = "-"))
  end_date <- as.Date(paste(ifelse(year == endyear, year, endyear), "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day") %m+% years(1)
  monthly_seq <- seq(start_date, end_date, by = "1 month")
  
  require(ncdf4)
  nc <- nc_open(y)
  nc_att <- attributes(nc$var)$names
  ncvar <- ncvar_get(nc, nc_att)
  tvar <- aperm(ncvar, c(3,2,1))
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0 "
  
  require(raster)
  rbrck <- brick(tvar, crs= proj)
  rm(list = c("ncvar", "tvar", "nc"))
  extent(rbrck) <- c(-124.793, -67.043, 25.04186, 49.41686)
  names(rbrck) <- paste(var, (year(monthly_seq)), 
                        ifelse(nchar((month(monthly_seq))) == 1, 
                               paste0("0", (month(monthly_seq))), 
                               (month(monthly_seq))),
                        sep = "_")
  rbrck <- mask(rbrck, mask)
  return(rbrck)
}      

netcdf_nc_import_dtm <- function(y, mask, fun_dm) {
  ### A function to import NetCDF and create a raster brick of monthly climate data
  # input: 
  #  - y : a data list of all NetCDF ys
  #  - mask : CONUS shapefile projected in WGS84
  # output:
  #  - rbrck: a raster brick of all processed months
  require(dplyr)
  
  y_split <- y %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- y_split[1]
  year <- substr(y_split[2], start = 1, stop = 4)
  
  require(lubridate)
  start_date <- as.Date(paste(year, "01", "01", sep = "-"))
  end_date <- as.Date(paste(year, "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day") %m+% years(1)
  monthly_seq <- seq(start_date, end_date, by = "1 month")
  
  require(ncdf4)
  nc <- nc_open(y)
  nc_att <- attributes(nc$var)$names
  ncvar <- ncvar_get(nc, nc_att)
  tvar <- aperm(ncvar, c(3,2,1))
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0 "
  
  require(raster)
  rbrck <- brick(tvar, crs= proj)
  rm(list = c("ncvar", "tvar", "nc"))
  extent(rbrck) <- c(-124.793, -67.043, 25.04186, 49.41686)
  rbrck <- stackApply(rbrck, month(date_seq), fun = fun_dm)
  names(rbrck) <- paste(var, (year(monthly_seq)), 
                        ifelse(nchar((month(monthly_seq))) == 1, 
                               paste0("0", (month(monthly_seq))), 
                               (month(monthly_seq))),
                        sep = "_")
  rbrck <- mask(rbrck, mask)
  return(rbrck)
}    

netcdf_nc_import_ndap <- function(y, mask, fun_dm, percentile){
  ### A function to compute the number of days per month (typically sum) that exceeds a percentile threshold
  #   break out fires into small, med, large
  # input: 
  #  - y : a data list of all NetCDF ys
  #  - mask : CONUS shapefile projected in WGS84
  #  - fun.a : The type of aggregation to be done on the days that exceeds the threshold
  #  - percentile : the percentile threshold, numeric
  # output:
  #  - res: a raster brick of all processed months
  y_split <- y %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- y_split[1]
  year <- substr(y_split[2], start = 1, stop = 4)
  
  start_date <- as.Date(paste(year, "01", "01", sep = "-"))
  end_date <- as.Date(paste(year, "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day") %m+% years(1)
  monthly_seq <- seq(start_date, end_date, by = "1 month")
  
  require(ncdf4)
  nc <- nc_open(y)
  nc_att <- attributes(nc$var)$names
  ncvar <- ncvar_get(nc, nc_att)
  tvar <- aperm(ncvar, c(3,2,1))
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0"
  
  require(raster)
  rbrck <- brick(tvar, crs= proj)
  rm(list = c("ncvar", "tvar", "nc"))
  extent(rbrck) <- c(-124.793, -67.043, 25.04186, 49.41686)
  q <- calc(rbrck, fun = function(x) {x > quantile(x, probs = percentile, na.rm =TRUE)})
  rbrck <- stackApply(q, month(date_seq), fun = fun_dm)
  names(rbrck) <- paste(var, (year(monthly_seq)), 
                        ifelse(nchar((month(monthly_seq))) == 1, 
                               paste0("0", (month(monthly_seq))), 
                               (month(monthly_seq))),
                        sep = "_")
  rbrck <- mask(rbrck, mask)
  return(rbrck)
}


raster_nc_import <- function(y, mask, fun_dm){
  ### A function to import NetCDF and create monthly means from daily climate data
  # input: 
  #  - y : a data list of all NetCDF ys
  #  - mask : CONUS shapefile projected in WGS84
  #  - fun.dm : The type of aggregation to be done on the days that exceeds the threshold
  # output:
  #  - rbrck: a raster brick of all processed months
  y = fm100_dl[1]
  mask =  mask
  fun_dm = mean
  require(dplyr)
  y_split <- y %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- y_split[1]
  year <- substr(y_split[2], start = 1, stop = 4)

  require(lubridate)
  start_date <- as.Date(paste(year, "01", "01", sep = "-"))
  end_date <- as.Date(paste(year, "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day") %m+% years(1)
  monthly_seq <- seq(start_date, end_date, by = "1 month")
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0 "
  
  require(raster)
  rbrck <- brick(y, crs= proj)
  rbrck <- stackApply(rbrck, month(date_seq), fun = fun_dm)
  rbrck <- flip(t(rbrck), direction = "x")
  
  names(rbrck) <- paste(var, (year(monthly_seq)), 
                        ifelse(nchar((month(monthly_seq))) == 1, 
                               paste0("0", (month(monthly_seq))), 
                               (month(monthly_seq))),
                        sep = "_")
  rbrck <- mask(rbrck, mask)
  return(rbrck)
}

raster_nc_import_ndap <- function(y, mask, fun_dm, percentile){
  ### A function to compute the number of days per month (typically sum) that exceeds a percentile threshold
  #   break out fires into small, med, large
  # input: 
  #  - y : a data list of all NetCDF ys
  #  - mask : CONUS shapefile projected in WGS84
  #  - fun.a : The type of aggregation to be done on the days that exceeds the threshold
  #  - percentile : the percentile threshold, numeric
  # output:
  #  - res: a raster brick of all processed months
  y_split <- y %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- y_split[1]
  year <- substr(y_split[2], start = 1, stop = 4)
  
  start_date <- as.Date(paste(year, "01", "01", sep = "-"))
  end_date <- as.Date(paste(year, "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day") %m+% years(1)
  monthly_seq <- seq(start_date, end_date, by = "1 month")
  
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0 "
  rbrck <- brick(y, crs= proj)
  rbrck <- calc(rbrck, fun = function(x){x > quantile(x, probs = percentile, na.rm =TRUE)})
  rbrck <- stackApply(rbrck, month(date_seq), fun = fun_dm)  # remove unneeded object for memory conservation
  rbrck <- flip(t(rbrck), direction = "x")
  names(rbrck) <- paste(var, (year(monthly_seq)), 
                      ifelse(nchar((month(monthly_seq))) == 1, 
                             paste0("0", (month(monthly_seq))), 
                             (month(monthly_seq))),
                      sep = "_")
  rbrck <- mask(rbrck, mask)
  return(rbrck)
}



raster_as <- function(y, var, fun, start, end){
  ### A function to computes the metric of previous 12 months. For instance, this could be sum or mean 
  #   precipitation over the previous 12 months (e.g., fuel accumulation proxy)
  # input: 
  #  - x : a data list of all NetCDF files
  #  - fun.a : The type of aggregation to be done to aggregate monthly to previous 1 year
  # output:
  #  - lagged_vals: a raster brick of all processed months
  
  start_date <- as.Date(paste(start, "01", "01", sep = "-"))
  end_date <- as.Date(paste(end, "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day") %m+% years(1)
  monthly_seq <- seq(start_date, end_date, by = "1 month")
  ym <- year(monthly_seq)
  mm <- month(monthly_seq)
  
  n_layers <- length(names(y))
  lag <- 12
  lagged_vals <- list()
  counter <- 1
  pb <- txtProgressBar(max = n_layers, style = 3)
  for (i in 1:n_layers) {
    if (i > lag) {
      raster_subset <- subset(y, (i - lag):(i - 1))
      if(fun == "mean") {
        lagged_vals[[counter]] <- mean(raster_subset)
      } else if (fun == "sum") {
        lagged_vals[[counter]] <- sum(raster_subset)
      } else {
        stop("Only sum() and mean() are supported")
      }
      names(lagged_vals)[counter] <- paste(var, ym, 
                                           ifelse(nchar(mm) == 1, 
                                                  paste0("0", mm), 
                                                  mm),
                                           sep = "_")
      counter <- counter + 1
    }
    setTxtProgressBar(pb, i)
  }
  
  # convert the list of lagged summaries to a raster stack
  lagged_vals <- stack(lagged_vals)
}

write_out <- function(y, var, outfolder) {
  ### A function to iteratively create folders for the processed data and write out GeoTifs
  # input: 
  #  - y : a raster stack of the timeseries
  #  - var : the variable you want to write out
  #  - outfolder: the name of the folder you want the data to be written to
  dir.create(paste0(dirc, dir_proc, var), showWarnings = FALSE)
  dir.create(paste0(dirc, dir_proc, var, "/", outfolder), showWarnings = FALSE)
  out <- paste0(dirc, dir_proc, var,  "/", outfolder, "/")
  writeRaster(y, filename = paste0(out, names(y)),
              format = "GTiff", bylayer=TRUE, overwrite = TRUE)
  return(paste("File", names(y), "written"))
}


