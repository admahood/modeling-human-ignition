
# Libaries --------------------------------------------------------------
library(raster)
library(ncdf4)
library(lubridate)
library(rgdal)
library(sf)
library(tidyverse)
library(doParallel)
library(foreach) 

# Directories -------------------------------------------------------------
UseCores <- detectCores() -1

dirc <- "/Volumes/LaCie-2TB/data"
dir_raw <- "/climate/raw/historical_gridmet"
dir_proc <- "/climate/processed/historical_gridmet/"

#EPSG:102003 USA_Contiguous_Albers_Equal_Area_Conic
proj_ea <- "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs"

#EPSG:102005 USA_Contiguous_Equidistant_Conic
proj_ed <- "+proj=eqdc +lat_0=39 +lon_0=-96 +lat_1=33 +lat_2=45 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs"

#WGS 84 the gridmet projection
proj_ll <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"

# Spatial masks -----------------------------------------------------------
#Import the USA States layer
usa_shp <- st_read(dsn = paste0(dirc, "/bounds/state"),
                   layer = "cb_2016_us_state_20m", quiet= TRUE) %>%
  st_transform(., proj_ea) %>%
  subset(., NAME != "Alaska" &
           NAME != "Hawaii" &
           NAME != "Puerto Rico") %>%
  mutate(area_m2 = as.numeric(st_area(geometry)),
         StArea_km2 = area_m2/1000000,
         group = 1) %>%
  st_simplify(., preserveTopology = TRUE)
plot(usa_shp[5])

ecoreg <- st_read(dsn = paste0(dirc, "/bounds/ecoregion/us_eco_l3"), layer = "us_eco_l3") %>%
  st_transform(., proj_ea) %>%
  subset(., US_L3NAME == "Southern Rockies" |
            US_L3NAME == "Arizona/New Mexico Mountains" |
            US_L3NAME == "Arizona/New Mexico Plateau" |
            US_L3NAME == "Wasatch and Uinta Mountains" |
            US_L3NAME == "Colorado Plateaus")
plot(ecoreg[5])

# Data lists --------------------------------------------------------------

# NetCDF data lists
def_dl <- list.files(paste0(dirc, dir_raw, "/def"), pattern = "nc",
                      recursive = TRUE, full.names = TRUE)
ffwi_dl <- list.files(paste0(dirc, dir_raw, "/ffwi"), pattern = "nc",
                       recursive = TRUE, full.names = TRUE)
fm100_dl <- list.files(paste0(dirc, dir_raw, "/fm100"), pattern = "nc",
                      recursive = TRUE, full.names = TRUE)
maxtmp_dl <- list.files(paste0(dirc, dir_raw, "/maxtmp"), pattern = "nc",
                      recursive = TRUE, full.names = TRUE)
pdsi_dl <- list.files(paste0(dirc, dir_raw, "/pdsi"), pattern = "nc",
                      recursive = TRUE, full.names = TRUE)
pet_dl <- list.files(paste0(dirc, dir_raw, "/pet"), pattern = "nc",
                      recursive = TRUE, full.names = TRUE)
precip_dl <- list.files(paste0(dirc, dir_raw, "/precip"), pattern = "nc",
                      recursive = TRUE, full.names = TRUE)
vpd_dl <- list.files(paste0(dirc, dir_raw, "/vpd"), pattern = "nc",
                        recursive = TRUE, full.names = TRUE)
windsp_dl <- list.files(paste0(dirc, dir_raw, "/windsp"), pattern = "nc",
                        recursive = TRUE, full.names = TRUE)
tmp_dl <- list.files(paste0(dirc, "/climate/raw/historical_gridmet/tmp"), pattern = "nc",
                        recursive = TRUE, full.names = TRUE)
# Functions ---------------------------------------------------------------
write_out <- function(y, var, outfolder) {
  dir.create(paste0(dirc, dir_proc, var), showWarnings = FALSE)
  dir.create(paste0(dirc, dir_proc, var, "/", outfolder), showWarnings = FALSE)
  out <- paste0(dirc, dir_proc, var,  "/", outfolder, "/")
  writeRaster(y, filename = paste0(out, names(y)),
              format = "GTiff", bylayer=TRUE, overwrite = TRUE)
  return(paste("File", names(y), "written"))
}

netcdf_nc_import <- function(y, mask) {
  ### A function to import NetCDF and create a raster brick of monthly climate data
  # input: 
  #  - y : a data list of all NetCDF ys
  #  - mask : CONUS shapefile projected in WGS84
  # output:
  #  - rbrck: a raster brick of all processed months
  y_split <- y %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- y_split[1]
  year <- substr(y_split[2], start = 1, stop = 4)
  endyear <- ifelse(nchar(y_split[2]) > 7,
                    substr(y_split[2], start = 5, stop = 8), year)
  
  start_date <- as.Date(paste(year, "01", "01", sep = "-"))
  end_date <- as.Date(paste(ifelse(year == endyear, year, endyear), "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day") %m+% years(1)
  monthly_seq <- seq(start_date, end_date, by = "1 month")
  
  nc <- nc_open(y)
  nc_att <- attributes(nc$var)$names
  ncvar <- ncvar_get(nc, nc_att)
  tvar <- aperm(ncvar, c(3,2,1))
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  rbrck <- brick(tvar, crs= proj)
  extent(rbrck) <- c(-124.793, -67.043, 25.04186, 49.41686)
  names(rbrck) <- paste(var, (year(monthly_seq)), 
                        ifelse(nchar((month(monthly_seq))) == 1, 
                               paste0("0", (month(monthly_seq))), 
                               (month(monthly_seq))),
                        (month(monthly_seq, label = TRUE)),
                        sep = "_")
  rm(nc); rm(ncvar)
  rbrck <- mask(rbrck, mask)
  return(rbrck)
}      

raster_nc_import <- function(y, mask, fun.dm){
  ### A function to import NetCDF and create monthly means from daily climate data
  # input: 
  #  - y : a data list of all NetCDF ys
  #  - mask : CONUS shapefile projected in WGS84
  #  - fun.dm : The type of aggregation to be done on the days that exceeds the threshold
  # output:
  #  - rbrck: a raster brick of all processed months
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
  
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  rbrck <- brick(y, crs= proj)
  rbrck <- stackApply(rbrck, month(date_seq), fun = fun.dm)
  rbrck <- flip(t(rbrck), direction = "x")
  names(rbrck) <- paste(var, (year(monthly_seq)), 
                        ifelse(nchar((month(monthly_seq))) == 1, 
                               paste0("0", (month(monthly_seq))), 
                               (month(monthly_seq))),
                        (month(monthly_seq, label = TRUE)),
                        sep = "_")
  rbrck <- mask(rbrck, mask)
  return(rbrck)
}

raster_as <- function(y, fun = "mean"){
  ### A function to computes the metric of previous 12 months. For instance, this could be sum or mean 
  #   precipitation over the previous 12 months (e.g., fuel accumulation proxy)
  # input: 
  #  - x : a data list of all NetCDF files
  #  - fun.a : The type of aggregation to be done to aggregate monthly to previous 1 year
  # output:
  #  - lagged_vals: a raster brick of all processed months
  
  n_layers <- length(names(y))
  lag <- 12
  lagged_vals <- list()
  counter <- 1
  pb <- txtProgressBar(max = n_layers, style = 3)
  for (i in 1:n_layers) {
    if (i > lag) {
      rater_subset <- subset(y, (i - lag):(i - 1))
      if(fun == "mean") {
        lagged_vals[[counter]] <- mean(rater_subset)
      } else if (fun == "sum") {
        lagged_vals[[counter]] <- sum(rater_subset)
      } else {
        stop("Only sum() and mean() are supported")
      }
      names(lagged_vals)[counter] <- paste("timestep", i, "lag", lag, sep = "_")
      counter <- counter + 1
    }
    setTxtProgressBar(pb, i)
  }
  
  # convert the list of lagged summaries to a raster stack
  lagged_vals <- stack(lagged_vals)
}

raster_nc_import_ndap <- function(y, mask, fun.a, percentile){
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
  
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  rbrck <- brick(y, crs= proj)
  q <- calc(rbrck, fun = function(x){x > quantile(x, probs = percentile, na.rm =TRUE)})
  res <- stackApply(q, month(date_seq), fun = fun.a)  # remove unneeded object for memory conservation
  res <- flip(t(res), direction = "x")
  names(res) <- paste(var, (year(monthly_seq)), 
                      ifelse(nchar((month(monthly_seq))) == 1, 
                             paste0("0", (month(monthly_seq))), 
                             (month(monthly_seq))),
                      (month(monthly_seq, label = TRUE)),
                      sep = "_")
  res <- mask(res, mask)
  return(res)
}

# Import NetCDF, process, Export as GTiff ---------------------------------
cl       <- makeCluster(UseCores)

# These data cannot be imported using the raster package.
# These data are already compiled monthly means from 1979-2016 in one NetCDF file
pdsi <- foreach(i = 1:length(pdsi_dl)) %dopar% {
  netcdf_nc_import(pdsi_dl[i], as(mask, "Spatial"))}
pdsi <- do.call(stack, pdsi)
pdsi <- write_out(pdsi, "pdsi", "monthly_mean")

aet <- foreach(i = 1:length(aet_dl)) %dopar% {
  netcdf_nc_import(aet_dl[i], as(mask, "Spatial"))}
aet <- do.call(stack, aet)
aet <- write_out(aet, "aet", "monthly_mean")

def <- foreach(i = 1:length(def_dl)) %dopar% {
  netcdf_nc_import(def_dl[i], as(mask, "Spatial"))}
def <- do.call(stack, def)
def <- write_out(def, "def", "monthly_mean")

vpd <- foreach(i = 1:length(vpd_dl)) %dopar% {
  netcdf_nc_import(vpd_dl[i], as(mask, "Spatial"))}
vpd <- do.call(stack, vpd)
vpd <- write_out(vpd, "vpd", "monthly_mean")

# These data can be imported using the raster package.
# These data are daily data from 1979-2016 in one NetCDF file per year

stopCluster(cl)

# Import GTiff and Cip to Southern Rockies --------------------------------

ffwi95th_proc_dl <- list.files(paste0(dirc, dir_proc, "ffwi/NumDaysAbove95th"), pattern = "tif",
                       recursive = TRUE, full.names = TRUE)

ras <- lapply(ffwi95th_proc_dl, raster) 
ffwi95_us <- stack(ras)
crp_ecreg <-  crop(ffwi95_us, ecoreg)
ffwi_95th_sw <- mask(crp_ecreg, ecoreg)

ffwimm_proc_dl <- list.files(paste0(dirc, dir_proc, "ffwi/MonthlyMean"), pattern = "tif",
                           recursive = TRUE, full.names = TRUE)

ffwimm_us <- stack(ffwimm_proc_dl)
crp_ecreg <-  crop(ffwimm_us, ecoreg)
ffwi_mm_sw <- mask(crp_ecreg, ecoreg)

defmm_proc_dl <- list.files(paste0(dirc, dir_proc, "def/MonthlyMean"), pattern = "tif",
                             recursive = TRUE, full.names = TRUE)

defmm_us <- stack(defmm_proc_dl)
crp_ecreg <-  crop(defmm_us, ecoreg)
defmm_sw <- mask(crp_ecreg, ecoreg)