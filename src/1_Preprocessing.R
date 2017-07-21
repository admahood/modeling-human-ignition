
# Liabaries --------------------------------------------------------------
library(raster)
library(ncdf4)
library(lubridate)
library(rgdal)
library(sf)
library(tidyverse)
library(doParallel)
library(foreach) 

# Directories -------------------------------------------------------------

dirc <- "/Volumes/LaCie-2TB/data"
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
  st_transform(., "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs") %>%
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
def_dl <- list.files(paste0(dirc, "/climate/raw/historical_gridmet/def"), pattern = "nc",
                      recursive = TRUE, full.names = TRUE)
ffwi_dl <- list.files(paste0(dirc, "/climate/raw/historical_gridmet/ffwi"), pattern = "nc",
                       recursive = TRUE, full.names = TRUE)
fm100_dl <- list.files(paste0(dirc, "/climate/raw/historical_gridmet/fm100"), pattern = "nc",
                      recursive = TRUE, full.names = TRUE)
maxtmp_dl <- list.files(paste0(dirc, "/climate/raw/historical_gridmet/maxtmp"), pattern = "nc",
                      recursive = TRUE, full.names = TRUE)
pdsi_dl <- list.files(paste0(dirc, "/climate/raw/historical_gridmet/pdsi"), pattern = "nc",
                      recursive = TRUE, full.names = TRUE)
pet_dl <- list.files(paste0(dirc, "/climate/raw/historical_gridmet/pet"), pattern = "nc",
                      recursive = TRUE, full.names = TRUE)
precip_dl <- list.files(paste0(dirc, "/climate/raw/historical_gridmet/precip"), pattern = "nc",
                      recursive = TRUE, full.names = TRUE)
vpd_dl <- list.files(paste0(dirc, "/climate/raw/historical_gridmet/vpd"), pattern = "nc",
                        recursive = TRUE, full.names = TRUE)
windsp_dl <- list.files(paste0(dirc, "/climate/raw/historical_gridmet/windsp"), pattern = "nc",
                        recursive = TRUE, full.names = TRUE)
tmp_dl <- list.files(paste0(dirc, "/climate/raw/historical_gridmet/tmp"), pattern = "nc",
                        recursive = TRUE, full.names = TRUE)
# Functions ---------------------------------------------------------------
write_out <- function(file, var, outfolder) {
  dir.create(paste0(dirc, dir_proc, var), showWarnings = FALSE)
  dir.create(paste0(dirc, dir_proc, var, "/", outfolder), showWarnings = FALSE)
  out <- paste0(dirc, dir_proc, var,  "/", outfolder, "/")
  writeRaster(file, filename = paste0(out, names(file)),
              format = "GTiff", bylayer=TRUE, overwrite = TRUE)
  return(paste("File", names(file), "written"))
}

netcdf_nc_import <- function(file, mask) {
  r <- raster(ncols = 585, nrows = 1386, res = 0.04166667)
  
  file_split <- file %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- file_split[1]
  year <- substr(file_split[2], start = 1, stop = 4)
  endyear <- ifelse(nchar(file_split[2]) > 7,
                    substr(file_split[2], start = 5, stop = 8), year)
  
  start_date <- as.Date(paste(year, "01", "01", sep = "-"))
  end_date <- as.Date(paste(ifelse(year == endyear, year, endyear), "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day") %m+% years(1)
  monthly_seq <- seq(start_date, end_date, by = "1 month")
  
  nc <- nc_open(file)
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

raster_nc_import <- function(file, mask, fun){
  file_split <- file %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- file_split[1]
  year <- substr(file_split[2], start = 1, stop = 4)
  
  start_date <- as.Date(paste(year, "01", "01", sep = "-"))
  end_date <- as.Date(paste(year, "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day") %m+% years(1)
  monthly_seq <- seq(start_date, end_date, by = "1 month")
  
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  rbrck <- brick(file, crs= proj)
  res <- stackApply(rbrck, month_seq, fun = fun)
  res <- flip(t(res), direction = "x")
  names(res) <- paste(var, year, unique(month(monthly_seq)), 
                      unique(month(monthly_seq, label = TRUE)),
                      sep = "_")
  res <- mask(res, mask)
  return(res)
}


raster_nc_import_ndap <- function(file, mask, fun, percentile){
### A function to compute the number of days per month (typically sum) that exceeds a percentile threshold
  #   break out fires into small, med, large
  # input: 
  #  - file : a data list of all NetCDF files
  #  - mask : CONUS shapefile projected in WGS84
  #  - fun : The type of aggregation to be done on the days that exceeds the threshold
  #  - percentile : the percentile threshold, numeric
  # output:
  #  - res: a raster brick of all processed months
  file_split <- file %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- file_split[1]
  year <- substr(file_split[2], start = 1, stop = 4)
  
  start_date <- as.Date(paste(year, "01", "01", sep = "-"))
  end_date <- as.Date(paste(year, "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day") %m+% years(1)
  monthly_seq <- seq(start_date, end_date, by = "1 month")
  
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  rbrck <- brick(file, crs= proj)
  q <- calc(rbrck, fun = function(x){x > quantile(x, probs = percentile, na.rm =TRUE)})
  res <- stackApply(q, month_seq, fun = fun)  # remove unneeded object for memory conservation
  res <- flip(t(res), direction = "x")
  names(res) <- paste(var, year, unique(month(monthly_seq)), 
                      unique(month(monthly_seq, label = TRUE)),
                      sep = "_")
  res <- mask(res, mask)
  return(res)
}

UseCores <- detectCores() -1
cl       <- makeCluster(UseCores)

# This parallelized code is equivilant to the lapply
pr <- foreach(i = 1:length(tmp_dl)) %dopar% {
  raster_nc_import(tmp_dl[i], as(usa_shp, "Spatial"), sum)}
pr <- do.call(stack, pr)
out <- write_out(pr, "tmp", "tmp_mean")








raster_nc_import_as <- function(file, mask, fun.dm){
  ### A function to computes the metric of previous 12 months. For instance, this could be sum or mean 
  #   precipitation over the previous 12 months (e.g., fuel accumulation proxy)
  # input: 
  #  - file : a data list of all NetCDF files
  #  - mask : CONUS shapefile projected in WGS84
  #  - fun.dm : The type of aggregation to be done to aggregate daily to monthly
  #  - fun.a : The type of aggregation to be done to aggregate monthly to previous 1 year
  # output:
  #  - lagged_vals: a raster brick of all processed months
  n_layers <- length(names(file))
  lag <- 12
  lagged_vals <- list()
  counter <- 1
  pb <- txtProgressBar(max = n_layers, style = 3)
  for (i in 1:n_layers) {
    if (i > lag) {
      lagged_vals[[counter]] <- sum(subset(r, (i - lag):(i - 1)))
      names(lagged_vals)[counter] <- paste("timestep", i, "lag", lag, sep = "_")
      counter <- counter + 1
    }
    setTxtProgressBar(pb, i)
  }
  
  lagged_vals <- stack(lagged_vals)
  names(lagged_vals) <- paste(var, year, unique(month(monthly_seq)), 
                              unique(month(monthly_seq, label = TRUE)),
                              sep = "_")
  lagged_vals <- mask(lagged_vals, mask)
  return(lagged_vals)
}

# This parallelized code is equivilant to the lapply
#Most likely we will have to import rasters at a monthly aggregate to run this..
pr2 <- lapply(tmp_dl, raster_nc_import_as, mask = as(usa_shp, "Spatial"), sum)

pr2 <- do.call(stack, pr2)
plot(pr[[6:9]], bty="n", box=FALSE)

out <- write_out(pr, "tmp", "tmp_mean")














pr <- foreach(i = 1:length(pdsi_dl)) %dopar% {
  netcdf_nc_import(pdsi_dl[i], as(usa_shp, "Spatial"))}
pr <- lapply(pdsi_dl, netcdf_nc_import, mask = as(usa_shp, "Spatial"))
pr <- do.call(stack, pr)
out <- write_out(pr, "pr", "tmp_mean")

plot(pr[[6:9]], bty="n", box=FALSE)


# Import NetCDF, process, Export as GTiff ---------------------------------
UseCores <- detectCores() -1
cl       <- makeCluster(UseCores)

ffwi_95 <- foreach(i = 1:length(ffwi_dl)) %dopar% {
  netcdf_to_tif_numabove95(ffwi_dl[i], usa_shp, "NumDaysAbove95th")}

ffwi_mean <-   foreach(i = 1:length(ffwi_dl)) %dopar% {
  netcdf_day_to_tif_month(ffwi_dl[i], usa_shp, "MonthlyMean")}

def_mean <- foreach(i = 1:length(ffwi_dl)) %dopar% {
  netcdf_month_to_tif_month(def_dl[i], usa_shp, "MonthlyMean")}

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