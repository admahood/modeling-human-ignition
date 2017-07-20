
# Liabaries --------------------------------------------------------------
library(raster)
library(ncdf4)
library(lubridate)
library(rgdal)
library(tidyverse)
library(ClusterR)
library(doParallel)
library(foreach) 

# Directories -------------------------------------------------------------

raw_dirc <- "/Volumes/LaCie 3TB/data/climate/GridMET_Historic_Daily_1979_2016/"
dirc <- "/Users/NateM/Dropbox/Professional/RScripts/Mietkiewicz_etal_HumanIgnProb/data"
dir_proc <- "/processed/historical_gridmet/"

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

ecoreg <- readOGR(dsn = paste0(dirc, "/raw/us_eco_l3"), layer = "us_eco_l3")
ecoreg <- spTransform(ecoreg, CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")) %>%
  subset(., US_L3NAME == "Southern Rockies" |
            US_L3NAME == "Arizona/New Mexico Mountains" |
            US_L3NAME == "Arizona/New Mexico Plateau" |
            US_L3NAME == "Wasatch and Uinta Mountains" |
            US_L3NAME == "Colorado Plateaus")

# Data lists --------------------------------------------------------------

# NetCDF data lists
def_dl <- list.files(paste0(raw_dirc, "def"), pattern = "nc",
                      recursive = TRUE, full.names = TRUE)
ffwi_dl <- list.files(paste0(raw_dirc, "ffwi"), pattern = "nc",
                       recursive = TRUE, full.names = TRUE)

# Functions ---------------------------------------------------------------

netcdf_to_tif_numabove95 <- function(file, mask, outfolder) {
  file_split <- file %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- file_split[1]
  srtyear <- ifelse(nchar(file_split[2]) == 7 | nchar(file_split[2]) <= 7,
                    substr(file_split[2], start = 1, stop = 4), "NA")
  endyear <- ifelse(nchar(file_split[2]) > 7,
                    substr(file_split[2], start = 5, stop = 8), srtyear)
  
  start_date <- as.Date(paste(srtyear, "01", "01", sep = "-"))
  end_date <- as.Date(paste(endyear, "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day")
  monthly_seq <- seq(start_date, end_date, by = "1 month")
  month_seq <- month(date_seq)
  
  nc <- nc_open(file)
  nc_att <- attributes(nc$var)$names
  ncvar <- ncvar_get(nc, nc_att)
  # remove unneeded object for memory conservation
  rm(ncvar)
  gc()
  tvar <- aperm(ncvar, c(3,2,1))
  # remove unneeded object for memory conservation
  rm(tvar)
  gc()
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  rbrck <- brick(tvar, crs= proj)
  extent(rbrck) <- c(-124.772163391113, -67.06383005778, 25.0626894632975, 49.3960227966309) #from metadata in Arc or IDRISI, when you open .nc file

  q95 <- calc(rbrck, fun = function(x){x > quantile(x, probs = 0.90, na.rm =TRUE)})
  # remove unneeded object for memory conservation
  rm(rbrck)
  gc()
  res <- stackApply(q95, month_seq, fun = sum)
  # remove unneeded object for memory conservation
  rm(q95)
  gc()
  names(res) <- paste(var, unique(year(monthly_seq)), unique(month(monthly_seq)), 
                                unique(month(monthly_seq, label = TRUE)),
                                sep = "_")
  masked_res <- mask(res, mask)
  dir.create(paste0(dirc, dir_proc, var), showWarnings = FALSE)
  dir.create(paste0(dirc, dir_proc, var, "/", outfolder), showWarnings = FALSE)
  out <- paste0(dirc, dir_proc, var,  "/", outfolder, "/")
  writeRaster(masked_res, filename = paste0(out,names(masked_res)),
              format = "GTiff", bylayer=TRUE, overwrite = TRUE)
  return(paste("File", names(masked_res), "written"))
}

# If the data are in daily netcdf format this unpacks, aggregates by MEAN, and converts to tiff with a systematic naming convention
netcdf_day_to_tif_month <- function(file, mask, outfolder) {
  file_split <- file %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- file_split[1]
  year <- substr(file_split[2], start = 1, stop = 4)

  start_date <- as.Date(paste(year, "01", "01", sep = "-"))
  end_date <- as.Date(paste(year, "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day")
  monthly_seq <- seq(start_date, end_date, by = "1 month")
  month_seq <- month(date_seq)
  
  nc <- nc_open(file)
  nc_att <- attributes(nc$var)$names
  ncvar <- ncvar_get(nc, nc_att)
  # remove unneeded object for memory conservation
  rm(ncvar)
  gc()
  tvar <- aperm(ncvar, c(3,2,1))
  # remove unneeded object for memory conservation
  rm(tvar)
  gc()
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  rbrck <- brick(tvar, crs= proj)
  extent(rbrck) <- c(-124.772163391113, -67.06383005778, 25.0626894632975, 49.3960227966309) #from metadata in Arc or IDRISI, when you open .nc file

  res <- stackApply(rbrck, month_seq, fun = sum)
  # remove unneeded object for memory conservation
  rm(rbrck)
  gc()
  names(res) <- paste(var, unique(year(monthly_seq)), unique(month(monthly_seq)), 
                      unique(month(monthly_seq, label = TRUE)),
                      sep = "_")
  masked_res <- mask(res, mask)
  dir.create(paste0(dirc, dir_proc, var), showWarnings = FALSE)
  dir.create(paste0(dirc, dir_proc, var, "/", outfolder), showWarnings = FALSE)
  out <- paste0(dirc, dir_proc, var,  "/", outfolder, "/")
  writeRaster(masked_res, filename = paste0(out,names(masked_res)),
              format = "GTiff", bylayer=TRUE, overwrite = TRUE)
  return(paste("File", names(masked_res), "written"))
}

# If the data are in monthly netcdf format this unpacks and converts to tiff with a systematic naming convention
netcdf_month_to_tif_month <- function(file, mask, outfolder) {
  file_split <- file %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- file_split[1]
  srtyear <- ifelse(nchar(file_split[2]) == 7 | nchar(file_split[2]) <= 7,
                    substr(file_split[2], start = 1, stop = 4), "NA")
  endyear <- ifelse(nchar(file_split[2]) > 7,
                    substr(file_split[2], start = 5, stop = 8), srtyear)
  
  start_date <- as.Date(paste(srtyear, "01", "01", sep = "-"))
  end_date <- as.Date(paste(endyear, "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day")
  monthly_seq <- seq(start_date, end_date, by = "1 month")
  month_seq <- month(date_seq)
  
  nc <- nc_open(file)
  nc_att <- attributes(nc$var)$names
  ncvar <- ncvar_get(nc, nc_att)
  # remove unneeded object for memory conservation
  rm(ncvar)
  gc()
  tvar <- aperm(ncvar, c(3,2,1))
  # remove unneeded object for memory conservation
  rm(tvar)
  gc()
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  rbrck <- brick(tvar, crs= proj)
  extent(rbrck) <- c(-124.772163391113, -67.06383005778, 25.0626894632975, 49.3960227966309) #from metadata in Arc or IDRISI, when you open .nc file

  names(rbrck) <- paste(var, unique(year(monthly_seq)), unique(month(monthly_seq)), 
                      unique(month(monthly_seq, label = TRUE)),
                      sep = "_")
  masked_res <- mask(rbrck, mask)
  dir.create(paste0(dirc, dir_proc, var), showWarnings = FALSE)
  dir.create(paste0(dirc, dir_proc, var, "/", outfolder), showWarnings = FALSE)
  out <- paste0(dirc, dir_proc, var,  "/", outfolder, "/")
  writeRaster(masked_res, filename = paste0(out,names(masked_res)),
              format = "GTiff", bylayer=TRUE, overwrite = TRUE)
  return(paste("File", names(masked_res), "written"))
}

# for layers 13 onward, compute the mean or sum of the previous twelve layers
netcdf_month_sumperyear <- function(file, mask, outfolder) {
  file_split <- file %>%
    basename %>%
    strsplit(split = "_") %>%
    unlist
  var <- file_split[1]
  srtyear <- ifelse(nchar(file_split[2]) == 7 | nchar(file_split[2]) <= 7,
                    substr(file_split[2], start = 1, stop = 4), "NA")
  endyear <- ifelse(nchar(file_split[2]) > 7,
                    substr(file_split[2], start = 5, stop = 8), srtyear)
  
  start_date <- as.Date(paste(srtyear, "01", "01", sep = "-"))
  end_date <- as.Date(paste(endyear, "12", "31", sep = "-"))
  date_seq <- seq(start_date, end_date, by = "1 day")
  monthly_seq <- seq(start_date, end_date, by = "1 month")
  month_seq <- month(date_seq)
  
  nc <- nc_open(file)
  nc_att <- attributes(nc$var)$names
  ncvar <- ncvar_get(nc, nc_att)
  # remove unneeded object for memory conservation
  rm(ncvar)
  gc()
  tvar <- aperm(ncvar, c(3,2,1))
  # remove unneeded object for memory conservation
  rm(tvar)
  gc()
  proj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  rbrck <- brick(tvar, crs= proj)
  extent(rbrck) <- c(-124.772163391113, -67.06383005778, 25.0626894632975, 49.3960227966309) #from metadata in Arc or IDRISI, when you open .nc file
  
  n_layers <- length(names(rbrck))
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
  # remove unneeded object for memory conservation
  rm(rbrck)
  gc()
  # convert the list of lagged summaries to a raster stack
  lagged_vals <- stack(lagged_vals)
  names(lagged_vals) <- paste(var, unique(year(monthly_seq)), unique(month(monthly_seq)), 
                      unique(month(monthly_seq, label = TRUE)),
                      sep = "_")
  masked_res <- mask(lagged_vals, mask)
  # remove unneeded object for memory conservation
  rm(lagged_vals)
  gc()
  dir.create(paste0(dirc, dir_proc, var), showWarnings = FALSE)
  dir.create(paste0(dirc, dir_proc, var, "/", outfolder), showWarnings = FALSE)
  out <- paste0(dirc, dir_proc, var,  "/", outfolder, "/")
  writeRaster(masked_res, filename = paste0(out,names(masked_res)),
              format = "GTiff", bylayer=TRUE, overwrite = TRUE)
  return(paste("File", names(masked_res), "written"))
}

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

ffwi95_us <- stack(ffwi95th_proc_dl)
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