
# # -----------------------------------------------------------------------
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

mask <- as(st_transform(usa_shp, proj_ll), "Spatial")

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
aet_dl <- list.files(paste0(dirc, dir_raw, "/aet"), pattern = "nc",
                     recursive = TRUE, full.names = TRUE)
def_dl <- list.files(paste0(dirc, dir_raw, "/def"), pattern = "nc",
                     recursive = TRUE, full.names = TRUE)
ffwi_dl <- list.files(paste0(dirc, dir_raw, "/ffwi"), pattern = "nc",
                      recursive = TRUE, full.names = TRUE)
fm100_dl <- list.files(paste0(dirc, dir_raw, "/fm100"), pattern = "nc",
                       recursive = TRUE, full.names = TRUE)
maxtmp_dl <- list.files(paste0(dirc, dir_raw, "/maxtmp"), pattern = "nc",
                        recursive = TRUE, full.names = TRUE)
mintmp_dl <- list.files(paste0(dirc, dir_raw, "/mintmp"), pattern = "nc",
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

# # -----------------------------------------------------------------------
# foreach for already monthly means ---------------------------------------
# Copy pasta central - try nested loops where the main input would be a list of the data lists that refer to a specific function
UseCores <- detectCores() - 2
cl <- makeCluster(UseCores)
registerDoParallel(cl)

# These data cannot be imported using the raster package.
# These data are already compiled monthly means from 1979-2016 in one NetCDF file
pdsi <- foreach(i = 1:length(pdsi_dl)) %dopar% {
  netcdf_nc_import(pdsi_dl[i], mask)}
pdsi <- do.call(stack, pdsi)
pdsi_out <- write_out(pdsi, "pdsi", "monthly_mean")

aet <- foreach(i = 1:length(aet_dl)) %dopar% {
  netcdf_nc_import(aet_dl[i], mask)}
aet <- do.call(stack, aet)
aet_out <- write_out(aet, "aet", "monthly_mean")

def <- foreach(i = 1:length(def_dl)) %dopar% {
  netcdf_nc_import(def_dl[i], mask)}
def <- do.call(stack, def)
def_out <- write_out(def, "def", "monthly_mean")

vpd <- foreach(i = 1:length(vpd_dl)) %dopar% {
  netcdf_nc_import(vpd_dl[i], mask)}
vpd <- do.call(stack, vpd)
vpd_out <- write_out(vpd, "vpd", "monthly_mean")

stopCluster(cl)

ffwi_mean <- lapply(ffwi_dl, netcdf_nc_import_dtm, mask =  mask, fun_dm = mean)
ffwi_mean <- do.call(stack, ffwi_mean)
ffwi_out <- write_out(ffwi_mean, "ffwi", "monthly_mean")

ffwi_sd <- lapply(ffwi_dl, netcdf_nc_import_dtm, mask =  mask, fun_dm = sd)
ffwi_sd <- do.call(stack, ffwi_sd)
ffwi_out <- write_out(ffwi_sd, "ffwi", "std_dev")

ffwi_90pct <- lapply(ffwi_dl, netcdf_nc_import_ndap, mask =  mask, fun_dm = mean, percentile = 0.90)
ffwi_90pct <- do.call(stack, ffwi_90pct)
ffwi_out <- write_out(ffwi_90pct, "ffwi", "mean_days_above_90th_percentile")
rm(ffwi_90pct)

ffwi_95pct <- lapply(ffwi_dl, netcdf_nc_import_ndap, mask =  mask, fun_dm = mean, percentile = 0.95)
ffwi_95pct <- do.call(stack, ffwi_95pct)
ffwi_out <- write_out(ffwi_95pct, "ffwi", "mean_days_above_95th_percentile")
rm(ffwi_95pct)

ffwi_90ndap <- lapply(ffwi_dl, netcdf_nc_import_ndap, mask =  mask, fun_dm = sum, percentile = 0.90)
ffwi_90ndap <- do.call(stack, ffwi_90ndap)
ffwi_90_out <- write_out(ffwi_90ndap, "ffwi", "sum_days_above_90th_percentile")
rm(ffwi_90ndap)

ffwi_95ndap <- lapply(ffwi_dl, netcdf_nc_import_ndap, mask =  mask, fun_dm = sum, percentile = 0.95)
ffwi_95ndap <- do.call(stack, ffwi_95ndap)
ffwi_95_out <- write_out(ffwi_95ndap, "ffwi", "sum_days_above_95th_percentile")
rm(ffwi_95ndap)
# # -----------------------------------------------------------------------
# attempting to clean the copy pasta --------------------------------------
UseCores <- detectCores() - 2
cl <- makeCluster(UseCores)
registerDoParallel(cl)

mean_agg <- list(ffwi_dl, maxtmp_dl)

test_lp <- lapply(mean_agg, raster_nc_import, mask =  mask, fun_dm = mean)

test <- foreach(i = 1:length(mean_agg)) %dopar% {
  raster_nc_import(mean_agg[[i]][i], mask, mean)
}

stopCluster(cl)

# # -----------------------------------------------------------------------
# foreach copy pasta ------------------------------------------------------

ffwi_mean <- foreach(i = 1:length(ffwi_dl)) %dopar% {
  raster_nc_import(ffwi_dl[i], mask, mean)}
fm100_mean <- foreach(i = 1:length(fm100_dl)) %dopar% {
  raster_nc_import(fm100_dl[i], mask, mean)}
maxtmp_mean <- foreach(i = 1:length(maxtmp_dl)) %dopar% {
  raster_nc_import(maxtmp_dl[i], mask, mean)}
precip_mean <- foreach(i = 1:length(precip_dl)) %dopar% {
  raster_nc_import(precip_dl[i], mask, mean)}
windsp_mean <- foreach(i = 1:length(windsp_dl)) %dopar% {
  raster_nc_import(windsp_dl[i], mask, mean)}

ffwi_sd <- foreach(i = 1:length(ffwi_dl)) %dopar% {
  raster_nc_import(ffwi_dl[i], mask, sd)}
fm100_sd <- foreach(i = 1:length(fm100_dl)) %dopar% {
  raster_nc_import(fm100_dl[i], mask, sd)}
maxtmp_sd <- foreach(i = 1:length(maxtmp_dl)) %dopar% {
  raster_nc_import(maxtmp_dl[i], mask, sd)}
precip_sd <- foreach(i = 1:length(precip_dl)) %dopar% {
  raster_nc_import(precip_dl[i], mask, sd)}
windsp_sd <- foreach(i = 1:length(windsp_dl)) %dopar% {
  raster_nc_import(windsp_dl[i], mask, sd)}

ffwi_90pct <- foreach(i = 1:length(ffwi_dl)) %dopar% {
  raster_nc_import_pctile(ffwi_dl[i], mask, mean, 0.90)}
fm100_90pct <- foreach(i = 1:length(fm100_dl)) %dopar% {
  raster_nc_import_pctile(fm100_dl[i], mask, mean, 0.90)}
maxtmp_90pct <- foreach(i = 1:length(maxtmp_dl)) %dopar% {
  raster_nc_import_pctile(maxtmp_dl[i], mask, mean, 0.90)}
precip_90pct <- foreach(i = 1:length(precip_dl)) %dopar% {
  raster_nc_import_pctile(precip_dl[i], mask, mean, 0.90)}
windsp_90pct <- foreach(i = 1:length(windsp_dl)) %dopar% {
  raster_nc_import_pctile(windsp_dl[i], mask, mean, 0.90)}

ffwi_95pct <- foreach(i = 1:length(ffwi_dl)) %dopar% {
  raster_nc_import_pctile(ffwi_dl[i], mask, mean, 0.95)}
fm100_95pct <- foreach(i = 1:length(fm100_dl)) %dopar% {
  raster_nc_import_pctile(fm100_dl[i], mask, mean, 0.95)}
maxtmp_95pct <- foreach(i = 1:length(maxtmp_dl)) %dopar% {
  raster_nc_import_pctile(maxtmp_dl[i], mask, mean, 0.95)}
precip_95pct <- foreach(i = 1:length(precip_dl)) %dopar% {
  raster_nc_import_pctile(precip_dl[i], mask, mean, 0.95)}
windsp_95pct <- foreach(i = 1:length(windsp_dl)) %dopar% {
  raster_nc_import_pctile(windsp_dl[i], mask, mean, 0.95)}

ffwi_90ndap <- foreach(i = 1:length(ffwi_dl)) %dopar% {
  raster_nc_import_ndap(ffwi_dl[i], mask, sum, 0.90)}
fm100_90ndap <- foreach(i = 1:length(fm100_dl)) %dopar% {
  raster_nc_import_ndap(fm100_dl[i], mask, sum, 0.90)}
maxtmp_90ndap <- foreach(i = 1:length(maxtmp_dl)) %dopar% {
  raster_nc_import_ndap(maxtmp_dl[i], mask, sum, 0.90)}
precip_90ndap <- foreach(i = 1:length(precip_dl)) %dopar% {
  raster_nc_import_ndap(precip_dl[i], mask, sum, 0.90)}
windsp_90ndap <- foreach(i = 1:length(windsp_dl)) %dopar% {
  raster_nc_import_ndap(windsp_dl[i], mask, sum, 0.90)}

ffwi_95ndap <- foreach(i = 1:length(ffwi_dl)) %dopar% {
  raster_nc_import_ndap(ffwi_dl[i], mask, sum, 0.95)}
fm100_95ndap <- foreach(i = 1:length(fm100_dl)) %dopar% {
  raster_nc_import_ndap(fm100_dl[i], mask, sum, 0.95)}
maxtmp_95ndap <- foreach(i = 1:length(maxtmp_dl)) %dopar% {
  raster_nc_import_ndap(maxtmp_dl[i], mask, sum, 0.95)}
precip_95ndap <- foreach(i = 1:length(precip_dl)) %dopar% {
  raster_nc_import_ndap(precip_dl[i], mask, sum, 0.95)}
windsp_95ndap <- foreach(i = 1:length(windsp_dl)) %dopar% {
  raster_nc_import_ndap(windsp_dl[i], mask, sum, 0.95)}


# # -----------------------------------------------------------------------
# lapply copy pasta --------------------------------------------------------
# These data can be imported using the raster package.
# These data are daily data from 1979-2016 in one NetCDF file per year


fm100_mean <- lapply(fm100_dl, raster_nc_import, mask =  mask, fun_dm = mean)
fm100_mean <- do.call(stack, fm100_mean)
fm100_out <- write_out(fm100_mean, "fm100", "monthly_mean")
rm(fm100_mean)

maxtmp_mean <- lapply(maxtmp_dl, raster_nc_import, mask =  mask, fun_dm = mean)
maxtmp_mean <- do.call(stack, maxtmp_mean)
maxtmp_out <- write_out(maxtmp_mean, "maxtmp", "monthly_mean")
rm(maxtmp_mean)

windsp_mean <- lapply(windsp_dl, raster_nc_import, mask =  mask, fun_dm = mean)
windsp_mean <- do.call(stack, windsp_mean)
windsp_out <- write_out(windsp_mean, "windsp", "monthly_mean")
rm(windsp_mean)

precip_mean <- lapply(precip_dl, raster_nc_import, mask =  mask, fun_dm = mean)
precip_mean <- do.call(stack, precip_mean)

precip_asum <- raster_as(precip_mean, "precip", "sum", "1979", "2016")
precip_out <- write_out(precip_asum, "precip", "sum_previous_12mnth")
rm(precip_asum)

precip_amean <- raster_as(precip_mean, "precip", "mean", "1979", "2016")
precip_out <- write_out(precip_amean, "precip", "mean_previous_12mnth")
rm(precip_amean)

precip_out <- write_out(precip_mean, "precip", "monthly_mean")
rm(precip_mean)

fm100_sd <- lapply(fm100_dl, raster_nc_import, mask =  mask, fun_dm = sd)
fm100_sd <- do.call(stack, fm100_sd)
fm100_out <- write_out(fm100_sd, "fm100", "std_dev")
rm(fm100_sd)

maxtmp_sd <- lapply(maxtmp_dl, raster_nc_import, mask =  mask, fun_dm = sd)
maxtmp_sd <- do.call(stack, maxtmp_sd)
maxtmp_out <- write_out(maxtmp_sd, "maxtmp", "std_dev")
rm(maxtmp_sd)

windsp_sd <- lapply(windsp_dl, raster_nc_import, mask =  mask, fun_dm = sd)
windsp_sd <- do.call(stack, windsp_sd)
windsp_out <- write_out(windsp_sd, "windsp", "std_dev")
rm(windsp_sd)

precip_sd <- lapply(precip_dl, raster_nc_import, mask =  mask, fun_dm = sd)
precip_sd <- do.call(stack, precip_sd)
precip_out <- write_out(precip_sd, "precip", "std_dev")
rm(precip_sd)


fm100_90pct <- lapply(fm100_dl, raster_nc_import_ndap, mask =  mask, 
                      fun_dm = mean, percentile = 0.90)
fm100_90pct <- do.call(stack, fm100_90pct)
fm100_out <- write_out(fm100_90pct, "fm100", "mean_days_above_90th_percentile")
rm(fm100_90pct)

maxtmp_90pct <- lapply(maxtmp_dl, raster_nc_import_ndap, mask =  mask, 
                       fun_dm = mean, percentile = 0.90)
maxtmp_90pct <- do.call(stack, maxtmp_90pct)
maxtmp_out <- write_out(maxtmp_90pct, "maxtmp", "mean_days_above_90th_percentile")
rm(maxtmp_90pct)

precip_90pct <- lapply(precip_dl, raster_nc_import_ndap, mask =  mask, 
                       fun_dm = mean, percentile = 0.90)
precip_90pct <- do.call(stack, precip_90pct)
precip_out <- write_out(precip_90pct, "precip", "mean_days_above_90th_percentile")
rm(precip_90pct)

windsp_90pct <- lapply(windsp_dl, raster_nc_import_ndap, mask =  mask, 
                       fun_dm = mean, percentile = 0.90)
windsp_90pct <- do.call(stack, windsp_90pct)
windsp_out <- write_out(windsp_90pct, "windsp", "mean_days_above_90th_percentile")
rm(windsp_90pct)


fm100_95pct <- lapply(fm100_dl, raster_nc_import_ndap, mask =  mask, 
                      fun_dm = mean, percentile = 0.95)
fm100_95pct <- do.call(stack, fm100_95pct)
fm100_out <- write_out(fm100_95pct, "fm100", "mean_days_above_95th_percentile")

maxtmp_95pct <- lapply(maxtmp_dl, raster_nc_import_ndap, mask =  mask, 
                       fun_dm = mean, percentile = 0.95)
maxtmp_95pct <- do.call(stack, maxtmp_95pct)
maxtmp_out <- write_out(maxtmp_95pct, "maxtmp", "mean_days_above_95th_percentile")
rm(maxtmp_95pct)

precip_95pct <- lapply(precip_dl, raster_nc_import_ndap, mask =  mask, 
                       fun_dm = mean, percentile = 0.95)
precip_95pct <- do.call(stack, precip_95pct)
precip_out <- write_out(precip_95pct, "precip", "mean_days_above_95th_percentile")
rm(precip_95pct)

windsp_95pct <- lapply(windsp_dl, raster_nc_import_ndap, mask =  mask, 
                       fun_dm = mean, percentile = 0.95)
windsp_95pct <- do.call(stack, windsp_95pct)
windsp_out <- write_out(windsp_95pct, "windsp", "mean_days_above_95th_percentile")
rm(windsp_95pct)

fm100_90ndap <- lapply(fm100_dl, raster_nc_import_ndap, mask =  mask, fun_dm = mean, percentile = 0.90)
fm100_90ndap <- do.call(stack, fm100_90ndap)
fm100_90_out <- write_out(fm100_90ndap, "fm100", "sum_days_above_90th_percentile")

maxtmp_90ndap <- lapply(maxtmp_dl, raster_nc_import_ndap, mask =  mask, fun_dm = mean, percentile = 0.90)
maxtmp_90ndap <- do.call(stack, maxtmp_90ndap)
maxtmp_90_out <- write_out(maxtmp_90ndap, "maxtmp", "sum_days_above_90th_percentile")

precip_90ndap <- lapply(precip_dl, raster_nc_import_ndap, mask =  mask, fun_dm = mean, percentile = 0.90)
precip_90ndap <- do.call(stack, precip_90ndap)
precip_90_out <- write_out(precip_90ndap, "precip", "sum_days_above_90th_percentile")

windsp_90ndap <- lapply(windsp_dl, raster_nc_import_ndap, mask =  mask, fun_dm = mean, percentile = 0.90)
windsp_90ndap <- do.call(stack, windsp_90ndap)
windsp_90_out <- write_out(windsp_90ndap, "windsp", "sum_days_above_90th_percentile")


fm100_95ndap <- lapply(fm100_dl, raster_nc_import_ndap, mask =  mask, fun_dm = mean, percentile = 0.95)
fm100_95ndap <- do.call(stack, fm100_95ndap)
fm100_95_out <- write_out(fm100_95ndap, "fm100", "sum_days_above_95th_percentile")

maxtmp_95ndap <- lapply(maxtmp_dl, raster_nc_import_ndap, mask =  mask, fun_dm = mean, percentile = 0.95)
maxtmp_95ndap <- do.call(stack, maxtmp_95ndap)
maxtmp_95_out <- write_out(maxtmp_95ndap, "maxtmp", "sum_days_above_95th_percentile")

precip_95ndap <- lapply(precip_dl, raster_nc_import_ndap, mask =  mask, fun_dm = mean, percentile = 0.95)
precip_95ndap <- do.call(stack, precip_95ndap)
precip_95_out <- write_out(precip_95ndap, "precip", "sum_days_above_95th_percentile")

windsp_95ndap <- lapply(windsp_dl, raster_nc_import_ndap, mask =  mask, fun_dm = mean, percentile = 0.95)
windsp_95ndap <- do.call(stack, windsp_95ndap)
windsp_95_out <- write_out(windsp_95ndap, "windsp", "sum_days_above_95th_percentile")


stopCluster(cl)

# # -----------------------------------------------------------------------
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