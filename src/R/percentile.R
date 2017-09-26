# Monthly mean above 90th percentile
if(!file.exists(file.path(dir_90th, paste0(var, "_", year, "_90th",".tif")))){
  mean_90thpct <- stackApply(raster, month_seq, fun = mean)
  test <- calc(mean_90thpct, forceapply = TRUE, 
               fun = function(x) {quantile(x, probs = 0.90, na.rm = TRUE) })
  
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
  mean_95thpct <- calc(raster, fun = function(x){ quantile(x, probs = 0.95, na.rm = TRUE)})
  mean_95thpct <- stackApply(mean_95thpct, month_seq, fun = mean)
  mean_95thpct <- flip(t(mean_95thpct), direction = "x")
  names(mean_95thpct) <- paste(var, year,
                               unique(month(date_seq, label = TRUE)),
                               sep = "_")
  p4string <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  projection(mean_95thpct) <- CRS(p4string)
  mean_95thpct <- mask(mean_95thpct, mask)
  writeRaster(mean_95thpct, filename = file.path(dir_95th,  paste0(var, "_", year, "_95th",".tif")),
              format = "GTiff") 
  rm(mean_95thpct) }

