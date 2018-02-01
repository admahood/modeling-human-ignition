fpa_fire # the shapefile

fpa_monthly <- stack()
causes <- c("Human", "Lightning")

for(i in 1992:2015) {
  for(j in 1:12){
    subed <- filter(fpa_clean$year == i & fpa_clean$month == j)
    rst <- rasterize(as(subed, "Spatial"), elevation, "fpa_bool") %>%
        crop(as(usa_shp, "Spatial")) %>%
        mask(as(usa_shp, "Spatial"))
    assign(rst, paste0("fpa_", i , "_", j))
    fpa_monthly <- stack(fpa_monthly, rst)
  }
  
  filename <- filepath(fire_dir, paste0("fpa_", i , "_", j, ".tif"))
  raster::writeRaster(fpa_monthly, filename, format = "GTiff")
  }
