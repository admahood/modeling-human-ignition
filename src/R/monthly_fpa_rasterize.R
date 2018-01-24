fpa_fire # the shapefile

fpa_monthly <- stack()
for(i in 1992:2015) {
  #for(j in 1:12){
    subed <- filter(fpa_fire$ DISCOVERY_YEAR == i )#& fpa_fod$DISCOVERY_MONTH == j)
    rst <- rasterize(as(subed, "Spatial"), elevation, "fpa_bool") %>%
        crop(as(usa_shp, "Spatial")) %>%
        mask(as(usa_shp, "Spatial"))
    assign(rst, paste0("fpa_", i )) #, "_", j))
    fpa_monthly <- stack(fpa_monthly, rst)
  }
}
