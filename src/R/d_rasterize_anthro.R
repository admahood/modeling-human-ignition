
# Create raster mask
# 2k Fishnet
fishnet_4k <- st_make_grid(usa_shp, cellsize = 4000, what = 'polygons') %>%
  st_sf('geometry' = ., data.frame('fishid4k' = 1:length(.))) %>%
  st_intersection(., st_union(usa_shp))

ras_mask <- raster(as(fishnet_2k, "Spatial"), res = 4000)

# Calculate distance to power lines
if (file.exists(file.path(anthro_dir, "dis_transmission_lines.tif"))) {

  dis_transmission_lines <- raster(file.path(anthro_dir, "dis_transmission_lines.tif")) 

  } else {
    tl <- st_read(file.path(anthro_dir, "tranmission_lns.gpkg"))
    
    sfInit(parallel = TRUE, cpus = ncor)
    sfExport(list = c("ncor", "usa_shp", "tl", "ras_mask"))
    tl_rst <- sfLapply(1:ncor, shp_rst, y = tl, lvl = "bool_tl", j = ras_mask)
    sfStop()
    dis_transmission_lines <- combine_rst(tl_rst) 
    writeRaster(dis_transmission_lines, filename = file.path(anthro_dir, paste0("dis_transmission_lines", ".tif")),
                format = "GTiff", overwrite=TRUE)
}

# Calculate distance to railroads
if (file.exists(file.path(anthro_dir, "dis_railroads.tif"))) {
  
  dis_railroads <- raster(file.path(anthro_dir, "dis_railroads.tif")) 
  
  } else {  sfInit(parallel = TRUE, cpus = ncor)
    rail_rds <- st_read(file.path(anthro_dir, "rail_rds.gpkg")) 
    
    sfExport(list = c("ncor", "usa_shp", "rail_rds", "ras_mask"))
    rail_rst <- sfLapply(1:ncor, shp_rst, y = rail_rds, lvl = "bool_rrds", j = ras_mask)
    sfStop()
    dis_railroads <- combine_rst(rail_rst)
    writeRaster(dis_railroads, filename = file.path(anthro_dir, paste0("dis_railroads", ".tif")),
                format = "GTiff")
}


# Calculate distance to primary roads
if (file.exists(file.path(anthro_dir, "dis_primary_rds.tif"))) {
  
  dis_primary_rds <- raster(file.path(anthro_dir, "dis_primary_rds.tif")) 
  
  } else {
    if (!exists("primary_rds")) {
      primary_rds <- st_read(file.path(anthro_dir, "primary_rds.gpkg"))
    }
    
    sfInit(parallel = TRUE, cpus = ncor)
    sfExport(list = c("ncor", "usa_shp", "primary_rds", "ras_mask"))
    prds_rst <- sfLapply(1:ncor, shp_rst, y = primary_rds, lvl = "bool_prds", j = ras_mask)
    sfStop()
    dis_primary_rds <- combine_rst(prds_rst)
    writeRaster(dis_primary_rds, filename = file.path(anthro_dir, paste0("dis_primary_rds", ".tif")),
                format = "GTiff", overwrite=TRUE)
  }

# Calculate distance to secondary roads
if (file.exists(file.path(anthro_dir, "dis_secondary_rds.tif"))) {
  
  dis_secondary_rds <- raster(file.path(anthro_dir, "dis_secondary_rds.tif")) 
  
  } else {
    if (! exists("secondary_rds")) {
      secondary_rds <- st_read(file.path(anthro_dir, "secondary_rds.gpkg"))
    }
    
    sfInit(parallel = TRUE, cpus = ncor)
    sfExport(list = c("ncor", "usa_shp", "secondary_rds", "ras_mask"))
    srds_rst <- sfLapply(1:ncor, shp_rst, y = secondary_rds, lvl = "bool_srds", j = ras_mask)
    sfStop()
    dis_secondary_rds <- combine_rst(srds_rst)
    writeRaster(dis_secondary_rds, filename = file.path(anthro_dir, paste0("dis_secondary_rds", ".tif")),
                format = "GTiff", overwrite=TRUE)
  }

# Calculate distance to primary and secondary roads
if (file.exists(file.path(anthro_dir, "dis_all_rds.tif"))) {
  
  dis_all_rds <- raster(file.path(anthro_dir, "dis_all_rds.tif")) 
  
  } else {
    if (!exists("all_rds")) {
      if (!exists("primary_rds")) {
        primary_rds <- st_read(file.path(anthro_dir, "primary_rds.gpkg"))
      }
      if (! exists("secondary_rds")) {
        secondary_rds <- st_read(file.path(anthro_dir, "secondary_rds.gpkg"))
      }
      
      psrds <- primary_rds %>%
        select(-starts_with("bool"))
      ssrds <- secondary_rds  %>%
        select(-starts_with("bool"))
      
      all_rds <- dplyr::bind_rows(psrds, ssrds) %>%
        mutate(bool_ards = 1)
    }
    
    sfInit(parallel = TRUE, cpus = ncor)
    sfExport(list = c("ncor", "usa_shp", "all_rds", "ras_mask"))
    ards_rst <- sfLapply(1:ncor, shp_rst, y = all_rds, lvl = "bool_ards", j = ras_mask)
    sfStop()
    dis_all_rds <- combine_rst(ards_rst)
    writeRaster(dis_all_rds, filename = file.path(anthro_dir, paste0("dis_all_rds", ".tif")),
                format = "GTiff", overwrite=TRUE)
  }



