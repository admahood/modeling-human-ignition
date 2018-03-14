subset_by_state <- function(fishnet, state_id){
  
  unique_states <- unique(fishnet[[state_id]])
  sub_fish <- list()
  
  for (k in 1:length(unique_states)) {
    
    # create a subdataframe based on state subset
    sub_fish[[k]] <- subset(fishnet, fishnet[[state_id]] == unique_states[k]) %>%
      mutate(length_km2 = 0,
             pixel_area_km2 = 0,
             density = 0)
  }
  return(sub_fish)
}


length_in_poly <- function(spatial_lines, fishnet, grouping_var){
  spatial_lines <- sub_rail_ds[1]
  fishnet <- sub_hexnet[1]
  grouping_var <- 'STUSPS'
  sub_out <- fishnet %>%
    map(~mutate(bb = split(., FPA_ID) %>% map(st_intersection(spatial_lines))))
  
    rowwise() %>%
    st_intersection(spatial_lines) %>%
    group_by(grouping_var) %>%
    summarize(length_km2 = sum(st_length(x)) * 0.000001,
              pixel_area_km2 = as.numeric(st_area(st_geometry(x[i,]))) * 0.000001,
              density = length_km2/pixel_area_km2)
  
  return(sub_out)
  }


# Create railroad density
if (!file.exists(file.path(processed_dir, 'rail_rds_density_hex4k.gpkg'))) {

  sub_rail_ds <- subset_by_state(rail_rds, state_id = 'STUSPS')
  sub_hexnet <- subset_by_state(hexnet_4k, state_id = 'STUSPS')
  
  rail_rds_density$density <- length_in_poly(spatial_lines = rail_rds,
                                             fishnet = rail_rds_density)

  sf::st_write(rail_rds_density, filename = file.path(processed_dir, 'rail_rds_density_hex4k.gpkg'))

  system(paste0("aws s3 sync ",
                processed_dir, " ",
                s3_proc_prefix))

} else {

  rail_rds_density <- sf::st_read(file.path(processed_dir, "rail_rds_density_hex4k.gpkg"))
}

# Create transmission line density
if (!file.exists(file.path(processed_dir, 'tranmission_lines_density_hex4k.gpkg'))) {

  if (!exists('rail_rds_density')) {
    tranmission_lines_density <- hexnet_4k
  }
  tranmission_lines_density$density <- length_in_poly(spatial_lines = tl,
                                                      fishnet = tranmission_lines_density)

  sf::st_write(tranmission_lines_density, filename = file.path(processed_dir, 'tranmission_lines_density_hex4k.gpkg'))

  system(paste0("aws s3 sync ",
                processed_dir, " ",
                s3_proc_prefix))

} else {

  tranmission_lines_density <- sf::st_read(file.path(processed_dir, "tranmission_lines_density_hex4k.gpkg"))
}


# Create primary density
if (!file.exists(file.path(processed_dir, 'primary_roads_density_hex4k.gpkg'))) {
  
  if (!exists('primary_roads_density')) {
    primary_roads_density <- hexnet_4k
  }
  primary_roads_density$density <- length_in_poly(spatial_lines = primary_rds,
                                                  fishnet = primary_roads_density)
  
  sf::st_write(primary_roads_density, filename = file.path(processed_dir, 'primary_roads_density_hex4k.gpkg'))
  
  system(paste0("aws s3 sync ",
                processed_dir, " ",
                s3_proc_prefix))
  
} else {
  
  primary_roads_density <- sf::st_read(file.path(processed_dir, "primary_roads_density_hex4k.gpkg"))
}


# Create secondary density
if (!file.exists(file.path(processed_dir, 'secondary_roads_density_hex4k.gpkg'))) {
  
  if (!exists('secondary_roads_density')) {
    secondary_roads_density <- hexnet_4k
  }
  secondary_roads_density$density <- length_in_poly(spatial_lines = secondary_rds,
                                                  fishnet = secondary_roads_density)
  
  sf::st_write(secondary_roads_density, filename = file.path(processed_dir, 'secondary_roads_density_hex4k.gpkg'))
  
  system(paste0("aws s3 sync ",
                processed_dir, " ",
                s3_proc_prefix))
  
} else {
  
  secondary_roads_density <- sf::st_read(file.path(processed_dir, "secondary_roads_density_hex4k.gpkg"))
}