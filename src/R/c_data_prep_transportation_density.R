
if (!file.exists(file.path(processed_dir, 'rail_rds_density_hex4k.gpkg'))) {

  # Create railroad density
  if (!exists('rail_rds_density')) {
    rail_rds_density <- hexnet_4k
  }

  rail_rds_density$density <- length_in_poly(spatial_lines = rail_rds,
                                             fishnet = rail_rds_density,
                                             ncores = 20,
                                             state_id = 'STUSPS',
                                             net_id = 'hexid')

  sf::st_write(rail_rds_density, filename = file.path(processed_dir, 'rail_rds_density_hex4k.gpkg'))

  system(paste0("aws s3 sync ",
                processed_dir, " ",
                s3_proc_prefix))

} else {

  rail_rds_density <- sf::st_read(file.path(processed_dir, "rail_rds_density_hex4k.gpkg"))
}

if (!file.exists(file.path(processed_dir, 'tranmission_lines_density_hex4k.gpkg'))) {

  # Create transmission line density
  if (!exists('rail_rds_density')) {
    tranmission_lines_density <- hexnet_4k
  }
  tranmission_lines_density$density <- length_in_poly(spatial_lines = tl,
                                                      fishnet = tranmission_lines_density,
                                                      ncores = 20)

  sf::st_write(tranmission_lines_density, filename = file.path(processed_dir, 'tranmission_lines_density_hex4k.gpkg'))

  system(paste0("aws s3 sync ",
                processed_dir, " ",
                s3_proc_prefix))

} else {

  tranmission_lines_density <- sf::st_read(file.path(processed_dir, "tranmission_lines_density_hex4k.gpkg"))
}


sne <- usa_shp %>%
  filter(STUSPS %in% c('RI', 'MA'))

rdensity <- st_intersection(hexnet_25k, (sne))
rail_rds_sne <- st_intersection(rail_rds, sne)

sub_fish <- length_in_poly(spatial_lines = rail_rds_sne,
                           fishnet = rdensity,
                           ncores = 32,
                           state_id = 'STUSPS',
                           net_id = 'hexid')
