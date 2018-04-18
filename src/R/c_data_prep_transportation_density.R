
unique_states <- unique(hexnet_4k$STUSPS)

grid_list <- split_fast_tibble(hexnet_4k, hexnet_4k$STUSPS)
line_list <- split_fast_tibble(rail_rds, rail_rds$STUSPS)

railroad_density <- get_density(unique_states, grid_list, line_list)

st_write(railroad_density, file.path(transportation_density_dir, "railroad_density.gpkg"))

system(paste0("aws s3 sync ",
              processed_dir, " ",
              s3_proc_prefix))


line_list <- split_fast_tibble(tl, tl$STUSPS)

transmission_lines_density <- get_density(unique_states, grid_list, line_list)

st_write(transmission_lines_density, file.path(transportation_density_dir, "transmission_lines_density.gpkg"))

system(paste0("aws s3 sync ",
              processed_dir, " ",
              s3_proc_prefix))

line_list <- split_fast_tibble(primary_rds, primary_rds$STUSPS)

primary_rds_density <- get_density(unique_states, grid_list, line_list)

st_write(primary_rds_density, file.path(transportation_density_dir, "primary_rds_density.gpkg"))

system(paste0("aws s3 sync ",
              processed_dir, " ",
              s3_proc_prefix))


line_list <- split_fast_tibble(secondary_rds, secondary_rds$STUSPS)

secondary_rds_density <- get_density(unique_states, grid_list, line_list)

st_write(secondary_rds_density, file.path(transportation_density_dir, "secondary_rds_density.gpkg"))

system(paste0("aws s3 sync ",
              processed_dir, " ",
              s3_proc_prefix))


line_list <- split_fast_tibble(tertiary_rds, tertiary_rds$STUSPS)

tertiary_rds_density <- get_density(unique_states, grid_list, line_list)

st_write(tertiary_rds_density, file.path(transportation_density_dir, "tertiary_rds_density.gpkg"))

system(paste0("aws s3 sync ",
              processed_dir, " ",
              s3_proc_prefix))