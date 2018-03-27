

if (!file.exists(file.path(transportation_density_dir, "railroad_density.gpkg"))) {

  rail_list <- split_fast_tibble(rail_rds, rail_rds$STUSPS)

  railroad_density <- get_density(unique_states, hex_list, rail_list, ncores = detectCores())

  st_write(railroad_density, file.path(transportation_density_dir, "railroad_density.gpkg"))

  system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))

}

if (!file.exists(file.path(transportation_density_dir, "transmission_lines_density.gpkg"))) {

  transmission_lines_list <- split_fast_tibble(tl, tl$STUSPS)

  transmission_lines_density <- get_density(unique_states, hex_list, transmission_lines_list, ncores = detectCores())

  st_write(transmission_lines_density, file.path(transportation_density_dir, "transmission_lines_density.gpkg"))

  system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))

}

if (!file.exists(file.path(transportation_density_dir, "primary_rds_density.gpkg"))) {

  primary_rds_list <- split_fast_tibble(primary_rds, primary_rds$STUSPS)

  primary_rds_density <- get_density(unique_states, hex_list, primary_rds_list, ncores = detectCores())

  st_write(primary_rds_density, file.path(transportation_density_dir, "primary_rds_density.gpkg"))

  system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))

}

if (!file.exists(file.path(transportation_density_dir, "secondary_rds_density.gpkg"))) {

  secondary_rds_list <- split_fast_tibble(secondary_rds, secondary_rds$STUSPS)

  secondary_rds_density <- get_density(unique_states, hex_list, secondary_rds_list, ncores = detectCores())

  st_write(secondary_rds_density, file.path(transportation_density_dir, "secondary_rds_density.gpkg"))

  system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))

}

if (!file.exists(file.path(transportation_density_dir, "tertiary_rds_density.gpkg"))) {
  num_cores <- 4 #parallel::detectCores()

  hexnet_4k <- hexnet_4k %>%
    mutate(sampled = sample(1:num_cores, nrow(.), replace = TRUE))

  if (!exists("tertiary_hex")) {
    if (!file.exists(file.path(transportation_processed_dir, "tertiary_rds_hex.gpkg"))) {

      tertiary_hex <- tertiary_rds %>%
        dplyr::select(LINEARID, STUSPS) %>%
        st_join(., hexnet_4k, join = st_intersects) %>%
        mutate(STUSPS = STUSPS.x) %>%
        dplyr::select(LINEARID, hexid4k, STUSPS, sampled)

      sf::st_write(tertiary_hex,
                   file.path(transportation_processed_dir, "tertiary_rds_hex.gpkg"),
                   driver = "GPKG")

      system(paste0("aws s3 sync ",
                    transportation_processed_dir, " ",
                    s3_proc_prefix, "transportation/processed"))
    } else {

      tertiary_hex <- sf::st_read(dsn = file.path(transportation_processed_dir, "tertiary_rds_hex.gpkg")) %>%
        as.data.frame() %>%
        dplyr::select(-sampled) %>%
        left_join(., as.data.frame(hexnet_4k), by = 'hexid4k') %>%
        dplyr::select(LINEARID, hexid4k, STUSPS.x, geom.x, sampled) %>%
        mutate(STUSPS = STUSPS.x,
               geom = geom.x) %>%
        dplyr::select(LINEARID, hexid4k, STUSPS, sampled, geom) %>%
        st_as_sf()

    }
  }

  hexnet_list <- hexnet_4k %>%
    split(., .$STUSPS)

  # sfInit(parallel = TRUE, cpus = num_cores)
  # sfExport('tertiary_hex')
  # sfSource('src/functions/helper_functions.R')

  tertiary_rds_density <- lapply(hexnet_list,
    function (input_list) {
      require(tidyverse)
      require(magrittr)
      require(lubridate)
      require(lubridate)
      require(sf)

      sub_grid <- dplyr:::bind_cols(input_list)
      unique_ids <- unique(sub_grid$hexid4k)
      state_name <- names(sub_grid)

      print(paste0('Working on ', state_name))

      got_density <- lapply(unique_ids,
        FUN = get_density,
        grids = sub_grid,
        lines = tertiary_hex)

      state_name <- names(got_density)
      print(paste0('Finishing ', state_name))

      print(paste0('Writing ', state_name))

      st_write(got_density, file.path(per_state, paste0('tertiary_density_', state_name, '.gpkg')))

      return(got_density)
    }
    )

  # sfStop()



  extraction_df <- flattenlist(tertiary_rds_density) %>%
    do.call(rbind, .)

  tertiary_rds_density <- get_density(unique_states, hex_list, tertiary_rds_list, ncores = 16)

  st_write(tertiary_rds_density, file.path(transportation_density_dir, "tertiary_rds_density.gpkg"))

  system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))
}
