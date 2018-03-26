

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

  hexnet_list <- hexnet_4k %>%
    mutate(sampled = sample(1:parallel::detectCores(), nrow(.), replace = TRUE)) %>%
    as.data.frame(.) %>%
    split_tibble_to_list(., .$sampled)

  sfInit(parallel = TRUE, cpus = parallel::detectCores())
  sfExport('tertiary_rds')
  sfSource('src/functions/helper_functions.R')

  tertiary_rds_density <- sfLapply(hexnet_list,
    function (input_list) {
      require(tidyverse)
      require(magrittr)
      require(lubridate)
      require(lubridate)
      require(sf)

      sub_tib <- do.call(cbind, input_list) %>%
        as_tibble %>%
        dplyr::select(-sampled)
      unique_ids <- unique(sub_tib$hexid4k)

      lapply(unique_ids,
        FUN = get_density,
        grids = sub_tib,
        lines = tertiary_rds)
        }
      )

  sfStop()

  tertiary_rds_density <- get_density(unique_states, hex_list, tertiary_rds_list, ncores = 16)

  st_write(tertiary_rds_density, file.path(transportation_density_dir, "tertiary_rds_density.gpkg"))

  system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))
}
