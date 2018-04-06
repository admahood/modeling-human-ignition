num_cores <- parallel::detectCores()

hexnet_4k <- hexnet_4k %>%
  mutate(sampled = sample(1:num_cores, nrow(.), replace = TRUE))

hexnet_list <- hexnet_4k %>%
  split(., .$STUSPS)

if (!file.exists(file.path(transportation_density_dir, "railroad_density.gpkg"))) {
  if (!exists("railroad_hex")) {
    if (!file.exists(file.path(transportation_processed_dir, "railroad_hex.gpkg"))) {
      
      railroad_hex <- railroad %>%
        dplyr::select(LINEARID, STUSPS) %>%
        st_join(., hexnet_4k, join = st_intersects) %>%
        mutate(STUSPS = STUSPS.x) %>%
        dplyr::select(LINEARID, hexid4k, STUSPS)
      
      sf::st_write(railroad_hex,
                   file.path(transportation_processed_dir, "railroad_hex.gpkg"),
                   driver = "GPKG")
      
      system(paste0("aws s3 sync ",
                    transportation_processed_dir, " ",
                    s3_proc_prefix, "transportation/processed"))
    } else {
      
      railroad_hex <- sf::st_read(dsn = file.path(transportation_processed_dir, "railroad_hex.gpkg")) %>%
        as.data.frame() %>%
        left_join(., as.data.frame(hexnet_4k), by = 'hexid4k') %>%
        dplyr::select(LINEARID, hexid4k, STUSPS.x, geom.x, sampled) %>%
        mutate(STUSPS = STUSPS.x,
               geom = geom.x) %>%
        dplyr::select(LINEARID, hexid4k, STUSPS, sampled, geom) %>%
        st_as_sf()
      
    }
  }
  
  sfInit(parallel = TRUE, cpus = num_cores)
  sfExport('railroad_hex')
  sfSource('src/functions/helper_functions.R')
  
  railroad_density <- lapply(hexnet_list,
                                       function (input_list) {
                                         require(tidyverse)
                                         require(magrittr)
                                         require(lubridate)
                                         require(lubridate)
                                         require(sf)
                                         
                                         sub_grid <- dplyr:::bind_cols(input_list)
                                         unique_ids <- unique(sub_grid$hexid4k)
                                         state_name <- unique(sub_grid$STUSPS)[1]
                                         
                                         print(paste0('Working on ', state_name))
                                         got_density <- lapply(unique_ids,
                                                               FUN = get_density,
                                                               grids = sub_grid,
                                                               lines = railroad_hex)
                                         print(paste0('Finishing ', state_name))
                                         
                                         return(got_density)
                                       }
  )
  sfStop()
  
  railroad_density <- flattenlist(railroad_density) %>%
    do.call(rbind, .)

  st_write(railroad_density, file.path(transportation_density_dir, "railroad_density.gpkg"))

  system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))

} else {
  railroad_density <- st_read(file.path(transportation_density_dir, "railroad_density.gpkg"))
}

if (!file.exists(file.path(transportation_density_dir, "transmission_lines_density.gpkg"))) {
  if (!exists("transmission_lines_hex")) {
    if (!file.exists(file.path(transportation_processed_dir, "transmission_lines_hex.gpkg"))) {
      
      transmission_lines_hex <- transmission_lines %>%
        dplyr::select(LINEARID, STUSPS) %>%
        st_join(., hexnet_4k, join = st_intersects) %>%
        mutate(STUSPS = STUSPS.x) %>%
        dplyr::select(LINEARID, hexid4k, STUSPS)
      
      sf::st_write(transmission_lines_hex,
                   file.path(transportation_processed_dir, "transmission_lines_hex.gpkg"),
                   driver = "GPKG")
      
      system(paste0("aws s3 sync ",
                    transportation_processed_dir, " ",
                    s3_proc_prefix, "transportation/processed"))
    } else {
      
      transmission_lines_hex <- sf::st_read(dsn = file.path(transportation_processed_dir, "transmission_lines_hex.gpkg")) %>%
        as.data.frame() %>%
        left_join(., as.data.frame(hexnet_4k), by = 'hexid4k') %>%
        dplyr::select(LINEARID, hexid4k, STUSPS.x, geom.x, sampled) %>%
        mutate(STUSPS = STUSPS.x,
               geom = geom.x) %>%
        dplyr::select(LINEARID, hexid4k, STUSPS, sampled, geom) %>%
        st_as_sf()
      
    }
  }
  
  sfInit(parallel = TRUE, cpus = num_cores)
  sfExport('transmission_lines_hex')
  sfSource('src/functions/helper_functions.R')
  
  transmission_lines_density <- lapply(hexnet_list,
                                function (input_list) {
                                  require(tidyverse)
                                  require(magrittr)
                                  require(lubridate)
                                  require(lubridate)
                                  require(sf)
                                  
                                  sub_grid <- dplyr:::bind_cols(input_list)
                                  unique_ids <- unique(sub_grid$hexid4k)
                                  state_name <- unique(sub_grid$STUSPS)[1]
                                  
                                  print(paste0('Working on ', state_name))
                                  got_density <- lapply(unique_ids,
                                                        FUN = get_density,
                                                        grids = sub_grid,
                                                        lines = transmission_lines_hex)
                                  print(paste0('Finishing ', state_name))
                                  
                                  return(got_density)
                                }
  )
  sfStop()
  
  transmission_lines_density <- flattenlist(transmission_lines_density) %>%
    do.call(rbind, .)

  st_write(transmission_lines_density, file.path(transportation_density_dir, "transmission_lines_density.gpkg"))

  system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))

} else {
  transmission_lines_density <- st_read(file.path(transportation_density_dir, "transmission_lines_density.gpkg"))
}

if (!file.exists(file.path(transportation_density_dir, "primary_rds_density.gpkg"))) {
  
  if (!exists("primary_hex")) {
    if (!file.exists(file.path(transportation_processed_dir, "primary_rds_hex.gpkg"))) {
      
      primary_hex <- primary_rds %>%
        dplyr::select(LINEARID, STUSPS) %>%
        st_join(., hexnet_4k, join = st_intersects) %>%
        mutate(STUSPS = STUSPS.x) %>%
        dplyr::select(LINEARID, hexid4k, STUSPS)
      
      sf::st_write(primary_hex,
                   file.path(transportation_processed_dir, "primary_rds_hex.gpkg"),
                   driver = "GPKG")
      
      system(paste0("aws s3 sync ",
                    transportation_processed_dir, " ",
                    s3_proc_prefix, "transportation/processed"))
    } else {
      
      primary_hex <- sf::st_read(dsn = file.path(transportation_processed_dir, "primary_rds_hex.gpkg")) %>%
        as.data.frame() %>%
        left_join(., as.data.frame(hexnet_4k), by = 'hexid4k') %>%
        dplyr::select(LINEARID, hexid4k, STUSPS.x, geom.x, sampled) %>%
        mutate(STUSPS = STUSPS.x,
               geom = geom.x) %>%
        dplyr::select(LINEARID, hexid4k, STUSPS, sampled, geom) %>%
        st_as_sf()
      
    }
  }
  
  sfInit(parallel = TRUE, cpus = num_cores)
  sfExport('primary_hex')
  sfSource('src/functions/helper_functions.R')
  
  primary_rds_density <- lapply(hexnet_list,
                                 function (input_list) {
                                   require(tidyverse)
                                   require(magrittr)
                                   require(lubridate)
                                   require(lubridate)
                                   require(sf)
                                   
                                   sub_grid <- dplyr:::bind_cols(input_list)
                                   unique_ids <- unique(sub_grid$hexid4k)
                                   state_name <- unique(sub_grid$STUSPS)[1]
                                   
                                   print(paste0('Working on ', state_name))
                                   got_density <- lapply(unique_ids,
                                                         FUN = get_density,
                                                         grids = sub_grid,
                                                         lines = primary_hex)
                                   print(paste0('Finishing ', state_name))
                                   
                                   return(got_density)
                                   }
                                 )
  sfStop()
  
  primary_rds_density <- flattenlist(primary_rds_density) %>%
    do.call(rbind, .)

  st_write(primary_rds_density, file.path(transportation_density_dir, "primary_rds_density.gpkg"))

  system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))

} else {
  primary_rds_density <- st_read(file.path(transportation_density_dir, "primary_rds_density.gpkg"))
}

if (!file.exists(file.path(transportation_density_dir, "secondary_rds_density.gpkg"))) {
  if (!exists("secondary_hex")) {
    if (!file.exists(file.path(transportation_processed_dir, "secondary_rds_hex.gpkg"))) {
      
      secondary_hex <- secondary_rds %>%
        dplyr::select(LINEARID, STUSPS) %>%
        st_join(., hexnet_4k, join = st_intersects) %>%
        mutate(STUSPS = STUSPS.x) %>%
        dplyr::select(LINEARID, hexid4k, STUSPS)
      
      sf::st_write(secondary_hex,
                   file.path(transportation_processed_dir, "secondary_rds_hex.gpkg"),
                   driver = "GPKG")
      
      system(paste0("aws s3 sync ",
                    transportation_processed_dir, " ",
                    s3_proc_prefix, "transportation/processed"))
    } else {
      
      secondary_hex <- sf::st_read(dsn = file.path(transportation_processed_dir, "secondary_rds_hex.gpkg")) %>%
        as.data.frame() %>%
        left_join(., as.data.frame(hexnet_4k), by = 'hexid4k') %>%
        dplyr::select(LINEARID, hexid4k, STUSPS.x, geom.x, sampled) %>%
        mutate(STUSPS = STUSPS.x,
               geom = geom.x) %>%
        dplyr::select(LINEARID, hexid4k, STUSPS, sampled, geom) %>%
        st_as_sf()
      
    }
  }
  
  sfInit(parallel = TRUE, cpus = num_cores)
  sfExport('secondary_hex')
  sfSource('src/functions/helper_functions.R')
  
  secondary_rds_density <- lapply(hexnet_list,
                                function (input_list) {
                                  require(tidyverse)
                                  require(magrittr)
                                  require(lubridate)
                                  require(lubridate)
                                  require(sf)
                                  
                                  sub_grid <- dplyr:::bind_cols(input_list)
                                  unique_ids <- unique(sub_grid$hexid4k)
                                  state_name <- unique(sub_grid$STUSPS)[1]
                                  
                                  print(paste0('Working on ', state_name))
                                  got_density <- lapply(unique_ids,
                                                        FUN = get_density,
                                                        grids = sub_grid,
                                                        lines = secondary_hex)
                                  print(paste0('Finishing ', state_name))
                                  
                                  return(got_density)
                                }
  )
  sfStop()
  
  secondary_rds_density <- flattenlist(secondary_rds_density) %>%
    do.call(rbind, .)

  st_write(secondary_rds_density, file.path(transportation_density_dir, "secondary_rds_density.gpkg"))

  system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))

} else {
  secondary_rds_density <- st_read(file.path(transportation_density_dir, "secondary_rds_density.gpkg"))
}

if (!file.exists(file.path(transportation_density_dir, "tertiary_rds_density.gpkg"))) {

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

      tertiary_hex <- sf::st_read(dsn = file.path(transportation_processed_dir, "tertiary_rds_hex.gpkg"))
    }
  }

  tertiary_rds_density <- lapply(hexnet_list,
    function (input_list) {
      require(tidyverse)
      require(magrittr)
      require(lubridate)
      require(lubridate)
      require(sf)

      sub_grid <- dplyr:::bind_cols(input_list)
      unique_ids <- unique(sub_grid$hexid4k)
      state_name <- unique(sub_grid$STUSPS)[1]

      print(paste0('Working on ', state_name))

      if (!file.exists(file.path(per_state, paste0('tertiary_density_', state_name, '.gpkg')))) {
      got_density <- lapply(unique_ids,
        FUN = get_density,
        grids = sub_grid,
        lines = tertiary_hex)

      print(paste0('Finishing ', state_name))
      print(paste0('Writing ', state_name))

      got_density <- flattenlist(got_density) %>%
        do.call(rbind, .)

      st_write(got_density, file.path(per_state, paste0('tertiary_density_', state_name, '.gpkg')))
      system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))
      } else {
        print(pate0('Skipping ', state_name, ' ; already created it!'))
      }

      return(got_density)
    }
    )
  
  per_state_list <- list.files(per_state, full.names = TRUE)

  tertiary_per_state <- lapply(per_state_list, function(x) st_read(x))
  
  tertiary_rds_density <- flattenlist(tertiary_per_state) %>%
    do.call(rbind, .) %>%
    mutate(length_line = if_else(is.na(length_line), 0, length_line),
           density = if_else(is.na(density), 0, density))

  st_write(tertiary_rds_density, file.path(transportation_density_dir, "tertiary_rds_density.gpkg"))

  system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))
}  else {
  tertiary_rds_density <- st_read(file.path(transportation_density_dir, "tertiary_rds_density.gpkg"))
}

