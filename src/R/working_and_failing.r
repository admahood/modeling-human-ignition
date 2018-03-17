
unique_states <- unique(hexnet_4k$STUSPS)

grid_list <- split_fast_tibble(hexnet_4k, hexnet_4k$STUSPS)
line_list <- split_fast_tibble(rail_rds, rail_rds$STUSPS)

for (k in unique_states) {
  require(tidyverse)
  require(lubridate)

  # create a subdataframe based on state subset
  sub_grid <- grid_list[k] %>%
    do.call(rbind, .) %>%
    st_as_sf(.)
  
  sub_line <- line_list[k] %>%
    do.call(rbind, .) %>%
    st_as_sf(.)
  
  sub_line <- sub_line %>%
    group_by(STATEFP) %>%
    summarise() 
  
  cl <- makeCluster(detectCores())
  registerDoParallel(cl)
  
  fish_length <- foreach (i = 1:nrow(sub_grid), .combine = rbind) %dopar% {
    require(tidyverse)
    require(sf)
    
    split_lines <- sub_line %>%
      st_cast(., "MULTILINESTRING", group_or_split = FALSE) %>%
      st_difference(., st_buffer(st_intersection(., sub_grid[i,]), dist = 1e-12)) %>%
      mutate(length = sum(st_length(.)))
    
    return(split_lines)
  }
  
  stopCluster(cl)

  sub_grid <- sub_grid %>%
    st_join(., fish_length, join = st_intersects) %>%
    mutate(hexid4k = hexid4k.x,
           length = ifelse(is.na(length), 0, length),
           pixel_area = as.numeric(st_area(geom)),
           density = length/pixel_area,
           STUSPS = STUSPS.x) %>%
    dplyr::select(hexid4k, STUSPS, length, pixel_area, density)
  return(sub_grid)
  }



 
