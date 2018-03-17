
unique_states <- unique(hexnet_4k_2$STUSPS)

hexnet_4k_2 <- hexnet_4k %>%
  filter(STUSPS %in% c('RI', 'CT'))
rail_rds_2 <- rail_rds %>%
  filter(STUSPS %in% c('RI', 'CT'))

grid_list <- split_fast_tibble(hexnet_4k_2, hexnet_4k_2$STUSPS)
line_list <- split_fast_tibble(rail_rds_2, rail_rds_2$STUSPS)

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
  
  test <- sub_line %>%
    st_cast(., "MULTILINESTRING", group_or_split = FALSE) %>%
    st_difference(., st_buffer(st_intersection(., sub_grid), dist = 1e-12))
  
  fish_length <-  sub_grid %>%
    split(.$hexid4k) %>%
    map_df(~ sum(st_length(test)))
  
  fish_length <-  sub_grid %>%
    rowwise %>% 
      do({
        result = as_data_frame(.)
        result$length = (sub_line %>%
                         st_cast(., "MULTILINESTRING", group_or_split = FALSE) %>%
                         st_difference(., st_buffer(st_intersection(., sub_grid), dist = 1e-12)) %>%
                         sum(st_length(.)))
        result
      } )
  
  return(fish_length)}

  fish_length <- fish_length %>%
    group_by(hexid4k.x) %>%
    summarise(length = first(length))
  
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



