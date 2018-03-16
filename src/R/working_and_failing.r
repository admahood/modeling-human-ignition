


  split_lines <-  st_as_sf(do.call(rbind, spatial_lines)) %>%
    st_cast(., "MULTILINESTRING", group_or_split = FALSE) %>%
    st_difference(., st_buffer(st_intersection(., fishnet), dist = 0)) #creates line segments that are unique to each polygon

  fish_length <- split_lines %>%
    group_by(hexid4k) %>%
    summarize(length = sum(st_length(.)))

fish_length



length_in_poly <- function(unique_states, input_grid, input_line, grouping_var) {

  unique_states <- unique(input_grid[[grouping_var]])

  grid_list <- split_fast_tibble(input_grid, input_grid[[grouping_var]])
  line_list <- split_fast_tibble(sub_line, sub_line[[grouping_var]])

  for (i in unique_states) {

  require(tidyverse)
  require(lubridate)

  # create a subdataframe based on state subset
    sub_grid <- grid_list[k] %>%
      st_as_sf(bind_rows(.))

    sub_line <- line_list[k] %>%
      st_as_sf(bind_rows(.))


  }
}
  rail_rds_density$density <- length_in_poly(spatial_lines = rail_rds,
                                             fishnet = sub_hexnet[1:2])


 
