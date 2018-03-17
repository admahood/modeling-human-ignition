
unique_states <- unique(hexnet_4k$STUSPS)

grid_list <- split_fast_tibble(hexnet_4k, hexnet_4k_2$STUSPS)
line_list <- split_fast_tibble(rail_rds, rail_rds_2$STUSPS)

railroad_density <- get_density(unique_states, grid_list, line_list)
