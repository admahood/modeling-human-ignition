length_in_poly <- function(spatial_lines, fishnet){
  spatial_lines <- sub_rails['RI']
  fishnet <- sub_hexnet['RI']

  fishnet <- st_as_sf(do.call(rbind, fishnet))

  split_lines <-  st_as_sf(do.call(rbind, spatial_lines)) %>%
    st_cast(., "MULTILINESTRING", group_or_split=FALSE) %>%
    st_difference(., st_buffer(st_intersection(., st_union(test_fish)), dist=1e-12))

  fish_intersection <- st_intersection(split_lines, fishnet)

  fish_length <- fishnet %>%
    mutate(length = split(., .$hexid4k)) %>%
    map(sum(st_length(fish_intersection))))

fish_length
}


# Create railroad density
if (!file.exists(file.path(processed_dir, 'rail_rds_density_hex4k.gpkg'))) {

  sub_hexnet <- split_fast_tibble(hexnet_4k, hexnet_4k$STUSPS)
  sub_rails <- split_fast_tibble(rail_rds, rail_rds$STUSPS)

  rail_rds_density$density <- length_in_poly(spatial_lines = rail_rds,
                                             fishnet = sub_hexnet[1:2])


  length_in_poly <- function(spatial_lines, fishnet, grouping_var){
    spatial_lines <- rail_rds['RI']
    fishnet <- sub_hexnet['RI']
    grouping_var <- 'STUSPS'

    list_length <- lapply(fishnet, function(x) dim(x))[[1]][1]

    fishnet %>%
      mutate(bb = split(., 1:list_length) %>% map(st_bbox))

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
