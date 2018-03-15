
# Create railroad density
if (!file.exists(file.path(processed_dir, 'rail_rds_density_hex4k.gpkg'))) {

  sub_hexnet <- split_fast_tibble(hexnet_4k, hexnet_4k$STUSPS) %>%
    lapply(st_as_sf)
  sub_rails <- split_fast_tibble(rail_rds, rail_rds$STUSPS) %>%
    lapply(st_as_sf)
  
  
length_in_poly <- function(spatial_lines, fishnet) {
  
  fishnet_tmp <- st_as_sf(do.call(rbind, fishnet_tmp))
  
  fish_length <- list()
  
  for (i in 1:nrow(fishnet_tmp)) {
   
   split_lines_tmp <-  st_as_sf(do.call(rbind, spatial_lines_tmp)) %>%
    st_cast(., "MULTILINESTRING", group_or_split = FALSE) %>%
    #st_difference(., st_buffer(st_intersection(., fishnet_tmp), dist = 0)) %>%
    st_intersection(., fishnet_tmp[i, ]) %>%
    dplyr::select(LINEARID, hexid4k, STUSPS, geometry) %>%
    mutate(lineid = row_number())

  fish_length[[i]] <- split_lines_tmp %>%
    mutate(length = sum(st_length(.)))
  }

  fish_length <-  do.call(rbind, fish_length) %>%
    group_by(hexid4k) %>%
    summarize(length = sum(length)) 
  
  fishnet_tmp <- fishnet_tmp%>%
    st_join(., fish_length, join = st_intersects) %>%
    mutate(hexid4k = hexid4k.x,
           length = ifelse(is.na(length), 0, length),
           pixel_area = as.numeric(st_area(geom)),
           density = length/pixel_area) %>%
    dplyr::select(hexid4k, STUSPS, length, pixel_area, density, geom) 
  fishnet_tmp
  }

  
  
  ###






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
