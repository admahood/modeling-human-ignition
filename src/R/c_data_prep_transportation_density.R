
# Create railroad density
#if (!file.exists(file.path(processed_dir, 'rail_rds_density_hex4k.gpkg'))) {
  mask_hex <- hexnet_4k %>%
    filter(STUSPS %in% c('RI', 'CT'))
  mask_hex$STUSPS <- droplevels(mask_hex$STUSPS)

  mask_rails <- rail_rds %>%
    filter(STUSPS %in% c('RI', 'CT'))
  mask_rails$STUSPS <- droplevels(mask_rails$STUSPS)

  sub_hexnet <- split_fast_tibble(mask_hex, mask_hex$STUSPS) %>%
    lapply(st_as_sf)

length_in_poly <- function(fishnet, spatial_lines) {
  require(sf)
  require(tidyverse)
  require(magrittr)

  fishnet <- st_as_sf(do.call(rbind, fishnet))
  spatial_lines <- st_as_sf(do.call(rbind, spatial_lines))

  fish_length <- list()

  for (i in 1:nrow(fishnet)) {

   split_lines <- spatial_lines %>%
    st_cast(., "MULTILINESTRING", group_or_split = FALSE) %>%
    st_intersection(., fishnet[i, ]) %>%
    mutate(lineid = row_number())

   fish_length[[i]] <- split_lines %>%
    mutate(length = sum(st_length(.)))
  }

  fish_length <-  do.call(rbind, fish_length) %>%
    group_by(hexid4k) %>%
    summarize(length = sum(length))

  fishnet <- fishnet %>%
    st_join(., fish_length, join = st_intersects) %>%
    mutate(hexid4k = hexid4k.x,
           length = ifelse(is.na(length), 0, length),
           pixel_area = as.numeric(st_area(geom)),
           density = length/pixel_area)
}

test <- lapply(fishnet = as.list(sub_hexnet),
         FUN = length_in_poly,
         spatial_lines = mask_rails)

sfInit(parallel = TRUE, cpus = parallel::detectCores())
sfExport(list = c("sub_hexnet", "mask_rails"))

extractions <- sfLapply(fishnet = sub_hexnet,
         fun = length_in_poly,
         spatial_lines = mask_rails)
sfStop()

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
