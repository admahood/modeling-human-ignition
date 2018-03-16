
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

  fishnet <- st_as_sf(do.call(rbind, sub_hexnet['RI']))
  spatial_lines <- subset(mask_rails, mask_rails$STUSPS == fishnet$STUSPS)


  cl <- makeCluster(detectCores())
  registerDoParallel(cl)

  fish_length <- foreach (i = 1:nrow(fishnet), .combine = rbind) %dopar% {
    require(tidyverse)
    require(sf)

    split_lines <- spatial_lines %>%
      st_cast(., "MULTILINESTRING", group_or_split = FALSE) %>%
      st_difference(., st_buffer(st_intersection(., fishnet), dist = 1e-12)) %>%
      mutate(length = sum(st_length(.)))

     return(split_lines)
    }
  stopCluster(cl)

  fish_length2 <- fish_length %>%
    distinct(., hexid4k, .keep_all = TRUE)

  fishnet <- fishnet %>%
    st_intersection(., fish_length) %>%
    mutate(hexid4k = hexid4k.x,
           length = ifelse(is.na(length), 0, length),
           pixel_area = as.numeric(st_area(geom)),
           density = length/pixel_area)
  return(fishnet)

  }

test <- lapply(fishnet = as.list(sub_hexnet),
         FUN = length_in_poly,
         spatial_lines = mask_rails)

# sfInit(parallel = TRUE, cpus = parallel::detectCores())
# sfExport(list = c("sub_hexnet", "mask_rails"))
#
# extractions <- sfLapply(fishnet = sub_hexnet,
#          fun = length_in_poly,
#          spatial_lines = mask_rails)
# sfStop()
