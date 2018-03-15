
# import and transform the SILVIS lab partial census block housing density data
pop_den <- st_read(file.path(pd_prefix, 'us_pbg00_2007.gdb')) %>%
  select(HDEN90:HDEN30) %>%
  st_transform(st_crs(usa_shp))

# spatially intersect the fpa_clean sf object with the population density polygons
# clean, rename, and group the housing denisty data by fpa_id
fp_pop_den <- fpa_clean  %>%
  st_intersection(., pop_den) %>%
  as_tibble() %>%
  dplyr::select(FPA_ID, STATE, HDEN90, HDEN00, HDEN10, HDEN20, HDEN30) %>%
  gather(variable, value, -FPA_ID, -STATE) %>%
  mutate(value = ifelse(value == -999, is.na(value), value)) %>%
  filter(!is.na(value)) %>%
  mutate(year = case_when(
    .$variable == 'HDEN90' ~ 1990,
    .$variable == 'HDEN00' ~ 2000,
    .$variable == 'HDEN10' ~ 2010,
    .$variable == 'HDEN20' ~ 2020,
    .$variable == 'HDEN30' ~ 2030
  )) %>%
  group_by(FPA_ID, year) %>%
  mutate(housing_density = value) %>%
  ungroup %>%
  dplyr::select(-value)

unique_states <- unique(fp_pop_den$STATE)

fp_pop_den_summaries <- list()
counter <- 1

# subset the fpa-fod data based on state grouping variable
# this increases the speed of this function and only needs ~40GB of memory max.
for (k in unique_states) {

  # create a subdataframe based on state subset
  sub_df <- subset(fp_pop_den, fp_pop_den$STATE == unique_states[k])

  print(paste0('Working on ', counter, " of ", length(unique_states)))

  sub_id <- faster_as_tibble(sub_df, sub_df$FPA_ID)

  sfInit(parallel = TRUE, cpus = parallel::detectCores())
  #sfExport(list = c("ecoregion_shp"))

  extractions <- sfLapply(sub_id,
                          fun = impute_density)
  sfStop()
}

res <- fp_pop_den %>%
  split(.$FPA_ID) %>%
  map(~impute_density(.)) %>%
  bind_rows

res %>%
  write_rds('data/processed/housing_density.rds')
