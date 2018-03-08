
# import and transform the SILVIS lab partial census block housing density data
pop_den <- st_read(file.path(pd_prefix, 'us_pbg00_2007.gdb')) %>%
  select(HDEN90:HDEN30) %>%
  st_transform(st_crs(usa_shp))

# spatially intersect the fpa_clean sf object with the population density polygons
# clean, rename, and group the housing denisty data by fpa_id
fp_pop_den <- fpa_clean  %>%
  st_intersection(., pop_den) %>%
  as_tibble() %>%
  dplyr::select(FPA_ID, HDEN90, HDEN00, HDEN10, HDEN20, HDEN30) %>%
  gather(variable, value, -FPA_ID) %>%
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


# Then interpolate for each month and year from 1984 - 2015
# using a simple linear sequence
impute_density <- function(df) {
  year_seq <- min(df$year):max(df$year)
  predict_seq <- seq(min(df$year),
                     max(df$year),
                     length.out = (length(year_seq) - 1) * 12)
  preds <- approx(x = df$year,
                  y = df$housing_density,
                  xout = predict_seq)
  res <- as_tibble(preds) %>%
    rename(t = x, housing_density = y) %>%
    mutate(year = floor(t),
           month = rep(1:12, times = length(year_seq) - 1)) %>%
    filter(year < 2030)
  res$FPA_ID <- unique(df$FPA_ID)
  res
}

res <- fp_pop_den %>%
  split(.$FPA_ID) %>%
  map(~impute_density(.)) %>%
  bind_rows

res %>%
  write_rds('data/processed/housing_density.rds')
