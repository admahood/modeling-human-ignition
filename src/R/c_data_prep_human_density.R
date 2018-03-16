
# import and transform the SILVIS lab partial census block housing density data
pop_den <- st_read(file.path(pd_prefix, 'us_pbg00_2007.gdb')) %>%
  select(HDEN90:HDEN20) %>%
  st_transform(st_crs(usa_shp))

# spatially intersect the fpa_clean sf object with the population density polygons
# clean, rename, and group the housing denisty data by fpa_id
fp_pop_den <- fpa_clean  %>%
  st_intersection(., pop_den) %>%
  as_tibble() %>%
  dplyr::select(FPA_ID, STATE, year_month_day, HDEN90, HDEN00, HDEN10, HDEN20) %>%
  gather(variable, value, -FPA_ID, -STATE, -year_month_day) %>%
  mutate(value = ifelse(value == -999, is.na(value), value)) %>%
  filter(!is.na(value)) %>%
  mutate(year = case_when(
    .$variable == 'HDEN90' ~ 1990,
    .$variable == 'HDEN00' ~ 2000,
    .$variable == 'HDEN10' ~ 2010,
    .$variable == 'HDEN20' ~ 2020
  )) %>%
  group_by(FPA_ID, year) %>%
  ungroup 

unique_states <- unique(fp_pop_den$STATE)

# subset the fpa-fod data based on state grouping variable
# this increases the speed of this function and only needs ~40GB of memory max.
cl <- makeCluster(detectCores())
registerDoParallel(cl)

fp_pop_den_summaries <- foreach (k = unique_states, .combine = rbind) %dopar% {
  require(tidyverse)
  require(lubridate)
  
  # create a subdataframe based on state subset
  sub_df <- subset(fp_pop_den, fp_pop_den$STATE == k)
  
  print(paste0('Working on ', counter, " of ", length(unique_states)))
  
  extraction_df <- sub_df %>%
    split(.$FPA_ID) %>%
    map(~impute_density(.)) %>%
    bind_rows %>%
    dplyr::select(-t) %>%
    mutate(day = '01',
           year_month_day = as.Date(paste(year, month, day, sep='-')))
  
  # reduce the size of the dataframe to be joined during the get_climate_lags
  sub_df <- sub_df %>%
    dplyr::select(FPA_ID, year_month_day) %>%
    distinct()
  
  # run get_climate_lags for the prior 24 months given a fpa-fod fire event
  # this will iteratively populate a list given each state grouping variable
  fp_pop_den_summaries <- get_lags(sub_df, extraction_df, sub_df$year_month_day, time_lag = 0) %>%
    dplyr::select(-year_month_day) 
  
  return(fp_pop_den_summaries)
}
stopCluster(cl)

fp_pop_den_summaries %>%
  write_rds(file.path(anthro_proc_dir, 'housing_density.rds'))

system(paste0("aws s3 sync ",
              summary_dir, " ",
              s3_proc_extractions))