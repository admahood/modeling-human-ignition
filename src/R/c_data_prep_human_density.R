
# import and transform the SILVIS lab partial census block housing density data
# clean, rename, and group the housing denisty data
# orignially grouped by FPA id but 3k polygons is a lot easier to compute than 1.8M fire points
if (!exists("pop_den")) {
  if (file.exists(file.path(processed_dir, "pop_den_long.gpkg"))){

    pop_den <- st_read(file.path(pd_prefix, 'us_pbg00_2007.gdb')) %>%
      select(PBG00, HDEN90:HDEN20) %>%
      st_transform(st_crs(usa_shp)) %>%
      st_join(., usa_shp, join = st_intersects) %>%
      st_intersection(fpa_clean, .) %>%
      dplyr::select(PBG00, STUSPS, HDEN90, HDEN00, HDEN10, HDEN20) %>%
      gather(variable, value, -PBG00, -STUSPS) %>%
      filter(!is.na(value)) %>%
      mutate(year = case_when(
        .$variable == 'HDEN90' ~ 1990,
        .$variable == 'HDEN00' ~ 2000,
        .$variable == 'HDEN10' ~ 2010,
        .$variable == 'HDEN20' ~ 2020
      )) %>%
      group_by(PBG00, year) %>%
      ungroup

    sf::st_write(pop_den,
                 file.path(processed_dir, "pop_den_long.gpkg"),
                 driver = "GPKG")

    system(paste0("aws s3 sync ",
                  processed_dir, " ",
                  s3_proc_prefix))
  }
}

slim <- pop_den %>%
  filter(STATE %in% c("RI", "CT"))
slim$STATE <- droplevels(slim$STATE)

unique_states <- unique(slim$STATE)

# Internal function
# Then interpolate for each month and year from 1984 - 2015
# using a simple linear sequence
impute_density <- function(df) {
  year_seq <- min(df$year):max(df$year)
  predict_seq <- seq(min(df$year),
                     max(df$year),
                     length.out = (length(year_seq) - 1) * 12)
  preds <- approx(x = df$year,
                  y = df$value,
                  xout = predict_seq)
  res <- as_tibble(preds) %>%
    rename(t = x, value = y) %>%
    mutate(year = floor(t),
           month = rep(1:12, times = length(year_seq) - 1)) %>%
    filter(year < 2016)
  res$PBG00 <- unique(df$PBG00)
  res
}

impute_in_parallel <- function (unique_groups, input_tibble, sub_groups) {

  # subset the fpa-fod data based on state grouping variable
  # this increases the speed of this function and only needs ~40GB of memory max.
  cl <- makeCluster(detectCores())
  registerDoParallel(cl)

  fp_pop_den_summaries <- foreach (k = unique_groups) %dopar% {
    require(tidyverse)
    require(lubridate)
    k <- 'RI'
    # create a subdataframe based on state subset
    sub_df <- subset(input_tibble, input_tibble[[sub_groups]] == k)

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

    write_rds(fp_pop_den_summaries, file.path(per_state, paste0('pop_den_summary_', k, '.rds')))

    system(paste0("aws s3 sync ",
                  summary_dir, " ",
                  s3_proc_extractions))

    return(fp_pop_den_summaries)
  }
  stopCluster(cl)
  return(fp_pop_den_summaries)
}


fp_pop_den_summaries <- impute_in_parallel(unique_groups= unique_states,
                                           input_tibble = slim,
                                           sub_groups = "STATE")

fp_pop_den_summaries %>%
  write_rds(file.path(anthro_proc_dir, 'housing_density.rds'))

system(paste0("aws s3 sync ",
              summary_dir, " ",
              s3_proc_extractions))
