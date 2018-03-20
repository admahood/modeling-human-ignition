
# import and transform the SILVIS lab partial census block housing density data
# clean, rename, and group the housing denisty data
# orignially grouped by FPA id but 3k polygons is a lot easier to compute than 1.8M fire points

if (!exists("pop_den")) {
  if (file.exists(file.path(processed_dir, "pop_den_long.gpkg"))){

    pop_den <- st_read(file.path(pd_prefix, 'us_pbg00_2007.gdb')) %>%
      select(PBG00, HDEN90:HDEN20) %>%
      st_transform(st_crs(usa_shp)) %>%
      st_join(., usa_shp, join = st_intersects)

    fpa_overlay <- st_intersects(fpa_clean, pop_den)

    pop_den <- pop_den[unlist(fpa_overlay), ] %>%
      dplyr::select(PBG00, STUSPS, HDEN90, HDEN00, HDEN10, HDEN20, SHAPE) %>%
      gather(variable, value, -PBG00, -STUSPS, -SHAPE) %>%
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
  filter(STUSPS %in% c("RI"))
slim$STUSPS <- droplevels(slim$STUSPS)

fpa_ri <- fpa_clean %>%
  filter(STATE == 'RI')
fpa_ri$STATE <- droplevels(fpa_ri$STATE)

# match each ignition to an ecoregion
if (!file.exists(file.path(processed_dir, "fpa_popden_intersect.rds"))) {
  st_over <- function(x, y) {
    sapply(st_intersects(x,y), function(z) if (length(z)==0) NA_integer_ else z[1])
  }
  ov <- st_over(fpa_ri, slim)
  write_rds(ov, file.path(processed_dir, "fpa_popden_intersect.rds"))
}
ov <- read_rds(file.path(processed_dir, "fpa_popden_intersect.rds"))

fpa_ri <- fpa_ri %>%
  mutate(PBG00 = slim$PBG00[ov],
         STUSPS = STATE) %>%
  filter(!is.na(PBG00)) %>%
  dplyr::select(FPA_ID, PBG00, STUSPS, year_month_day)



unique_states <- unique(slim$STUSPS)

pop_list <- split_fast_tibble(slim, slim$STUSPS)
fpa_list <- split_fast_tibble(fpa_ri, fpa_ri$STUSPS)

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
           month = rep(1:12, times = length(year_seq) - 1),
           day = '01',
           year_month_day = as.Date(paste(year, month, day, sep='-'))) %>%
    filter(year < 2016)
  res$PBG00 <- unique(df$PBG00)
  res
}

impute_in_parallel <- function (unique_groups, pop_tibble, fpa_tibble, sub_groups) {

  # subset the fpa-fod data based on state grouping variable
  # this increases the speed of this function and only needs ~40GB of memory max.
  #cl <- makeCluster(detectCores())
  #registerDoParallel(cl)

  k <- 'RI'
  sub_groups <- 'STUSPS'
  pop_tibble <- pop_list
  fpa_tibble <- fpa_list

  fp_pop_den_summaries <- for (k in unique_groups) {
    require(tidyverse)
    require(lubridate)

    pop_df <- pop_tibble[k] %>%
      do.call(rbind, .)

    fpa_df <- fpa_tibble[k] %>%
      do.call(rbind, .)

    each_id_in_pop_list <- split_fast_tibble(pop_df, pop_df$PBG00)
    each_id_in_fpa_list <- split_fast_tibble(fpa_df, fpa_df$PBG00)

    unique_popid <- unique(each_pop_id_list$PBG00)

    foreach (j = unique_popid) %dopar% {
    # create a subdataframe based on state subset

      extraction_df <- pop_df %>%
        split(.$PBG00) %>%
        map(~impute_density(.)) %>%
        bind_rows %>%
        dplyr::select(-t) %>%
        mutate(day = '01',
               year_month_day = as.Date(paste(year, month, day, sep='-')))

      # reduce the size of the dataframe to be joined during the get_climate_lags
      fpa_df <- fpa_df %>%
        dplyr::select(PBG00, year_month_day) %>%
        distinct()

      # run get_climate_lags for the prior 24 months given a fpa-fod fire event
      # this will iteratively populate a list given each state grouping variable
      fp_pop_den_summaries <- get_lags(sub_df, extraction_df, sub_df$year_month_day, time_lag = 0) %>%
        dplyr::select(-year_month_day)

      write_rds(fp_pop_den_summaries, file.path(per_state, paste0('pop_den_summary_', k, '.rds')))

      system(paste0("aws s3 sync ",
                    summary_dir, " ",
                    s3_proc_extractions))
    }
    stopCluster(cl)
    return(fp_pop_den_summaries)
  }
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
