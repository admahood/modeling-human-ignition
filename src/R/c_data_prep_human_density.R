


# import and transform the SILVIS lab partial census block housing density data
# clean, rename, and group the housing denisty data
# orignially grouped by FPA id but 3k polygons is a lot easier to compute than 1.8M fire points

if (!exists("pop_den_cleaned")) {
  if (!file.exists(file.path(anthro_proc_dir, "fpa_overlay_pop_den.gpkg"))) {
    pop_den_cleaned <-
      st_read(file.path(pd_prefix, 'us_pbg00_2007.gdb')) %>%
      select(PBG00, HDEN90:HDEN30) %>%
      st_transform(st_crs(usa_shp)) %>%
      st_intersection(fpa_clean, .)

    sf::st_write(
      pop_den_cleaned,
      file.path(anthro_proc_dir, "fpa_overlay_pop_den.gpkg"),
      driver = "GPKG"
    )

    system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))

  } else {
    pop_den_cleaned <-
      st_read(file.path(anthro_proc_dir, "fpa_overlay_pop_den.gpkg"))

  }
}

impute_in_parallel <-
  function (input_tibble, x) {
    require(tidyverse)
    require(magrittr)

    # Interpolate for each month and year from 1992 - 2015
    # using a simple linear sequence
    impute_density <- function(df) {
      require(tidyverse)
      require(magrittr)

      year_seq <- min(df$year):max(df$year)
      predict_seq <- seq(min(df$year),
                         max(df$year),
                         length.out = (length(year_seq) - 1) * 12)
      preds <- approx(x = df$year,
                      y = df$value,
                      xout = predict_seq)
      res <- as_tibble(preds) %>%
        rename(t = x, value = y) %>%
        mutate(
          year = floor(t),
          month = rep(1:12, times = length(year_seq) - 1),
          day = '01',
          year_month_day = as.Date(paste(year, month, day, sep =
                                           '-'))
        ) %>%
        filter(year < 2016)
      res$PBG00 <- unique(df$PBG00)
      res$FPA_ID <- unique(df$FPA_ID)
      res
    }

    get_lags <-
      function(extract_to,
               extract_from,
               start_date,
               time_lag) {
        # capture the variable name and statistic to be incorporated in the output column name
        if (exists('extract_from$statistic')) {
          variable <-
            paste0(extract_from$variable[1],
                   '_',
                   extract_from$statistic[1])
        } else {
          variable <- 'housing_density'
        }

        # internal function to create a lagged date
        lag_date <- function(start_date, time_lag) {
          require(magrittr)
          require(tidyverse)
          require(lubridate)

          # breakup the start date into its components
          y <- year(start_date)
          m <- month(start_date)
          d <- day(start_date)

          # calculate the new lagged year
          y <- y + (m - time_lag - 1) %/% 12

          # calculate the new lagged month
          m <-
            ifelse(((m - time_lag) %% 12) == 0, 12, (m - time_lag) %% 12)

          # stitch the new lagged date together
          as.Date(paste0(y, "-", m, "-", d))
        }

        # pair down to the extract_from to allow for easier left_join
        extract_from <- extract_from %>%
          dplyr::select('FPA_ID', 'year_month_day', 'value')

        for (j in 0:time_lag) {
          require(magrittr)
          require(tidyverse)

          # create a lagged data year column that can be joined and extracted upon
          extract_to[, paste0(variable, '_lag_', j)]  <-
            extract_to %>%
            dplyr::mutate(ymd_lagged = lag_date(start_date = start_date, time_lag = j)) %>%
            left_join(.,
                      extract_from,
                      by = c('FPA_ID', 'ymd_lagged' = 'year_month_day')) %>%
            dplyr::select(value)
        }
        return(extract_to)
      }

    extraction_df <- input_tibble[x,] %>%
      dplyr::select(-year_month_day) %>%
      gather(variable, value, -FPA_ID, -PBG00, -STATE) %>%
      mutate(value = ifelse(value == -999, is.na(value), value)) %>%
      filter(!is.na(value)) %>%
      mutate(
        year = case_when(
          .$variable == 'HDEN90' ~ 1990,
          .$variable == 'HDEN00' ~ 2000,
          .$variable == 'HDEN10' ~ 2010,
          .$variable == 'HDEN20' ~ 2020
        )
      ) %>%
      do(impute_density(.))

    # reduce the size of the dataframe to be joined during the get_climate_lags
    sub_df <- input_tibble[x,] %>%
      dplyr::select(FPA_ID, PBG00, year_month_day)

    fpa_out <-
      get_lags(
        extract_to = sub_df,
        extract_from = extraction_df,
        start_date = sub_df$year_month_day,
        time_lag = 0
      ) %>%
      dplyr::select(-year_month_day)

    return(fpa_out)
  }

pop_den_tibble <- as.data.frame(pop_den_cleaned) %>%
  dplyr::select(PBG00, FPA_ID, STATE, HDEN90, HDEN00, HDEN10, HDEN20, year_month_day) %>%
  as_tibble()

sfInit(parallel = TRUE, cpus = parallel::detectCores()/2)
sfExport('pop_den_tibble')

fp_pop_den_summaries <- sfLapply(seq_along(1:nrow(pop_den_tibble)),
                              fun = impute_in_parallel,
                              input_tibble = pop_den_tibble)

sfStop()

# convert to a data frame
extraction_df <- fp_pop_den_summaries %>%
  bind_cols %>%
  as_tibble %>%
  write_rds(., file.path(popden_extract, "pop_den_extraction.rds"))

system(paste0("aws s3 sync ", summary_dir, " ", s3_proc_extractions))
