
if (!exists("fpa_ll")) {
  if (file.exists(file.path(processed_dir, "fpa_ll.gpkg"))) {

    fpa_ll <- st_read(file.path(processed_dir, "fpa_ll.gpkg")) %>%
      mutate(ymd = floor_date(ymd(DISCOVERY_DATE), "month"))

  } else if (!file.exists(file.path(processed_dir, "fpa_clean.gpkg"))) {

    fpa_ll <- fpa_clean %>%
      st_transform(crs = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0") %>%
      mutate(ymd = floor_date(ymd(DISCOVERY_DATE), "month"))

    st_write(fpa_ll, file.path(processed_dir, "fpa_ll.gpkg"), overwrite = TRUE)

    system(paste0("aws s3 sync ",
                  processed_dir, " ",
                  s3_proc_prefix))

  } else if (exists("fpa_clean")) {

    fpa_ll <- st_read(file.path(processed_dir, "fpa_clean.gpkg")) %>%
      st_transform(crs = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0") %>%
      mutate(ymd = floor_date(ymd(DISCOVERY_DATE), "month"))

    st_write(fpa_ll, file.path(processed_dir, "fpa_ll.gpkg"))

    system(paste0("aws s3 sync ",
                  processed_dir, " ",
                  s3_proc_prefix))
  }
}

extract_one <- function(filename, shapefile_extractor) {
  # function to extract all climate time series based on shapefile input
  # this results in large list of all months/years within the raster climate data
  # each list is written out to a csv so this only needs to be run once.
  # inputs:
  # filename -> a list of all tif filenames with full path
  # shapefile_extractor -> the shapefile (point or polygon) to extract climate data

  out_name <- gsub('.tif', '.csv', filename)
  if (!file.exists(out_name)) {
    res <- raster::extract(raster::stack(filename), shapefile_extractor,
                           na.rm = TRUE, fun = 'mean', df = TRUE)
    write.csv(res, file = out_name)
  } else {
    res <- read.csv(out_name)
  }
  res
}

check_tifs <- function(j, i, ...) {
  # checks whether the statistic being evaluated has the variable data
  # if not then returns NULL which allows the larger loop below to skip the inputs

  tif <- tryCatch(list.files(file.path(climate_prefix, j),
                             pattern = i,
                             recursive = TRUE,
                             full.names = TRUE) %>%
                    Filter(function(x) grepl(".tif", x), .),
                  error = function(c) {
                    c$message <- paste0(c$message, " (in the", i, 'variable and ', j, 'statistic)')
                    warning(c)
                  }
  )
  # if the length of the tryCatch is greater than one then that indicates there
  # are data in the statistic/variable combination
  if (length(tif) > 1) {
    tif <- list.files(file.path(climate_prefix, j),
                      pattern = i,
                      recursive = TRUE,
                      full.names = TRUE) %>%
      Filter(function(x) grepl(".tif", x), .)
  } else {
    # if the tif length is 0 then that indicates there are no climate data for
    # that statistic/variable combination

    tif <- NULL
  }
}

get_climate_lags <- function(fpa_df, climate_df, start_date, time_lag) {

  # capture the variable name and statistic to be incorporated in the output column name
  variable <- paste0(climate_df$variable[1], '_', climate_df$statistic[1])

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
    y <- y + (m + time_lag - 1) %/% 12

    # calculate the new lagged month
    m <- ifelse(((m + time_lag) %% 12) == 0, 12, (m + time_lag) %% 12)

    # stitch the new lagged date together
    as.Date(paste0(y, "-", m, "-", d))
  }

  # remove the sf data - increases efficiency
  fpa_df <- fpa_df %>%
    as.data.frame() %>%
    select(-geom)

  # pair down to the climate_df to allow for easier left_join
  climate_df <- climate_df %>%
    select('FPA_ID', 'ymd', 'value')

  for (j in 0:time_lag) {
    require(magrittr)
    require(tidyverse)

    # create a lagged data year column that can be joined and extracted upon
    fpa_df <- fpa_df %>%
      dplyr::mutate(ymd_lagged = lag_date(start_date, -time_lag))

    # the meat and potatoes.  This joins the fpa and climate data based on
    #climate data dates and lagged fpa dates.
    fpa_df[, paste0(variable, '_lag_', j)] <- left_join(fpa_df, climate_df, by = c('FPA_ID', 'ymd_lagged' = 'ymd')) %>%
      dplyr::select(value)
  }
  return(fpa_df)
}

stat <- c('mean', 'days-above-95th', '95th-percentile')
vars <- c('aet', 'def', 'ffwi', 'fm100', 'pdsi', 'pr', 'tmmx', 'vpd', 'vs')

for (j in stat){
  for (i in vars) {

    tifs <- check_tifs(j, i)

    if (is.null(tifs)) {

      print(paste0('Skipping ', j, ' ', i))

    } else if (!file.exists(file.path(summaries_dir, paste0('fpa_', j, '_', i, '_summaries.rds')))) {

      sfInit(parallel = TRUE, cpus = parallel::detectCores())
      sfExport(list = c("fpa_ll"))

      extractions <- sfLapply(as.list(tifs),
                              fun = extract_one,
                              shapefile_extractor = fpa_ll)
      sfStop()

      # ensure that they all have the same length
      stopifnot(all(lapply(extractions, nrow) == nrow(fpa_ll)))

      # convert to a data frame
      extraction_df <- extractions %>%
        bind_cols %>%
        as_tibble %>%
        mutate(index = ID) %>%
        select(-starts_with("ID")) %>%
        rename(ID = index) %>%
        mutate(FPA_ID = data.frame(fpa_ll)$FPA_ID) %>%
        dplyr::select(-starts_with('X')) %>%
        gather(variable, value, -FPA_ID, -ID) %>%
        filter(!is.na(value)) %>%
        mutate(FPA_ID = as.character(FPA_ID),
               ID = as.character(ID)) %>%
        # clean the final, long climate data frame with linked fpa ids
        separate(variable,
                 into = c("variable", 'year', "statistic", "month"),
                 sep = "_|\\.") %>%
        mutate(day = '01',
               ymd = as.Date(paste(year, month, day, sep='-'))) %>%
        select(FPA_ID, ymd, variable, statistic)

      fpa_summaries <- get_climate_lags(fpa_ll, extraction_df, fpa_ll$ymd, time_lag = 24)

      # save raw monthly extractions
      extract_name <- file.path(summaries_dir, paste0('fpa_', j, '_', i, '_extractions.rds'))
      if (!file.exists(extract_name)) {
        write_rds(extractions, extract_name)
      }

      # save processed/cleaned monthly extractions
      # this data frame is in wide format
      extract_df_name <- file.path(summaries_dir, paste0('fpa_', j, '_', i, '_extractions_df.rds'))
      if (!file.exists(extract_df_name)) {
        write_rds(extraction_df, extract_df_name)
      }

      # save the final cleaned climate extractions
      # this dataframe is in long format to be used as a lookup table
      summary_name <- file.path(summaries_dir, paste0('fpa_', j, '_', i, '_summaries.rds'))
      write_rds(fpa_summaries, summary_name)

      # push to S3
      system(paste0('aws s3 cp data/summary s3://earthlab-modeling-human-ignitions/summary --recursive'))

      # to conserve space delete the large files
      unlink(extract_name)
      unlink(summary_name)
      unlink(extract_df_name)

      # creating the final dataframe takes about 150 GB of RAM, this clears the cache so it can be run again
      gc()
    }
  }
}
