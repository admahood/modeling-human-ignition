
if (!exists("fpa_ll")) {
  if (file.exists(file.path(processed_dir, "fpa_ll.gpkg"))) {

    fpa_ll <- st_read(file.path(processed_dir, "fpa_ll.gpkg")) %>%
      mutate(year_month_day = floor_date(ymd(DISCOVERY_DATE), "month"))

  } else if (!file.exists(file.path(processed_dir, "fpa_clean.gpkg"))) {

    fpa_ll <- fpa_clean %>%
      st_transform(crs = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0") %>%
      mutate(year_month_day = floor_date(ymd(DISCOVERY_DATE), "month"))

    st_write(fpa_ll, file.path(processed_dir, "fpa_ll.gpkg"), overwrite = TRUE)

    system(paste0("aws s3 sync ",
                  processed_dir, " ",
                  s3_proc_prefix))

  } else if (exists("fpa_clean")) {

    fpa_ll <- st_read(file.path(processed_dir, "fpa_clean.gpkg")) %>%
      st_transform(crs = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0") %>%
      mutate(year_month_day = floor_date(ymd(DISCOVERY_DATE), "month"))

    st_write(fpa_ll, file.path(processed_dir, "fpa_ll.gpkg"))

    system(paste0("aws s3 sync ",
                  processed_dir, " ",
                  s3_proc_prefix))
  }
}

stat <- c('numdays95th', '95th')
vars <- c('aet', 'def', 'ffwi', 'fm100', 'pdsi', 'pr', 'tmmx', 'vpd', 'vs')

unique_states <- unique(fpa_ll$STATE)

sub_fpa <- fpa_ll %>%
  select(FPA_ID, STATE, year_month_day)

for (j in stat){
  for (i in vars) {
    fpa_summaries <- list()

    tifs <- check_tifs(j, i)

    if (is.null(tifs)) {

      print(paste0('Skipping ', j, ' ', i))

    } else {

      if (!file.exists(file.path(climate_prefix, j, paste0('fpa_', i, '_', j, '_extraction.rds')))) {

        # setup parallel environment
        cl <- makeCluster(detectCores())
        registerDoParallel(cl)

        # extract climate time series data based on point or polygon locations.
        # this extract is pulling in point data, so fun = mean does not matter
        print(paste0('Parallel ', j, ' ', i))

        extractions <- foreach (l = tifs) %dopar% {

          res <- extract_one(l, shapefile_extractor = sub_fpa)

        }
        stopCluster(cl)

        extraction_df <- extractions %>%
          bind_cols %>%
          as_tibble %>%
          mutate(index = ID) %>%
          select(-starts_with("ID")) %>%
          rename(ID = index) %>%
          mutate(FPA_ID = data.frame(sub_fpa)$FPA_ID) %>%
          dplyr::select(-starts_with('X')) %>%
          left_join(., sub_fpa, by = 'FPA_ID')

        write_rds(extraction_df, file.path(climate_prefix, j, paste0('fpa_', i, '_', j, '_extraction.rds')))
        system(paste0("aws s3 sync ", climate_prefix, " ", s3_proc_climate))

      }

      if (!file.exists(file.path(summaries_dir, j, paste0('fpa_', i, '_', j, '_summaries.rds')))) {

        extraction_df <- read_rds(file.path(climate_prefix, j, paste0('fpa_', i, '_', j, '_extraction.rds')))

        extraction_mini <- extraction_df %>%
          slice(1:10)
        print(paste0('Creating extraction tibble for ', j, ' ', i))

        # subset the fpa-fod data based on state grouping variable
        # this increases the speed of this function and only needs ~40GB of memory max.

        fpa_summaries <- for (k in 1:length(unique_states)) {
        #fpa_summaries <- for (k in 1:length(unique_states)) {
          require(tidyverse)

          sub_extract <- extraction_mini %>%
            filter(STATE == unique_states[k])

          sub_extract$STATE <- droplevels(sub_extract$STATE)

          unique_fpa_id <- unique(sub_extract$FPA_ID)

          cl <- makeCluster(12)
          registerDoParallel(cl)

          fpa_summaries <-foreach(m = 1:length(unique_fpa_id), .combine = 'rbind') %dopar% {
          #for (j in 1:length(unique_fpa_id)) {
            require(tidyverse)

            # create a subdataframe based on state subset
            sub_extraction_df <- sub_extract %>%
              filter(FPA_ID == unique_fpa_id[m])
            sub_extraction_df$FPA_ID <- droplevels(sub_extraction_df$FPA_ID)

            sub_extraction_df <- sub_extraction_df %>%
              dplyr::select(-geom, -year_month_day, -ID, -STATE) %>%
              gather(variable, value, -FPA_ID) %>%
              mutate(FPA_ID = as.factor(FPA_ID)) %>%
              separate(variable,
                       into = c("variable", 'year', "statistic", "month"),
                       sep = "_|\\.") %>%
              mutate(day = '01',
                     year_month_day = as.Date(paste(year, month, day, sep='-')))

            fpa_summaries <- get_lags(sub_fpa, sub_extraction_df, sub_fpa$year_month_day, time_lag = 24) %>%
              dplyr::select(-year_month_day)

            fpa_summaries
          }
          stopCluster(cl)

          fpa_summaries <- do.call(rbind, fpa_summaries) # Convert to data frame format
        }

        # save the final cleaned climate extractions joined with the fpa-fod database
        summary_name <- file.path(summaries_dir, j, paste0('fpa_', i, '_', j, '_summaries.rds'))
        write_rds(fpa_summaries, summary_name)

        # push to S3
        system('aws s3 sync data/extractions s3://earthlab-modeling-human-ignitions/extractions')

      }
    }
  }
}
