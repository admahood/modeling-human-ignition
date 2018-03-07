
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
      print(paste("Creating extraction dataframe for ", i, j, ' summaries'))
      start_time <- Sys.time()

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
        mutate(FPA_ID = as.factor(FPA_ID),
               ID = as.factor(ID)) %>%
        # clean the final, long climate data frame with linked fpa ids
        separate(variable,
                 into = c("variable", 'year', "statistic", "month"),
                 sep = "_|\\.") %>%
        mutate(day = '01',
               ymd = as.Date(paste(year, month, day, sep='-')))
      end_time <- Sys.time()
      time_taken <- end_time - start_time
      print(paste("Creating extraction dataframe ", i, j, ' took ', time_taken, ' long'))
      
      print(paste("Creating fpa summaries for ", i, j))
      start_time <- Sys.time()
      
      fpa_summaries <- get_climate_lags(fpa_ll, extraction_df, fpa_ll$ymd, time_lag = 24)
      
      end_time <- Sys.time()
      time_taken <- end_time - start_time
      print(paste("Creating fpa summaries ", i, j, ' took ', time_taken, ' long'))
      
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
