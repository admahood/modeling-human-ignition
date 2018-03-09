
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

stat <- c('mean', 'days-above-95th', '95th-percentile')
vars <- c('aet', 'def', 'ffwi', 'fm100', 'pdsi', 'pr', 'tmmx', 'vpd', 'vs')
fpa <- fpa_ll %>%
  slice(1:26) 
unique_states <- unique(fpa$STATE)

for (j in stat){
  for (i in vars) {
    fpa_summaries <- list()
    
    for (k in 1:length(unique_states)) {
      
      sub_df <- subset(fpa, fpa$STATE == unique_states[k])

      tifs <- check_tifs(j, i)
  
      if (is.null(tifs)) {
  
        print(paste0('Skipping ', j, ' ', i))
  
      } else if (!file.exists(file.path(summaries_dir, paste0('fpa_', i, '_', j, '_summaries.rds')))) {
  
        print(paste0('Working on ', j, ' ', i))
        
        #sfInit(parallel = TRUE, cpus = parallel::detectCores())
        #sfExport(list = c("sub_df"))
  
        extractions <- lapply(as.list(tifs),
                                FUN = extract_one,
                                shapefile_extractor = as(sub_df, 'Spatial'))}
        #sfStop()
  
        # ensure that they all have the same length
        stopifnot(all(lapply(extractions, nrow) == nrow(sub_df)))}
  
        # convert to a data frame
        print(paste0("Creating extraction dataframe for ", i, ' ', j, ' summaries'))
  
        extraction_df <- extractions %>%
          bind_cols %>%
          as_tibble %>%
          mutate(index = ID) %>%
          select(-starts_with("ID")) %>%
          rename(ID = index) %>%
          mutate(FPA_ID = data.frame(sub_df)$FPA_ID) %>%
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
                 year_month_day = as.Date(paste(year, month, day, sep='-')))
        
        print(paste0("Creating fpa summaries for ", cause, i, ' ', j))
        
        sub_df <- sub_df %>%
          dplyr::select(FPA_ID, year_month_day)
        
        fpa_summaries[[k]] <- get_climate_lags(sub_df, extraction_df, sub_df$year_month_day, time_lag = 24) %>%
          dplyr::select(-year_month_day, - ymd_lagged)

        # creating the final dataframe takes about 150 GB of RAM, this clears the cache so it can be run again
        gc()
      }
    }
    fpa_summaries <- do.call(rbind, fpa_summaries) # Convert to data frame format
    
    # save the final cleaned climate extractions joined with the fpa-fod database
    summary_name <- file.path(summaries_dir, paste0('fpa_', i, '_', j, '_summaries.rds'))
    write_rds(fpa_out, summary_name)
    
    # push to S3
    #system('aws s3 sync modeling-human-ignition/data/summary s3://earthlab-modeling-human-ignitions/summary')
  }
}
