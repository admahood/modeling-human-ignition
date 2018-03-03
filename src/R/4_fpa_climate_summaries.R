
if (!exists("fpa_ll")) {
  if (file.exists(file.path(processed_dir, "fpa_ll.gpkg"))) {

    fpa_ll <- st_read(file.path(processed_dir, "fpa_ll.gpkg"))

    } else if (!file.exists(file.path(processed_dir, "fpa_clean.gpkg"))) {

      fpa_ll <- fpa_clean %>%
        st_transform(crs = st_crs(usa_shp)) %>%
        st_intersection(., st_union(usa_shp)) %>%
        st_transform(crs = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")

      st_read(fpa_ll, file.path(processed_dir, "fpa_ll.gpkg"))

      system(paste0("aws s3 sync ",
                    processed_dir, " ",
                    s3_proc_prefix))

      } else if (exists("fpa_clean")) {

        fpa_ll <- st_read(file.path(processed_dir, "fpa_clean.gpkg")) %>%
            st_transform(crs = st_crs(usa_shp)) %>%
            st_intersection(., st_union(usa_shp)) %>%
            st_transform(crs = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")

        st_read(fpa_ll, file.path(processed_dir, "fpa_ll.gpkg"))

        system(paste0("aws s3 sync ",
                      processed_dir, " ",
                      s3_proc_prefix))
      }
}

extract_one <- function(filename, fpa_ll) {
  out_name <- gsub('.tif', '.csv', filename)
  if (!file.exists(out_name)) {
    res <- raster::extract(raster::stack(filename), fpa_ll,
                           na.rm = TRUE, fun = mean, df = TRUE)
    write.csv(res, file = out_name)
  } else {
    res <- read.csv(out_name)
  }
  res
}

stat <- c('mean', 'days-above-95th', '95th-percentile')
vars <- c('aet', 'def', 'ffwi', 'fm100', 'pdsi', 'pr', 'tmmx', 'vpd', 'vs')

for (j in stat){
  for (i in vars) {

    # checks whether the statistic being evaluated has the variable data
    # if not then returns NULL
    check_tifs <- function(j, i, ...) {
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
      if (length(tif) > 1) {
        tif <- list.files(file.path(climate_prefix, j),
                          pattern = i,
                          recursive = TRUE,
                          full.names = TRUE) %>%
          Filter(function(x) grepl(".tif", x), .)
      } else {
        tif <- NULL
      }
    }

    tifs <- check_tifs(j, i)

    if (is.null(tifs)) {

        print(paste0('Skipping ', j, i))

      } else if (!file.exists(file.path(summaries_dir, paste0('fpa_', j, '_', i, '_summaries.rds')))) {

        sfInit(parallel = TRUE, cpus = 30) #parallel::detectCores())
        sfExport(list = c("fpa_ll"))

        extractions <- sfLapply(as.list(tifs),
                                fun = extract_one,
                                fpa_ll = fpa_ll)
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
          mutate(FPA_ID = data.frame(fpa_ll)$fpa_id) %>%
          dplyr::select(-starts_with('X')) %>%
          gather(variable, value, -FPA_ID, -ID) %>%
          filter(!is.na(value)) %>%
          mutate(FPA_ID = as.character(FPA_ID),
                 ID = as.integer(ID))

        # quick check to know where we are in the processing
        print(paste0('Creating final ', j, i, 'summaries'))

        # clean the final, long climate data frame with linked fpa ids
        fpa_summaries <- extraction_df %>%
          separate(variable,
                   into = c("variable", 'year', "varmonth"),
                   sep = "_") %>%
          separate(varmonth,
                   into = c("statistic", "month"),
                   sep = "\\.") %>%
          mutate(year = parse_number(year),
                 month = parse_number(month)) %>%
          arrange(ID, FPA_ID, year, month, variable)

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
