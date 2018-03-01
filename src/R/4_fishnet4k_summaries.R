
if (!exists("fpa_ll")) {
  
  fpa_ll <- st_read(file.path(processed_dir, "fpa_clean.gpkg")) %>%
    st_transform(st_crs(usa_shp)) %>%
    st_intersection(., st_union(usa_shp)) %>%
    st_transform("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")
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
    
    tifs <- list.files(paste0('data/', j),
                       pattern = i,
                       recursive = TRUE,
                       full.names = TRUE) %>%
      Filter(function(x) grepl(".tif", x), .)
    
    sfInit(parallel = TRUE, cpus = parallel::detectCores())
    sfExport(list = c("fpa_ll"))
    
    extractions <- sfLapply(as.list(tifs),
                            fun = extract_one,
                            fpa_ll = fpa_ll)
    sfStop()
    
    # ensure that they all have the same length
    stopifnot(all(lapply(extractions, nrow) == nrow(fpa_ll)))
    
    if (!file.exists(paste0('data/', j, '/fpa_', j, '_', i, '_summaries.rds'))) {
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
               ID = as.integer(ID))
      
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
      
      summary_name <- paste0('data/', j, '/fpa_', j, '_', i, '_summaries.rds')
      write_rds(fpa_summaries, summary_name)
    }
    
    # save extraction and summary file for each variable
    extract_name <- paste0('data/', j, '/fpa_', j, '_', i, '_extractions.rds')
    if (!file.exists(extract_name)) {
      write_rds(extractions, extract_name)
    }
    
    # push to S3
    system(paste0('aws s3 sync ', 'data/', j, ' ', 's3://earthlab-natem/data/climate/key-vars/', j))
    
    # to conserve space delete the large files
    unlink(extract_name)
    unlink(summary_name)
  }
}
