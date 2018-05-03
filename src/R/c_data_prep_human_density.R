
# List all decadal tiffs from the CIESIN gridded census products
anthro_list <- list.files(file.path(anthro_dir, 'gridded_census'),
                          pattern = '*.tif',
                          full.names = TRUE,
                          recursive = TRUE)

if (!file.exists(file.path(anthro_dir, 'gridded_census', "ciesin_extraction.rds"))) {

  # Extract all of the gridded products by each FPA-FOD fire iginition point in parallel
  sfInit(parallel = TRUE, cpus = parallel::detectCores())
  sfExport(list = c("fpa_clean"))

  extractions <- sfLapply(as.list(anthro_list),
                          fun = extract_anthro,
                          shp_mask = fpa_clean)
  sfStop()

  # Push the csv outputs to S3 so we do not have to re-run this
  system("aws s3 sync data/anthro/gridded_census s3://earthlab-modeling-human-ignitions/anthro/gridded_census")

  # Quick check to make sure the extractions have been taken for each of the FPA-FOD points
  stopifnot(all(lapply(extractions, nrow) == nrow(fpa_clean)))

  # Create a 'master' extractions table with all of the raw decadal values
  extraction_df <- extractions %>%
    bind_cols %>%
    as_tibble %>%
    mutate(index = ID) %>%
    dplyr::select(-starts_with("ID")) %>%
    rename(ID = index) %>%
    mutate(FPA_ID = data.frame(fpa_clean)$FPA_ID,
           year_month_day = data.frame(fpa_clean)$year_month_day,
           STATE = data.frame(fpa_clean)$STATE) %>%
    dplyr::select(-starts_with('X'))

  extraction_df %>%
    write_rds(., file.path(anthro_dir, 'gridded_census', "ciesin_extraction.rds"))

  system("aws s3 sync data/anthro/gridded_census s3://earthlab-modeling-human-ignitions/anthro/gridded_census")
} else {
  extraction_df <- read_rds(file.path(anthro_dir, 'gridded_census', "ciesin_extraction.rds"))
}

# Clean extraction data frame for imputing --------------------------------

extraction_vars <- extraction_df %>%
  dplyr::select(-usarea00, -usarea10, -usarea90) %>%
  mutate(usba10 = NA,
         usba20 = NA,
         ussevp10 = NA,
         ussevp20 = NA,
         uspov10 = NA,
         uspov20 = NA,
         ushu20 = NA,
         ushs10 = NA,
         ushs20 = NA,
         uslowi10 = NA,
         uslowi20 = NA,
         uspop20 = NA,
         ussea20 = NA) %>%
  gather(variable, value, -FPA_ID, -ID, -year_month_day, -STATE) %>%
  mutate(year = case_when(
    .$variable == 'usba90' ~ 1990, # Population with a bachelors degree
    .$variable == 'usba00' ~ 2000,
    .$variable == 'usba10' ~ 2010,
    .$variable == 'usba20' ~ 2020,
    .$variable == 'ussevp90' ~ 1990, # Population living 50% below the poverty line
    .$variable == 'ussevp00' ~ 2000,
    .$variable == 'ussevp10' ~ 2010,
    .$variable == 'ussevp20' ~ 2020,
    .$variable == 'uspov90' ~ 1990, # Population living 200% below the poverty line
    .$variable == 'uspov00' ~ 2000,
    .$variable == 'uspov10' ~ 2010,
    .$variable == 'uspov20' ~ 2020,
    .$variable == 'ushs90' ~ 1990, # Population with a high school degree
    .$variable == 'ushs00' ~ 2000,
    .$variable == 'ushs10' ~ 2010,
    .$variable == 'ushs20' ~ 2020,
    .$variable == 'ushu90' ~ 1990, # Number of housing units
    .$variable == 'ushu00' ~ 2000,
    .$variable == 'ushu10' ~ 2010,
    .$variable == 'ushu20' ~ 2020,
    .$variable == 'uslowi90' ~ 1990, # Population living below poverty level
    .$variable == 'uslowi00' ~ 2000,
    .$variable == 'uslowi10' ~ 2010,
    .$variable == 'uslowi20' ~ 2020,
    .$variable == 'uspop90' ~ 1990, # Population
    .$variable == 'uspop00' ~ 2000,
    .$variable == 'uspop10' ~ 2010,
    .$variable == 'uspop20' ~ 2020,
    .$variable == 'ussea90' ~ 1990, # Vacant housing unit, for seasonal, recreational or occasional use
    .$variable == 'ussea00' ~ 2000,
    .$variable == 'ussea10' ~ 2010,
    .$variable == 'ussea20' ~ 2020
  )) %>%
  mutate(variable = if_else(grepl("usba", variable), 'bachelors_degree',
                            if_else(grepl("ussevp", variable), 'pop_poverty_below_50',
                                    if_else(grepl("uspov", variable), 'pop_poverty_below_200',
                                            if_else(grepl("ushs", variable), 'highschool_degree',
                                                    if_else(grepl("ushu", variable), 'housing_units',
                                                            if_else(grepl("uslowi", variable), 'pop_poverty_below_line',
                                                                    if_else(grepl("uspop", variable), 'population',
                                                                            if_else(grepl("ussea", variable), 'seasonal_housing_units', variable))))))))) %>%
  dplyr::group_by(FPA_ID, variable) %>%
  arrange(FPA_ID, variable, year) %>%
  dplyr::mutate(
    value = spline(x = year, y = value, xout = year)$y,
    value = if_else(value < 0, 0, value)) %>%
  arrange(FPA_ID, variable, year) %>%
  dplyr::ungroup()

# Impute census variables -------------------------------------------------
# Housing units
if (!file.exists(file.path(anthro_extract, "housing_units_extraction.rds"))) {

  hu <- extraction_vars %>%
    filter(variable == 'housing_units') %>%
    droplevels() %>%
    split(.$STATE)

  sfInit(parallel = TRUE, cpus = parallel::detectCores())
  sfSource('src/functions/helper_functions.R')

  hu_summaries <- sfLapply(hu,
                         function (input_list) {
                           require(tidyverse)
                           require(magrittr)
                           require(lubridate)
                           require(lubridate)
                           require(sf)

                           sub_grid <- dplyr:::bind_cols(input_list)
                           unique_ids <- unique(sub_grid$FPA_ID)
                           state_name <- unique(sub_grid$STATE)[1]

                           print(paste0('Working on ', state_name))

                           got_density <- lapply(unique_ids,
                                                 FUN = impute_in_parallel_ciesin,
                                                 data = sub_grid)
                           print(paste0('Finishing ', state_name))

                           return(got_density)
                         }
  )

  sfStop()

  hu_df <- flattenlist(hu_summaries) %>%
    bind_rows() %>%
    distinct(.keep_all = TRUE) %>%
    mutate(housing_units = housing_units_lag_0,
           housing_units = if_else(housing_units < 0, 0, housing_units)) %>%
    dplyr::select(housing_units)

  hu_df %>%
    write_rds(., file.path(anthro_extract, "housing_units_extraction.rds"))

  system(paste0("aws s3 sync ", summary_dir, " ", s3_proc_extractions))
  rm(list = c('hu_summaries', 'hu_list', 'hu_df'))
}

# Seasonal housing units
if (!file.exists(file.path(anthro_extract, "seasonal_housing_units_extraction.rds"))) {
  shu_list <- extraction_vars %>%
    filter(variable == 'vacant_housing_units') %>%
    split(., .$STATE)

  sfInit(parallel = TRUE, cpus = parallel::detectCores())
  sfSource('src/functions/helper_functions.R')

  shu_summaries <- sfLapply(shu_list, function (input_tibble) {
    require(tidyverse)
    require(magrittr)
    require(lubridate)

    sub_tib <- bind_cols(input_tibble) %>%
      as_tibble
    unique_ids <- unique(sub_tib$FPA_ID)

    lapply(unique_ids,
           FUN = impute_in_parallel_ciesin,
           data = sub_tib)
  }
  )

  sfStop()

  shu_df <- flattenlist(shu_summaries) %>%
    bind_rows() %>%
    distinct(.keep_all = TRUE) %>%
    mutate(seasonal_housing_units = seasonal_housing_units_lag_0,
           seasonal_housing_units = if_else(seasonal_housing_units < 0, 0, seasonal_housing_units)) %>%
    dplyr::select(seasonal_housing_units)

  shu_df %>%
    write_rds(., file.path(anthro_extract, "seasonal_housing_units_extraction.rds"))

  system(paste0("aws s3 sync ", summary_dir, " ", s3_proc_extractions))
  rm(list = c('shu_summaries', 'shu_list', 'shu_df'))

}

# Population
if (!file.exists(file.path(anthro_extract, "pop_extraction.rds"))) {
  pop_list <- extraction_vars %>%
    filter(variable == 'population') %>%
    split(., .$STATE)

  sfInit(parallel = TRUE, cpus = parallel::detectCores())
  sfSource('src/functions/helper_functions.R')

  pop_summaries <- sfLapply(pop_list, function (input_tibble) {
    require(tidyverse)
    require(magrittr)
    require(lubridate)

    sub_tib <- bind_cols(input_tibble) %>%
      as_tibble
    unique_ids <- unique(sub_tib$FPA_ID)

    lapply(unique_ids,
           FUN = impute_in_parallel_ciesin,
           data = sub_tib)
  }
  )

  sfStop()

  pop_df <- flattenlist(pop_summaries) %>%
    bind_rows() %>%
    distinct(.keep_all = TRUE) %>%
    mutate(population = population_lag_0,
           population = if_else(population < 0, 0, population)) %>%
    dplyr::select(FPA_ID, population)

  pop_df %>%
    write_rds(., file.path(anthro_extract, "bpov_200_extraction.rds"))

  system(paste0("aws s3 sync ", summary_dir, " ", s3_proc_extractions))
  rm(list = c('pop_summaries', 'pop_list', 'pop_df'))

}

# Population with a high school degree
if (!file.exists(file.path(anthro_extract, "hsd_extraction.rds"))) {
  hsd_list <- extraction_vars %>%
    filter(variable == 'highschool_degree') %>%
    split(., .$STATE)

  sfInit(parallel = TRUE, cpus = parallel::detectCores())
  sfSource('src/functions/helper_functions.R')

  hsd_summaries <- sfLapply(hsd_list, function (input_tibble) {
    require(tidyverse)
    require(magrittr)
    require(lubridate)

    sub_tib <- bind_cols(input_tibble) %>%
      as_tibble
    unique_ids <- unique(sub_tib$FPA_ID)

    lapply(unique_ids,
           FUN = impute_in_parallel_ciesin,
           data = sub_tib)
  }
  )

  sfStop()

  hsd_df <- flattenlist(hsd_summaries) %>%
    bind_rows() %>%
    distinct(.keep_all = TRUE) %>%
    mutate(highschool_degree = highschool_degree_lag_0,
           highschool_degree = if_else(highschool_degree < 0, 0, highschool_degree)) %>%
    dplyr::select(FPA_ID, highschool_degree)

  hsd_df %>%
    write_rds(., file.path(anthro_extract, "hsd_extraction.rds"))

  system(paste0("aws s3 sync ", summary_dir, " ", s3_proc_extractions))
  rm(list = c('hsd_summaries', 'hsd_list', 'hsd_df'))

}

# Population with a bachelors degree
if (!file.exists(file.path(anthro_extract, "bad_extraction.rds"))) {
  bad_list <- extraction_vars %>%
    filter(variable == 'bachelors_degree') %>%
    split(., .$STATE)

  sfInit(parallel = TRUE, cpus = parallel::detectCores())
  sfSource('src/functions/helper_functions.R')

  bad_summaries <- sfLapply(bad_list, function (input_tibble) {
    require(tidyverse)
    require(magrittr)
    require(lubridate)

    sub_tib <- bind_cols(input_tibble) %>%
      as_tibble
    unique_ids <- unique(sub_tib$FPA_ID)

    lapply(unique_ids,
           FUN = impute_in_parallel_ciesin,
           data = sub_tib)
  }
  )

  sfStop()

  bad_df <- flattenlist(bad_summaries) %>%
    bind_rows() %>%
    distinct(.keep_all = TRUE) %>%
    mutate(highschool_degree = highschool_degree_lag_0,
           highschool_degree = if_else(highschool_degree < 0, 0, highschool_degree)) %>%
    dplyr::select(FPA_ID, highschool_degree)

  bad_df %>%
    write_rds(., file.path(anthro_extract, "bad_extraction.rds"))

  system(paste0("aws s3 sync ", summary_dir, " ", s3_proc_extractions))
  rm(list = c('bad_summaries', 'bad_list', 'bad_df'))

}

# Below the poverty line
if (!file.exists(file.path(anthro_extract, "bpov_extraction.rds"))) {
  bpv_list <- extraction_vars %>%
    filter(variable == 'pop_poverty_below_line') %>%
    split(., .$STATE)

  sfInit(parallel = TRUE, cpus = parallel::detectCores())
  sfSource('src/functions/helper_functions.R')

  bpv_summaries <- sfLapply(bpv_list, function (input_tibble) {
    require(tidyverse)
    require(magrittr)
    require(lubridate)

    sub_tib <- bind_cols(input_tibble) %>%
      as_tibble
    unique_ids <- unique(sub_tib$FPA_ID)

    lapply(unique_ids,
           FUN = impute_in_parallel_ciesin,
           data = sub_tib)
  }
  )

  sfStop()

  bpv_df <- flattenlist(bpv_summaries) %>%
    bind_rows() %>%
    distinct(.keep_all = TRUE) %>%
    mutate(pop_poverty_below_line = pop_poverty_below_line_lag_0,
           pop_poverty_below_line = if_else(pop_poverty_below_line < 0, 0, pop_poverty_below_line)) %>%
    dplyr::select(FPA_ID, pop_poverty_below_line)

  bpv_df %>%
    write_rds(., file.path(anthro_extract, "bpov_extraction.rds"))

  system(paste0("aws s3 sync ", summary_dir, " ", s3_proc_extractions))
  rm(list = c('bpv_summaries', 'bpv_list', 'bpv_df'))

}

# Below the poverty line by 50%
if (!file.exists(file.path(anthro_extract, "bpov_50_extraction.rds"))) {
  bpv50_list <- extraction_vars %>%
    filter(variable == 'pop_poverty_below_50') %>%
    split(., .$STATE)

  sfInit(parallel = TRUE, cpus = parallel::detectCores())
  sfSource('src/functions/helper_functions.R')

  bpv50_summaries <- sfLapply(bpv50_list, function (input_tibble) {
    require(tidyverse)
    require(magrittr)
    require(lubridate)

    sub_tib <- bind_cols(input_tibble) %>%
      as_tibble
    unique_ids <- unique(sub_tib$FPA_ID)

    lapply(unique_ids,
           FUN = impute_in_parallel_ciesin,
           data = sub_tib)
  }
  )

  sfStop()

  bpv50_df <- flattenlist(bpv50_summaries) %>%
    bind_rows() %>%
    distinct(.keep_all = TRUE) %>%
    mutate(pop_poverty_below_50 = pop_poverty_below_50_lag_0,
           pop_poverty_below_50 = if_else(pop_poverty_below_50 < 0, 0, pop_poverty_below_50)) %>%
    dplyr::select(FPA_ID, pop_poverty_below_50)

  bpv50_df %>%
    write_rds(., file.path(anthro_extract, "bpov_50_extraction.rds"))

  system(paste0("aws s3 sync ", summary_dir, " ", s3_proc_extractions))
  rm(list = c('bpv50_summaries', 'bpv50_list', 'bpv50_df'))

}

# Below the poverty line by 200%
if (!file.exists(file.path(anthro_extract, "bpov_200_extraction.rds"))) {
  bpv200_list <- extraction_vars %>%
    filter(variable == 'pop_poverty_below_200') %>%
    split(., .$STATE)

  sfInit(parallel = TRUE, cpus = parallel::detectCores())
  sfSource('src/functions/helper_functions.R')

  bpv200_summaries <- sfLapply(bpv200_list, function (input_tibble) {
    require(tidyverse)
    require(magrittr)
    require(lubridate)

    sub_tib <- bind_cols(input_tibble) %>%
      as_tibble
    unique_ids <- unique(sub_tib$FPA_ID)

    lapply(unique_ids,
           FUN = impute_in_parallel_ciesin,
           data = sub_tib)
  }
  )

  sfStop()

  bpv200_df <- flattenlist(bpv200_summaries) %>%
    bind_rows() %>%
    distinct(.keep_all = TRUE) %>%
    mutate(pop_poverty_below_200 = pop_poverty_below_200_lag_0,
           pop_poverty_below_200 = if_else(pop_poverty_below_200 < 0, 0, pop_poverty_below_200)) %>%
    dplyr::select(FPA_ID, pop_poverty_below_200)

  bpv200_df %>%
    write_rds(., file.path(anthro_extract, "bpov_200_extraction.rds"))

  system(paste0("aws s3 sync ", summary_dir, " ", s3_proc_extractions))
  rm(list = c('bpv200_summaries', 'bpv200_list', 'bpv200_df'))

}
