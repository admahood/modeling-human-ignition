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
}
