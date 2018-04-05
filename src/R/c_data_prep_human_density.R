
# List all decadal tiffs from the CIESIN gridded census products
anthro_list <- list.files(file.path(anthro_dir, 'gridded_census'),
                          pattern = '*.tif',
                          full.names = TRUE,
                          recursive = TRUE)

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

var_list <- extraction_df %>%
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
                                                                            if_else(grepl("ussea", variable), 'vacant_housing_units', variable))))))))) %>%
  dplyr::group_by(FPA_ID, variable) %>%
  arrange(FPA_ID, variable, year) %>%
  dplyr::mutate(
    value = spline(x = year, y = value, xout = year)$y,
    value = if_else(value < 0, 0, value)) %>%
  arrange(FPA_ID, variable, year) %>%
  dplyr::ungroup() %>%
  split(., .$variable)

sfInit(parallel = TRUE, cpus = parallel::detectCores())
sfSource('src/functions/helper_functions.R')
sfExport('var_list')

var_summaries <- sfLapply(var_list, function (input_tibble) {
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

ba_df <- flattenlist(ba_summaries) %>%
  bind_rows() %>%
  distinct(.keep_all = TRUE) %>%
  mutate(bachelor_degree = pop_bach_degree_lag_0,
         bachelor_degree = if_else(bachelor_degree < 0, 0, bachelor_degree)) %>%
  dplyr::select(FPA_ID, bachelor_degree)

ba_df %>%
  write_rds(., file.path(anthro_extract, "bachelor_degree_extraction.rds"))

system(paste0("aws s3 sync ", summary_dir, " ", s3_proc_extractions))

# Below the poverty line by 200%
bpv200_list <- extraction_df %>%
  dplyr::select(ID, FPA_ID, year_month_day, STATE, uspov00, uspov90) %>%
  mutate(uspov10 = NA,
         uspov20 = NA) %>%
  gather(variable, value, -FPA_ID, -ID, -year_month_day, -STATE) %>%
  mutate(year = case_when(
    .$variable == 'uspov90' ~ 1990,
    .$variable == 'uspov00' ~ 2000,
    .$variable == 'uspov10' ~ 2010,
    .$variable == 'uspov20' ~ 2020
  )) %>%
  dplyr::group_by(FPA_ID) %>%
  arrange(FPA_ID, year) %>%
  dplyr::mutate(
    value = spline(x = year, y = value, xout = year)$y,
    value = if_else(value < 0, 0, value),
    variable = 'bpov_200') %>%
  dplyr::ungroup() %>%
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
  mutate(bpov_200 = bpov_200_lag_0,
         bpov_200 = if_else(bpov_200 < 0, 0, bpov_200)) %>%
  dplyr::select(FPA_ID, bpov_200)

bpv200_df %>%
  write_rds(., file.path(anthro_extract, "bpov_200_extraction.rds"))

system(paste0("aws s3 sync ", summary_dir, " ", s3_proc_extractions))


# import and transform the SILVIS lab partial census block housing density data
# clean, rename, and group the housing denisty data
# orignially grouped by FPA id but 3k polygons is a lot easier to compute than 1.8M fire points

if (!exists("hden_cleaned")) {
  if (!file.exists(file.path(anthro_proc_dir, "fpa_overlay_hden.gpkg"))) {
    hden_cleaned <-
      st_read(file.path(pd_prefix, 'us_pbg00_2007.gdb')) %>%
      select(PBG00, HDEN90:HDEN30) %>%
      st_transform(st_crs(usa_shp)) %>%
      st_intersection(fpa_clean, .)

    sf::st_write(
      hden_cleaned,
      file.path(anthro_proc_dir, "fpa_overlay_hden.gpkg"),
      driver = "GPKG"
    )

    system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))

  } else {
    hden_cleaned <-
      st_read(file.path(anthro_proc_dir, "fpa_overlay_hden.gpkg"))

  }
}

# we take random rows to each cluster, by sampleid
hden_cleaned <- hden_cleaned %>%
  mutate(sampled = sample(1:parallel::detectCores(), nrow(.), replace = TRUE))

hden_list <- as.data.frame(hden_cleaned) %>%
  dplyr::select(PBG00, FPA_ID, STATE, year_month_day, HDEN90, HDEN00, HDEN10, HDEN20, sampled) %>%
  split_tibble_to_list(., .$sampled)

sfInit(parallel = TRUE, cpus = parallel::detectCores())
sfSource('src/functions/helper_functions.R')

fp_hden_summaries <- sfLapply(hden_list, function (input_tibble) {
  require(tidyverse)
  require(magrittr)
  require(lubridate)

  sub_tib <- do.call(cbind, input_tibble) %>%
    as_tibble %>%
    dplyr::select(-sampled)
  unique_ids <- sub_tib$FPA_ID

  lapply(unique_ids,
    FUN = impute_in_parallel,
    data = sub_tib)
    }
  )

sfStop()

# convert to a data frame
flattenlist <- function(x){
  morelists <- sapply(x, function(xprime) class(xprime)[1]=="list")
  out <- c(x[!morelists], unlist(x[morelists], recursive=FALSE))
  if(sum(morelists)){
    Recall(out)
  }else{
    return(out)
  }
}

extraction_df <- flattenlist(fp_hden_summaries) %>%
  bind_rows()

extraction_df %>%
  write_rds(., file.path(popden_extract, "hden_extraction.rds"))


system(paste0("aws s3 sync ", summary_dir, " ", s3_proc_extractions))
