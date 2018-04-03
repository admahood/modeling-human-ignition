
anthro_list <- list.files(file.path(anthro_dir, 'gridded_census'),
                          pattern = '*.tif',
                          full.names = TRUE,
                          recursive = TRUE)

sfInit(parallel = TRUE, cpus = parallel::detectCores())
sfExport(list = c("fpa_clean"))

extractions <- sfLapply(as.list(anthro_list),
                        fun = extract_anthro,
                        shp_mask = fpa_clean)
sfStop()

stopifnot(all(lapply(extractions, nrow) == nrow(fpa_clean)))

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

# Bachelors degree
extraction_df$STATE <- droplevels(extraction_df$STATE)
ba_list <- extraction_df %>%
  dplyr::select(ID, FPA_ID, year_month_day, STATE, usba00, usba90) %>%
  mutate(usba10 = NA,
         usba20 = NA) %>%
  gather(variable, value, -FPA_ID, -ID, -year_month_day, -STATE) %>%
  mutate(year = case_when(
    .$variable == 'usba90' ~ 1990,
    .$variable == 'usba00' ~ 2000,
    .$variable == 'usba10' ~ 2010,
    .$variable == 'usba20' ~ 2020
  )) %>% 
  dplyr::group_by(FPA_ID) %>%
  arrange(FPA_ID, year) %>% 
  dplyr::mutate(
    value = spline(x = year, y = value, xout = year)$y,
    value = if_else(value < 0, 0, value),
    variable = 'pop_bach_degree') %>% 
  dplyr::ungroup() %>%
  split(., .$STATE)


sfInit(parallel = TRUE, cpus = parallel::detectCores())
sfSource('src/functions/helper_functions.R')

ba_summaries <- sfLapply(ba_list, function (input_tibble) {
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

extraction_df <- flattenlist(ba_summaries) %>%
  bind_rows() %>%
  distinct(.keep_all = TRUE) %>%
  mutate(bachelor_degree = pop_bach_degree_lag_0,
         bachelor_degree = if_else(bachelor_degree < 0, 0, bachelor_degree)) %>%
  dplyr::select(FPA_ID, bachelor_degree)


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
