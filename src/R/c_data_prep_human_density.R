# import and transform the SILVIS lab partial census block housing density data
# clean, rename, and group the housing denisty data
# orignially grouped by FPA id but 3k polygons is a lot easier to compute than 1.8M fire points

if (!exists("pop_den_cleaned")) {
  if (!file.exists(file.path(anthro_proc_dir, "fpa_overlay_pop_den.gpkg"))) {
    pop_den_cleaned <-
      st_read(file.path(pd_prefix, 'us_pbg00_2007.gdb')) %>%
      select(PBG00, HDEN90:HDEN30) %>%
      st_transform(st_crs(usa_shp)) %>%
      st_intersection(fpa_clean, .)

    sf::st_write(
      pop_den_cleaned,
      file.path(anthro_proc_dir, "fpa_overlay_pop_den.gpkg"),
      driver = "GPKG"
    )

    system(paste0("aws s3 sync ", processed_dir, " ", s3_proc_prefix))

  } else {
    pop_den_cleaned <-
      st_read(file.path(anthro_proc_dir, "fpa_overlay_pop_den.gpkg"))

  }
}

num_of_cores <- parallel::detectCores()
data_per_core <- floor(nrow(pop_den_cleaned)/num_of_cores)

# we take random rows to each cluster, by sampleid
pop_den_cleaned <- pop_den_cleaned %>%
  mutate(sampled = sample(1:num_of_cores, nrow(.), replace = T))

pop_den_list <- as.data.frame(pop_den_cleaned) %>%
  dplyr::select(PBG00, FPA_ID, STATE, year_month_day, HDEN90, HDEN00, HDEN10, HDEN20, sampled) %>%
  split_tibble_to_list(., .$sampled)

sfInit(parallel = TRUE, cpus = parallel::detectCores())
sfSource('src/functions/helper_functions.R')

fp_pop_den_summaries <- sfLapply(pop_den_list, function (input_tibble) {
  require(tidyverse)
  require(magrittr)
  require(lubridate)

  sub_tib <- do.call(cbind, input_tibble) %>%
    as_tibble %>%
    dplyr::select(-sampled)
  unique_ids <- sub_tib$FPA_ID

    lapply(unique_ids,
         data = sub_tib,
         function(data, x) {

           sub_data <- subset(data, data$FPA_ID == x)

           extraction_df <- sub_data %>%
             dplyr::select(-year_month_day) %>%
             gather(variable, value, -FPA_ID, -PBG00, -STATE) %>%
             mutate(value = ifelse(value == -999, is.na(value), value)) %>%
             filter(!is.na(value)) %>%
             mutate(
               year = case_when(
                 .$variable == 'HDEN90' ~ 1990,
                 .$variable == 'HDEN00' ~ 2000,
                 .$variable == 'HDEN10' ~ 2010,
                 .$variable == 'HDEN20' ~ 2020
               )
             ) %>%
             do(impute_density(.))

           # reduce the size of the dataframe to be joined during the get_climate_lags
           sub_df <- sub_data %>%
             dplyr::select(FPA_ID, PBG00, year_month_day) %>%
             mutate(year_month_day = as.Date(year_month_day, origin="1970-01-01"))

           fpa_out <-
             get_lags(
               extract_to = sub_df,
               extract_from = extraction_df,
               start_date = sub_df$year_month_day,
               time_lag = 0
             ) %>%
             dplyr::select(-year_month_day)

           fpa_out
         })
})

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
extraction_df <- flattenlist(fp_pop_den_summaries) %>%
  bind_rows()


write_rds(., file.path(popden_extract, "pop_den_extraction.rds"))

system(paste0("aws s3 sync ", summary_dir, " ", s3_proc_extractions))
