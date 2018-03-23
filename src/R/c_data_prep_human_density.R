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

pop_den_tibble <- as.data.frame(pop_den_cleaned) %>%
  dplyr::select(PBG00, FPA_ID, STATE, year_month_day, HDEN90, HDEN00, HDEN10, HDEN20) %>%
  as_tibble()

sfInit(parallel = TRUE, cpus = parallel::detectCores())
sfExport('pop_den_tibble')
sfSource('src/functions/housing_density_helper_functions.r')

fp_pop_den_summaries <- sfLapply(seq_along(1:nrow(pop_den_tibble)),
                              fun = impute_in_parallel,
                              input_tibble = pop_den_tibble)
sfStop()

# convert to a data frame
extraction_df <- fp_pop_den_summaries %>%
  bind_cols %>%
  as_tibble %>%
  write_rds(., file.path(popden_extract, "pop_den_extraction.rds"))

system(paste0("aws s3 sync ", summary_dir, " ", s3_proc_extractions))
