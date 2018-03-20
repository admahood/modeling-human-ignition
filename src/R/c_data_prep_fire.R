# Load and process FPA-FOD wildfire iginition data
if (!exists("fpa_clean")) {
  if (file.exists(file.path(processed_dir, "fpa_clean.gpkg"))){
    
    fpa_clean <- st_read(file.path(processed_dir, "fpa_clean.gpkg"))

    } else {
      if (!exists("fpa")) {
        fpa <- sf::st_read(dsn = file.path(fpa_prefix, "Data", "FPA_FOD_20170508.gdb"),
                             layer = "Fires", quiet= FALSE) %>%
            sf::st_transform(st_crs(usa_shp)) %>%
            sf::st_intersection(., st_union(usa_shp))
        }

        fpa_clean <- fpa %>%
          dplyr::select(FPA_ID, LATITUDE, LONGITUDE, ICS_209_INCIDENT_NUMBER, ICS_209_NAME, MTBS_ID, MTBS_FIRE_NAME,
                        FIRE_YEAR, DISCOVERY_DATE, DISCOVERY_DOY, STAT_CAUSE_DESCR, FIRE_SIZE, STATE)  %>%
          dplyr::mutate(cause = ifelse(STAT_CAUSE_DESCR == "Lightning", "Lightning", "Human"),
                        FIRE_SIZE_km2 = (FIRE_SIZE*4046.86)/1000000,
                        doy = day(DISCOVERY_DATE),
                        day = day(DISCOVERY_DATE),
                        month = month(DISCOVERY_DATE),
                        year = FIRE_YEAR,
                        year_month_day = floor_date(ymd(DISCOVERY_DATE), "month"))

        sf::st_write(fpa_clean,
                     file.path(processed_dir, "fpa_clean.gpkg"),
                     driver = "GPKG")

        system(paste0("aws s3 sync ",
                      processed_dir, " ",
                      s3_proc_prefix))
    }
  }
