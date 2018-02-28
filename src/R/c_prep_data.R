source("src/functions/download-data.R")

# Download and import the Level 3 Ecoregions data
# Download will only happen once as long as the file exists
if (!exists("ecoregions")){
  ecoregions <- load_data(url = "ftp://newftp.epa.gov/EPADataCommons/ORD/Ecoregions/us/us_eco_l3.zip",
                          dir = ecoregion_prefix, layer = "us_eco_l3", outname = "ecoregion") %>%
    st_simplify(., preserveTopology = TRUE, dTolerance = 1000)  %>%
    mutate(NA_L3NAME = as.character(NA_L3NAME),
           NA_L3NAME = ifelse(NA_L3NAME == 'Chihuahuan Desert',
                              'Chihuahuan Deserts',
                              NA_L3NAME))
}

# Download and import CONUS states
# Download will only happen once as long as the file exists
if (!exists("usa_shp")){
  usa_shp <- load_data(url = "https://www2.census.gov/geo/tiger/GENZ2016/shp/cb_2016_us_state_20m.zip",
                      dir = us_prefix,
                      layer = "cb_2016_us_state_20m",
                      outname = "usa") %>%
    st_transform(st_crs(ecoregions)) %>%
    filter(!STUSPS %in% c("HI", "AK", "PR"))
}

# Create raster mask
# 4k Fishnet
if (!exists("fishnet_4k")) {
 if (!file.exists(file.path(fishnet_path, "fishnet_4k.gpkg"))) {
  fishnet_4k <- st_make_grid(usa_shp, cellsize = 4000, what = 'polygons') %>%
  st_sf('geometry' = ., data.frame('fishid4k' = 1:length(.))) %>%
  st_intersection(., st_union(usa_shp))

  st_write(fishnet_4k,
           file.path(fishnet_path, "fishnet_4k.gpkg"),
           driver = "GPKG")
  system(paste0("aws s3 cp ",
                fishnet_path, "/fishnet_4k.gpkg ",
                s3_anc_prefix, "fishnet/fishnet_4k.gpkg"))
 }
}

# Load and process FPA-FOD wildfire iginition data
if (!exists("fpa_clean")) {
  if (!file.exists(file.path(processed_dir, "fpa_clean.gpkg"))){
    if (!exists("fpa")) {
      fpa <- st_read(dsn = file.path(fpa_prefix, "Data", "FPA_FOD_20170508.gdb"),
                     layer = "Fires", quiet= FALSE) %>%
        st_transform(st_crs(usa_shp)) %>%
        st_intersection(., st_union(usa_shp))
    }

    fpa_clean <- fpa %>%
    filter(FIRE_SIZE >= 1 & STAT_CAUSE_DESCR != "HUMAN") %>%
    dplyr::select(FPA_ID, LATITUDE, LONGITUDE, ICS_209_INCIDENT_NUMBER, ICS_209_NAME, MTBS_ID, MTBS_FIRE_NAME,
                  FIRE_YEAR, DISCOVERY_DATE, DISCOVERY_DOY, STAT_CAUSE_DESCR, FIRE_SIZE, STATE)  %>%
    mutate(cause = ifelse(STAT_CAUSE_DESCR == "Lightning", "Lightning", "Human"),
           FIRE_SIZE_km2 = (FIRE_SIZE*4046.86)/1000000,
           doy = day(DISCOVERY_DATE),
           month = month(DISCOVERY_DATE),
           year = FIRE_YEAR,
           ym = as.yearmon(paste(year, sprintf("%02d", month),
                                 sep = "-"))) %>%
    st_join(., fishnet_4k, join = st_intersects)

    st_write(fpa_clean,
             file.path(processed_dir, "fpa_clean.gpkg"),
             driver = "GPKG")
    system(paste0("aws s3 cp ",
                  processed_dir, "/fpa_clean.gpkg",
                  s3_proc_prefix, "fpa_clean.gpkg"))
    }
  }

# count the number of human and lightning fires in each pixel in each month for each year
# count_df <- fpa_clean %>%
#   tbl_df %>%
#   dplyr::select(-Shape) %>%
#   group_by(fishid4k, cause, year, month) %>%
#   summarize(n_fire = n()) %>%
#   ungroup %>%
#   mutate(n_fire = ifelse(is.na(n_fire), 0, n_fire),
#          ym = as.yearmon(paste(year, sprintf("%02d", month), sep = "-"))) %>%
#   arrange(ym)
#
# assert_that(0 == sum(is.na(count_df$fishid4k)))
# assert_that(sum(count_df$n_fire) == nrow(fpa_clean))
# assert_that(all(fishnet_4k$fishid4k %in% count_df$fishid4k))
# last line is throwing back errors...


#### This DOESNT WORK AS OF 2/7/2018 - still compiling the summaries table!#################
# load covariate data and link to count data frame

# ecoregion_summaries <- read_csv(<add url here>,
#                                 col_types = cols(
#                                   NA_L3NAME = col_character(),
#                                   variable = col_character(),
#                                   year = col_number(),
#                                   month = col_number(),
#                                   wmean = col_number())
# ) %>%
#   mutate(year = ifelse(year == 2, 2000, year),
#          year = parse_number(year),
#          ym = as.yearmon(paste(year,
#                                sprintf("%02d", month),
#                                sep = "-"))) %>%
#   spread(variable, wmean)

# Import ancillary data
# Roads

# if (!exists("rds")) {
#   rds <-
#     st_read(dsn = file.path(roads_prefix, "tlgdb_2015_a_us_roads.gdb"), layer = 'Roads')
# }

# Primary Roads
if (!exists("primary_rds")) {
  if (!file.exists(file.path(processed, "primary_rds.gpkg"))) {
    if (!exists("rds")) {
    rds <-
      st_read(dsn = file.path(roads_prefix, "tlgdb_2015_a_us_roads.gdb"), layer = 'Roads')}
    primary_rds <- rds %>%
    filter(MTFCC == "S1100") %>%
    st_transform(p4string_ea) %>%
    st_intersection(., st_transform(usa_shp, p4string_ea)) %>%
    mutate(bool_prds = 1)

    st_write(primary_rds,
           file.path(processed, "primary_rds.gpkg"),
           driver = "GPKG")
    }
  }

# Secondary roads
if (!exists("secondary_rds")) {
  if (!file.exists(file.path(processed, "secondary_rds.gpkg"))) {
    if (!exists("rds")) {
      rds <-
        st_read(dsn = file.path(roads_prefix, "tlgdb_2015_a_us_roads.gdb"), layer = 'Roads')}
  secondary_rds <- rds %>%
    filter(MTFCC == "S1200") %>%
    st_transform(p4string_ea) %>%
    st_intersection(., usa_shp) %>%
    mutate(bool_srds = 1)


    st_write(secondary_rds,
             file.path(processed, "secondary_rds.gpkg"),
             driver = "GPKG")
    }
  }

# All major roads
if (!exists("all_rds")) {
  if (!file.exists(file.path(processed, "all_rds.gpkg"))) {
    if (!exists("rds")) {
      rds <-
        st_read(dsn = file.path(roads_prefix, "tlgdb_2015_a_us_roads.gdb"), layer = 'Roads')}
  all_rds <- rds %>%
    filter(MTFCC == "S1200" | MTFCC == "S1200") %>%
    st_transform(p4string_ea) %>%
    st_intersection(., usa_shp) %>%
    mutate(bool_ards = 1)


    st_write(all_rds,
         file.path(processed, "all_rds.gpkg"),
         driver = "GPKG")
  }
}

# Railrods
if (!exists("rail_rds")) {
  if (!file.exists(file.path(processed, "rail_rds.gpkg"))) {
  rail_rds <- st_read(dsn = file.path(rails_prefix, 'tlgdb_2015_a_us_rails.gdb'), layer = 'Rails') %>%
    st_transform(p4string_ea) %>%
    st_intersection(., usa_shp) %>%
    mutate(bool_rrds = 1)

    st_write(rail_rds,
         file.path(anthro_dir, "rail_rds.gpkg"),
         driver = "GPKG",
         update=TRUE,
         delete_dsn=TRUE)
  }
}


# Power transmission lines
if (!exists("tl")) {
  if (!file.exists(file.path(processed, "power_lines.gpkg"))) {
    tl <- st_read(dsn = file.path(tl_prefix, 'Electric_Power_Transmission_Lines.shp')) %>%
      st_transform(p4string_ea) %>%
      st_intersection(., usa_shp) %>%
      mutate(bool_tl = 1) %>%
      filter(st_is(., c("LINESTRING")))

      st_write(tl,
           file.path(anthro_dir, "power_lines.gpkg"),
           driver = "GPKG")
  }
}



## terrain data
## see how max interpolated housing density

raster::terrain()
