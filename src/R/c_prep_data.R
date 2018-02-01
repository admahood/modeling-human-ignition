source("src/R/a_make_dirs.R")
source("src/functions/download-data.R")

# Import the Level 3 Ecoregions data
ecoregions <- load_data(url = "ftp://newftp.epa.gov/EPADataCommons/ORD/Ecoregions/us/us_eco_l3.zip",
                        dir = ecoregion_prefix,
                        layer = "us_eco_l3",
                        outname = "ecoregion") %>%
  st_simplify(., preserveTopology = TRUE, dTolerance = 1000)  %>%
  mutate(NA_L3NAME = as.character(NA_L3NAME),
         NA_L3NAME = ifelse(NA_L3NAME == 'Chihuahuan Desert',
                            'Chihuahuan Deserts',
                            NA_L3NAME))

# CONUS states
usa_shp <- load_data(url = "https://www2.census.gov/geo/tiger/GENZ2016/shp/cb_2016_us_state_20m.zip",
                    dir = us_prefix,
                    layer = "cb_2016_us_state_20m",
                    outname = "usa") %>%
  st_transform(st_crs(ecoregions)) %>%
  filter(!STUSPS %in% c("HI", "AK", "PR"))

# Read fire data ----------------------
fpa <- st_read(dsn = file.path(fpa_prefix, "Data", "FPA_FOD_20170508.gdb"),
                    layer = "Fires", quiet= FALSE) %>%
  st_transform(st_crs(ecoregions)) %>%
  st_intersection(., usa_shp)
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
                               sep = "-")))

# match each ignition to an ecoregion file.path(ov, "ov.rds")
if (!file.exists("ov.rds")) {
  st_over <- function(x, y) {
    sapply(st_intersects(x,y),
           function(z) if (length(z)==0) NA_integer_ else z[1])
  }
  ov <- st_over(fpa_clean, ecoregions)
  write_rds(ov, "ov.rds")
}
ov <- read_rds("ov.rds")

fpa_clean <- fpa_clean %>%
  mutate(NA_L3NAME = ecoregions$NA_L3NAME[ov]) %>%
  filter(!is.na(NA_L3NAME))

unique_er_yms <- expand.grid(
  NA_L3NAME = unique(ecoregions$NA_L3NAME),
  year = unique(fpa_clean$year),
  month = unique(fpa_clean$month)) %>%
  as_tibble

# count the number of fires in each ecoregion in each month
count_df <- fpa_clean %>%
  tbl_df %>%
  dplyr::select(-Shape) %>%
  group_by(NA_L3NAME, cause, year, month) %>%
  summarize(n_fire = n()) %>%
  ungroup %>%
  full_join(unique_er_yms) %>%
  mutate(n_fire = ifelse(is.na(n_fire), 0, n_fire),
         ym = as.yearmon(paste(year, sprintf("%02d", month), sep = "-"))) %>%
  arrange(ym) 

assert_that(0 == sum(is.na(count_df$NA_L3NAME)))
assert_that(sum(count_df$n_fire) == nrow(fpa_clean))
assert_that(all(ecoregions$NA_L3NAME %in% count_df$NA_L3NAME))

# load covariate data and link to count data frame
ecoregion_summaries <- read_csv('https://s3-us-west-2.amazonaws.com/earthlab-gridmet/ecoregion_summaries.csv',
                                col_types = cols(
                                  NA_L3NAME = col_character(),
                                  variable = col_character(),
                                  year = col_number(),
                                  month = col_number(),
                                  wmean = col_number())
) %>%
  mutate(year = ifelse(year == 2, 2000, year),
         year = parse_number(year),
         ym = as.yearmon(paste(year,
                               sprintf("%02d", month),
                               sep = "-"))) %>%
  spread(variable, wmean)
