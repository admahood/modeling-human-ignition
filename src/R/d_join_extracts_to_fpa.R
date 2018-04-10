
# This script will build out an extensive spatial dataframe
# The base layer will be the FPA-FOD 1992-2015 point shapefile
# The layers that will be added will be the extracted dataframes, including:
#   1. 4 terrain metrics
#   2. 9 anthropogenic census derrived vars
#   3. 1 housing density from SILVIS
#   4. All monthly climate vars at the mean, 95th percentile, and number of days above the 95th percentile per month

# Convert the sf objects to dataframes that can be joined into the FPA dataframe
rrden <- as.data.frame(railroad_density) %>%
  dplyr::select(-STUSPS, -pixel_area, -geom)

tlden <- as.data.frame(transmission_lines_density) %>%
  dplyr::select(-STUSPS, -pixel_area, -geom)

prden <- as.data.frame(primary_rds_density) %>%
  dplyr::select(-STUSPS, -pixel_area, -geom)

srden <- as.data.frame(secondary_rds_density) %>%
  dplyr::select(-STUSPS, -pixel_area, -geom)

trden <- as.data.frame(tertiary_rds_density) %>%
  dplyr::select(-pixel_area, -geom)

# Import WUI information
wui <- st_read(dsn = file.path(anthro_dir, "wui_conus.gpkg"))

fpa_ext_var <- fpa_clean %>%
  st_intersection(., hexnet_4k) %>%
  # Add WUI information
  st_intersection(., wui) %>%
  dplyr::select(-BLK10, -WATER10, -SUM_AREA_M, -POPDEN10, -HDEN10, -HHDEN10, -SHDEN10, -AREA, -PUBFLAG, -VEG06, -NONVEG06,  -VEG06PC, -FOREST06,
                -VEG75_06, -HU10, -SHU10, -HHU10, -POP10, -Shape_Length, -Shape_Area, -Class) %>%
  # Add in the ecoregion level 1,2,3,4 information
  st_intersection(., ecoregions_l4) %>%
  dplyr::select(-US_L4CODE, -US_L3CODE, -NA_L3CODE, -NA_L3NAME, -NA_L2CODE, -NA_L1CODE, -L4_KEY, -L3_KEY, -L2_KEY,
                -L1_KEY, -Shape_Leng, -Shape_Area) %>%
  # Add railroad density
  left_join(., as.data.frame(rrden), by = 'hexid4k') %>%
  mutate(railroad_length = length_line,
         railroad_density = density) %>%
  dplyr::select(-length_line,  -density) %>%
  # Add transmission line density
  left_join(., as.data.frame(tlden), by = 'hexid4k') %>%
  mutate(transmission_line_length = length_line,
         transmission_line_density = density) %>%
  dplyr::select(-length_line,  -density) %>%
  # Add primary roads density
  left_join(., as.data.frame(prden), by = 'hexid4k') %>%
  mutate(primary_rd_length = length_line,
         primary_rd_density = density) %>%
  dplyr::select(-length_line,  -density) %>%
  # Add secondary roads density
  left_join(., as.data.frame(srden), by = 'hexid4k') %>%
  mutate(secondary_rd_length = length_line,
         secondary_rd_density = density) %>%
  dplyr::select( -length_line,  -density) %>%
  # Add tertiary roads density
  left_join(., as.data.frame(trden), by = 'hexid4k') %>%
  mutate(tertiary_rd_length = length_line,
         tertiary_rd_density = density) %>%
  dplyr::select(-length_line,  -density)

# Add terrain variables
fpa_ext_var <- read_rds(file.path(terrain_extract, 'terrain_extractions.rds')) %>%
  left_join(fpa_ext_var, ., by = 'FPA_ID')

st_write(fpa_ext_var, file.path(summary_dir, 'fpa_ext_var_anthro.gpkg'))

system("aws s3 sync data s3://earthlab-modeling-human-ignitions")
