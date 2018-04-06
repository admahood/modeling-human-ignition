
# This script will build out an extensive spatial dataframe
# The base layer will be the FPA-FOD 1992-2015 point shapefile
# The layers that will be added will be the extracted dataframes, including:
#   1. 4 terrain metrics
#   2. 9 anthropogenic census derrived vars
#   3. 1 housing density from SILVIS
#   4. All monthly climate vars at the mean, 95th percentile, and number of days above the 95th percentile per month

# Add in the ecoregion level 1,2,3,4 information
fpa_ext_var <- fpa_clean %>%
  st_join(., ecoregions_l4, join = st_intersects) %>%
  dplyr::select(-US_L4CODE, -US_L3CODE, -NA_L3CODE, -NA_L3NAME, -NA_L2CODE, -NA_L1CODE, -L4_KEY, -L3_KEY, -L2_KEY,
                -L1_KEY, -Shape_Leng, -Shape_Area)

# Add WUI information
fpa_ext_var <- fpa_ext_var

# Add terrain variables
fpa_ext_var <- read_rds(file.path(terrain_extract, 'terrain_extractions.rds')) %>%
  left_join(fpa_ext_var, ., by = 'FPA_ID')  %>%
  # Add railroad density
  st_join(., railroad_density, join = st_intersects) %>%
  mutate(railroad_length = length_line,
         railroad_density = density) %>%
  dplyr::select(-hexid4k, - STUSPS, - length_line, - pixel_area, -density) %>%
  # Add transmission line density
  st_join(., transmission_lines_density, join = st_intersects) %>%
  mutate(transmission_line_length = length_line,
         transmission_line_density = density) %>%
  dplyr::select(-hexid4k, - STUSPS, - length_line, - pixel_area, -density) %>%
  # Add primary roads density
  st_join(., primary_rds_density, join = st_intersects) %>%
  mutate(primary_rd_length = length_line,
         primary_rd_density = density) %>%
  dplyr::select(-hexid4k, - STUSPS, - length_line, - pixel_area, -density) %>%
  # Add secondary roads density
  st_join(., secondary_rds_density, join = st_intersects) %>%
  mutate(secondary_rd_length = length_line,
         secondary_rd_density = density) %>%
  dplyr::select(-hexid4k, - STUSPS, - length_line, - pixel_area, -density) %>%
  # Add tertiary roads density
  st_join(., tertiary_rds_density, join = st_intersects) %>%
  mutate(tertiary_rd_length = length_line,
         tertiary_rd_density = density) %>%
  dplyr::select(-hexid4k, - STUSPS, - length_line, - pixel_area, -density)
