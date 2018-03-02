hex_points <- spsample(as(usa_shp, 'Spatial'), type = "hexagonal", cellsize = 4000)
hex_grid <- HexPoints2SpatialPolygons(hex_points, dx = 4000)
hex_grid_4k <- st_as_sf(hex_grid) %>%
  mutate(hexid = row_number()) %>%
  st_intersection(., st_union(usa_shp)) %>%
  st_transform("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")

hex_points <- spsample(as(usa_shp, 'Spatial'), type = "hexagonal", cellsize = 8000)
hex_grid <- HexPoints2SpatialPolygons(hex_points, dx = 8000)
hex_grid_8k <- st_as_sf(hex_grid) %>%
  mutate(hexid = row_number()) %>%
  st_intersection(., st_union(usa_shp)) %>%
  st_transform("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")

hex_points <- spsample(as(usa_shp, 'Spatial'), type = "hexagonal", cellsize = 10000)
hex_grid <- HexPoints2SpatialPolygons(hex_points, dx = 10000)
hex_grid_10k <- st_as_sf(hex_grid) %>%
  mutate(hexid = row_number()) %>%
  st_intersection(., st_union(usa_shp)) %>%
  st_transform("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")

hex_points <- spsample(as(usa_shp, 'Spatial'), type = "hexagonal", cellsize = 12000)
hex_grid <- HexPoints2SpatialPolygons(hex_points, dx = 12000)
hex_grid_12k <- st_as_sf(hex_grid) %>%
  mutate(hexid = row_number()) %>%
  st_intersection(., st_union(usa_shp)) %>%
  st_transform("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")

hex_points <- spsample(as(usa_shp, 'Spatial'), type = "hexagonal", cellsize = 25000)
hex_grid <- HexPoints2SpatialPolygons(hex_points, dx = 25000)
hex_grid_25k <- st_as_sf(hex_grid) %>%
  mutate(hexid = row_number()) %>%
  st_intersection(., st_union(usa_shp)) %>%
  st_transform("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")

paste('At 4k resolution there are', nrow(hex_grid_4k), 'hexagons equating to', nrow(hex_grid_4k) * 12 * (2015 - 1992), 'hexagons per climate time series')

paste('At 8k resolution there are', nrow(hex_grid_8k), 'hexagons equating to', nrow(hex_grid_8k) * 12 * (2015 - 1992), 'hexagons per climate time series')

paste('At 10k resolution there are', nrow(hex_grid_10k), 'hexagons equating to', nrow(hex_grid_10k) * 12 * (2015 - 1992), 'hexagons per climate time series')

paste('At 12k resolution there are', nrow(hex_grid_12k), 'hexagons equating to', nrow(hex_grid_12k) * 12 * (2015 - 1992), 'hexagons per climate time series')

paste('At 25k resolution there are', nrow(hex_grid_25k), 'hexagons equating to', nrow(hex_grid_25k) * 12 * (2015 - 1992), 'hexagons per climate time series')

fishnet_4k <- st_make_grid(usa_shp, cellsize = 4000, what = 'polygons') %>%
  st_sf('geometry' = ., data.frame('fishid' = 1:length(.))) %>%
  st_intersection(., st_union(usa_shp)) %>%
  st_transform("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")

fishnet_8k <- st_make_grid(usa_shp, cellsize = 8000, what = 'polygons') %>%
  st_sf('geometry' = ., data.frame('fishid' = 1:length(.))) %>%
  st_intersection(., st_union(usa_shp)) %>%
  st_transform("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")

fishnet_10k <- st_make_grid(usa_shp, cellsize = 10000, what = 'polygons') %>%
  st_sf('geometry' = ., data.frame('fishid' = 1:length(.))) %>%
  st_intersection(., st_union(usa_shp)) %>%
  st_transform("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")

fishnet_12k <- st_make_grid(usa_shp, cellsize = 12000, what = 'polygons') %>%
  st_sf('geometry' = ., data.frame('fishid' = 1:length(.))) %>%
  st_intersection(., st_union(usa_shp)) %>%
  st_transform("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")

fishnet_25k <- st_make_grid(usa_shp, cellsize = 25000, what = 'polygons') %>%
  st_sf('geometry' = ., data.frame('fishid' = 1:length(.))) %>%
  st_intersection(., st_union(usa_shp)) %>%
  st_transform("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")

paste('At 4k resolution there are', nrow(fishnet_4k), 'pixels equating to', nrow(fishnet_4k) * 12 * (2015 - 1992), 'pixels per climate time series')

paste('At 8k resolution there are', nrow(fishnet_8k), 'pixels equating to', nrow(fishnet_8k) * 12 * (2015 - 1992), 'pixels per climate time series')

paste('At 10k resolution there are', nrow(fishnet_10k), 'pixels equating to', nrow(fishnet_10k) * 12 * (2015 - 1992), 'pixels per climate time series')

paste('At 12k resolution there are', nrow(fishnet_12k), 'pixels equating to', nrow(fishnet_12k) * 12 * (2015 - 1992), 'pixels per climate time series')

paste('At 25k resolution there are', nrow(fishnet_25k), 'pixels equating to', nrow(fishnet_25k) * 12 * (2015 - 1992), 'pixels per climate time series')



# when ready this will create a grouped weighted mean based on the aggregation of fpa data per pixel/hexagon
ecoregion_summaries <- extraction_df %>%
  separate(variable,
           into = c("variable", 'year', "varmonth"),
           sep = "_") %>%
  separate(varmonth,
           into = c("statistic", "month"),
           sep = "\\.") %>% 
  group_by(FPA_ID, variable, year, month) %>%
  summarize(wmean = weighted.mean(value, Shape_Area)) %>%
  ungroup %>%
  mutate(year = parse_number(year),
         month = parse_number(month)) %>%
  arrange(year, month, variable, fishid4k)

# count the number of fires in each ecoregion in each month
count_df <- mtbs %>%
  tbl_df %>%
  dplyr::select(-geometry) %>%
  group_by(NA_L3NAME, FIRE_YEAR, FIRE_MON) %>%
  summarize(n_fire = n()) %>%
  ungroup %>%
  full_join(unique_er_yms) %>%
  mutate(n_fire = ifelse(is.na(n_fire), 0, n_fire),
         ym = as.yearmon(paste(FIRE_YEAR, sprintf("%02d", FIRE_MON), sep = "-"))) %>%
  arrange(ym) %>%
  filter(ym > 'Jan 1984') # first record is feb 1984 in mtbs data

assert_that(0 == sum(is.na(count_df$NA_L3NAME)))
assert_that(sum(count_df$n_fire) == nrow(mtbs))
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
