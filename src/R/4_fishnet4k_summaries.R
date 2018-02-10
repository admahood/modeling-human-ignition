source("src/R/a_make_dirs.R")
source("src/functions/download-data.R")

if (!exists("fishnet_4k")) {

  fishnet_4k <- st_make_grid(usa_shp, cellsize = 4000, what = 'polygons') %>%
    st_sf('geometry' = ., data.frame('fishid4k' = 1:length(.))) %>%
    st_intersection(., st_union(usa_shp)) %>%
    st_transform("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")
}

tifs <- list.files("data/mean",
                   pattern = ".tif",
                   recursive = TRUE,
                   full.names = TRUE)

extract_one <- function(filename, fishnet_4k) {
  out_name <- gsub('.tif', '.csv', filename)
  if (!file.exists(out_name)) {
    res <- raster::extract(raster::stack(filename), fishnet_4k,
                    na.rm = TRUE, fun = mean, df = TRUE)
    write.csv(res, file = out_name)
  } else {
    res <- read.csv(out_name)
  }
  res
}

sfInit(parallel = TRUE, cpus = parallel::detectCores())
sfExport(list = c("fishnet_4k"))

extractions <- sfLapply(as.list(tifs),
                        fun = extract_one,
                        fishnet_4k = fishnet_4k)
sfStop()

# ensure that they all have the same length
stopifnot(all(lapply(extractions, nrow) == nrow(fishnet_4k)))

library(tidyverse)

# push to S3
write_rds(extractions, 'fishid_mean_extractions.rds')
system('aws s3 cp fishid_mean_extractions.rds s3://earthlab-natem/fishid_mean_extractions.rds')

# convert to a data frame
extraction_df <- extractions %>%
  bind_cols %>%
  as_tibble %>%
  mutate(index = ID) %>%
  select(-starts_with("ID")) %>%
  rename(ID = index) %>%
  mutate(fishid4k = data.frame(fishnet_4k)$fishid4k,
         Shape_Area = data.frame(fishnet_4k)$Shape_Area) %>%
  dplyr::select(-starts_with('X')) %>%
  gather(variable, value, -fishid4k, -Shape_Area, -ID) %>%
  filter(!is.na(value)) %>%
  mutate(fishid4k = as.character(fishid4k))

ecoregion_summaries <- extraction_df %>%
  separate(variable,
           into = c("interval", "variable", "timestep"),
           sep = "_") %>%
  separate(timestep, into = c("year", "month"), sep = "\\.") %>%
  select(-interval) %>%
  mutate(fishid4k = as.character(fishid4k)) %>%
  group_by(fishid4k, variable, year, month) %>%
  summarize(wmean = weighted.mean(value, Shape_Area)) %>%
  ungroup %>%
  mutate(year = parse_number(year),
         month = parse_number(month)) %>%
  arrange(year, month, variable, fishid4k)

write_csv(ecoregion_summaries, "data/processed/ecoregion_summaries.csv")
