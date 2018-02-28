source("src/R/a_make_dirs.R")
source("src/functions/download-data.R")

if (!exists("fpa_ll")) {

  fpa_ll <- st_read(file.path(processed_dir, "fpa_clean.gpkg")) %>%
    st_transform(st_crs(usa_shp)) %>%
    st_intersection(., st_union(usa_shp)) %>%
    st_transform("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs +towgs84=0,0,0")
}

tifs <- list.files("data/mean",
                   pattern = "ffwi",
                   recursive = TRUE,
                   full.names = TRUE)

extract_one <- function(filename, fpa_ll) {
  out_name <- gsub('.tif', '.csv', filename)
  if (!file.exists(out_name)) {
    res <- raster::extract(raster::stack(filename), fpa_ll,
                           na.rm = TRUE, fun = mean, df = TRUE)
    write.csv(res, file = out_name)
  } else {
    res <- read.csv(out_name)
  }
  res
}

sfInit(parallel = TRUE, cpus = parallel::detectCores())
sfExport(list = c("fpa_ll"))

extractions <- sfLapply(as.list(tifs),
                        fun = extract_one,
                        fpa_ll = fpa_ll)
sfStop()

# ensure that they all have the same length
stopifnot(all(lapply(extractions, nrow) == nrow(fpa_ll)))

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
  mutate(FPA_ID = data.frame(d)$FPA_ID) %>%
  dplyr::select(-starts_with('X')) %>%
  gather(variable, value, -FPA_ID, -ID) %>%
  filter(!is.na(value)) %>%
  mutate(FPA_ID = as.character(FPA_ID),
         ID = as.integer(ID))

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

write_csv(ecoregion_summaries, "data/processed/ecoregion_summaries.csv")
