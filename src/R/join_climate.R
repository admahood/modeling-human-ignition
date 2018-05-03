source("src/R/a_make_dirs.R")

# get stuff from the bucket -----------------------------------------
# climate summaries
system("aws s3 sync s3://earthlab-modeling-human-ignitions/extractions/climate_extractions/95th data/extractions/climate_summaries")
system("aws s3 sync s3://earthlab-modeling-human-ignitions/extractions/climate_extractions/mean data/extractions/climate_summaries")
system("aws s3 sync s3://earthlab-modeling-human-ignitions/extractions/climate_extractions/numdays95th data/extractions/climate_summaries")

# fpa with landcover
system("aws s3 cp s3://earthlab-modeling-human-ignitions/extractions/fpa_w_all_landcover.gpkg data/extractions/fpa_w_all_landcover.gpkg")
system("aws s3 cp s3://earthlab-modeling-human-ignitions/extractions/terrain_extractions/terrain_extractions.rds data/extractions/terrain_extractions.rds")

# import data and join -----------------------------------------------------
climate_files <- list.files("data/extractions/climate_summaries")
ter <- readRDS("data/extractions/terrain_extractions.rds")
ter$folded_aspect <- abs(180 - abs(ter$aspect - 225))

all <- st_read("data/extractions/fpa_w_all_landcover.gpkg") %>%
  left_join(y=ter) %>%
  st_set_geometry(NULL) %>%
  as_tibble()

# checking to make sure they're all 1.8 million rows and joining -------------------------------
for(i in 1:length(climate_files)){
  x = readRDS(paste0("data/extractions/climate_summaries/",climate_files[i])) %>%
    as_tibble()
  print(paste(climate_files[i],nrow(x)))
  all <- left_join(all,y=x, by = "FPA_ID")
}

# write to s3 --------------------------------------------------------------------------------
write_rds(all, "data/extractions/fpa_w_climate_landcover.rds")
system("aws s3 cp data/extractions/fpa_w_climate_landcover.rds s3://earthlab-modeling-human-ignitions/processed/fpa_w_climate_landcover.rds")

