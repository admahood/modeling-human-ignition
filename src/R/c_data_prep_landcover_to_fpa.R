# extracting landcover to fpa points 
# Used a m4.xlarge EC2 instance

source("src/R/a_make_dirs.R")
source("src/functions/landcover_extract.R")


# landfire ------------------------------------------------------------------
lf_path <- "data/raw/landfire_esp/us_140esp1.tif"
landfire <- raster(lf_path)
#landfiredb <- read.dbf("data/raw/landfire_esp/esp.dbf")

system(paste("aws s3 sync",
               "s3://earthlab-modeling-human-ignitions/raw/nlcd_2011_impervious_2011_edition_2014_10_10",
               "modeling-human-ignition/data/raw/nlcd_2011_impervious_2011_edition_2014_10_10"))

# nlcd
nlcd_2001_path <-"data/raw/nlcd_2011_impervious_2011_edition_2014_10_10/nlcd_2011_impervious_2011_edition_2014_10_10.img"
nlcd_2001 <- raster(nlcd_2001_path)

# fpa ----------------------------------------------------------------------------
t0 <- Sys.time()
fpa <- st_read("data/processed/fpa_ll.gpkg") %>%
  select(FPA_ID,STATE) # %>%
 # st_transform(crs = crs(landfire, asText=TRUE))
print(Sys.time()-t0)

# R does not have a native function for mode?????? --------------------------------
getmode <- function(v) {
  uniqv <- na.omit(unique(v))
  uniqv[base::which.max(tabulate(match(v, uniqv)))]
}



st_write(df, "fpa_w_nlcd_imp_2011.gpkg")
system("aws s3 cp /home/rstudio/modeling-human-ignition/fpa_w_nlcd_imp_2011.gpkg s3://earthlab-modeling-human-ignitions/processed/")
print(Sys.time()-t0)

# st_write(df, "fpa_w_landfire_esp.gpkg")
# system("aws s3 cp /home/rstudio/modeling-human-ignition/fpa_w_landfire_esp.gpkg s3://earthlab-modeling-human-ignitions/processed/")

# FORE-SCE extraction -----------------------------------------------------------------------
zip <- "data/CONUS_Landcover_A1B.zip"
exdir <- "data/raw/landcover/"

unzip(zipfile=zip, exdir=exdir) # file corrupted


