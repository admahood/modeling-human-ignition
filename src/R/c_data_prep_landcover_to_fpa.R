# extracting landcover to fpa points 
# Used a m4.xlarge EC2 instance

# landfire ------------------------------------------------------------------

landfire <- raster("data/raw/landfire_esp/us_140esp1.tif")
#landfiredb <- read.dbf("data/raw/landfire_esp/esp.dbf")

system(paste("aws s3 sync",
               "s3://earthlab-modeling-human-ignitions/raw/nlcd_2011_impervious_2011_edition_2014_10_10",
               "modeling-human-ignition/data/raw/nlcd_2011_impervious_2011_edition_2014_10_10"))

# nlcd
nlcd_2001_path <-"data/raw/nlcd_2011_impervious_2011_edition_2014_10_10/nlcd_2011_impervious_2011_edition_2014_10_10.img"
nlcd_2001 <- raster(nlcd_2001_path)

# fpa ----------------------------------------------------------------------------
fpa <- st_read("data/processed/fpa_ll.gpkg")

fpa_s <- select(fpa, FPA_ID,STATE) %>%
  st_transform(crs = crs(landfire, asText=TRUE))

rm(fpa)

# R does not have a native function for mode?????? --------------------------------
getmode <- function(v) {
  uniqv <- na.omit(unique(v))
  uniqv[base::which.max(tabulate(match(v, uniqv)))]
}

# landfire extraction -----------------------------------------------------------------------

t0 <- Sys.time()
corz <- detectCores()-1
states <- unique(fpa_s$STATE)

cl <- makeCluster(corz)
registerDoParallel(cl)

results <- list()
results <- foreach(i = 1:length(states)) %dopar% {
  require(sf)
  require(raster)
  
  sub_df <- fpa_s[fpa_s$STATE == states[i],]
  bb <- st_bbox(sub_df)
  bb[1] <- bb[1]-1000
  bb[2] <- bb[2]-1000
  bb[3] <- bb[3]+1000
  bb[4] <- bb[4]+1000
  pol <- st_as_sfc(bb)
  pol <- as_Spatial(pol)
  rst <- raster::crop(nlcd_2001, pol)
  rm(pol)
  rm(bb)
  
  #sub_df <- sub_df[1:10,]

    
    
    # sub_df$lf <- raster::extract(rst, sub_df, buffer = 1000,
    #                        na.rm = TRUE, fun = function(x,...)getmode(x))
  sub_df$lf <- raster::extract(rst, sub_df, buffer = 1000,
                               na.rm = TRUE, fun = mean) 
  
  
   return(sub_df)
   rm(rst)
   rm(sub_df)
   gc()
}
print(Sys.time()-t0)

stopCluster(cl)

t0 <- Sys.time()
print(t0)
df<- do.call("rbind",results)
st_write(df, "fpa_w_nlcd_imp_2011.gpkg")
system("aws s3 cp /home/rstudio/modeling-human-ignition/fpa_w_nlcd_imp_2011.gpkg s3://earthlab-modeling-human-ignitions/processed/")
print(Sys.time()-t0)

# st_write(df, "fpa_w_landfire_esp.gpkg")
# system("aws s3 cp /home/rstudio/modeling-human-ignition/fpa_w_landfire_esp.gpkg s3://earthlab-modeling-human-ignitions/processed/")

# FORE-SCE extraction -----------------------------------------------------------------------
zip <- "data/CONUS_Landcover_A1B.zip"
exdir <- "data/raw/landcover/"

unzip(zipfile=zip, exdir=exdir) # file corrupted


