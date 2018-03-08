library(sp)
library(raster)
library(maptools)
library(spatstat)
library(rgdal)
library(rgeos)
library(foreach)
library(doParallel)
library(sf)

## functions

# Function to calculate lengths of lines in given raster cell
lengthInCell <- function(f, sl) {
  # i: iterator
  # f: fishnet
  # l: shapefile of lines
  # https://stat.ethz.ch/pipermail/r-sig-geo/2015-March/022475.html
  # thanks!
  require(snowfall)
  require(raster)
  require(sf)
  require(tidyverse)
  features <- 1:nrow(f[,])
  parts <- split(features, cut(features, detectCores()))
  
  # parts[i] <- 1
  # rpoly <- rasterToPolygons(parts, na.rm=T)
  
  lc <- st_intersection(sl, f[parts[[detectCores()]][1],])
  
  # if (!is.null(lc)) {
  #   return(st_length(lc))
  # } else {
  #   return(0)
  # }
}

source("src/functions/helper_functions.R")

#density <- function(rast, layer){
  # get the template raster (ras_mask)
  # rLength <- raster(rast)
  
  # read in shapefile, transform to raster crs
ri <- usa_shp %>%
  filter(STUSPS == "RI")

  f <- st_read("data/ancillary/fishnet/fishnet_4k.gpkg") %>%
    st_intersection(., ri) 
  
  sl <- st_read(layer) %>%
      st_transform(crs = st_crs(f)) %>%
    st_intersection(., stunion(usa_shp)) %>%
    filter(STUSPS == "RI")
  
 # lengths <- sapply(1:ncell(rLength), lengthInCell, rLength, sl)
  lengths <- sfLapply(1:detectCores(),
                      fun = lengthInCell,
                      f = fs,
                      sl = sl
                      )
  sfStop()
  
  # cl <- makeCluster(detectCores())
  # lengths <- parSapply(cl, 1:ncell(rLength), FUN = lengthInCell, r=rLength, l=sl )
  # stopCluster(cl)
  rLength[] <- lengths
  
  writeRaster(rLength, "primary_rd_density.tif")
  system('aws s3 cp primary_rd_density.tif s3://earthlab-modeling-human-ignitions/processed/primary_rd_density.tif')
  
  #output
  return(rLength)
#}

#### do the stuff ####
dsn <- ""
layer <- "data/processed/primary_rds.gpkg"
rast <- "data/ancillary/ras_mask/ras_mask.tif"

test = density(layer=layer,rast=rast)


# fishnet_4k <- st_make_grid(usa_shp, cellsize = 4000, what = 'polygons') %>%
#   st_sf('geometry' = ., data.frame('fishid4k' = 1:length(.))) %>%
#   st_intersection(., st_union(usa_shp))
# 
# ras_mask <- raster(as(fishnet_4k, "Spatial"), res = 4000)



plot(rLength); plot(sl, add=T)

# delete
# # road density script from last year
# listofshps=Sys.glob("*.shp")
# listofshps=substr(x=listofshps, start=1, stop=19)
# registerDoParallel(2)
# 
# foreach(i=1:3108) %do% {
#   stepone=readOGR(".", listofshps[i])
#   steptwo=as.psp(tl)
#   stepthree=pixellate(steptwo, eps=1000)
#   stepfour=raster(stepthree)
#   stepfive=stepfour/1000
#   writeRaster(stepfive, filename=paste(listofshps[i],".tif"), format="GTiff", overwrite=T)
# }
# 
# listoftifs=Sys.glob("*.tif")

registerDoParallel(detectCores())
f$length <- 0
f$length <- length_in_poly(sl,f)
  
length_in_poly <- function(sl,f){
  return(foreach(i = 1:nrow(f), .combine = rbind) %dopar% {
  x <- st_intersection(sl, f[i,])
  if(nrow(x) > 0){
    f$length[i] <-  sum(st_length(x))/st_area(f[i,])
  } else{
    f$length[i] <-0
    }
  
})}
stopCluster()
