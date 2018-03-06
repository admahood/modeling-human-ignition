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
lengthInCell <- function(i, r, l) {
  # https://stat.ethz.ch/pipermail/r-sig-geo/2015-March/022475.html
  # thanks!
  r[i] <- 1
  rpoly <- rasterToPolygons(r, na.rm=T)
  lc <- crop(l, rpoly)
  if (!is.null(lc)) {
    return(gLength(lc))
  } else {
    return(0)
  }
}

density <- function(rast, layer){
  # get the template raster (ras_mask)
  rLength <- raster(rast)
  
  # read in shapefile, transform to raster crs
  sl <- st_read(layer) %>%
    st_transform(crs = crs(rLength, asText = TRUE)) %>%
    as("Spatial")
  
  # Calculate lengths
  lengths <- sapply(1:ncell(rLength), lengthInCell, rLength, sl)
  
  # cl <- makeCluster(detectCores())
  # lengths <- parSapply(cl, 1:ncell(rLength), FUN = lengthInCell, r=rLength, l=sl )
  # stopCluster(cl)
  rLength[] <- lengths
  
  writeRaster(rLength, "primary_rd_density.tif")
  system('aws s3 cp primary_rd_density.tif s3://earthlab-modeling-human-ignitions/processed/primary_rd_density.tif')
  
  #output
  return(rLength)
}

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