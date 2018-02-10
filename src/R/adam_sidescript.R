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

density <- function(rast, dsn, layer){
  # get the template raster (ras_mask)
  rLength <- raster(rast)
  
  # read in shapefile, transform to raster crs
  sl <- st_read(dsn,layer) %>%
    st_transform(crs = crs(rast, asText = TRUE)) %>%
    as("Spatial")
  
  # Calculate lengths
  lengths <- sapply(1:ncell(rLength), lengthInCell, rLength, sl)
  rLength[] <- lengths
  
  #output
  return(rLength)
}

#### do the stuff ####
dsn <- "data/raw/road_sample/"
layer <- "tl_2017_27037_roads"
rast <- raster(as(usa_shp, "Spatial"), res = 4000)

test = density(dsn=dsn,layer=layer,rast=rast)


fishnet_4k <- st_make_grid(usa_shp, cellsize = 4000, what = 'polygons') %>%
  st_sf('geometry' = ., data.frame('fishid4k' = 1:length(.))) %>%
  st_intersection(., st_union(usa_shp))

ras_mask <- raster(as(fishnet_2k, "Spatial"), res = 4000)



plot(rLength); plot(sl, add=T)

# road density script from last year
listofshps=Sys.glob("*.shp")
listofshps=substr(x=listofshps, start=1, stop=19)
registerDoParallel(2)

foreach(i=1:3108) %do% {
  stepone=readOGR(".", listofshps[i])
  steptwo=as.psp(tl)
  stepthree=pixellate(steptwo, eps=1000)
  stepfour=raster(stepthree)
  stepfive=stepfour/1000
  writeRaster(stepfive, filename=paste(listofshps[i],".tif"), format="GTiff", overwrite=T)
}

listoftifs=Sys.glob("*.tif")