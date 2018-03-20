# extracting landcover to fpa points -------------------

landfire <- raster("data/raw/landfire_esp/us_140esp1.tif")
landfiredb <- read.dbf("data/raw/landfire_esp/esp.dbf")

fpa <- st_read("data/processed/fpa_ll.gpkg")

# R does not have a native function for mode?????? -------------------
getmode <- function(v) {
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v, uniqv)))]
}

# test -----------------
fpa_small <- fpa[1:1000,]
t0 <- Sys.time()
fpa_small$lf <- raster::extract(landfire, fpa_small, buffer = 1000, fun = function(x,...)getmode(x))
print(Sys.time()-t0)

t0 <- Sys.time()
beginCluster()
fpa_small$lf <- raster::extract(landfire, fpa_small, buffer = 1000, fun = function(x,...)getmode(x))
endCluster()
print(Sys.time()-t0)
fpa_small




# we're doing it
beginCluster(detectCores())
fpa$landfire <- raster::extract(landfire, fpa, buffer = 1000, fun = function(x,...)getmode(x))
endCluster()

# trying out the raster parallel thing
# beginCluster()
# fpa$landfire <- clusterR(landfire, extract, args = list(y=fpa, buffer = 1000, fun=function(x,...)getmode(x)))
# endCluster()
# doesn't seem to work