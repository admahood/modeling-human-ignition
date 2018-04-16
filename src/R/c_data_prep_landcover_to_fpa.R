# extracting landcover to fpa points -------------------

landfire <- raster("data/raw/landfire_esp/us_140esp1.tif")
landfiredb <- read.dbf("data/raw/landfire_esp/esp.dbf")

fpa <- st_read("data/processed/fpa_ll.gpkg")

fpa_s <- select(fpa, FPA_ID,STATE) %>%
  st_transform(crs = crs(landfire, asText=TRUE))

# R does not have a native function for mode?????? -------------------
getmode <- function(v) {
  uniqv <- na.omit(unique(v))
  uniqv[base::which.max(tabulate(match(v, uniqv)))]
}

# 

t0 <- Sys.time()
corz <- detectCores()
states <- unique(fpa_s$STATE)

cl <- makeCluster(detectCores())
registerDoParallel(cl)

results <- list()
results <- foreach(i = 44:length(states)) %dopar% {
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
  rst <- raster::crop(landfire, pol)
  
  sub_df <- sub_df[1:10,]

    
    
    sub_df$lf <- raster::extract(landfire, sub_df, buffer = 1000,
                           na.rm = TRUE, fun = function(x,...)getmode(x))

   
   
    
   return(sub_df)
}
print(Sys.time()-t0)

stopCluster(cl)

# current ending point with serialized method ----------------------------------------------
# 
# if (!exists("sp_grd")){
#   pol <- st_as_sfc(st_bbox(landfire))
#   grd <- st_make_grid(pol,n=c(corz,1))
#   sp_grd <- sf::as_Spatial(grd)
# }
# 
# t1 <- Sys.time()
# registerDoParallel(cores=corz)
# splits <- foreach(j=1:length(sp_grd)) %dopar% {raster::crop(landfire, sp_grd[j])}
# print("time for splitting raster")
# print(Sys.time() - t1)
# #rm(landfire)

## also need to split the fpa data
t1 <- Sys.time()
registerDoParallel(cores=corz)
pnts <- foreach(j=1:length(sp_grd)) %dopar% {st_intersection(fpa, grd[j])} # gotta use the sf grid
print("time for splitting fpa")
print(Sys.time() - t1)


spl_rcl <- list() #possibly unnecesary
t1 <- Sys.time()
registerDoParallel(cores=corz)
print(paste("reclassifying"))

spl_rcl <- foreach(k=1:length(splits)) %dopar% {
  # making a filename based on location
  #xmin <- (substr(as.character(sp_grd[k]@bbox[[1]]),1,4))
  #filename <- paste0("scrap/rcl", year,"_", xmin, ".tif")
  # applying the function
  spl_rcl[[k]] <- raster::extract(splits[[k]], pnts[[k]], buffer=1000, fun = function(x,...)getmode(x))
}
print("time for reclassifying")
print(Sys.time()-t1)
rm(splits)



# # test runs-----------------
# fpa_small <- fpa[1:1000,]
# 
# t0 <- Sys.time()
# fpa_small$lf <- raster::extract(landfire, fpa_small, buffer = 1000, fun = function(x,...)getmode(x))
# print(Sys.time()-t0) # 20 minutes 
# 
# t0 <- Sys.time()
# beginCluster()
# fpa_small$lf <- raster::extract(landfire, fpa_small, buffer = 1000, fun = function(x,...)getmode(x))
# endCluster()
# print(Sys.time()-t0) #
# fpa_small
# 
# # we're doing it for real --------------- but it's going to take 12.5 days so maybe  not
# beginCluster(detectCores())
# fpa$landfire <- raster::extract(landfire, fpa, buffer = 1000, fun = function(x,...)getmode(x))
# endCluster()

