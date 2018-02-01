
load_data <- function(url, dir, layer, outname) {
  file <- paste0(dir, "/", layer, ".shp")

  if (!file.exists(file)) {
    download.file(url, destfile = paste0(dir, ".zip"))
    unzip(paste0(dir, ".zip"),
          exdir = dir)
  }
 name <- paste0(outname, "_shp")
 name <- sf::st_read(dsn = dir, layer = layer)
 
 name
}
