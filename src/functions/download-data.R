
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


download_data <-  function(url, dir, layer, fld_name) {
  dest <- paste0(raw_prefix, ".zip")

  if (!file.exists(layer)) {
    download.file(url, dest)
    unzip(dest,
          exdir = raw_prefix)
    unlink(dest)
    assert_that(file.exists(layer))

    system(paste0('aws s3 sync ',
                  dir, " ",
                  s3_raw_prefix, fld_name))
  }
}
