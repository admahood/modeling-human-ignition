
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
  file <- paste0(dir, "/", layer)

  if (!file.exists(file)) {
    dest <- paste0(dir, ".zip")
    download.file(url, destfile = paste0(dir, ".zip"))
    unzip(dest,
          exdir = dir)
    unlink(dest)
    assert_that(file.exists(file))

    system(paste0('aws s3 sync ',
                  file, " ",
                  s3_raw_prefix, fld_name))
  }
