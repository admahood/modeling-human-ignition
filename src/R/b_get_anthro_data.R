
# Download the FPA-FOD data

fpa_gdb <- file.path(fpa_prefix, "Data", "FPA_FOD_20170508.gdb")
if (!file.exists(fpa_gdb)) {
  pg <- read_html("https://www.fs.usda.gov/rds/archive/Product/RDS-2013-0009.4/")
  fils <- html_nodes(pg, xpath=".//dd[@class='product']//li/a[contains(., 'zip') and contains(., 'GDB')]")
  dest <- paste0(fpa_prefix, ".zip")
  walk2(html_attr(fils, 'href'),  html_text(fils),
        ~GET(sprintf("https:%s", .x), write_disk(dest), progress()))
  unzip(dest, exdir = fpa_prefix)
  unlink(dest)
  assert_that(file.exists(fpa_gdb))
}

#Download the railrods

rails_shp <- file.path(rails_prefix, 'tlgdb_2015_a_us_rails.gdb')
if (!file.exists(rails_shp)) {
  loc <- "ftp://ftp2.census.gov/geo/tiger/TGRGDB15/tlgdb_2015_a_us_rails.gdb.zip"
  dest <- paste0(rails_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = rails_prefix)
  unlink(dest)
  assert_that(file.exists(rails_shp))
}

#Download the tramission lines

tl_shp <- file.path(tl_prefix, 'Electric_Power_Transmission_Lines.shp')
if (!file.exists(tl_shp)) {
  loc <- "https://hifld-dhs-gii.opendata.arcgis.com/datasets/75af06441c994aaf8e36208b7cd44014_0.zip"
  dest <- paste0(tl_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = tl_prefix)
  unlink(dest)
  assert_that(file.exists(tl_shp))
}

# Download population density by county from 2010-2100

pd_shp <- file.path(raw_prefix, "county_pop", 'cofips_upp.shp')
if (!file.exists(pd_shp)) {
  loc <- "https://edg.epa.gov/data/Public/ORD/NCEA/county_pop.zip"
  dest <- paste0(raw_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = raw_prefix)
  unlink(dest)
  assert_that(file.exists(pd_shp))
}

# Download housing density baseline scenario

iclus_nc <- file.path(iclus_prefix, 'hd_iclus_bc.nc')
if (!file.exists(iclus_nc)) {
  loc <- "https://cida.usgs.gov/thredds/fileServer/ICLUS/files/housing_density/hd_iclus_bc.nc"
  dest <- paste0(iclus_prefix, "/hd_iclus_bc.nc")
  download.file(loc, dest)
  assert_that(file.exists(iclus_nc))
}

# Download elevation

elev_nc <- file.path(elev_prefix, 'metdata_elevationdata.nc')
if (!file.exists(elev_nc)) {
  loc <- "https://climate.northwestknowledge.net/METDATA/data/metdata_elevationdata.nc"
  dest <- paste0(elev_prefix, "/metdata_elevationdata.nc")
  download.file(loc, dest)
  assert_that(file.exists(elev_nc))
}


#Download the NLCD 2011
source("src/functions/decompress_function.R")

# Adam says:
# I had some problems with this decompress function working
# This was the error message:
# Error in setwd(directory) : cannot change working directory
# Ijust unzipped manually and moved on....

nlcd_img <- file.path(nlcd_prefix, 'nlcd_2011_landcover_2011_edition_2014_10_10.img')
if (!file.exists(nlcd_img)) {
  loc <- "http://www.landfire.gov/bulk/downloadfile.php?TYPE=nlcd2011&FNAME=nlcd_2011_landcover_2011_edition_2014_10_10.zip"
  dest <- paste0(raw_prefix, ".zip")
  download.file(loc, dest)
  decompress_file("../data/", "raw.zip", .file_cache = FALSE)
  file.rename("../data/nlcd_2011_landcover_2011_edition_2014_10_10",
              to = "../data/raw/nlcd_2011_landcover_2011_edition_2014_10_10")
  unlink(dest)
  assert_that(file.exists(nlcd_img))
}

#Download the roads

roads_shp <- file.path(roads_prefix, 'tlgdb_2015_a_us_roads.gdb')
if (!file.exists(roads_shp)) {
  loc <- "ftp://ftp2.census.gov/geo/tiger/TGRGDB15/tlgdb_2015_a_us_roads.gdb.zip"
  dest <- paste0(roads_prefix, ".zip")
  download.file(loc, dest)
  unzip(dest, exdir = raw_prefix)
  unlink(dest)
  assert_that(file.exists(roads_shp))
}
