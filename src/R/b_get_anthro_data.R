s3_raw_prefix <- 's3://earthlab-modeling-human-ignitions/raw/'

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
  system(paste0('aws s3 cp ', #command
                fpa_gdb, "/ ", #source file
                s3_raw_prefix, "fpa-fod/",  "FPA_FOD_20170508.gdb/",
                " --recursive")) #destination
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
  system(paste0('aws s3 cp ', 
                rails_shp, " ", 
                s3_raw_prefix, "tlgdb_2015_a_us_rails/",  "tlgdb_2015_a_us_rails.gdb"))
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
  system(paste0('aws s3 cp ', #command
                tl_prefix, "/ ", #source DIRECTORY
                s3_raw_prefix, "Electric_Power_Transmission_Lines/ ", #destination directory
                "--recursive")) # making it recursive
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
  system(paste0('aws s3 cp ',
                raw_prefix, "/county_pop/ ",
                s3_raw_prefix, "county_pop/ ",
                "--recursive"))
}

# Download housing density baseline scenario

iclus_nc <- file.path(iclus_prefix, 'hd_iclus_bc.nc')
if (!file.exists(iclus_nc)) {
  loc <- "https://cida.usgs.gov/thredds/fileServer/ICLUS/files/housing_density/hd_iclus_bc.nc"
  dest <- paste0(iclus_prefix, "/hd_iclus_bc.nc")
  download.file(loc, dest)
  assert_that(file.exists(iclus_nc))
  system(paste0('aws s3 cp ',
                iclus_nc, " ",
                s3_raw_prefix, "housing_den/",  "hd_iclus_bc.nc"))
}

# Download elevation

elev_nc <- file.path(elev_prefix, 'metdata_elevationdata.nc')
if (!file.exists(elev_nc)) {
  loc <- "https://climate.northwestknowledge.net/METDATA/data/metdata_elevationdata.nc"
  dest <- paste0(elev_prefix, "/metdata_elevationdata.nc")
  download.file(loc, dest)
  assert_that(file.exists(elev_nc))
  system(paste0('aws s3 cp ',
                elev_nc, " ",
                s3_raw_prefix, "metadata_elevationdata/",  "metadata_elevationdata.nc"))
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
  decompress_file("data/raw.zip", #downloaded file
                  "data/raw/", #destination directory
                  .file_cache = FALSE)
  unlink(dest)
  assert_that(file.exists(nlcd_img))
  system(paste0("aws s3 cp ",
                nlcd_prefix, "/ ",
                s3_raw_prefix, "nlcd_2011_landcover_2011_edition_2014_10_10/ ",
                "--recursive"))
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
  system(paste0('aws s3 cp ',
                roads_shp, " ",
                s3_raw_prefix, "tlgdb_2015_a_us_roads/",  "tlgdb_2015_a_us_roads.gdb"))
}
