# extracting landcover to fpa points 
# Used a m4.xlarge EC2 instance

source("src/R/a_make_dirs.R")
source("src/functions/landcover_extract.R")

# create scrap folder for results and s3 path

result_path <- "data/results"
result_s3 <- "s3://earthlab-modeling-human-ignitions/processed/landcover"
dir.create(result_path)


# fpa import ---------------------------------------------------------------

fpa <- st_read("data/processed/fpa_ll.gpkg") %>%
  select(FPA_ID,STATE)


# landfire ----------------------------------------------------------------
lf_file <- "us_140esp1.tif"
lf_path <- "data/raw/landfire_esp"
lf_s3 <- "s3://earthlab-modeling-human-ignitions/raw/landfire_esp"
lf_result_file <- "fpa_w_landfire_esp.gpkg"


system(paste("aws s3 sync",
             lf_s3,
             lf_path))

df <- ext_landcover(rst_ = file.path(lf_path, lf_file),
                    pts = fpa)

st_write(df, file.path(result_path
                       , lf_result_file))
system(paste("aws s3 cp",
             file.path(result_path, lf_file),
             result_s3))

# nlcd ----------------------------------------------------------------------
year <- (c("2001","2006", "2011"))
type <- (c("impervious","landcover"))

for(y in 1:length(years)){
  for(t in 1:length(type)){
    file <- paste0("nlcd_",year[y],"_",type[t],"_2011_edition_2014_10_10.img")
    path <-paste0("modeling-human-ignition/data/raw/nlcd_",year[y],"_",type[t],"_2011_edition_2014_10_10")
    s3 <-paste0("s3://earthlab-modeling-human-ignitions/raw/nlcd_",year[y],"_",type[t],"_2011_edition_2014_10_10")
    result_file <- paste0("fpa_w_nlcd_",type[t],year[y],".gpkg")
    
    system(paste("aws s3 sync", s3, path))
    
    if(type[t] == 'landcover'){
      df <- ext_landcover(rst_ = file.path(path, file),
                          pts = fpa)
    }
    if(type[t] == 'impervious'){
      df <- ext_landcover(rst_ = file.path(path, file),
                          pts = fpa,
                          FUN = 'mean')
    }
    
    st_write(df, file.path(result_path,result_file))
    system(paste("aws s3 cp",
                 file.path(result_path, result_file),
                 result_s3))
    
  }
}
