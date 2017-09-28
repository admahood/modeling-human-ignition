x <- c("raster", "tidyverse", "sf", "rasterVis", "gridExtra")
lapply(x, library, character.only = TRUE, verbose = FALSE)

raw_prefix <- ifelse(Sys.getenv("LOGNAME") == "NateM", file.path("data", "raw"), 
                     ifelse(Sys.getenv("LOGNAME") == "nami1114", file.path("data", "raw"), 
                            file.path("../data", "raw")))

p4string_ed <- "+proj=eqdc +lat_0=0 +lon_0=0 +lat_1=33 +lat_2=45 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"   #http://spatialreference.org/ref/esri/102005/
p4string_ea <- "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs"   #http://spatialreference.org/ref/sr-org/6903/

# CONUS states
usa_shp <- st_read(dsn = file.path(raw_prefix, "cb_2016_us_state_20m"),
                   layer = "cb_2016_us_state_20m") %>%
  st_transform(p4string_ea) %>%
  filter(!STUSPS %in% c("HI", "AK", "PR")) %>%
  mutate(region = as.factor(ifelse(STUSPS %in% c("CO", "WA", "OR", "NV", "CA", "ID", "UT",
                                                 "WY", "NM", "AZ", "MT"), 1, 2)))

# Import the Level 3 Ecoregions
ecoreg <- st_read(dsn = file.path(raw_prefix, "us_eco_l3"), 
                  layer = "us_eco_l3", quiet= TRUE) %>%
  st_transform(p4string_ea) %>%
  st_simplify(., preserveTopology = TRUE, dTolerance = 1000)

eco_reg_1 <- ecoreg %>%
  group_by(NA_L1NAME) %>%
  summarise()

ecoreg_names <- ecoreg %>%
  as.data.frame(.) %>%
  select(NA_L1CODE, NA_L1NAME)

# This will be the raster "template" for all shapefile to raster conversions
elevation <- raster(file.path(raw_prefix, "metdata_elevationdata", "metdata_elevationdata.nc")) %>%
  projectRaster(crs = p4string_ea, res = 4000) %>%
  crop(as(usa_shp, "Spatial")) %>%
  mask(as(usa_shp, "Spatial"))
elevation <- calc(elevation, fun = function(x){x[x < 0] <- NA; return(x)})

region <- rasterize(as(usa_shp, "Spatial"), elevation, "region")
ecoregion <- rasterize(as(ecoreg, "Spatial"), elevation, "NA_L1CODE")

rsts <- stack("data/ancillary/fpa_density.tif", 
              "data/ancillary/dis_primary_rds.tif",
              "data/ancillary/dis_secondary_rds.tif",
              "data/ancillary/dis_all_rds.tif",
              "data/ancillary/dis_railroads.tif",
              "data/ancillary/dis_transmission_lines.tif",
              elevation, region, ecoregion) 

rst_df <- as.data.frame(as(rsts, "SpatialPixelsDataFrame"))
rst_df <-  rst_df %>%
  mutate(elevation = layer.1,
         region = ifelse(layer.2 == 1, "West", "East"),
         NA_L1CODE = as.factor(layer.3)) %>%
  select(-layer.1, -layer.2, -layer.3)  %>%
  left_join(., ecoreg_names, by = "NA_L1CODE") %>%
  na.omit()

p1 <- rst_df %>%
  ggplot(aes(x = dis_primary_rds/1000, y = fpa_density)) +
  geom_point(aes(color = region), alpha = 0.10, shape = 16) + theme_pub() +
  scale_color_manual(values = c("#1F77B4", "#D62728")) +
  theme(legend.position = "none") +
  xlab("Distance to Primary Roads (km)") + 
  ylab("Human ignition wildfire density per 4km")

p2 <- rst_df %>%
  ggplot(aes(x = dis_secondary_rds/1000, y = fpa_density)) +
  geom_point(aes(color = region), alpha = 0.10, shape = 16) + theme_pub() +
  theme(legend.position = "none") +
  scale_color_manual(values = c("#1F77B4", "#D62728")) +
  xlab("Distance to Secondary Roads (km)") + ylab("")

p3 <- rst_df %>%
  ggplot(aes(x = dis_all_rds/1000, y = fpa_density)) +
  geom_point(aes(color = region), alpha = 0.10, shape = 16) + theme_pub() +
  scale_color_manual(values = c("#1F77B4", "#D62728")) +
  theme(legend.position = "none") +
  xlab("Distance to All Roads (km)") + ylab("") 

p4 <- rst_df %>%
  ggplot(aes(x = dis_transmission_lines/1000, y = fpa_density)) +
  geom_point(aes(color = region), alpha = 0.10, shape = 16) + theme_pub() +
  scale_color_manual(values = c("#1F77B4", "#D62728")) +
  xlab("Distance to Transmission Lines (km)") + ylab("")

p5 <- rst_df %>%
  ggplot(aes(x = dis_railroads/1000, y = fpa_density)) +
  geom_point(aes(color = region), alpha = 0.10, shape = 16) + theme_pub() +
  scale_color_manual(values = c("#1F77B4", "#D62728")) +
  theme(legend.position = "none") +
  xlab("Distance to Railroads (km)") + ylab("Human ignition wildfire density per 4km")

p6 <- rst_df %>%
  ggplot(aes(x = elevation, y = fpa_density)) +
  geom_point(aes(color = region), alpha = 0.10, shape = 16) + theme_pub() +
  scale_color_manual(values = c("#1F77B4", "#D62728")) +
  xlab("Elevation (m)") + ylab("")

#grid.arrange(p1, p2, p3, p4, p5, p6, nrow = 3)
g <- arrangeGrob(p1, p2, p3, p4, p5, p6, nrow = 2)
ggsave(file = "results/fireden_per_var.png", g, width = 8, height = 6, dpi=600, units = "cm", scale = 3) #saves g


p1 <- rst_df %>%
  ggplot(aes(x = dis_primary_rds/1000, y = fpa_density)) +
  geom_point(alpha = 0.5, shape = 16) + theme_pub() +
  scale_color_manual(values = "#D62728") +
  theme(legend.position = "none") +
  xlab("Distance to Primary Roads (km)") + ylab("Human ignition wildfire density (4km)") +
  facet_wrap(~NA_L1NAME, labeller = label_wrap_gen(10),
             scales = "free")
ggsave(file = "results/fireden_prirds_ecoreg.jpg", p1, width = 6, 
       height = 8, dpi=600, units = "cm", scale = 3) #saves g

p2 <- rst_df %>%
  ggplot(aes(x = dis_secondary_rds/1000, y = fpa_density)) +
  geom_point(alpha = 0.5, shape = 16) + theme_pub() +
  scale_color_manual(values = "#D62728") +
  xlab("Distance to Secondary Roads (km)") + ylab("Human ignition wildfire density (4km)") 
ggsave(file = "results/fireden_secrds_ecoreg.jpg", p2, width = 6, 
       height = 8, dpi=600, units = "cm", scale = 3) #saves g

p3 <- rst_df %>%
  ggplot(aes(x = dis_all_rds/1000, y = fpa_density)) +
  geom_point(alpha = 0.5, shape = 16) + theme_pub() +
  scale_color_manual(values = "#D62728") +
  xlab("Distance to All Roads (km)") + ylab("Human ignition wildfire density (4km)")
ggsave(file = "results/fireden_allrds_ecoreg.jpg", p3, width = 6, 
       height = 8, dpi=600, units = "cm", scale = 3) #saves g

p4 <- rst_df %>%
  ggplot(aes(x = dis_transmission_lines/1000, y = fpa_density)) +
  geom_point(alpha = 0.5, shape = 16) + theme_pub() +
  scale_color_manual(values = "#D62728") +
  xlab("Distance to Transmission Lines (km)") + ylab("Human ignition wildfire density (4km)")
ggsave(file = "results/fireden_translines_ecoreg.jpg", p4, width = 6, 
       height = 8, dpi=600, units = "cm", scale = 3) #saves g

p5 <- rst_df %>%
  ggplot(aes(x = dis_railroads/1000, y = fpa_density)) +
  geom_point(alpha = 0.5, shape = 16) + theme_pub() +
  scale_color_manual(values = "#D62728") +
  theme(legend.position = "none") +
  xlab("Distance to Railroads (km)") + ylab("Human ignition wildfire density (4km)") +
  facet_wrap(~NA_L1NAME, labeller = label_wrap_gen(10),
             scales = "free")
ggsave(file = "results/fireden_railrds_ecoreg.jpg", p5, width = 6, 
       height = 8, dpi=600, units = "cm", scale = 3) #saves g

p6 <- rst_df %>%
  ggplot(aes(x = elevation, y = fpa_density)) +
  geom_point(alpha = 0.5, shape = 16) + theme_pub() +
  scale_color_manual(values = "#D62728") +
  xlab("Elevation (m)") + ylab("Human ignition wildfire density (4km)") +
  facet_wrap(~NA_L1NAME, labeller = label_wrap_gen(10),
             scales = "free")
ggsave(file = "results/fireden_elevation_ecoreg.jpg", p6, width = 6, 
       height = 8, dpi=600, units = "cm", scale = 3) #saves g


jpeg("results/anthro_var_maps/fpa_density.jpeg", width=500, height=700, quality = 100)  #0.707 is a convenient aspect.ratio
p <- levelplot(rsts[[1]], layers = 1, margin = list(FUN = 'median'), par.settings = RdBuTheme)
p + layer(sp.polygons(as(eco_reg_1, "Spatial"), lwd=0.8, col='black'))
dev.off()

jpeg("results/anthro_var_maps/primary_roads.jpeg", width=500, height=700, quality = 100)  #0.707 is a convenient aspect.ratio
p <- levelplot(rsts[[2]], layers = 1, margin = list(FUN = 'median'), par.settings = RdBuTheme)
p + layer(sp.polygons(as(eco_reg_1, "Spatial"), lwd=0.8, col='black'))
dev.off()

jpeg("results/anthro_var_maps/secondary_roads.jpeg", width=500, height=700, quality = 100)  #0.707 is a convenient aspect.ratio
p <- levelplot(rsts[[3]], layers = 1, margin = list(FUN = 'median'), par.settings = RdBuTheme)
p + layer(sp.polygons(as(eco_reg_1, "Spatial"), lwd=0.8, col='black'))
dev.off()

jpeg("results/anthro_var_maps/all_roads.jpeg", width=500, height=700, quality = 100)  #0.707 is a convenient aspect.ratio
p <- levelplot(rsts[[4]], layers = 1, margin = list(FUN = 'median'), par.settings = RdBuTheme)
p + layer(sp.polygons(as(eco_reg_1, "Spatial"), lwd=0.8, col='black'))
dev.off()

jpeg("results/anthro_var_maps/rail_roads.jpeg", width=500, height=700, quality = 100)  #0.707 is a convenient aspect.ratio
p <- levelplot(rsts[[5]], layers = 1, margin = list(FUN = 'median'), par.settings = RdBuTheme)
p + layer(sp.polygons(as(eco_reg_1, "Spatial"), lwd=0.8, col='black'))
dev.off()

jpeg("results/anthro_var_maps/tranmission_lines.jpeg", width=500, height=700, quality = 100)  #0.707 is a convenient aspect.ratio
p <- levelplot(rsts[[6]], layers = 1, margin = list(FUN = 'median'), par.settings = RdBuTheme)
p + layer(sp.polygons(as(eco_reg_1, "Spatial"), lwd=0.8, col='black'))
dev.off()

jpeg("results/anthro_var_maps/elevation.jpeg", width=500, height=700, quality = 100)  #0.707 is a convenient aspect.ratio
p <- levelplot(rsts[[7]], layers = 1, margin = list(FUN = 'median'), par.settings = RdBuTheme)
p + layer(sp.polygons(as(eco_reg_1, "Spatial"), lwd=0.8, col='black'))
dev.off()


