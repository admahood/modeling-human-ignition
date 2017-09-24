prds <- st_read("data/ancillary/primary_rds.gpkg") %>%
  select(-starts_with("bool"))

srds <- st_read("data/ancillary/secondary_rds.gpkg")   %>%
  select(-starts_with("bool"))

ards <- rbind(prds, srds) %>%
  mutate(bool_ards = 1)

strds <- st_read("data/ancillary/secondary_rds.gpkg") 

region <- rasterize(as(usa_shp, "Spatial"), elevation, "region")

rsts <- stack("data/ancillary/fpa_density.tif", 
              "data/ancillary/dis_primary_rds.tif",
              "data/ancillary/dis_railroads.tif",
              "data/ancillary/dis_transmission_lines.tif",
              elevation, region) 

rst_df <- as.data.frame(as(rsts, "SpatialPixelsDataFrame"))
rst_df <-  rst_df %>%
  mutate(elevation = layer.1,
         region = ifelse(layer.2 == 1, "West", "East")) %>%
  select(-layer.1, -layer.2) %>%
  na.omit()

p1 <- rst_df %>%
  ggplot(aes(x = dis_primary_rds/1000, y = fpa_density)) +
  geom_point(aes(color = region), alpha = 0.5, shape = 16) + theme_pub() +
  scale_color_manual(values = c("#1F77B4", "#D62728")) +
  theme(legend.position = "none") +
  xlab("Distance to Primary Roads (km)") + ylab("Human ignition wildfire density per 4km")

p2 <- rst_df %>%
  ggplot(aes(x = dis_transmission_lines/1000, y = fpa_density)) +
  geom_point(aes(color = region), alpha = 0.5, shape = 16) + theme_pub() +
  scale_color_manual(values = c("#1F77B4", "#D62728")) +
  xlab("Distance to Transmission Lines (km)") + ylab("")

p3 <- rst_df %>%
  ggplot(aes(x = dis_railroads/1000, y = fpa_density)) +
  geom_point(aes(color = region, size), alpha = 0.5, shape = 16) + theme_pub() +
  scale_color_manual(values = c("#1F77B4", "#D62728")) +
  theme(legend.position = "none") +
  xlab("Distance to Railroads (km)") + ylab("Human ignition wildfire density per 4km")

p4 <- rst_df %>%
  ggplot(aes(x = elevation, y = fpa_density)) +
  geom_point(aes(color = region), alpha = 0.5, shape = 16) + theme_pub() +
  scale_color_manual(values = c("#1F77B4", "#D62728")) +
  xlab("Elevation (m)") + ylab("")

library(gridExtra)

grid.arrange(p1, p2, p3, p4, nrow = 2)
g <- arrangeGrob(p1, p2, p3, p4, nrow = 2)
ggsave(file = "results/fireden_per_var.png", g, width = 8, height = 6, dpi=600, units = "cm", scale = 3) #saves g

