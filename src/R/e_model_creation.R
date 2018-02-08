
# Model Creation

# Notes and justification
# The thought behind the model is to use the framework outlined in Barbero et al 2014 (Modeling very large-fire occurrences over the continental United States from weather and climate forcing).  Quote form the Table 1 caption: "Predictors were selected with stepwise regression from 1000 Monte-Carlo samples including all VLF weeks and 50 000 random non-VLF weeks. We used the most frequent equation among the 50 000 simulations."  To note this was done using VLF weeks, but we can use the monthly human -started wildfire frequency above 10 ha, which is the smallest fire catpured inthe BAECV data.

rrds <- raster("data/ancillary/anthro/dis_railroads.tif") 
prds <- raster("data/ancillary/anthro/dis_primary_rds.tif") 
srds <- raster("data/ancillary/anthro/dis_secondary_rds.tif")
tlines <- raster("data/ancillary/anthro/dis_transmission_lines.tif")

mtbs <- st_read("/Users/nathanmietkiewicz/Dropbox/Professional/projects/wildfire-acc/data/fire/mtbs/mtbs_wui.shp") %>%
  st_transform(., crs(rrds, asText = TRUE))

pt_vars <- mtbs %>%
  sfc_as_cols(.) %>%
  mutate(cause = ignitin) %>%
  mutate(railroads_dist = raster::extract(rrds, as(., "Spatial")), #buffer =  500, fun = mean),
         prim_rds_dist = raster::extract(prds, as(., "Spatial")),
         sec_rds_dist = raster::extract(srds, as(., "Spatial")),
         trans_lines_dist = raster::extract(tlines, as(., "Spatial"))) %>%
  na.omit 

# pre-processing
vars <- pt_vars %>%
  select(r_acres, railroads_dist, prim_rds_dist, sec_rds_dist, trans_lines_dist, class, cause) %>%
  as.tibble %>% select(-geometry)
preProcPrms <- preProcess(vars, method = c("center", "scale"))
prevars <- predict(preProcPrms, vars)

# Divide the data into training and testing 75% training and 25% testing
bh_index <- createDataPartition(prevars$cause, p = .75, list = FALSE)
bh_tr <- prevars[ bh_index, ]
bh_te <- prevars[-bh_index, ] 

# Or pull a Max and divide temporally for better forcasting skill
# bh_tr <- pt_vars %>% filter(fire_yr < 2010) %>% as.tibble() %>%
#   na.omit %>% droplevels()
# bh_te <- pt_vars %>% filter(fire_yr >= 2010) %>% as.tibble() %>%
#   na.omit %>% droplevels()

controlList <- trainControl(method="repeatedcv", repeats = 3)
tuneMatrix <- expand.grid(size = 6, decay = 0)

set.seed(1)
caret_net <- train(cause ~ railroads_dist + prim_rds_dist + sec_rds_dist + trans_lines_dist + class + r_acres,
                   data = bh_tr,
                   method = "nnet",
                   linout = FALSE,
                   TRACE = FALSE,
                   maxit = 100,
                   tuneGrid = tuneMatrix,
                   trControl = controlList)

set.seed(1)
nnet_net <- nnet(cause ~ railroads_dist + prim_rds_dist + sec_rds_dist +trans_lines_dist + class + r_acres,
                 data = bh_tr,
                 linout = caret_net$finalModel$param$linout,
                 TRACE = caret_net$finalModel$param$TRACE,
                 size = caret_net$bestTune$size,
                 decay = caret_net$bestTune$decay,
                 #entropy = caret_net$finalModel$entropy,
                 maxit = 100)

x_test <- bh_te[,2:7]
y_test <- bh_te[, "cause"]

yhat <- predict(nnet_net, bh_te, type = 'class')
confusionMatrix(as.factor(yhat), bh_te$cause)

# summarize results
postResample(pred = yhat, obs = bh_te$cause)
confusionMatrix(data = yhat, reference = bh_te$cause)

# evaluate variable importance
cols<-colorRampPalette(c('lightgreen','lightblue'))(num.vars)
gar.fun('y', nnet_net, col = cols)

