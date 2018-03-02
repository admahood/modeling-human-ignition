
bh_index <- createDataPartition(pt_vars$cause, p = .75, list = FALSE)
bh_tr <- pt_vars[ bh_index, ]%>%
  select(r_acres, railroads_dist, prim_rds_dist, sec_rds_dist, trans_lines_dist, class, cause) %>%
  as.tibble %>% select(-geometry)
bh_te <- pt_vars[-bh_index, ] %>%
  select(r_acres, railroads_dist, prim_rds_dist, sec_rds_dist, trans_lines_dist, class, cause) %>%
  as.tibble %>% select(-geometry)


controlList <- trainControl(method = "none", seeds = 1, entropy = FALSE)
tuneMatrix <- expand.grid(size = 2, decay = 0)

set.seed(1)
caret_net <- train(cause ~ railroads_dist + prim_rds_dist + sec_rds_dist +trans_lines_dist + class + log(r_acres),
                   data = bh_tr,
                   method = "nnet",
                   linout = FALSE,
                   TRACE = FALSE,
                   maxit = 100,
                   tuneGrid = tuneMatrix,
                   trControl = controlList)

set.seed(1)
nnet_net <- nnet(cause ~ railroads_dist + prim_rds_dist + sec_rds_dist +trans_lines_dist + class + log(r_acres),
                 data = bh_tr,
                 linout = caret_net$finalModel$param$linout,
                 TRACE = caret_net$finalModel$param$TRACE,
                 size = caret_net$bestTune$size,
                 decay = caret_net$bestTune$decay,
                 #entropy = caret_net$finalModel$entropy,
                 maxit = 100)

y_caret <- predict(caret_net, bh_te)
y_nnet <- predict(nnet_net, bh_te)

boxplot(bh_te$cause, y_caret, col='blue',main='Real vs predicted lm',pch=18, cex=0.7)


all.equal(as.vector(y_caret[,1]), y_nnet[,1])
