#!/bin/bash

# execute R script to rasterize shapefiles
Rscript --vanilla ../HumanIgnProb/src/R/prepare_data.R

# get ancillary data from S3
aws s3 cp ../data/processed s3://earthlab-natem/data/processed --recursive
