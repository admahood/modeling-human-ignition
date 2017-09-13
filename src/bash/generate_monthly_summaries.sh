#!/bin/bash

# download the data from remote server
source src/bash/metdata_wget.sh

# execute R script to produce monthly GeoTIFFs
Rscript --vanilla src/R/aggregate-climate-data.R

# push monthly GeoTIFFS to AWS S3
aws s3 cp ../data/processed/pet s3://earthlab-natem/data/climate/historical/pet --recursive
aws s3 cp ../data/processed/pr s3://earthlab-natem/data/climate/historical/pr --recursive
aws s3 cp ../data/processed/tmmx s3://earthlab-natem/data/climate/historical/tmmx --recursive
aws s3 cp ../data/processed/tmmn s3://earthlab-natem/data/climate/historical/tmmn --recursive
aws s3 cp ../data/processed/pdsi s3://earthlab-natem/data/climate/historical/pdsi --recursive
aws s3 cp ../data/processed/fm100 s3://earthlab-natem/data/climate/historical/fm100 --recursive
aws s3 cp ../data/processed/fm1000 s3://earthlab-natem/data/climate/historical/fm1000 --recursive
aws s3 cp ../data/processed/vs s3://earthlab-natem/data/climate/historical/vs --recursive
aws s3 cp ../data/processed/rmin s3://earthlab-natem/data/climate/historical/rmin --recursive
aws s3 cp ../data/processed/rmax s3://earthlab-natem/data/climate/historical/rmax --recursive
