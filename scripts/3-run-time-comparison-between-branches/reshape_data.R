rm(list = ls())
setwd('~/revealgenomics/custom_scripts/run-time-comparison-between-branches/')
##-----------------------------------------------------------------------------------------------##
# This script modifies long form run time data written using get_downloadTime_data.R into a list 
# with each element holding data for a particular study.
#
# The input data for this script is ./data/raw/runTime_YYYYmmdd.csv i.e. raw tun time data.
# Example:
#   branch biosample_down_time_sec biosample_size expression_down_time_sec exp_size   ds_name ds_id         ms_name ms_id
# 1 master                   0.724         0.3 Mb                   15.076  69.2 Mb TCGA-LAML     1 HTSeq - FPKM-UQ     1
# 2 master                   0.803         0.3 Mb                   13.923  69.2 Mb TCGA-LAML     1    HTSeq - FPKM     2
# 3 master                   0.817         0.3 Mb                   15.406  69.2 Mb TCGA-LAML     1  HTSeq - Counts     3
# 4 master                   0.727         0.2 Mb                   12.305  38.1 Mb  TCGA-ACC     2 HTSeq - FPKM-UQ    34
# 5 master                   0.789         0.2 Mb                   10.562  38.1 Mb  TCGA-ACC     2    HTSeq - FPKM    35
# 6 master                   0.861         0.2 Mb                   10.335  38.1 Mb  TCGA-ACC     2  HTSeq - Counts    36
#
# The output of this scrips is saved at ./data/processed/ as test_processed_YYYYmmdd.RData
# Output eample: An element of the output list -
# $ds_name
# [1] "TCGA-ACC"
# $ds_id
# [1] 2
# $ms_name
# [1] "HTSeq - Counts"
# $ms_id
# [1] 36
# $bio_size
# [1] "0.2 Mb"
# $exp_size
# [1] "38.1 Mb"
# $bio_dt
# master dt_enc_v2
# 1  0.861     0.983
# 2  0.919     0.887
# 3  1.243     0.943
# 4  1.668     0.804
# 5  1.244     0.730
# 6  1.188     0.738
# $exp_dt
# master dt_enc_v2
# 1 10.335     9.835
# 2 10.414     9.541
# 3 12.647     9.391
# 4 12.942     9.249
# 5 12.036     8.901
# 6 12.273     9.858

##-----------------------------------------------------------------------------------------------##

## Input file date
# Provide the input file date for raw run time dataset
ip_date <- '20190730'

## Read runtime data
rtdat <- read.csv(paste('data/raw/', 'runTime_', ip_date, '.csv', sep = ''),
                  stringsAsFactors = FALSE, 
                  header = FALSE)
colnames(rtdat) = c('branch', 'biosample_down_time_sec', 'biosample_size', 'expression_down_time_sec', 
                    'exp_size', 'ds_name', 'ds_id', 'ms_name', 'ms_id')

## Create a run time data key and split
rtdat$key <- paste(rtdat$ds_name, rtdat$ds_id, rtdat$ms_name, rtdat$ms_id, sep = '_')
rtdat <- split(rtdat, rtdat$key)

## Reshape data (The next few lines are entirely hardcoded)
rtdat <- lapply(X = rtdat,
                FUN = function(k) {
                  ret <- list(ds_name = unique(k$ds_name), ds_id = unique(k$ds_id),
                              ms_name = unique(k$ms_name), ms_id = unique(k$ms_id),
                              bio_size = unique(k$biosample_size), exp_size = unique(k$exp_size))
                  ret$bio_dt = data.frame(                                                         
                    master = k$biosample_down_time_sec[which(k$branch %in% 'master')],             #-----name of branch1-----#
                    dt_enc_v2 = k$biosample_down_time_sec[which(k$branch %in% 'dt_enc_v2')],       #-----name of branch2-----#
                    stringsAsFactors = FALSE)
                  ret$exp_dt = data.frame(
                    master = k$expression_down_time_sec[which(k$branch %in% 'master')],            #-----name of branch1-----#
                    dt_enc_v2 = k$expression_down_time_sec[which(k$branch %in% 'dt_enc_v2')],      #-----name of branch2-----#
                    stringsAsFactors = FALSE)
                  return(ret)
                })

## Save data
save(rtdat, file = paste('data/processed/', 'test_processed_', ip_date, '.RData', sep = ''))
