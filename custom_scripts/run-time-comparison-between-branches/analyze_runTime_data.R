rm(list = ls())
##-----------------------------------------------------------------------------------------------##
# This script has functions for analyzing/comparing the performance of various branches.
# Scripts in this folder have been written specifically for the structure of the data returned by
# reshape_data.R. This data is a list with each element holding data for a particular study.
# Please refer to reshape_data.R for understanding the exact structure of the data.
##-----------------------------------------------------------------------------------------------##

## Load data
ip_date = '20190730'
load(paste('data/processed/test_processed_', ip_date, '.RData', sep = ''))

##-----------------------------------------------------------------------------------------------##
#### functions for analysing run_time data ####

##----------------------
# get_numNAobs()
# This function gets the number of runs where data was not returned while executing 
# search_expression() or search_biosamples()
# INPUT:
# dat: A list data-set with the exact same structure as returned by reshape_data.R. Refer to 
# reshape_data.R for understanding the exact structure of this list data-set.
# dType: Expression data or biosample data. Expected inputs are exp_dt for expression data
# bio_dt for biosample data
# OUTPUT:
# A data frame with the number of failed runs for each study in dat.
#
get_numNAobs <- function(dat, dType) {
  ret <- lapply(X = dat, FUN = function(k) apply(X = k[[dType]], MARGIN = 2, 
                                                 FUN = function(m) length(which(is.na(m)))))
  ret <- as.data.frame(do.call(rbind, ret), stringsAsFactors = FALSE)
  return(ret)
}
tt  = get_numNAobs(rtdat, 'exp_dt')
get_numNAobs(rtdat, 'bio_dt')

##----------------------
## MEAN
get_mean <- function(dat, dType = NULL, branch = 'ALL') {
  if(branch == 'ALL') {
    ret <- lapply(X = dat, FUN = function(k) apply(X = k[[dType]], MARGIN = 2, 
                                                   FUN = function(m) mean(m, na.rm = TRUE)))
    ret <- as.data.frame(do.call(rbind, ret), stringsAsFactors = FALSE)
  } else {
    ret1 <- lapply(X = dat, 
                   FUN = function(k) mean(k$bio_dt[[branch]], na.rm = TRUE))                       ##--------------------
    ret2 <- lapply(X = dat, 
                   FUN = function(k) mean(k$exp_dt[[branch]], na.rm = TRUE))                       ##--------------------
    ret <- as.data.frame(cbind(unlist(ret1), unlist(ret2)), stringsAsFactors = FALSE)
    colnames(ret) <- c(paste('bio_dt', branch, sep = '-'),                                       ##--------------------
                       paste('exp_dt', branch, sep = '-'))                   ##--------------------
  }
  return(ret)
}
get_mean(dat = rtdat, dType = 'bio_dt')
get_mean(dat = rtdat, dType = 'exp_dt')
get_mean(dat = rtdat, branch = 'master')
get_mean(dat = rtdat, branch = 'dt_enc_v2')

diff_mean_performance <- (get_mean(dat = rtdat, dType = 'bio_dt')[[1]] - 
                            get_mean(dat = rtdat, dType = 'bio_dt')[[2]])
diff_meanOFmean_performance <- mean(diff_mean_performance)

diff_mean_performance <- (get_mean(dat = rtdat, dType = 'exp_dt')[[1]] - 
                            get_mean(dat = rtdat, dType = 'exp_dt')[[2]])
diff_meanOFmean_performance <- mean(diff_mean_performance, na.rm = TRUE)

##----------------------
