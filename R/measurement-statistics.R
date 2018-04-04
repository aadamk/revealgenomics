#
# BEGIN_COPYRIGHT
#
# PARADIGM4 INC.
# This file is part of the Paradigm4 Enterprise SciDB distribution kit
# and may only be used with a valid Paradigm4 contract and in accord
# with the terms and conditions specified by that contract.
#
# Copyright (C) 2011 - 2017 Paradigm4 Inc.
# All Rights Reserved.
#
# END_COPYRIGHT
#

#' Calculate statistics on Variant data
#' 
#' @export
calculate_statistics_variant = function(measurementset_id = NULL, con = NULL) {
  con = scidb4gh:::use_ghEnv_if_null(con = con)
  db = con$db
  if (!is.null(measurementset_id)) {
    res = iquery(db, 
                 paste0("aggregate(
                        filter(", 
                        scidb4gh:::full_arrayname(.ghEnv$meta$arrVariant), 
                        ", measurementset_id=", measurementset_id, 
                        "), count(*) AS variant_count, measurementset_id, biosample_id)"),
                 return = TRUE)
    m = get_measurementsets(measurementset_id = measurementset_id)
  } else { # for all measurementsets
    res = iquery(db, 
                 paste0("aggregate(",
                        scidb4gh:::full_arrayname(.ghEnv$meta$arrVariant), 
                        ", count(*) AS variant_count, measurementset_id, biosample_id)"),
                 return = TRUE)
    res = res[order(res$measurementset_id, res$biosample_id), ]
    res
    
    m = get_measurementsets(measurementset_id = sort(unique(res$measurementset_id)))
  }
  
  if (unique(m$entity) != 'VARIANT') {
    if (!is.null(measurementset_id)) stop("entity entries should have been of VARIANT type here")
  }
  m = plyr::rename(m, c('name' = 'pipeline'))
  
  d = get_datasets(dataset_id = unique(sort(m$dataset_id)))
  d = plyr::rename(d, c('name' = 'study'))
  
  md = merge(d[, c('dataset_id', 'dataset_version', 'study')],
             m[, c('dataset_id', 'dataset_version', 'measurementset_id', 'pipeline')],
             by = c('dataset_id', 'dataset_version'))
  
  # res = plyr::rename(res, c('count' = 'variant_count'))
  res = res %>% 
    group_by(measurementset_id) %>% 
    dplyr::summarise(mean_variants_per_sample = mean(variant_count),
                     num_samples = n())
  mdc = merge(md, res,
              by = 'measurementset_id')
  
  mdc
}

#' Calculate statistics on Gene-Expression data
#' 
#' @export
calculate_statistics_rnaquantification = function(measurementset_id = NULL, decorateResults = TRUE, con = NULL){
  con = use_ghEnv_if_null(con)
  idname = get_base_idname(.ghEnv$meta$arrMeasurementSet)
  if (is.null(measurementset_id)){
    c = iquery(con$db, 
               paste0("aggregate(", 
                      custom_scan(), "(", 
                      full_arrayname(.ghEnv$meta$arrRnaquantification), "),", 
                      "count(*), ", 
                      idname, 
                      ", dataset_version)"), 
               return = TRUE)
  } else { #specific measurementset_id is specified
    qq = paste0("filter(", 
                custom_scan(), "(", 
                full_arrayname(.ghEnv$meta$arrRnaquantification), "), ", 
                idname , "=", measurementset_id, ")")
    qq = paste0("aggregate(", qq, ", count(*), ", idname, ", dataset_version)")
    c = iquery(con$db, qq, return = T)
  }
  c = c[order(c[, idname], c$dataset_version), ]
  if (!decorateResults) {
    return(c)
  } else {
    ms = get_measurementsets(measurementset_id = unique(sort(c$measurementset_id)), 
                             all_versions =  TRUE,
                             con = con)
    ms = plyr::rename(ms, c('name' = 'pipeline'))
    
    d = get_datasets(dataset_id = unique(sort(c$dataset_id)),
                     all_versions =  TRUE,
                     con = con)
    d = plyr::rename(d, c('name' = 'study_name'))
    
    res = merge(ms[, c('measurementset_id', 'dataset_id', 'dataset_version', 'pipeline')], 
                c,
                by = c('measurementset_id', 'dataset_version'))
    res = merge(d[, c('dataset_id', 'dataset_version', 'study_name')],
                res, 
                by = c('dataset_id', 'dataset_version'))
    return(res[order(res$dataset_id, 
                     res$dataset_version,
                     res$measurementset_id), ])
  }
}

