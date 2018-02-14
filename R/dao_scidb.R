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

# Functions for dao

#' @export
dao_get_measurementset = function(con){
  res = get_measurementsets(con = con)
  stop("id and name are potentially not going to be returned by get_measurementsets()")
  cols1 = c('dataset_id', 'dataset_version', 'experimentset_id', 'entity', 'id', 'name')
  othercols = allcols[!(allcols %in% cols1)]
  
  res[, c(cols1, othercols)]
}

#' dao code for `get_ENTITY`
#' 
#' after changes in secure_scan branch, no namespace merging is required (This was the 
#' faster path used here)
#' We might get rid of this function and call `get_ENTITY()` directly
dao_get_entity = function(entity, con){
  
  arrayname = entity
  
  idname = scidb4gh:::get_idname(arrayname)
  
  inner_query = paste0(custom_scan(), "(", full_arrayname(entity), 
                       "), ", custom_scan(), "(", full_arrayname(entity), "_INFO)")
  
  qq = paste0("equi_join(", 
                       inner_query, 
                        ", 'left_names=", paste(idname, collapse = ","), "', 
                           'right_names=", paste(idname, collapse = ","), "', 
                           'left_outer=true', 'keep_dimensions=true')")
  
  x2 = iquery(con$db, qq, return = TRUE)
  
  if ((nrow(x2) > 0 & sum(colnames(x2) %in% c("key", "val")) == 2)) {
    x3 = scidb4gh:::unpivot_key_value_pairs(df = x2, arrayname = arrayname)
  } else {
    if (nrow(x2) > 0) {
      x3 = x2[, c(idname,
                  names(.ghEnv$meta$L$array[[entity]]$attributes))]
    } else {
      x3 = x2[, names(.ghEnv$meta$L$array[[entity]]$attributes)]
    }
  }
  scidb4gh:::join_ontology_terms(df = x3, con = con)
}

#' @export
dao_get_project = function(con){
  dao_get_entity(entity = 'PROJECT', con = con)
}

#' @export
dao_get_dataset = function(con){
  dao_get_entity(entity = 'DATASET', con = con)
}

#' @export
dao_get_biosample = function(con){
  dao_get_entity(entity = 'BIOSAMPLE', con = con)
}

#' Faster implementation of `search_rnaquantification` for UI development
#' 
#' @param biosample_ref Reference dataframe containing all biosamples available to the user (retrieved by `dao_get_biosample(con = con)`)
#' 
#' @export
dao_search_rnaquantification = function(measurementset, 
                                        biosample_ref, 
                                        feature = NULL, 
                                        con) {
  rqs_id = unique(measurementset$measurementset_id)
  stopifnot(length(rqs_id)==1)
  
  dataset_version = unique(measurementset$dataset_version)
  stopifnot(length(dataset_version) == 1)
  
  dataset_id = unique(measurementset$dataset_id)
  stopifnot(length(dataset_id) == 1)
  
  ftr_id = unique(feature$feature_id)
  
  arr0 = full_arrayname(.ghEnv$meta$arrRnaquantification)
  
  qq = paste0("filter(", custom_scan(), "(", arr0, "), measurementset_id = ", rqs_id, ")")
  
  if (!is.null(feature)) {
    K_THRESH = 500
    if (length(ftr_id) <= K_THRESH) {
      path = "filter_features"
    } else {
      path = "build_literal_and_cross_join_features"
    }
    if (path == 'filter_features') {
      expr = paste0("feature_id=", ftr_id, collapse = " OR ")
      qq2 = paste0("filter(", qq, ", ", expr, ")")
    } else if (path == "build_literal_and_cross_join_features") {
      expr = paste0(ftr_id, collapse = ",")
      qq2a = paste0("build(<feature_id:int64>[i=1:", length(ftr_id), "], '[",
                    expr, "]', true)")
      qq2b = paste0("redimension(
                       apply(", qq2a, ", 
                            flag, int32(1)), <flag:int32>[feature_id])")
      qq2c = paste0("cross_join(", qq, " as X,",
                                 qq2b, " as Y, X.feature_id, Y.feature_id)")
      qq2 = paste0("project(", qq2c, ",", 
                   paste0(names(.ghEnv$meta$L$array$RNAQUANTIFICATION$attributes), collapse = ","),
                          ")")
  
    }
  } else { # user has not supplied features; try to download full data
    cat("Estimating download size: ")
    download_size = iquery(con$db, 
                       query = paste0("project(summarize(", qq, "), bytes)"), 
                       return = TRUE)$bytes
    cat(download_size/1024/1024, " MB\n")
    download_limit_mb = 500
    if (download_size > download_limit_mb * 1024 * 1024) {
      cat("Trying to download more than", download_limit_mb, "MB at a time! 
Post an issue at https://github.com/Paradigm4/scidb4gh/issues\n")
      return(NULL)
    }
    qq2 = qq
  }
  
  res = iquery(con$db, query = qq2, return = TRUE)

  # Retrieved biosamples
  biosample_id = unique(res$biosample_id)
  biosample = biosample_ref[biosample_ref$biosample_id %in% biosample_id, ]
  
  # Retrieved features
  feature_id = unique(res$feature_id)
  if (!is.null(feature)) {
    feature_sel = feature[feature$feature_id %in% feature_id, ]
  } else { # user has not supplied features; need to download features from DB
    cat("Downloading features to form ExpressionSet\n")
    feature_sel = get_features(feature_id = feature_id, fromCache = FALSE,
                               con = con)
  }
  expressionSet = formulate_list_expression_set(expr_df = res, 
                                                dataset_version = dataset_version, 
                                                measurementset = measurementset,
                                                biosample = biosample,
                                                feature = feature_sel)
  
  return(expressionSet)
}
