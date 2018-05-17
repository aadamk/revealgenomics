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

#' @export
search_rnaquantification = function(measurementset = NULL,
                                    biosample = NULL,
                                    feature = NULL,
                                    formExpressionSet = TRUE,
                                    con = NULL){
  if (!is.null(measurementset)) {measurementset_id = measurementset$measurementset_id} else {
    stop("measurementset must be supplied"); measurementset_id = NULL
  }
  if (length(unique(measurementset$dataset_version)) != 1) {
    stop("multiple dataset versions in supplied measurementset");
  }
  dataset_version = unique(measurementset$dataset_version)
  dataset_id =      unique(measurementset$dataset_id)
  if (!is.null(biosample)) {
    stopifnot(length(unique(biosample$dataset_version))==1)
    stopifnot(unique(biosample$dataset_version)==dataset_version)
  }
  arrayname = full_arrayname(.ghEnv$meta$arrRnaquantification)
  if (!is.null(biosample)) {
    biosample_id = biosample$biosample_id
    if (dataset_id != unique(biosample$dataset_id)) {
      stop("conflicting dataset_id in measurementset and biosample") 
    }
  } else {
    biosample_id = NULL
    }
  if (!is.null(feature)) {
    feature_id = feature$feature_id
  } else {
    feature_id = NULL
  }
  
  if (exists('debug_trace')) cat("retrieving expression data from server\n")
  res = search_rnaquantification_scidb(arrayname,
                                       measurementset_id,
                                       biosample_id,
                                       feature_id,
                                       dataset_version = dataset_version,
                                       dataset_id = dataset_id, 
                                       con = con)
  if (nrow(res) == 0) return(NULL)
  if (!formExpressionSet) return(res)
  
  # If user did not provide biosample, then query the server for it, or retrieve from global biosample list
  if (is.null(biosample)) {
    biosample_id = unique(res$biosample_id)
    if (FALSE) { # avoiding this path right now (might be useful when the download of all accessible biosamples is prohibitive)
      cat("query the server for matching biosamples\n")
      biosample = get_biosamples(biosample_id, con = con)
    } else{
      biosample_ref = get_biosamples_from_cache(con = con)
      biosample = biosample_ref[biosample_ref$biosample_id %in% biosample_id, ]
      biosample = drop_na_columns(biosample)
    }
  }
  
  # If user did not provide feature, then query the server for it, or retrieve from global feature list
  if (is.null(feature)) {
    feature_id = unique(res$feature_id)
    if (FALSE) { # avoiding this path for now (the download of all features registered on the system is not expected to be too time-consuming)
      cat("query the server for matching features\n")
      feature = get_features(feature_id, con = con)
    } else{
      feature_ref = get_feature_from_cache(con = con)
      
      feature = feature_ref[feature_ref$feature_id %in% feature_id, ]
      feature = drop_na_columns(feature)
    }
  }
  
  expressionSet = formulate_list_expression_set(expr_df = res, dataset_version, measurementset, biosample, feature)
  
  return(expressionSet)
}

formulate_list_expression_set = function(expr_df, dataset_version, measurementset, biosample, feature){
  if (nrow(measurementset) > 1) {stop("currently does not support returning expressionSets for multiple rnaquantification sets")}
  if (length(dataset_version) != 1) {stop("currently does not support returning expressionSets for multiple dataset_verions")}
  
  convertToExpressionSet(expr_df, biosample_df = biosample, feature_df = feature)
}

search_rnaquantification_scidb = function(arrayname,
                                          measurementset_id,
                                          biosample_id,
                                          feature_id,
                                          dataset_version,
                                          dataset_id,
                                          con = NULL){
  con = use_ghEnv_if_null(con)
  tt = scidb(con$db, arrayname)
  
  if (is.null(dataset_version)) dataset_version = "NULL"
  if (length(dataset_version) != 1) {stop("cannot specify one dataset_version at a time")}
  
  qq = paste0(custom_scan(), "(", arrayname, ")")
  if (!is.null(measurementset_id) & !is.null(biosample_id) & !is.null(feature_id)) { # all 3 selections made by user
    if (length(measurementset_id) == 1 & length(biosample_id) == 1 & length(feature_id) == 1) {
      qq = paste0("filter(",
                 qq,
                 ", dataset_version =", dataset_version,
                 " AND measurementset_id =", measurementset_id,
                 " AND biosample_id = ", biosample_id,
                 " AND feature_id = ", feature_id,
                 ")")
      res = iquery(con$db, qq, return = T)
    }
    else {
      selector = merge(
        merge(
          merge(data.frame(dataset_version = as.integer(dataset_version)),
                data.frame(measurementset_id = as.integer(measurementset_id))),
          data.frame(biosample_id = as.integer(biosample_id))),
        data.frame(feature_id = as.integer(feature_id)))
      if (TRUE){ # Return data using join
        selector$dataset_id = dataset_id
        selector$flag = -1
        # t1 = proc.time()
        xx = as.scidb_int64_cols(db = con$db, df1 = selector, 
                                 int64_cols = colnames(selector))
        qq2 = paste0("redimension(", xx@name, 
                    ", <flag:int64>[", 
                                   paste0(get_idname(.ghEnv$meta$arrRnaquantification), collapse = ", "),
                                  "])")

        qq = paste("join(",
                   qq, ",",
                   qq2, ")")
        qq = paste("project(", qq, ", value)")
        res = iquery(con$db, qq, return = T)
      } else { # Return data using cross_between_
        stop("Code needs to be added here to support inclusion of `dataset_version` in expression matrix schema")
        df = selector[order(selector$measurementset_id, selector$biosample_id, selector$feature_id), ]
        sqq = NULL
        for (nn in 1:nrow(df)){
          row = df[nn, ]
          si = paste(row, collapse = ", ")
          si = paste(si, si, sep = ", ")
          sqq = paste(sqq, si, sep=ifelse(is.null(sqq), "", ", "))
        }
        qq = paste("cross_between_(", qq, ", ", sqq, ")")
        res = iquery(con$db, qq, return = T)
      }
    }
  } else if (!is.null(measurementset_id) & !is.null(biosample_id) & is.null(feature_id)) { # user selected rqs and bs, not f
    selected_names = c('measurementset_id', 'biosample_id')
    val1 = measurementset_id
    val2 = biosample_id
    res = cross_between_select_on_two(qq, tt, val1, val2, selected_names, dataset_version = dataset_version, con = con)
  } else if (is.null(measurementset_id) & !is.null(biosample_id) & !is.null(feature_id)) { # user selected bs and f, not rqs
    selected_names = c('biosample_id', 'feature_id')
    val1 = biosample_id
    val2 = feature_id
    res = cross_between_select_on_two(qq, tt, val1, val2, selected_names, dataset_version = dataset_version, con = con)
  } else if (!is.null(measurementset_id) & is.null(biosample_id) & !is.null(feature_id)) { # user selected rqs and f, not bs
    selected_names = c('measurementset_id', 'feature_id')
    val1 = measurementset_id
    val2 = feature_id
    res = cross_between_select_on_two(qq, tt, val1, val2, selected_names, dataset_version = dataset_version, con = con)
  } else if (!is.null(measurementset_id) & is.null(biosample_id) & is.null(feature_id)) { # user selected rqs only
    if (exists('debug_trace')) cat("Only measurementset is selected.\n")
    if (length(measurementset_id) == 1){
      qq = paste0("filter(", qq, ", measurementset_id=", measurementset_id, 
                                 " AND dataset_version=", dataset_version, ")")
    } else {
      stop("code for multiple measurementset_id to be added. Alternatively, call the search function by individual measurementset_id.")
    }
    res = iquery(con$db, qq, return = TRUE)
  } else if (is.null(measurementset_id) & !is.null(biosample_id) & is.null(feature_id)) { # user selected bs only
    stop("Only biosample is selected. Downloaded data could be quite large. Consider downselecting by MeasurementSet_id or feature_id. ")
  }
  res
}





#' search variants
#' 
#' Search variants by pipeline, sample and/or  feature
#' 
#' @param measurementset pipeline dataframe (mandatory)
#' 
#' @export
search_variants = function(measurementset, biosample = NULL, feature = NULL, 
                           con = NULL){
  if (!is.null(measurementset)) {measurementset_id = measurementset$measurementset_id} else {
    stop("measurementset must be supplied"); measurementset_id = NULL
  }
  if (length(unique(measurementset$dataset_version)) != 1) {
    stop("multiple dataset versions in supplied measurementset");
  }
  dataset_version = unique(measurementset$dataset_version)
  if (!is.null(biosample)) {
    stopifnot(length(unique(biosample$dataset_version))==1)
    if (!(unique(biosample$dataset_version)==dataset_version)) stop("dataset_version-s of measurementset and biosample must be same")
  }
  arrayname = full_arrayname(.ghEnv$meta$arrVariant)
  if (!is.null(biosample))            {biosample_id = biosample$biosample_id}                                  else {biosample_id = NULL}
  if (!is.null(feature))              {feature_id = feature$feature_id}                                        else {feature_id = NULL}
  
  if (exists('debug_trace')) cat("retrieving expression data from server\n")
  res = search_variants_scidb(arrayname,
                              measurementset_id,
                              biosample_id,
                              feature_id,
                              dataset_version = dataset_version, 
                              con = con)
  res
}

#' Inner function for searching variants
#' 
#' @param use_cross_join use `cross_join` if TRUE, else use `equi_join`
search_variants_scidb = function(arrayname, measurementset_id, biosample_id = NULL, feature_id = NULL, dataset_version, 
                                 use_cross_join = FALSE, 
                                 con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (is.null(dataset_version)) stop("dataset_version must be supplied")
  if (length(dataset_version) != 1) stop("can handle only one dataset_version at a time")
  
  if (is.null(measurementset_id)) stop("measurementset_id must be supplied")
  if (length(measurementset_id) != 1) stop("can handle only one measurementset_id at a time")
  
  var_nonflex_q = paste0("filter(", custom_scan(), "(", arrayname, 
                             "), dataset_version=", dataset_version, 
                             " AND measurementset_id=", measurementset_id, ")")
  var_flex_q = paste0("filter(", custom_scan(), "(", arrayname, 
                      "_INFO), dataset_version=", dataset_version, 
                              " AND measurementset_id=", measurementset_id, ")")
  
  if (!is.null(biosample_id)){
    if (length(biosample_id) == 1) {
      var_nonflex_q = paste0("filter(", var_nonflex_q,
                         ", biosample_id=", biosample_id, ")")
      var_flex_q = paste0("filter(", var_flex_q,
                          ", biosample_id=", biosample_id, ")")
    } else {
      var_nonflex_q = paste0("filter(", var_nonflex_q, 
                         ", ", formulate_base_selection_query(fullarrayname = 'BIOSAMPLE',
                                                              id = biosample_id), ")")
      var_flex_q = paste0("filter(", var_flex_q, 
                          ", ", formulate_base_selection_query(fullarrayname = 'BIOSAMPLE',
                                                               id = biosample_id), ")")
    }
  }
  
  if (!is.null(feature_id)){
    if (length(feature_id) == 1) {
      var_nonflex_q = paste0("filter(", var_nonflex_q,
                         ", feature_id=", feature_id, ")")
      var_flex_q = paste0("filter(", var_flex_q,
                          ", feature_id=", feature_id, ")")
    } else {
      var_nonflex_q = paste0("filter(", var_nonflex_q, 
                         ", ", formulate_base_selection_query(fullarrayname = 'FEATURE',
                                                              id = feature_id), ")")
      var_flex_q = paste0("filter(", var_flex_q, 
                          ", ", formulate_base_selection_query(fullarrayname = 'FEATURE',
                                                               id = feature_id), ")")
    }
  }
  
  if (use_cross_join) {
    query = paste0("cross_join(", var_flex_q, " as X,",
                   var_nonflex_q, "as Y, ",
                   "X.biosample_id, Y.biosample_id, ",
                   "X.feature_id,   Y.feature_id, ", 
                   "X.per_gene_variant_number, Y.per_gene_variant_number)")
    var_raw = iquery(con$db, query = query, return = TRUE)
    res = unpivot(df1 = var_raw, arrayname = .ghEnv$meta$arrVariant)
  } else {
    res = join_info_ontology_and_unpivot(qq = var_nonflex_q, 
                                         arrayname = strip_namespace(arrayname), 
                                         replicate_query_on_info_array = TRUE, 
                                         con = con)
  }
  res
}



#' Search Fusion data
#' 
#' @export
search_fusion = function(measurementset, biosample = NULL, feature = NULL, 
                         dataset_lookup_ref = NULL,
                         con = NULL){
  if (!is.null(measurementset)) {measurementset_id = measurementset$measurementset_id} else {
    stop("measurementset must be supplied"); measurementset_id = NULL
  }
  if (length(unique(measurementset$dataset_version)) != 1) {
    stop("multiple dataset versions in supplied measurementset");
  }
  dataset_version = unique(measurementset$dataset_version)
  if (!is.null(biosample)) {
    stopifnot(length(unique(biosample$dataset_version))==1)
    if (!(unique(biosample$dataset_version)==dataset_version)) stop("dataset_version-s of measurementset and biosample must be same")
  }
  
  arrayname = paste0(custom_scan(), 
                     "(", full_arrayname(.ghEnv$meta$arrFusion), ")")
  if (!is.null(biosample))            {biosample_id = biosample$biosample_id}                                  else {biosample_id = NULL}
  if (!is.null(feature))              {feature_id = feature$feature_id}                                        else {feature_id = NULL}
  
  if (exists('debug_trace')) cat("retrieving fusion data from server\n")
  res = search_fusions_scidb(arrayname,
                             measurementset_id,
                             biosample_id,
                             feature_id,
                             dataset_version = dataset_version, 
                             con = con)
  res
}

search_fusions_scidb = function(arrayname, measurementset_id, biosample_id = NULL, feature_id = NULL, dataset_version, 
                                con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (is.null(dataset_version)) stop("dataset_version must be supplied")
  if (length(dataset_version) != 1) stop("can handle only one dataset_version at a time")
  
  if (is.null(measurementset_id)) stop("measurementset_id must be supplied")
  if (length(measurementset_id) != 1) stop("can handle only one measurementset_id at a time")
  
  left_query = paste0("filter(", arrayname,
                     ", dataset_version=", dataset_version, " AND measurementset_id=", measurementset_id, ")")
  
  if (!is.null(biosample_id)){
    filter_expr = formulate_base_selection_query(.ghEnv$meta$arrBiosample, id = biosample_id)
    left_query = paste("filter(", left_query,
                       ", ", filter_expr, ")", sep = "")
  }
  
  if (!is.null(feature_id)){
    filter_expr = formulate_base_selection_query(.ghEnv$meta$arrFeature, id = feature_id)
    filter_expr_left = gsub("feature_id", "feature_id_left", filter_expr)
    filter_expr_right = gsub("feature_id", "feature_id_right", filter_expr)

    left_query = paste("filter(", left_query,
                       ", (", filter_expr_left, ") OR (", 
                       filter_expr_right, "))", sep = "")
  }
  
  iquery(con$db, left_query, return = TRUE)
}


#' @export
search_copynumber_mat = function(measurementset, biosample = NULL, feature = NULL, con = NULL){
  if (!is.null(measurementset)) {measurementset_id = measurementset$measurementset_id} else {
    stop("measurementset must be supplied"); measurementset_id = NULL
  }
  if (length(unique(measurementset$dataset_version)) != 1) {
    stop("multiple dataset versions in supplied measurementset");
  }
  dataset_version = unique(measurementset$dataset_version)
  if (!is.null(biosample)) {
    stopifnot(length(unique(biosample$dataset_version))==1)
    if (!(unique(biosample$dataset_version)==dataset_version)) stop("dataset_version-s of measurementset and biosample must be same")
  }
  arrayname = paste0(custom_scan(), 
                     "(", full_arrayname(.ghEnv$meta$arrCopynumber_mat), ")")
  if (!is.null(biosample))            {biosample_id = biosample$biosample_id}                                  else {biosample_id = NULL}
  if (!is.null(feature))              {feature_id = feature$feature_id}                                        else {feature_id = NULL}
  
  if (exists('debug_trace')) cat("retrieving CopyNumber_mat data from server\n")
  res = search_copynumber_mats_scidb(arrayname,
                             measurementset_id,
                             biosample_id,
                             feature_id,
                             dataset_version = dataset_version, 
                             con = con)
  res
}

search_copynumber_mats_scidb = function(arrayname, measurementset_id, biosample_id = NULL, feature_id = NULL, dataset_version, 
                                        con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (is.null(dataset_version)) stop("dataset_version must be supplied")
  if (length(dataset_version) != 1) stop("can handle only one dataset_version at a time")
  
  if (is.null(measurementset_id)) stop("measurementset_id must be supplied")
  if (length(measurementset_id) != 1) stop("can handle only one measurementset_id at a time")
  
  left_query = paste0("filter(", arrayname,
                      ", dataset_version=", dataset_version, " AND measurementset_id=", measurementset_id, ")")
  
  if (!is.null(biosample_id)){
    filter_expr = formulate_base_selection_query(.ghEnv$meta$arrBiosample, id = biosample_id)
    left_query = paste0("filter(", left_query,
                       ", ", filter_expr, ")")
  }
  
  if (!is.null(feature_id)){
    filter_expr = formulate_base_selection_query(.ghEnv$meta$arrFeature, id = feature_id)

    left_query = paste0("filter(", left_query,
                       ", ", filter_expr, ")")
  }
  
  iquery(con$db, left_query, return = TRUE)
}

#' @export
search_copynumber_seg = function(experimentset, biosample = NULL, con = NULL){
  if (!is.null(experimentset)) {experimentset_id = experimentset$experimentset_id} else {
    stop("experimentset must be supplied"); experimentset_id = NULL
  }
  if (length(unique(experimentset$dataset_version)) != 1) {
    stop("multiple dataset versions in supplied experimentset");
  }
  dataset_version = unique(experimentset$dataset_version)
  if (!is.null(biosample)) {
    stopifnot(length(unique(biosample$dataset_version))==1)
    if (!(unique(biosample$dataset_version)==dataset_version)) stop("dataset_version-s of experimentset and biosample must be same")
  }
  
  arrayname = paste0(custom_scan(), 
                     "(", full_arrayname(.ghEnv$meta$arrCopynumber_seg), ")")
  if (!is.null(biosample))            {biosample_id = biosample$biosample_id}                                  else {biosample_id = NULL}

  if (exists('debug_trace')) cat("retrieving CopyNumber_seg data from server\n")
  res = search_copynumber_segs_scidb(arrayname,
                                     experimentset_id,
                                     biosample_id,
                                     dataset_version = dataset_version, 
                                     con = con)
  res
}

search_copynumber_segs_scidb = function(arrayname, experimentset_id, biosample_id = NULL, dataset_version, con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (is.null(dataset_version)) stop("dataset_version must be supplied")
  if (length(dataset_version) != 1) stop("can handle only one dataset_version at a time")
  
  if (is.null(experimentset_id)) stop("experimentset_id must be supplied")
  if (length(experimentset_id) != 1) stop("can handle only one experimentset_id at a time")
  
  left_query = paste0("filter(", arrayname,
                      ", dataset_version=", dataset_version, " AND experimentset_id=", experimentset_id, ")")
  
  if (!is.null(biosample_id)){
    filter_expr = formulate_base_selection_query(.ghEnv$meta$arrBiosample, id = biosample_id)
    left_query = paste("filter(", left_query,
                       ", ", filter_expr, ")", sep = "")
  }
  
  iquery(con$db, left_query, return = TRUE)
}
