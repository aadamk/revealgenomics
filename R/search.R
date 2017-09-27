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
search_rnaquantification = function(rnaquantificationset = NULL,
                                    biosample = NULL,
                                    feature = NULL,
                                    formExpressionSet = TRUE,
                                    con = NULL){
  if (!is.null(rnaquantificationset)) {rnaquantificationset_id = rnaquantificationset$rnaquantificationset_id} else {
    stop("rnaquantificationset must be supplied"); rnaquantificationset_id = NULL
  }
  if (length(unique(rnaquantificationset$dataset_version)) != 1) {
    stop("multiple dataset versions in supplied rnaquantificationset");
  }
  dataset_version = unique(rnaquantificationset$dataset_version)
  if (!is.null(biosample)) {
    stopifnot(length(unique(biosample$dataset_version))==1)
    stopifnot(unique(biosample$dataset_version)==dataset_version)
  }
  namespace = find_namespace(id = rnaquantificationset_id,
                             entitynm = .ghEnv$meta$arrRnaquantificationset,
                             dflookup = get_rnaquantificationset_lookup(con = con),
                             con = con)
  arrayname = paste(namespace, .ghEnv$meta$arrRnaquantification, sep = ".")
  if (!is.null(biosample))            {biosample_id = biosample$biosample_id}                                  else {biosample_id = NULL}
  if (!is.null(feature))              {feature_id = feature$feature_id}                                        else {feature_id = NULL}
  
  if (exists('debug_trace')) cat("retrieving expression data from server\n")
  res = search_rnaquantification_scidb(arrayname,
                                       rnaquantificationset_id,
                                       biosample_id,
                                       feature_id,
                                       dataset_version = dataset_version,
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
  
  return(formulate_list_expression_set(expr_df = res, dataset_version, rnaquantificationset, biosample, feature))
}

formulate_list_expression_set = function(expr_df, dataset_version, rnaquantificationset, biosample, feature){
  if (nrow(rnaquantificationset) > 1) {stop("currently does not support returning expressionSets for multiple rnaquantification sets")}
  if (length(dataset_version) != 1) {stop("currently does not support returning expressionSets for multiple dataset_verions")}
  
  convertToExpressionSet(expr_df, biosample_df = biosample, feature_df = feature)
}

search_rnaquantification_scidb = function(arrayname,
                                          rnaquantificationset_id,
                                          biosample_id,
                                          feature_id,
                                          dataset_version,
                                          con = NULL){
  con = use_ghEnv_if_null(con)
  tt = scidb(con$db, arrayname)
  
  if (is.null(dataset_version)) dataset_version = "NULL"
  if (length(dataset_version) != 1) {stop("cannot specify one dataset_version at a time")}
  
  qq = arrayname
  if (!is.null(rnaquantificationset_id) & !is.null(biosample_id) & !is.null(feature_id)) { # all 3 selections made by user
    if (length(rnaquantificationset_id) == 1 & length(biosample_id) == 1 & length(feature_id) == 1) {
      qq = paste("between(",
                 qq,
                 ", ", dataset_version,
                 ", ", rnaquantificationset_id,
                 ", ", biosample_id,
                 ", ", feature_id,
                 ", ", dataset_version,
                 ", ", rnaquantificationset_id,
                 ", ", biosample_id,
                 ", ", feature_id,
                 ")", sep="")
      res = iquery(con$db, qq, return = T)
    }
    else {
      selector = merge(
        merge(
          merge(data.frame(dataset_version = as.integer(dataset_version)),
                data.frame(rnaquantificationset_id = as.integer(rnaquantificationset_id))),
          data.frame(biosample_id = as.integer(biosample_id))),
        data.frame(feature_id = as.integer(feature_id)))
      if (TRUE){ # Return data using join
        selector$flag = TRUE
        # t1 = proc.time()
        xx = as.scidb(con$db, selector)
        # print(proc.time()-t1)
        qq2 = paste("apply(", xx@name, ",
                    dataset_version_, int64(dataset_version),
                    rnaquantificationset_id_, int64(rnaquantificationset_id),
                    biosample_id_, int64(biosample_id),
                    feature_id_, int64(feature_id))")
        qq2 = paste("cast(", qq2, ", <
                    dataset_version_old:int32,
                    rnaquantificationset_id_old:int32,
                    biosample_id_old:int32,
                    feature_id_old:int32,
                    flag:bool,
                    dataset_version:int64,
                    rnaquantificationset_id:int64,
                    biosample_id:int64,
                    feature_id:int64
                    >
                    [tuple_no,dst_instance_id,src_instance_id] )")
        qq2 = paste("redimension(", qq2, ", <flag:bool>[", yaml_to_dim_str(.ghEnv$meta$L$array[[.ghEnv$meta$arrRnaquantification]]$dims), "])")

        qq = paste("join(",
                   qq, ",",
                   qq2, ")")
        qq = paste("project(", qq, ", expression_count)")
        res = iquery(con$db, qq, return = T)
      } else { # Return data using cross_between_
        stop("Code needs to be added here to support inclusion of `dataset_version` in expression matrix schema")
        df = selector[order(selector$rnaquantificationset_id, selector$biosample_id, selector$feature_id), ]
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
  } else if (!is.null(rnaquantificationset_id) & !is.null(biosample_id) & is.null(feature_id)) { # user selected rqs and bs, not f
    selected_names = c('rnaquantificationset_id', 'biosample_id')
    val1 = rnaquantificationset_id
    val2 = biosample_id
    res = cross_between_select_on_two(qq, tt, val1, val2, selected_names, dataset_version = dataset_version, con = con)
  } else if (is.null(rnaquantificationset_id) & !is.null(biosample_id) & !is.null(feature_id)) { # user selected bs and f, not rqs
    selected_names = c('biosample_id', 'feature_id')
    val1 = biosample_id
    val2 = feature_id
    res = cross_between_select_on_two(qq, tt, val1, val2, selected_names, dataset_version = dataset_version, con = con)
  } else if (!is.null(rnaquantificationset_id) & is.null(biosample_id) & !is.null(feature_id)) { # user selected rqs and f, not bs
    selected_names = c('rnaquantificationset_id', 'feature_id')
    val1 = rnaquantificationset_id
    val2 = feature_id
    res = cross_between_select_on_two(qq, tt, val1, val2, selected_names, dataset_version = dataset_version, con = con)
  } else if (!is.null(rnaquantificationset_id) & is.null(biosample_id) & is.null(feature_id)) { # user selected rqs only
    if (exists('debug_trace')) cat("Only RNAQuantificationSet is selected.\n")
    if (length(rnaquantificationset_id) == 1){
      qq = paste("between(", qq, ",", dataset_version, ",", rnaquantificationset_id, ",NULL,NULL,", dataset_version, ",", rnaquantificationset_id, ",NULL,NULL)", sep = "")
    } else {
      stop("code for multiple rnaquantificationset_id to be added. Alternatively, call the search function by individual rnaquantificationset_id.")
    }
    res = iquery(con$db, qq, return = TRUE)
  } else if (is.null(rnaquantificationset_id) & !is.null(biosample_id) & is.null(feature_id)) { # user selected bs only
    stop("Only biosample is selected. Downloaded data could be quite large. Consider downselecting by RNAQuantificationSet_id or feature_id. ")
  }
  res
}





#' @export
search_variants = function(variantset, biosample = NULL, feature = NULL, con = NULL){
  if (!is.null(variantset)) {variantset_id = variantset$variantset_id} else {
    stop("variantset must be supplied"); variantset_id = NULL
  }
  if (length(unique(variantset$dataset_version)) != 1) {
    stop("multiple dataset versions in supplied variantset");
  }
  dataset_version = unique(variantset$dataset_version)
  if (!is.null(biosample)) {
    stopifnot(length(unique(biosample$dataset_version))==1)
    if (!(unique(biosample$dataset_version)==dataset_version)) stop("dataset_version-s of variantset and biosample must be same")
  }
  namespace = find_namespace(id = variantset_id,
                             entitynm = .ghEnv$meta$arrVariantset,
                             dflookup = get_variantset_lookup(con = con), 
                             con = con)
  cat("Found namespace: ", namespace, "\n")
  arrayname = paste(namespace, .ghEnv$meta$arrVariant, sep = ".")
  if (!is.null(biosample))            {biosample_id = biosample$biosample_id}                                  else {biosample_id = NULL}
  if (!is.null(feature))              {feature_id = feature$feature_id}                                        else {feature_id = NULL}
  
  if (exists('debug_trace')) cat("retrieving expression data from server\n")
  res = search_variants_scidb(arrayname,
                              variantset_id,
                              biosample_id,
                              feature_id,
                              dataset_version = dataset_version, 
                              con = con)
  res
}


search_variants_scidb = function(arrayname, variantset_id, biosample_id = NULL, feature_id = NULL, dataset_version, con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (is.null(dataset_version)) stop("dataset_version must be supplied")
  if (length(dataset_version) != 1) stop("can handle only one dataset_version at a time")
  
  if (is.null(variantset_id)) stop("variantset_id must be supplied")
  if (length(variantset_id) != 1) stop("can handle only one variantset_id at a time")
  
  left_query = paste("between(", arrayname,
                     ", ", dataset_version, ", ", variantset_id, ", null, null, null",
                     ", ", dataset_version, ", ", variantset_id, ", null, null, null)", sep = "")
  right_query = paste("between(", arrayname, "_INFO",
                      ", ", dataset_version, ", ", variantset_id, ", null, null, null, null",
                      ", ", dataset_version, ", ", variantset_id, ", null, null, null, null)", sep = "")
  
  if (!is.null(biosample_id)){
    if (length(biosample_id) == 1) {
      left_query = paste("between(", left_query,
                         ", null, null, ", biosample_id, ", null, null",
                         ", null, null, ", biosample_id, ", null, null)", sep = "")
      right_query = paste("between(", right_query,
                          ", null, null, ", biosample_id, ", null, null, null",
                          ", null, null, ", biosample_id, ", null, null, null)", sep = "")
    } else {
      left_query = paste("filter(", left_query, 
                         ", ", formulate_base_selection_query(fullarrayname = 'BIOSAMPLE',
                                                              id = biosample_id), ")")
      right_query = paste("filter(", right_query, 
                          ", ", formulate_base_selection_query(fullarrayname = 'BIOSAMPLE',
                                                               id = biosample_id), ")")
      # print(left_query)
      # print(right_query)
    }
  }
  
  if (!is.null(feature_id)){
    if (length(feature_id) == 1) {
      left_query = paste("between(", left_query,
                         ", null, null, null, ", feature_id, ", null",
                         ", null, null, null, ", feature_id, ", null)", sep = "")
      right_query = paste("between(", right_query,
                          ", null, null, null, ", feature_id, ", null, null",
                          ", null, null, null, ", feature_id, ", null, null)", sep = "")
    } else {
      left_query = paste("filter(", left_query, 
                         ", ", formulate_base_selection_query(fullarrayname = 'FEATURE',
                                                              id = feature_id), ")")
      right_query = paste("filter(", right_query, 
                          ", ", formulate_base_selection_query(fullarrayname = 'FEATURE',
                                                               id = feature_id), ")")
      # print(left_query)
      # print(right_query)
    }
  }
  
  xx = join_info_ontology_and_unpivot(qq = left_query, arrayname = strip_namespace(arrayname), namespace = get_namespace(arrayname),
                                      con = con)
  xx
}



#' @export
search_fusion = function(fusionset, biosample = NULL, feature = NULL, con = NULL){
  if (!is.null(fusionset)) {fusionset_id = fusionset$fusionset_id} else {
    stop("fusionset must be supplied"); fusionset_id = NULL
  }
  if (length(unique(fusionset$dataset_version)) != 1) {
    stop("multiple dataset versions in supplied fusionset");
  }
  dataset_version = unique(fusionset$dataset_version)
  if (!is.null(biosample)) {
    stopifnot(length(unique(biosample$dataset_version))==1)
    if (!(unique(biosample$dataset_version)==dataset_version)) stop("dataset_version-s of fusionset and biosample must be same")
  }
  namespace = find_namespace(id = fusionset_id,
                             entitynm = .ghEnv$meta$arrFusionset,
                             dflookup = get_fusionset_lookup(con = con), 
                             con = con)
  cat("Found namespace: ", namespace, "\n")
  arrayname = paste(namespace, .ghEnv$meta$arrFusion, sep = ".")
  if (!is.null(biosample))            {biosample_id = biosample$biosample_id}                                  else {biosample_id = NULL}
  if (!is.null(feature))              {feature_id = feature$feature_id}                                        else {feature_id = NULL}
  
  if (exists('debug_trace')) cat("retrieving fusion data from server\n")
  res = search_fusions_scidb(arrayname,
                             fusionset_id,
                             biosample_id,
                             feature_id,
                             dataset_version = dataset_version, 
                             con = con)
  res
}

search_fusions_scidb = function(arrayname, fusionset_id, biosample_id = NULL, feature_id = NULL, dataset_version, con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (is.null(dataset_version)) stop("dataset_version must be supplied")
  if (length(dataset_version) != 1) stop("can handle only one dataset_version at a time")
  
  if (is.null(fusionset_id)) stop("fusionset_id must be supplied")
  if (length(fusionset_id) != 1) stop("can handle only one fusionset_id at a time")
  
  left_query = paste("between(", arrayname,
                     ", ", dataset_version, ", ", fusionset_id, ", null, null",
                     ", ", dataset_version, ", ", fusionset_id, ", null, null)", sep = "")
  
  if (!is.null(biosample_id)){
    if (length(biosample_id) == 1) {
      left_query = paste("between(", left_query,
                         ", null, null, ", biosample_id, ", null",
                         ", null, null, ", biosample_id, ", null)", sep = "")
    } else {
      filter_expr = formulate_base_selection_query('BIOSAMPLE', id = biosample_id)
      left_query = paste("filter(", left_query,
                         ", ", filter_expr, ")", sep = "")
    }
  }
  
  if (!is.null(feature_id)){
    if (length(feature_id) == 1) {
      left_query = paste("filter(", left_query,
                         ", feature_id_left = ", feature_id, " OR feature_id_right = ", feature_id, ")", sep = "")
    } else {
      filter_expr = formulate_base_selection_query('FEATURE', id = feature_id)
      filter_expr_left = gsub("feature_id", "feature_id_left", filter_expr)
      filter_expr_right = gsub("feature_id", "feature_id_right", filter_expr)
      
      left_query = paste("filter(", left_query,
                         ", (", filter_expr_left, ") OR (", 
                         filter_expr_right, "))", sep = "")
    }
  }
  
  iquery(con$db, left_query, return = TRUE)
}


#' @export
search_copynumber_mat = function(copynumberset, biosample = NULL, feature = NULL, con = NULL){
  if (!is.null(copynumberset)) {copynumberset_id = copynumberset$copynumberset_id} else {
    stop("copynumberset must be supplied"); copynumberset_id = NULL
  }
  if (length(unique(copynumberset$dataset_version)) != 1) {
    stop("multiple dataset versions in supplied copynumberset");
  }
  dataset_version = unique(copynumberset$dataset_version)
  if (!is.null(biosample)) {
    stopifnot(length(unique(biosample$dataset_version))==1)
    if (!(unique(biosample$dataset_version)==dataset_version)) stop("dataset_version-s of copynumberset and biosample must be same")
  }
  namespace = find_namespace(id = copynumberset_id,
                             entitynm = .ghEnv$meta$arrCopyNumberSet,
                             dflookup = get_copynumberset_lookup(con = con), 
                             con = con)
  cat("Found namespace: ", namespace, "\n")
  arrayname = paste(namespace, .ghEnv$meta$arrCopynumber_mat, sep = ".")
  if (!is.null(biosample))            {biosample_id = biosample$biosample_id}                                  else {biosample_id = NULL}
  if (!is.null(feature))              {feature_id = feature$feature_id}                                        else {feature_id = NULL}
  
  if (exists('debug_trace')) cat("retrieving CopyNumber_mat data from server\n")
  res = search_copynumber_mats_scidb(arrayname,
                             copynumberset_id,
                             biosample_id,
                             feature_id,
                             dataset_version = dataset_version, 
                             con = con)
  res
}

search_copynumber_mats_scidb = function(arrayname, copynumberset_id, biosample_id = NULL, feature_id = NULL, dataset_version, 
                                        con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (is.null(dataset_version)) stop("dataset_version must be supplied")
  if (length(dataset_version) != 1) stop("can handle only one dataset_version at a time")
  
  if (is.null(copynumberset_id)) stop("copynumberset_id must be supplied")
  if (length(copynumberset_id) != 1) stop("can handle only one copynumberset_id at a time")
  
  left_query = paste("between(", arrayname,
                     ", ", dataset_version, ", ", copynumberset_id, ", null, null",
                     ", ", dataset_version, ", ", copynumberset_id, ", null, null)", sep = "")
  
  if (!is.null(biosample_id)){
    if (length(biosample_id) == 1) {
      left_query = paste("between(", left_query,
                         ", null, null, ", biosample_id, ", null",
                         ", null, null, ", biosample_id, ", null)", sep = "")
    } else {
      filter_expr = formulate_base_selection_query('BIOSAMPLE', id = biosample_id)
      left_query = paste("filter(", left_query,
                         ", ", filter_expr, ")", sep = "")
    }
  }
  
  if (!is.null(feature_id)){
    if (length(feature_id) == 1) {
      left_query = paste("filter(", left_query,
                         ", feature_id_left = ", feature_id, " OR feature_id_right = ", feature_id, ")", sep = "")
    } else {
      filter_expr = formulate_base_selection_query('FEATURE', id = feature_id)

      left_query = paste("filter(", left_query,
                         ", ", filter_expr, ")", sep = "")
    }
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
  namespace = find_namespace(id = experimentset_id,
                             entitynm = .ghEnv$meta$arrExperimentSet,
                             dflookup = get_experimentset_lookup(con = con), 
                             con = con)
  cat("Found namespace: ", namespace, "\n")
  arrayname = paste(namespace, .ghEnv$meta$arrCopynumber_seg, sep = ".")
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
  
  left_query = paste("between(", arrayname,
                     ", ", dataset_version, ", ", experimentset_id, ", null, null",
                     ", ", dataset_version, ", ", experimentset_id, ", null, null)", sep = "")
  
  if (!is.null(biosample_id)){
    if (length(biosample_id) == 1) {
      left_query = paste("between(", left_query,
                         ", null, null, ", biosample_id, ", null",
                         ", null, null, ", biosample_id, ", null)", sep = "")
    } else {
      filter_expr = formulate_base_selection_query('BIOSAMPLE', id = biosample_id)
      left_query = paste("filter(", left_query,
                         ", ", filter_expr, ")", sep = "")
    }
  }
  
  iquery(con$db, left_query, return = TRUE)
}
