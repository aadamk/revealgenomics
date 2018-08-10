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

################### METADATA (secured by dataset_id) ############################################################

#' @export
search_datasets = function(project_id = NULL, dataset_version = NULL, all_versions = TRUE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  check_args_search(dataset_version, all_versions)
  entitynm = .ghEnv$meta$arrDataset
  
  if (!is.null(project_id)) {
    fullnm = paste0(custom_scan(), "(", full_arrayname(entitynm), ")")
    if (is.null(dataset_version)) {
      qq = paste0("filter(", fullnm, ", ", "project_id=", project_id, ")")
    } else {
      qq = paste0(
        "filter(", fullnm, ", ", "project_id=", project_id, 
        " AND dataset_version=", dataset_version, ")")
    }
  } else {
    stop(cat("Must specify project_id To retrieve all datasets, use get_datasets()", sep = ""))
  }
  
  df = join_info_unpivot(qq,
                          entitynm,
                          con = con)
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' Search individuals by dataset
#' 
#' `search_individuals()` can be used to retrive
#' all individuals in a particular dataset
#' @export
search_individuals = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrIndividuals, 
                                          dataset_id, dataset_version, all_versions,
                                          con = con)
}

#' @export
search_biosamples = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrBiosample, 
                                          dataset_id, dataset_version, all_versions,
                                          con = con)
  
}

#' @export
search_copynumbersets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrCopyNumberSet, 
                                          dataset_id, dataset_version, all_versions, con = con)
}

#' @export
search_experimentsets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrExperimentSet, 
                                          dataset_id, dataset_version, all_versions, con = con)
}

#' @export
search_measurements = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrMeasurement, 
                                          dataset_id, dataset_version, all_versions, con = con)
}

#' @export
search_measurementsets = function(dataset_id = NULL, dataset_version = NULL, 
                                  all_versions = FALSE, 
                                  measurement_type = NULL,
                                  con = NULL){
  df_msmtset = search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrMeasurementSet, 
                                                       dataset_id = dataset_id,
                                                       dataset_version = dataset_version,
                                                       all_versions = all_versions,
                                                       con = con)
  if (!is.null(measurement_type)) {
    stopifnot(length(measurement_type) == 1)
    entity_df = get_entity_info()
    entity_df = entity_df[entity_df$class == 'measurementdata', ]
    
    if (!(measurement_type %in% as.character(entity_df$entity))) {
      cat("Unexpected measurement entity:", measurement_type, "\n")
      stop("Allowed measurement entities: ", pretty_print(entity_df$entity))
    }    
    
    df_msmtset = df_msmtset[df_msmtset$entity == measurement_type, ]
  }
  df_msmtset
}

################### METADATA (not secured by dataset_id) ############################################################

#' @export
search_ontology = function(terms, 
                           category = 'uncategorized', 
                           exact_match = TRUE, updateCache = FALSE, con = NULL){
  ont = get_ontology(updateCache = updateCache, con = con)
  if (!is.null(category)) ont = ont[ont$category == category, ]
  if (nrow(ont) == 0) return(NA)
  ont_ids = ont$ontology_id
  names(ont_ids) = ont$term
  if (exact_match){
    res = ont_ids[terms]
    if (any(is.na(res)) & !updateCache) {
      cat("Updating ontology cache\n")
      search_ontology(terms, exact_match = exact_match, updateCache = TRUE, con = con)
    }
    names(res) = terms
    as.integer(res)
  } else {
    if (length(terms) != 1) {
      # do a recursive call
      if (updateCache) stop("cannot do inexact searching on multiple terms with updateCache set to TRUE")
      lapply(terms, FUN = function(term) {search_ontology(terms = term, exact_match = FALSE, con = con)})
    } else {
      ont[grep(terms, ignore.case = TRUE, ont$term), ]
    }
  }
}

#' @export
search_definitions = function(dataset_id, updateCache = FALSE, con = NULL) {
  stopifnot(length(dataset_id) == 1)
  df1 = get_definitions(updateCache = updateCache, con = con)
  if (nrow(df1) == 0) {
    return(df1)
  } else {
    drop_na_columns(df1[df1$dataset_id == dataset_id, ])
  }
}

#' @export
search_genelist_gene = function(genelist = NULL, 
                                genelist_id = NULL, con = NULL){
  con = use_ghEnv_if_null(con)
  
  arrayname = full_arrayname(.ghEnv$meta$arrGenelist_gene)
  
  if (!is.null(genelist) & !is.null(genelist_id)) {
    stop("Use only one method for searching. Preferred method is using genelist")
  }
  
  # API level security (TODO: replace with secure_scan() operator)
  if (is.null(genelist_id)) {
    genelist_id = genelist$genelist_id
  } else {
    genelist = get_genelist(genelist_id = genelist_id)
  }
  if (!genelist$public & 
      !(get_logged_in_user(con = con) %in% c('root', 'scidbadmin', genelist$owner))) {
    stop("Do not have permissions to search genelist_id: ", genelist_id)
  }
  
  qq = arrayname
  if (length(genelist_id)==1){
    qq = paste("filter(", qq, ", genelist_id = ", genelist_id, ")", sep="")
  } else {stop("Not covered yet")}
  
  res = iquery(con$db, qq, return = TRUE)
  
  
}

###################### FEATUREDATA ##########################################################

#' Search features by synonym
#' 
#' @param synonym: A name for a gene by any convention e.g. ensembl_gene_id, entrez_id, vega_id
#' @param id_type: (Optional) The id type by which to search e.g. ensembl_gene_id, entrez_id, vega_id
#' @param featureset_id: (Optional) The featureset within which to search
#' @return feature(s) associated with provided synonym
#' @export
search_feature_by_synonym = function(synonym, id_type = NULL, featureset_id = NULL, updateCache = FALSE, con = NULL){
  syn = get_feature_synonym(updateCache = updateCache, con = con)
  f1 = syn[syn$synonym %in% synonym, ]
  if (!is.null(id_type)) {f1 = f1[f1$source == id_type, ]}
  if (!is.null(featureset_id)) {f1 = f1[f1$featureset_id == f1$featureset_id, ]}
  if (nrow(f1) > 0) {
    get_features(feature_id = unique(f1$feature_id), con = con)
  } else {
    get_features(feature_id = -1, con = con)
  }
}

#' @export
search_features = function(gene_symbol = NULL, feature_type = NULL, featureset_id = NULL, con = NULL){
  arrayname = full_arrayname(.ghEnv$meta$arrFeature)
  
  qq = arrayname
  if (!is.null(featureset_id)){
    if (length(featureset_id)==1){
      qq = paste("filter(", qq, ", featureset_id = ", featureset_id, ")", sep="")
    } else if (length(featureset_id)==2){
      qq = paste("filter(", qq, ", featureset_id = '", featureset_id[1], "' OR featureset_id = '", featureset_id[2], "')", sep="")
    } else {stop("Not covered yet")}
  }
  
  if (!is.null(feature_type)){
    if (length(feature_type)==1){
      qq = paste("filter(", qq, ", feature_type = '", feature_type, "')", sep="")
    } else if (length(feature_type)==2){
      qq = paste("filter(", qq, ", feature_type = '", feature_type[1], "' OR feature_type = '", feature_type[2], "')", sep="")
    } else {stop("Not covered yet")}
  }
  
  if (!is.null(gene_symbol)) {
    subq = paste(sapply(gene_symbol, FUN = function(x) {paste("gene_symbol = '", x, "'", sep = "")}), collapse = " OR ")
    qq = paste("filter(", qq, ", ", subq, ")", sep="")
  }
  
  join_info_unpivot(qq, arrayname, 
                    con = con)
}

##################### MEASUREMENTDATA ###########################################################

#' Search gene expression data 
#' 
#' Function to search gene expression data array; allows slicing across multiple 
#' dimensions. However `measurementset` (i.e. pipeline) must be supplied 
#' (currently allows searching one pipeline at a time)
#' 
#' @param measurementset (Mandatory) dataframe containing pipeline information; 
#'                       typically output of a 
#'                       `get_measurementsets(measurementset_id = ...)` or 
#'                       `search_measurementsets(dataset_id = ..)` call
#' @param biosample (Optional) dataframe containing biosample information; 
#'                  typically output of a 
#'                  `search_biosamples(dataset_id = ..)` call.
#'                  If not specified, function returns all biosamples available 
#'                  by other search parameters
#' @param feature (Optional) dataframe containing feature information;
#'                typically output of a 
#'                `search_features(gene_symbol = ...)` call.
#'                If not specified, function returns all features available 
#'                by other search parameters
#' @param formExpressionSet (default: TRUE) whether to return result as a Bioconductor
#'                          ExpressionSet object
#' @param biosample_ref (Optional) data-frame containing all biosamples in a study,
#'                      i.e. typically output of `search_biosamples(dataset_id = ..)` call,
#'                      or all biosamples available to logged in user,
#'                      i.e. output of `get_biosamples(biosample_id = NULL)`.
#'                      When not searching by `biosample`, and when requesting
#'                      to return an ExpressionSet object, supplying this parameter can
#'                      optimize function exection time because function does not have to
#'                      do biosample lookup internally
#' @param con (Optional) database connection object; typically output of `gh_connect2()` 
#'            call. If not specified, connection object is formulated from internally stored
#'            values of `gh_connect()` call
#'            
#' @export
search_rnaquantification = function(measurementset = NULL,
                                    biosample = NULL,
                                    feature = NULL,
                                    formExpressionSet = TRUE,
                                    biosample_ref = NULL,
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
  
  optPathBiosamples = FALSE
  if (is.null(biosample)) {
    if (formExpressionSet & is.null(biosample_ref)) {
      message("WARNING! You are trying to 
         - search by one pipeline 
         - search by zero or more features 
         - you are NOT slicing by biosample
         - You want to return data as ExpressionSet
         To use optimized search path, supply `biosample_ref` parameter, or 
              set `formExpressionSet = FALSE`.\n")
    } else {
      optPathBiosamples = TRUE
    }
  }
  
  if (optPathBiosamples) { # use optimized function when not searching by biosample
    cat("Not searching by biosample; Using optimized search path\n")
    dao_search_rnaquantification(measurementset = measurementset, 
                                 biosample_ref = biosample_ref, 
                                 feature = feature, 
                                 formExpressionSet = formExpressionSet, 
                                 con = con)
    
  } else {
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
      cat("query the server for matching features\n")
      feature = get_features(feature_id, con = con)
    }
    
    expressionSet = formulate_list_expression_set(expr_df = res, dataset_version, measurementset, biosample, feature)
    
    return(expressionSet)
  }
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
      # Return data using join
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
    }
  } else if (!is.null(measurementset_id) & !is.null(biosample_id) & is.null(feature_id)) { # user selected rqs and bs, not f
    selected_names = c('measurementset_id', 'biosample_id')
    val1 = measurementset_id
    val2 = biosample_id
    res = cross_join_select_by_two_dims(qq, tt, val1, val2, selected_names, dataset_version = dataset_version, con = con)
  } else if (is.null(measurementset_id) & !is.null(biosample_id) & !is.null(feature_id)) { # user selected bs and f, not rqs
    selected_names = c('biosample_id', 'feature_id')
    val1 = biosample_id
    val2 = feature_id
    res = cross_join_select_by_two_dims(qq, tt, val1, val2, selected_names, dataset_version = dataset_version, con = con)
  } else if (!is.null(measurementset_id) & is.null(biosample_id) & !is.null(feature_id)) { # user selected rqs and f, not bs
    selected_names = c('measurementset_id', 'feature_id')
    val1 = measurementset_id
    val2 = feature_id
    res = cross_join_select_by_two_dims(qq, tt, val1, val2, selected_names, dataset_version = dataset_version, con = con)
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


#' Faster implementation of `search_rnaquantification` for UI development
#' 
#' Here, there is no option to select by specific biosamples
#' 
#' @param biosample_ref Reference dataframe containing at least the biosample data for 
#'                      current study (e.g. by calling 
#'                      `search_biosamples(dataset_id = measurementset$dataset_id)`). 
#'                      OK if more biosample rows are provided (e.g. by calling
#'                      `get_biosamples()`)
#' 
dao_search_rnaquantification = function(measurementset, 
                                        biosample_ref, 
                                        feature = NULL, 
                                        formExpressionSet = TRUE, 
                                        con = NULL) {
  con = use_ghEnv_if_null(con = con)
  stopifnot(nrow(measurementset) == 1)
  rqs_id = unique(measurementset$measurementset_id)
  stopifnot(length(rqs_id)==1)
  
  dataset_version = unique(measurementset$dataset_version)
  stopifnot(length(dataset_version) == 1)
  if (dataset_version != 1) stop("Need to add in `dataset_version` parameter to AFL queries below")
  
  dataset_id = unique(measurementset$dataset_id)
  stopifnot(length(dataset_id) == 1)
  
  arr0 = full_arrayname(.ghEnv$meta$arrRnaquantification)
  
  qq = paste0(custom_scan(), "(", arr0, ")")
  
  if (!is.null(feature)) {
    if (! (measurementset$featureset_id %in% feature$featureset_id) ) {
      stop(paste0("Featureset specified py pipeline: ", measurementset$featureset_id, 
                  " not present in features dataframe"))
    }
    feature = feature[feature$featureset_id == measurementset$featureset_id, ]
    ftr_id = sort(unique(feature$feature_id))
    
    K_THRESH = 20
    if (length(ftr_id) <= K_THRESH) {
      path = "filter_features"
    } else {
      path = "build_literal_and_cross_join_features"
    }
    if (path == 'filter_features') {
      expr = formulate_base_selection_query(fullarrayname = .ghEnv$meta$arrFeature,
                                            id = ftr_id)
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
    qq2 = paste0("filter(", qq2, ", measurementset_id = ", rqs_id, ")")
    res = iquery(con$db, query = qq2, binary = FALSE, return = TRUE)
  } else { # user has not supplied features; try to download full data
    qq2 = paste0("filter(", qq, 
                 ", ", get_base_idname(.ghEnv$meta$arrMeasurementSet), 
                 "=", measurementset$measurementset_id, 
                 ")")
    req_ids = get_base_idname(.ghEnv$meta$arrRnaquantification)
    req_ids = req_ids[ !(req_ids %in% get_base_idname(.ghEnv$meta$arrMeasurementSet)) ]
    apply_str = paste0(req_ids, ",", req_ids, collapse = ", ")
    qq3 = paste0("apply(", qq2, ", ", apply_str, ")")
    cat("Estimating download size: ")
    download_size = iquery(con$db, 
                           query = 
                             paste0(
                               "project(
                                 summarize(",
                                   qq2, 
                                   "), bytes)"), 
                           return = TRUE)$bytes
    cat(download_size/1024/1024, " MB\n")
    download_limit_mb = 1000
    if (download_size > download_limit_mb * 1024 * 1024) {
      cat("Trying to download more than", download_limit_mb, "MB at a time! 
          Post an issue at https://github.com/Paradigm4/revealgenomics/issues\n")
      return(NULL)
    }
    res = iquery(con$db, query = qq3, binary = TRUE, only_attributes = TRUE, return = TRUE)
  }
  
  if (!formExpressionSet) return(res)
  
  # Retrieved biosamples
  biosample_id = unique(res$biosample_id)
  biosample = biosample_ref[biosample_ref$biosample_id %in% biosample_id, ]
  
  # Retrieved features
  feature_id = unique(res$feature_id)
  if (!is.null(feature)) {
    feature_sel = feature[feature$feature_id %in% feature_id, ]
  } else { # user has not supplied features; need to download features from DB
    cat("Downloading features to form ExpressionSet\n")
    feature_sel = get_features(feature_id = feature_id, 
                               con = con)
  }
  cat("Forming ExpressionSet\n")
  expressionSet = formulate_list_expression_set(expr_df = res, 
                                                dataset_version = dataset_version, 
                                                measurementset = measurementset,
                                                biosample = biosample,
                                                feature = feature_sel)
  
  return(expressionSet)
}



#' search variants
#' 
#' Search variants by pipeline, sample and/or  feature
#' 
#' @param measurementset pipeline dataframe (mandatory)
#' 
#' @export
search_variants = function(measurementset, biosample = NULL, feature = NULL, 
                           autoconvert_characters = TRUE,
                           variants_in_one_array = TRUE,
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
  if (!is.null(feature)) {
    if (! (measurementset$featureset_id %in% feature$featureset_id) ) {
      stop(paste0("Featureset specified py pipeline: ", measurementset$featureset_id, 
                  " not present in features dataframe"))
    }
    feature = feature[feature$featureset_id == measurementset$featureset_id, ]
    
    feature_id = sort(unique(feature$feature_id))
  } else {
    feature_id = NULL
  }
  
  if (exists('debug_trace')) cat("retrieving expression data from server\n")
  t1 = proc.time()
  res = search_variants_scidb(arrayname,
                              measurementset_id,
                              biosample_id,
                              feature_id,
                              dataset_version = dataset_version, 
                              variants_in_one_array = variants_in_one_array,
                              con = con)
  cat(paste0("search_variants_scidb time: ", (proc.time()-t1)[3], "\n"))
  
  t1 = proc.time()
  if (autoconvert_characters) {
    res = autoconvert_char(df1 = res, convert_logicals = FALSE)
  }
  cat(paste0("Autoconvert time: ", (proc.time()-t1)[3], "\n"))
  res
}

#' Inner function for searching variants
#' 
#' @param use_cross_join use `cross_join` if TRUE, else use `equi_join`
search_variants_scidb = function(arrayname, measurementset_id, biosample_id = NULL, feature_id = NULL, 
                                 dataset_version, 
                                 use_cross_join = FALSE, 
                                 variants_in_one_array, 
                                 con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (is.null(dataset_version)) stop("dataset_version must be supplied")
  if (length(dataset_version) != 1) stop("can handle only one dataset_version at a time")
  
  if (is.null(measurementset_id)) stop("measurementset_id must be supplied")
  if (length(measurementset_id) != 1) stop("can handle only one measurementset_id at a time")
  
  if (!variants_in_one_array) { # Data exists in two arrays: VARIANT and VARIANT_INFO
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
      res = join_info_unpivot(qq = var_nonflex_q, 
                              arrayname = strip_namespace(arrayname), 
                              replicate_query_on_info_array = TRUE, 
                              profile_timing = TRUE,
                              con = con)
    }
  } else { # VARIANT and VARIANT_INFO in one array
    if (!is.null(biosample_id)) stop("Code path not implemented: 
                                     Selection of biosample id from Variants in one array")
    if (use_cross_join) stop("`use_cross_join` not supposed to be TRUE 
                             when `variants_in_one_array` is TRUE")
    
    if (is.null(feature_id)) stop("Expect feature_id to be non-null")
    
    leftq = paste0("apply(build(<feature_id:int64>[idx_ftr=0:*], '[", 
                    paste0(sort(feature_id), collapse = ","), 
                    "]', true), measurementset_id, ", measurementset_id, 
                   ", dataset_version, ", dataset_version, ")")
    
    query = paste0("equi_join(", 
                      custom_scan(), "(", full_arrayname(.ghEnv$meta$arrVariant), "),",
                      leftq, ", 'left_names=feature_id,dataset_version,measurementset_id', 
                     'right_names=feature_id,dataset_version,measurementset_id', 
                     'algorithm=hash_replicate_right', 'keep_dimensions=1')")
    t1 = proc.time()
    var_raw = iquery(con$db, query, return = T, only_attributes = T)
    var_raw[, 'idx_ftr'] = NULL # drop the dimension added through the join
    cat("Selection from variant array:", (proc.time()-t1)[3], "\n")
    
    t1 = proc.time()
    res = tidyr::spread(data = var_raw, key = "key_id", value = "val")
    cat("Unpivot:", (proc.time()-t1)[3], "\n")
    
    t1 = proc.time()
    VAR_KEY = get_variant_key(con = con)
    M = find_matches_and_return_indices(colnames(res), VAR_KEY$key_id)
    
    matched_colnames = c(colnames(res)[M$source_unmatched_idx], 
                         VAR_KEY[M$target_matched_idx, ]$key)
    stopifnot(all(!is.na(matched_colnames)))
    colnames(res) = matched_colnames
    cat("Replacing variant keys:", (proc.time()-t1)[3], "\n")
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
