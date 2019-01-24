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

  # Apply definition constraints
  L1 = lapply(
    df$dataset_id, 
    function(dataset_id) {
      df1 = df[df$dataset_id == dataset_id, ]
      apply_definition_constraints(df1 = df1,
                                   dataset_id = dataset_id,
                                   entity = entitynm,
                                   con = con)
    }
  )
  df = do.call(what = "rbind", args = L1)
  
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
search_measurements = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, 
                               measurementset_id = NULL, 
                               con = NULL){
  if (is.null(measurementset_id)) { # regular search by dataset id and/or version 
    search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrMeasurement, 
                                            dataset_id, dataset_version, all_versions, con = con)
  } else { # need to search by pipeline id
    entity = .ghEnv$meta$arrMeasurement
    qq = paste0(
      "filter(", 
        custom_scan(), "(", full_arrayname(entity), ")", 
        ", measurementset_id = ", measurementset_id, ")")
    
    df1 = join_info_unpivot(qq = qq,
                      arrayname = entity,
                      replicate_query_on_info_array = FALSE,
                      con = con)
    # reorder the output by the dimensions
    # from https://stackoverflow.com/questions/17310998/sort-a-dataframe-in-r-by-a-dynamic-set-of-columns-named-in-another-data-frame
    if (nrow(df1) == 0) return(df1)
    df1 = df1[do.call(order, df1[get_idname(entity)]), ] 
    
    apply_definition_constraints(df1 = df1,
                                 dataset_id = unique(df1$dataset_id),
                                 entity = entity,
                                 con = con)
  }
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
search_gene_symbols = function(gene_symbol, updateCache = FALSE, con = NULL) {
  stopifnot(class(gene_symbol) == 'character')
  df1 = get_gene_symbol(updateCache = updateCache, con = con)
  m1 = find_matches_and_return_indices(
    source = gene_symbol, 
    target = df1$gene_symbol
  )
  if (length(m1$target_matched_idx) > 0) {
    res = df1[m1$target_matched_idx, ]
  } else {
    res = data.frame(
      gene_symbol_id = integer(),
      gene_symbol = character(), 
      full_name = character(), 
      stringsAsFactors = FALSE
    )
  }
  return(res)
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
search_features = function(
  gene_symbol = NULL, 
  feature_type = c('gene', 'probeset', 'transcript', 
                   'protein_probe'), 
  featureset_id = NULL, 
  con = NULL) {
  arrayname = full_arrayname(.ghEnv$meta$arrFeature)
  
  qq = arrayname
  if (!is.null(featureset_id)){
    if (length(featureset_id)==1){
      qq = paste("filter(", qq, ", featureset_id = ", featureset_id, ")", sep="")
    } else if (length(featureset_id)==2){
      qq = paste("filter(", qq, ", featureset_id = '", featureset_id[1], "' OR featureset_id = '", featureset_id[2], "')", sep="")
    } else {stop("Not covered yet")}
  }
  
  if (!is.null(gene_symbol)) {
    gene_symbol_df = search_gene_symbols(gene_symbol = gene_symbol, con = con)
    if (nrow(gene_symbol_df) > 0) {
      subq = paste(
        sapply(
          gene_symbol_df$gene_symbol_id, 
          FUN = function(x) {
            paste0("gene_symbol_id=", x)
            }), 
        collapse = " OR ")
    } else {
      subq = 'FALSE'
    }
    qq = paste("filter(", qq, ", ", subq, ")", sep="")
  }
  
  df1 = join_info_unpivot(qq, arrayname, 
                    replicate_query_on_info_array = TRUE, 
                    con = con)
  
  if (!is.null(feature_type)){
    df1[df1$feature_type %in% feature_type, ]
  } else {
    df1
  }
}

##################### MEASUREMENTDATA ###########################################################

#' Deprecated function to search gene expression data 
#' 
#' This function is kept around for backward compatibility. 
#' Use \code{\link{search_expression}} instead. 
#' 
#' Function to search gene expression data array; allows slicing across multiple 
#' dimensions. However \code{measurementset} (i.e. pipeline) must be supplied 
#' (currently allows searching one pipeline at a time)
#' 
#' @param ... refer parameters for function \code{\link{search_expression}}           
#' @export
search_rnaquantification = function(...) {
  search_expression(...)
}

#' Search expression data (RNA-seq / GXP by Microarray, Proteomics) 
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
#' @param con (Optional) database connection object; typically output of `rg_connect2()` 
#'            call. If not specified, connection object is formulated from internally stored
#'            values of `rg_connect()` call
#'            
#' @export
search_expression = function(measurementset = NULL,
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
  entity = unique(measurementset$entity)
  stopifnot(entity %in% c(.ghEnv$meta$arrRnaquantification,
                          .ghEnv$meta$arrProteomics, 
                          .ghEnv$meta$arrCopynumber_mat))
  arrayname = full_arrayname(entity)
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
    if (getOption("revealgenomics.debug", FALSE)) {
      cat("Not searching by biosample; Using optimized search path\n")
    }
    dao_search_expression(entity = entity,
                          measurementset = measurementset, 
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
      options(scidb.aio = TRUE) # try faster path: Shim uses aio_save with atts_only=0 which will include dimensions
                                # https://github.com/Paradigm4/SciDBR/commit/85895a77549a24c16766e6adf4dcc6311ee21acc
      res = iquery(con$db, qq, return = T)
      options(scidb.aio = FALSE)
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


#' Faster implementation of `search_expression` for UI development
#' 
#' Here, there is no option to select by specific biosamples
#' 
#' @param biosample_ref Reference dataframe containing at least the biosample data for 
#'                      current study (e.g. by calling 
#'                      `search_biosamples(dataset_id = measurementset$dataset_id)`). 
#'                      OK if more biosample rows are provided (e.g. by calling
#'                      `get_biosamples()`)
#' 
dao_search_expression = function(entity, 
                                 measurementset, 
                                 biosample_ref, 
                                 feature = NULL, 
                                 formExpressionSet = TRUE, 
                                 con = NULL) {
  stopifnot(entity %in% c(.ghEnv$meta$arrRnaquantification, 
                          .ghEnv$meta$arrProteomics,
                          .ghEnv$meta$arrCopynumber_mat))
  con = use_ghEnv_if_null(con = con)
  stopifnot(nrow(measurementset) == 1)
  rqs_id = unique(measurementset$measurementset_id)
  stopifnot(length(rqs_id)==1)
  
  dataset_version = unique(measurementset$dataset_version)
  stopifnot(length(dataset_version) == 1)
  if (dataset_version != 1) stop("Need to add in `dataset_version` parameter to AFL queries below")
  
  dataset_id = unique(measurementset$dataset_id)
  stopifnot(length(dataset_id) == 1)
  
  arr0 = full_arrayname(entity)
  
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
    req_ids = get_base_idname(entity)
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


#' Deprecated function for searching variant data
#' 
#' This function is kept around for backward compatibility. 
#' Use \code{\link{search_variant}} instead. 
#' 
#' Function to search variant data array; allows slicing across multiple 
#' dimensions. \code{measurementset} (i.e. pipeline) must be supplied 
#' (currently allows searching one pipeline at a time)
#' 
#' @param ... refer parameters for function \code{\link{search_variant}}           
#' @export
search_variants = function(...) {
  search_variant(...)
}

#' search variants
#' 
#' Search variants by pipeline, sample and/or  feature
#' 
#' @param measurementset pipeline dataframe (mandatory)
#' 
#' @export
search_variant = function(measurementset, biosample = NULL, feature = NULL, 
                           autoconvert_characters = TRUE,
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
  if (exists('debug_trace')) {t1 = proc.time()}
  res = search_variants_scidb(arrayname,
                              measurementset_id,
                              biosample_id,
                              feature_id,
                              dataset_version = dataset_version, 
                              con = con)
  if (exists('debug_trace')) {
    cat(paste0("search_variants_scidb time: ", (proc.time()-t1)[3], "\n"))
  }
  
  # Unpivot
  res = unpivot_variant_data(var_raw = res, con = con)
  
  # Auto-convert characters
  if (exists('debug_trace')) {t1 = proc.time()}
  if (autoconvert_characters) {
    res = autoconvert_char(df1 = res, convert_logicals = FALSE)
  }
  if (exists('debug_trace')) {
    cat(paste0("Autoconvert time: ", (proc.time()-t1)[3], "\n"))
  }
  res
}

#' Inner function for searching variants
#' 
search_variants_scidb = function(arrayname, 
                                 measurementset_id, 
                                 biosample_id = NULL, 
                                 feature_id = NULL, 
                                 dataset_version, 
                                 con = NULL){
  con = use_ghEnv_if_null(con)
  
  entitynm = strip_namespace(arrayname)
  scanned_array = paste0(custom_scan(), "(", full_arrayname(entitynm), ")")
  if (is.null(dataset_version)) stop("dataset_version must be supplied")
  if (length(dataset_version) != 1) stop("can handle only one dataset_version at a time")
  
  if (is.null(measurementset_id)) stop("measurementset_id must be supplied")
  if (length(measurementset_id) != 1) stop("can handle only one measurementset_id at a time")
  
  left_query = paste0("filter(", scanned_array,
                      ", dataset_version=", dataset_version, " AND measurementset_id=", measurementset_id, ")")
  
  if (!is.null(biosample_id)){
    filter_expr = formulate_base_selection_query(.ghEnv$meta$arrBiosample, id = biosample_id)
    left_query = paste("filter(", left_query,
                       ", ", filter_expr, ")", sep = "")
  }
  
  query_formulation = list(
    left_query = left_query, 
    equi_joined_result = FALSE # whether equi_join will be used later on (not used by default)
  )
  if (!is.null(feature_id)){
    # formulate_base_selection_query() fails after 400 operands so putting code in a try catch
    query_formulation = 
      tryCatch({
        filter_expr = formulate_base_selection_query(.ghEnv$meta$arrFeature, id = feature_id)
        list(
          left_query = paste("filter(", left_query,
                             ", ", filter_expr,  
                             ")", sep = ""),
          equi_joined_result = FALSE
        )
      }, error = function(e) {
        leftq = paste0("apply(build(<feature_id:int64>[idx_ftr=0:*], '[", 
                       paste0(sort(feature_id), collapse = ","), 
                       "]', true), measurementset_id, ", measurementset_id, 
                       ", dataset_version, ", dataset_version, ")")
        
        left_query = 
        list(
          left_query = paste0("equi_join(", 
                              scanned_array, ", ", 
                              leftq, ", 'left_names=feature_id,dataset_version,measurementset_id', 
                              'right_names=feature_id,dataset_version,measurementset_id', 
                              'algorithm=hash_replicate_right', 'keep_dimensions=1')"),
          equi_joined_result = TRUE
        )
      }) 
  }
  
  if (query_formulation$equi_joined_result) {
    var_raw = iquery(con$db, query_formulation$left_query, return = T, only_attributes = T)
    var_raw[, 'idx_ftr'] = NULL # drop the dimension added through the join
  } else {
    var_raw = iquery(con$db, query_formulation$left_query, return = TRUE)
  }
  var_raw
}

unpivot_variant_data = function(var_raw, con = NULL) {
  if (exists('debug_trace')) {t1 = proc.time()}
  res = tidyr::spread(data = var_raw, key = "key_id", value = "val")
  if (exists('debug_trace')) cat("Unpivot:", (proc.time()-t1)[3], "\n")
  
  if (exists('debug_trace')) {t1 = proc.time()}
  VAR_KEY = get_variant_key(con = con)
  M = find_matches_and_return_indices(colnames(res), VAR_KEY$key_id)
  
  matched_colnames = c(colnames(res)[M$source_unmatched_idx], 
                       VAR_KEY[M$target_matched_idx, ]$key)
  stopifnot(all(!is.na(matched_colnames)))
  colnames(res) = matched_colnames
  if (exists('debug_trace')) cat("Replacing variant keys:", (proc.time()-t1)[3], "\n")
  
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
  
  if (!is.null(biosample)) {
    biosample_id = biosample$biosample_id
  }
  else {
    biosample_id = NULL
  }
  
  if (!is.null(feature)) {
    feature = feature[feature$featureset_id == measurementset$featureset_id, ]
    feature_id = feature$feature_id
  }
  else {
    feature_id = NULL
  }
  
  if (exists('debug_trace')) cat("retrieving fusion data from server\n")
  res = search_fusions_scidb(arrayname = .ghEnv$meta$arrFusion,
                             measurementset_id,
                             biosample_id,
                             feature_id,
                             dataset_version = dataset_version, 
                             con = con)
  
  if (nrow(res) > 0) {
    # Unpivot
    res = unpivot_variant_data(var_raw = res, con = con)
    
    # Auto-convert characters
    if (exists('debug_trace')) {t1 = proc.time()}
    res = autoconvert_char(df1 = res, convert_logicals = FALSE)
    if (exists('debug_trace')) cat(paste0("Autoconvert time: ", (proc.time()-t1)[3], "\n"))
  } 
  drop_na_columns(
    res[, colnames(res)[
      !(colnames(res) %in% 
          c('key_id', 'val', 'per_gene_pair_fusion_number'))]])
}

search_fusions_scidb = function(arrayname, 
                                measurementset_id, 
                                biosample_id = NULL, 
                                feature_id = NULL, 
                                dataset_version, 
                                con = NULL){
  con = use_ghEnv_if_null(con)
  
  scanned_array = paste0(custom_scan(), "(", full_arrayname(arrayname), ")")
  if (is.null(dataset_version)) stop("dataset_version must be supplied")
  if (length(dataset_version) != 1) stop("can handle only one dataset_version at a time")
  
  if (is.null(measurementset_id)) stop("measurementset_id must be supplied")
  if (length(measurementset_id) != 1) stop("can handle only one measurementset_id at a time")
  
  left_query = paste0("filter(", scanned_array,
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

#' Unified function to download entire pipeline worth of data
#' 
#' @param measurementset dataframe containing one row of pipeline information
#' @param con            connection object (optional  if using \code{rg_connect()})
#' 
#' @export
search_measurementdata = function(measurementset, con = NULL) {
  if (nrow(measurementset) != 1) {
    stop("This function currently works on one measurementSet at a time")
  }
  
  # Check entity
  entity = measurementset$entity
  xx = get_entity_info()
  xx = xx[xx$class == 'measurementdata', ]
  if (!(entity %in% xx$entity)) {
    stop("Entity of current measurementSet: ", 
         entity, " is not supported")
  }
  lookup = c(
    'search_expression',
    'search_variant',
    'search_fusion', 
    'search_expression',
    'search_copynumber_mat'
  )
  names(lookup) = c(
    .ghEnv$meta$arrRnaquantification,
    .ghEnv$meta$arrVariant,
    .ghEnv$meta$arrFusion,
    .ghEnv$meta$arrProteomics,
    .ghEnv$meta$arrCopynumber_mat
  )
  fn_name = lookup[entity]
  fn = get(fn_name)
  fn(measurementset = measurementset, con = con)
}

###### DATA ESTIMATION #####

#' Estimate downloaded size for measurement data
#' 
#' (1.) This currently estimates data per pipeline (cannot estimate a subset by feature i.e. genes or biosamples)
#' (2.) The estimate is a rough estimate e.g. RNA-seq data will have estimate for multiple 
#' columns that are eventually not present in the expression matrix (e.g. \code{dataset_id, dataset_version, 
#' biosample_id, feature_id, measurementset_id}). Some of these columns are important to link 
#' metadata to the measurement data, and only those are eventually downloaded from the 
#' database.
#' (3.) For now this is only  used to estimate the data size in the measurement array 
#' (not the feature and any other metadata that might also be downloaded to form a compound 
#' object e.g. \code{\link{search_expression}} returns a Bioconductor ExpressionSet object).
#' 
#' @param measurementset measurementset dataframe -- e.g. output of \code{get_measurementset()}
#'                       or \code{search_measurementset(dataset_id = ...)}
#' @param units return estimated size as either of "\code{KB}", "\code{MB}" (default) or "\code{GB}"
#' @param con the connection object (optional)
#' @export
estimate_measurementdata_download_size = function(
  measurementset, 
  units = c("MB", "KB", "GB"),
  con = NULL
) {
  units = match.arg(units)
  con = use_ghEnv_if_null(con = con)
  if (! 'entity' %in% colnames(measurementset)) {
    stop("Expected to find the column `entity` in a measurementset data.frame.
         See the output of `mandatory_fields()[['MEASUREMENTSET']]`") 
  }
  if (! 'measurementset_id' %in% colnames(measurementset)) {
    stop("Expected to find the column `measurementset_id` in a measurementset data.frame.
         See the output of `mandatory_fields()[['MEASUREMENTSET']]`") 
  }
  if (nrow(measurementset) != 1) {
    stop("Estimation is provided for 1 pipeline at a time")
  }
  query = paste0(custom_scan(), "(", full_arrayname(measurementset$entity), ")")
  query = paste0(
    "project(summarize(filter(", 
    query,
    ", measurementset_id=",
    measurementset$measurementset_id,
    ")), bytes)"
  )
  size_bytes = iquery(con$db, query, return = TRUE)$bytes
  list(
    entity = measurementset$entity,
    size = switch (
      units,
      "KB" = size_bytes/1024,
      "MB" = size_bytes/1024/1024,
      "GB" = size_bytes/1024/1024/1024
    ),
    units = units
  )
}
