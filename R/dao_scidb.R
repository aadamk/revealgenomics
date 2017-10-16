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
  nmsp_list = con$cache$nmsp_list
  
  if (length(nmsp_list) == 1) {
    rqs = iquery(con$db, 
                 "apply(project(public.RNAQUANTIFICATIONSET, name, dataset_id, experimentset_id),
                 entity, 'RNAQUANTIFICATION', id, rnaquantificationset_id)", return = TRUE)
    if (nrow(rqs) == 0) rqs = list()
    
    var = iquery(con$db,
                 "apply(project(public.VARIANTSET, name, dataset_id),
                 entity, 'VARIANT', id, variantset_id)",           return = TRUE)
    if (nrow(var) == 0) var = list()
    
    fus = iquery(con$db,
                 "apply(project(public.FUSIONSET, name, dataset_id),
                 entity, 'FUSION', id, fusionset_id)",            return = TRUE)
    if (nrow(fus) == 0) fus = list()

    cnv = iquery(con$db,
                 "apply(project(public.COPYNUMBERSET, name, dataset_id),
                 entity, 'COPYNUMBER_MAT', id, copynumberset_id)", return = TRUE)
    if (nrow(cnv) == 0) cnv = list()
    # res = rqs
  } else if (length(nmsp_list) == 2) {
    rqs = iquery(con$db, 
                 "apply(project(merge(public.RNAQUANTIFICATIONSET, collaboration.RNAQUANTIFICATIONSET), name, dataset_id, experimentset_id),
                    entity, 'RNAQUANTIFICATION', id, rnaquantificationset_id)", return = TRUE)
    var = iquery(con$db, 
                 "apply(project(merge(public.VARIANTSET, collaboration.VARIANTSET), name, dataset_id, experimentset_id),
                    entity, 'VARIANT', id, variantset_id)",           return = TRUE)
    fus = iquery(con$db, 
                 "apply(merge(project(public.FUSIONSET, name, dataset_id),           project(collaboration.FUSIONSET, name, dataset_id)),
                    entity, 'FUSION', id, fusionset_id)",            return = TRUE)
    cnv = iquery(con$db, 
                 "apply(merge(project(public.COPYNUMBERSET, name, dataset_id),           project(collaboration.COPYNUMBERSET, name, dataset_id)),
                    entity, 'COPYNUMBER_MAT', id, copynumberset_id)", return = TRUE)
  } else { stop("More scidb4gh namespaces than expected") }
  
  res = rbindlist(list(rqs, var, fus, cnv), fill = TRUE)
  res = data.frame(res)
  allcols = colnames(res)
  cols1 = c('dataset_id', 'dataset_version', 'entity', 'id', 'name')
  othercols = allcols[!(allcols %in% cols1)]
  
  res[, c(cols1, othercols)]
}

#' Faster version of `get_project`
#' 
#' Instead of running the query per namespace, merge the namespaces into one array at query-time
#' The code is based off the code at `join_info_ontology_and_unpivot`
dao_get_entity = function(entity, con){
  
  arrayname = entity
  
  idname = scidb4gh:::get_idname(arrayname)
  
  nmsp_list = con$cache$nmsp_list
  
  if (length(nmsp_list) == 1) {
    inner_query = paste0(arrayname, ", ", arrayname, "_INFO")
  } else if (length(nmsp_list) == 2) {
    inner_query = paste0("merge(public.", arrayname, ", collaboration.", arrayname, "), 
                          merge(public.", arrayname, "_INFO, collaboration.", arrayname, "_INFO)")
  } else { stop("More scidb4gh namespaces than expected") }
  
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
                  names(.ghEnv$meta$L$array[[scidb4gh:::strip_namespace(arrayname)]]$attributes))]
    } else {
      x3 = x2[, names(.ghEnv$meta$L$array[[scidb4gh:::strip_namespace(arrayname)]]$attributes)]
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
dao_search_rnaquantification = function(rnaquantificationset, feature, biosample_ref, 
                                        dataset_lookup_ref,
                                        formDataModel = FALSE, 
                                        con) {
  rqs_id = unique(rnaquantificationset$rnaquantificationset_id)
  stopifnot(length(rqs_id)==1)
  
  dataset_version = unique(rnaquantificationset$dataset_version)
  stopifnot(length(dataset_version) == 1)
  
  dataset_id = unique(rnaquantificationset$dataset_id)
  stopifnot(length(dataset_id) == 1)
  
  namespace = dataset_lookup_ref[dataset_lookup_ref$dataset_id == dataset_id, ]$namespace
  stopifnot(namespace %in% con$cache$nmsp_list)
  
  
  ftr_id = unique(feature$feature_id)
  
  arr0 = paste0(namespace, ".", .ghEnv$meta$arrRnaquantification)
  
  qq = paste0("filter(", arr0, ", rnaquantificationset_id = ", rqs_id, ")")
  
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
                          flag, int32(1)), <flag:int32>[feature_id=0:*:0:",
                                .ghEnv$meta$L$array$FEATURE$dims$feature_id$chunk_interval,
                                                                      "])")
    qq2c = paste0("cross_join(", qq, " as X,",
                               qq2b, " as Y, X.feature_id, Y.feature_id)")
    qq2 = paste0("project(", qq2c, ",", 
                 paste0(names(.ghEnv$meta$L$array$RNAQUANTIFICATION$attributes), collapse = ","),
                        ")")

  }
  
  res = iquery(con$db, query = qq2, return = TRUE)

  # Retrieved biosamples
  biosample_id = unique(res$biosample_id)
  biosample = biosample_ref[biosample_ref$biosample_id %in% biosample_id, ]
  
  # Retrieved features
  feature_id = unique(res$feature_id)
  feature_sel = feature[feature$feature_id %in% feature_id, ]
  
  expressionSet = formulate_list_expression_set(expr_df = res, dataset_version, rnaquantificationset, biosample, feature)
  
  if (!formDataModel) return(expressionSet)
  
  # Form custom data model
  es = expressionSetObject$new(NULL, NULL, NULL)
  es$setExpressionMatrix(expressionMatrix = exprs(expressionSet))
  es$setFeatureData(featureData = expressionSet@featureData@data)
  es$setPhenotypeData(phenotypeData = expressionSet@phenoData@data)
  
  return(es)
}
