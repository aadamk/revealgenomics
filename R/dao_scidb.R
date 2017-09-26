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
                 "apply(project(public.RNAQUANTIFICATIONSET, name, dataset_id),
                 entity, 'RNAQUANTIFICATION', id, rnaquantificationset_id)", return = TRUE)
    # var = iquery(con$db, 
    #              "apply(project(public.VARIANTSET, name, dataset_id),
    #              entity, 'VARIANT', id, variantset_id)",           return = TRUE)
    # fus = iquery(con$db, 
    #              "apply(project(public.FUSIONSET, name, dataset_id),
    #              entity, 'FUSION', id, fusionset_id)",            return = TRUE)
    # cnv = iquery(con$db, 
    #              "apply(project(public.COPYNUMBERSET, name, dataset_id),
    #              entity, 'COPYNUMBER', id, copynumberset_id)", return = TRUE)
    res = rqs
  } else if (length(nmsp_list) == 2) {
    rqs = iquery(con$db, 
                 "apply(merge(project(public.RNAQUANTIFICATIONSET, name, dataset_id), project(collaboration.RNAQUANTIFICATIONSET, name, dataset_id)),
                    entity, 'RNAQUANTIFICATION', id, rnaquantificationset_id)", return = TRUE)
    var = iquery(con$db, 
                 "apply(merge(project(public.VARIANTSET, name, dataset_id),           project(collaboration.VARIANTSET, name, dataset_id)),
                    entity, 'VARIANT', id, variantset_id)",           return = TRUE)
    fus = iquery(con$db, 
                 "apply(merge(project(public.FUSIONSET, name, dataset_id),            project(collaboration.FUSIONSET, name, dataset_id)),
                    entity, 'FUSION', id, fusionset_id)",            return = TRUE)
    cnv = iquery(con$db, 
                 "apply(merge(project(public.COPYNUMBERSET, name, dataset_id),        project(collaboration.COPYNUMBERSET, name, dataset_id)),
                    entity, 'COPYNUMBER', id, copynumberset_id)", return = TRUE)
    res = rbindlist(list(rqs, var, fus, cnv), fill = TRUE)
  } else { stop("More scidb4gh namespaces than expected") }
  
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

