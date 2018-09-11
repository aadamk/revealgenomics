#
# BEGIN_COPYRIGHT
#
# PARADIGM4 INC.
# This file is part of the Paradigm4 Enterprise SciDB distribution kit
# and may only be used with a valid Paradigm4 contract and in accord
# with the terms and conditions specified by that contract.
#
# Copyright (C) 2011 - 2018 Paradigm4 Inc.
# All Rights Reserved.
#
# END_COPYRIGHT
#

###################################################################################
# BASE FUNCTIONS: Begin
update_entity_cache = function(entitynm, con = NULL) {
  con = use_ghEnv_if_null(con)
  
  arraynm =  full_arrayname(entitynm)
  if (get_entity_infoArrayExists(entitynm)) { # INFO array exists
    idname = get_base_idname(entitynm)
    qq = paste0("equi_join(", 
                arraynm, ", ", 
                arraynm, "_INFO, ",
                "'left_names=", idname, "', ", 
                "'right_names=", idname, "', ",
                "'left_outer=1', 'keep_dimensions=1')"
    )
    zz = iquery(con$db, qq, return = TRUE)
    zz[, 'instance_id'] = NULL
    zz[, 'value_no'] = NULL
    zz = unpivot(df1 = zz, arrayname = entitynm)
    if (nrow(zz) > 1) zz = zz[order(zz[, idname]), ]
    
    if (entitynm != .ghEnv$meta$arrOntology) zz = autoconvert_char(df1 = zz)
    .ghEnv$cache[[entitynm]] = zz
  } else { # INFO array does not exist
    .ghEnv$cache[[entitynm]] = iquery(con$db, arraynm, return = TRUE)
  }
}

#' get_ENTITY for cached entities
#' 
#' Currently works for ONTOLOGY, DEFINITION, VARIANT_KEY, FEATURE_SYNONYM,
#' GENE_SYMBOL
#' 
#' @param cache_df typically result of a `get_entity_from_cache()` call
#' @param entitynm entity name
#' @param id       the id-s to be selected (can be `NULL`)
get_entity_from_cache = function(entitynm, id, updateCache, con = NULL) {
  con = use_ghEnv_if_null(con)
  
  if (updateCache | is.null(.ghEnv$cache[[entitynm]])){
    update_entity_cache(entitynm = entitynm, con = con)
  }
  if (nrow(.ghEnv$cache[[entitynm]]) == 0) {
    update_entity_cache(entitynm = entitynm, con = con)
  } 
  
  cache_df = .ghEnv$cache[[entitynm]]
  base_ids = get_base_idname(entitynm)
  if (!is.null(id)){
    matches = match(id, cache_df[, base_ids])
    matches = matches[which(!is.na(matches))]
    res_df = cache_df[matches, ]
  } else {
    res_df = cache_df
  }
  if (length(base_ids) > 1) {
    stop("This caching function currently supports entities with
         one base idname only. Current entity: ", entitynm, 
         "has multiple base ids: ", pretty_print(base_ids))
  }
  if (all(base_ids %in% 
          colnames(res_df))) {
    row.names(res_df) = res_df[, base_ids]
  }
  res_df
}

# BASE FUNCTIONS: End
###################################################################################

##### VARIANT_KEY #####
update_variant_key_cache = function(con = NULL){
  update_entity_cache(entitynm = .ghEnv$meta$arrVariantKey, 
                      con = con)
}

get_variant_key_from_cache = function(variant_key_id, updateCache, con = NULL){
  get_entity_from_cache(entitynm = .ghEnv$meta$arrVariantKey, 
                        id = variant_key_id, 
                        updateCache = updateCache, 
                        con = con)
}

##### DEFINITION ##### 
update_definition_cache = function(con = NULL){
  update_entity_cache(entitynm = .ghEnv$meta$arrDefinition, 
                      con = con)
}

get_definition_from_cache = function(definition_id, updateCache, con = NULL){
  get_entity_from_cache(entitynm = .ghEnv$meta$arrDefinition, 
                        id = definition_id,
                        updateCache = updateCache, 
                        con = con)
}

##### ONTOLOGY ##### 
update_ontology_cache = function(con = NULL){
  update_entity_cache(entitynm = .ghEnv$meta$arrOntology, 
                      con = con)
}

get_ontology_from_cache = function(ontology_id, updateCache, con = NULL){
  get_entity_from_cache(entitynm = .ghEnv$meta$arrOntology, 
                        id = ontology_id, 
                        updateCache = updateCache, 
                        con = con)
}

##### FEATURE-SYNONYM ##### 
update_feature_synonym_cache = function(con = NULL){
  update_entity_cache(entitynm = .ghEnv$meta$arrFeatureSynonym, 
                      con = con)
}

get_feature_synonym_from_cache = function(feature_synonym_id, updateCache, con = NULL){
  get_entity_from_cache(entitynm = .ghEnv$meta$arrFeatureSynonym, 
                        id = feature_synonym_id, 
                        updateCache = updateCache, 
                        con = con)
}

##### GENE-SYMBOLS ##### 
update_gene_symbol_cache = function(con = NULL){
  update_entity_cache(entitynm = .ghEnv$meta$arrGeneSymbol, 
                      con = con)
}

get_gene_symbol_from_cache = function(gene_symbol_id, updateCache, con = NULL){
  get_entity_from_cache(entitynm = .ghEnv$meta$arrGeneSymbol, 
                        id = gene_symbol_id, 
                        updateCache = updateCache, 
                        con = con)
}