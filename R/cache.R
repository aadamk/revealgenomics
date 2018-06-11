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
    
    zz = autoconvert_char(df1 = zz)
    .ghEnv$cache[[entitynm]] = zz
  } else { # INFO array does not exist
    .ghEnv$cache[[entitynm]] = iquery(con$db, arraynm, return = TRUE)
  }
}

get_entity_from_cache = function(entitynm, updateCache, con = NULL) {
  con = use_ghEnv_if_null(con)
  
  if (updateCache | is.null(.ghEnv$cache[[entitynm]])){
    update_entity_cache(entitynm = entitynm, con = con)
  }
  if (nrow(.ghEnv$cache[[entitynm]]) == 0) {
    update_entity_cache(entitynm = entitynm, con = con)
  } 
  
  return(.ghEnv$cache[[entitynm]])
}
# BASE FUNCTIONS: End
###################################################################################
# VARIANT_KEY 
update_variant_key_cache = function(con = NULL){
  update_entity_cache(entitynm = .ghEnv$meta$arrVariantKey, 
                      con = con)
}

get_variant_key_from_cache = function(updateCache, con = NULL){
  get_entity_from_cache(entitynm = .ghEnv$meta$arrVariantKey, updateCache = updateCache, 
                        con = con)
}

# DEFINITION
update_definition_cache = function(con = NULL){
  update_entity_cache(entitynm = .ghEnv$meta$arrDefinition, 
                      con = con)
}

get_definition_from_cache = function(updateCache, con = NULL){
  get_entity_from_cache(entitynm = .ghEnv$meta$arrDefinition, updateCache = updateCache, 
                        con = con)
}

# ONTOLOGY
update_ontology_cache = function(con = NULL){
  update_entity_cache(entitynm = .ghEnv$meta$arrOntology, 
                      con = con)
}

get_ontology_from_cache = function(updateCache, con = NULL){
  get_entity_from_cache(entitynm = .ghEnv$meta$arrOntology, updateCache = updateCache, 
                        con = con)
}

