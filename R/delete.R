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
delete_entity = function(entity, ids, dataset_version = NULL){
  if (!(entity %in% get_entity_names())) stop("Entity '", entity, "' does not exist")
  if (is.null(ids)) return()
  if (dataset_versioning_exists(entity)) {
    if (is.null(dataset_version)) {
      stop("must supply dataset_version for versioned entities.
           Use dataset_version = 1 if only one version exists for relevant clinical study")
    } else {
      stopifnot(length(dataset_version) == 1)
    }
  }
  # So far, have coded the case where consecutive ids is provided
  if ( !( identical( as.integer(sort(ids)), 
                     (min(ids): max(ids)) ) ) ) stop("Delete only works on consecutive set of entity_id-s")
  
  # First check that entity exists at this id
  entities = get_entity(entity = entity, ids = ids)
  if (nrow(entities) == 0) {
    cat("No entries at ids: ", paste(ids, collapse = ", "), " for entity: ", entity, "\n", sep = "")
  } else { # if entities exist
    # Find the correct namespace
    if (lookup_exists(entity)) {
      namespaces = find_namespace(id = ids, entitynm = entity)
      nmsp = unique(namespaces)
    } else { nmsp = 'public' }
    
    if (length(nmsp) != 1) stop("Can run delete only at one namespace at a time")
    arr = paste(nmsp, entity, sep = ".")
    
    # Clear out the array
    if (dataset_versioning_exists(entity)) {
      qq = paste("filter(", arr, ", (",  get_base_idname(arr), " < ", min(ids), " OR ",
                 get_base_idname(arr), " > ", max(ids), ") OR (dataset_version != ", dataset_version, "))", sep = "")
    } else { # Entity does not have dataset_version-s
      qq = paste("filter(", arr, ", ",  get_base_idname(arr), " < ", min(ids), " OR ",
                 get_base_idname(arr), " > ", max(ids), ")", sep = "")
    }
    qq = paste("store(", qq, ", ", arr, ")", sep = "")
    cat("Deleting entries for ids ", paste(sort(ids), collapse = ", "), " from ", arr, " entity\n", sep = "")
    iquery(jdb$db, qq)
    
    # Clear out the info array
    infoArray = jdb$meta$L$array[[entity]]$infoArray
    if (infoArray){
      delete_info_fields(fullarrayname = arr, ids = ids, dataset_version = dataset_version)
    }
    
    # Clear out the lookup array if required
    if (lookup_exists(entity)){
      # Check if there are no entities at this ID
      qcount = paste("op_count(filter(", arr, ", ",
                     get_base_idname(entity), " >= ", min(ids), " AND ",
                     get_base_idname(entity), " <= ", max(ids), "))" )
      newcount = iquery(jdb$db, qcount, return = TRUE)$count
      if (newcount == 0){ # there are no entities at this level
        arrLookup = paste(entity, "_LOOKUP", sep = "")
        qq = paste("filter(", arrLookup, ", ",  get_base_idname(arr), " < ", min(ids), " OR ",
                   get_base_idname(arr), " > ", max(ids), ")", sep = "")
        qq = paste("store(", qq, ", ", arrLookup, ")", sep = "")
        cat("Deleting entries for ids ", paste(sort(ids), collapse = ", "), " from lookup array: ", arrLookup, "\n", sep = "")
        iquery(jdb$db, qq)
        updatedcache = entity_lookup(entityName = entity, updateCache = TRUE)
      }
    }
  } # end of check that some data existed in the first place
}

delete_info_fields = function(fullarrayname, ids, dataset_version){
  # So far, have coded the case where consecutive ids is provided
  if ( !( identical( as.integer(sort(ids)), 
                     (min(ids): max(ids)) ) ) ) stop("Delete only works on consecutive set of entity_id-s")
  
  arr = fullarrayname
  entity = strip_namespace(fullarrayname)
  arrInfo = paste(arr, "_INFO", sep = "")
  if (dataset_versioning_exists(entity)) {
    stopifnot(length(dataset_version) == 1)
    qq = paste("filter(", arrInfo, ", (",  get_base_idname(arr), " < ", min(ids), " OR ",
               get_base_idname(arr), " > ", max(ids), ") OR (dataset_version != ", dataset_version, "))", sep = "")
  } else { # Entity does not have dataset_version-s
    qq = paste("filter(", arrInfo, ", ",  get_base_idname(arr), " < ", min(ids), " OR ",
               get_base_idname(arr), " > ", max(ids), ")", sep = "")
  }
  qq = paste("store(", qq, ", ", arrInfo, ")", sep = "")
  cat("Deleting entries for ids ", paste(sort(ids), collapse = ", "), " from info array: ", arrInfo, "\n", sep = "")
  iquery(jdb$db, qq)
}
