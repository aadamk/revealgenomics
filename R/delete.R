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

formulate_base_selection_query = function(fullarrayname, id){
  THRESH_K = 100
  sorted=sort(id)
  breaks=c(0, which(diff(sorted)!=1), length(sorted))
  idname = get_base_idname(fullarrayname)
  if (length(breaks) <= (THRESH_K + 2)) # few sets of contiguous tickers; use `cross_between`
  {
    expr_query1 = paste( sapply(seq(length(breaks)-1), function(i) {
      left = sorted[breaks[i]+1]
      right = sorted[breaks[i+1]]
      if (left == right) {
        paste("(", idname, "=", right, ")")
      } else {
        sprintf("(%s >= %d AND %s <= %d)", idname, left,
                idname, right)
      }
    }), collapse=" OR ")
  } else {
    stop("Try fewer ids at a time")
  }
  return(expr_query1)
}

formulate_versioned_selection_query = function(entity, base_selection_query, dataset_version) {
  # Clear out the array
  if (is_entity_versioned(entity)) {
    expr_query = paste("(dataset_version = ", dataset_version, ") AND (", base_selection_query, ")", sep = "")
  } else {
    expr_query = base_selection_query
  }
  return(expr_query)
}

#' @export
delete_entity = function(entity, id, dataset_version = NULL, delete_by_entity = NULL){
  if (is.null(delete_by_entity)) {
    delete_by_entity = get_delete_by_entity(entity)
  } else {
    if (delete_by_entity != get_delete_by_entity(entity)) {
      stop("Deletion of ", entity, " can be done only via ", get_delete_by_entity(entity), " entity")
    }
  }
  
  if (!(entity %in% get_entity_names())) stop("Entity '", entity, "' does not exist")
  if (is.null(id)) return()
  if (is_entity_versioned(entity)) {
    if (is.null(dataset_version)) {
      stop("must supply dataset_version for versioned entities.
           Use dataset_version = 1 if only one version exists for relevant clinical study")
    } else {
      if (length(dataset_version) != 1) {stop("Can supply only one dataset_version at a time")}
    }
  }

  if (get_entity_class(entity = entity) == 'measurementdata') {
    if (length(id) != 1) stop("Delete of entity ", entity, " can be done only 1 ", 
                              get_base_idname(delete_by_entity), " at a time")
  } 
  # First check that entity exists at this id
  if (is_entity_versioned(entitynm = entity)){
    status = try(check_entity_exists_at_id(entity = delete_by_entity, id = id, 
                                           dataset_version = dataset_version, all_versions = F))
  } else {
    status = try(check_entity_exists_at_id(entity = delete_by_entity, id = id))
  }

  if (class(status) == 'try-error') {
    stop()
  }
  # Now that search_by_entities are proven to exist at specified id-s, delete them
  
  # Find the correct namespace
  if (is_entity_secured(entity)) {
    namespaces = find_namespace(id = id, entitynm = delete_by_entity)
    nmsp = unique(namespaces)
  } else { nmsp = 'public' }
  if (length(nmsp) != 1) stop("Can run delete only at one namespace at a time")
  arr = paste(nmsp, entity, sep = ".")
  
  # Delete the mandatory fields array
  if (get_entity_class(entity = entity)  == 'measurementdata') { # Special handling for measurement data class
                                                                          # -- they do not have get_<ENTITY>() calls
                                                                          # -- only allow deleting by one preferred id at a time
    cat("Deleting entries for ", get_base_idname(delete_by_entity), " = ",  
        id, " from ", arr, " entity\n", sep = "")
    qq = paste("delete(", arr, ", ", get_base_idname(delete_by_entity), " = ",  
               id, " AND dataset_version = ", dataset_version, ")", sep = "")
    print(qq)
    iquery(.ghEnv$db, qq)
  } else {
    base_selection_query  = formulate_base_selection_query(fullarrayname = arr, id = id)
    versioned_selection_query = formulate_versioned_selection_query(entity = entity, 
                                                                    base_selection_query = base_selection_query, 
                                                                    dataset_version = dataset_version)
    cat("Deleting entries for ids ", pretty_print(sort(id)), " from ", arr, " entity\n", sep = "")
    qq = paste("delete(", arr, ", ", versioned_selection_query, ")", sep = "")
    print(qq)
    iquery(.ghEnv$db, qq)
  }
  
  # Clear out the info array
  infoArray = .ghEnv$meta$L$array[[entity]]$infoArray
  if (infoArray){
    delete_info_fields(fullarrayname = arr, id = id, dataset_version = dataset_version)
  }
  
  # Clear out the lookup array if required
  if ( get_entity_class(entity = entity) != 'measurementdata' ) { # No lookup handling for measurement data class
    if (is_entity_secured(entity)){
      # Check if there are no remaining entities at this ID at any version
      qcount = paste("op_count(filter(", arr, ", ",
                     base_selection_query, "))" )
      newcount = iquery(.ghEnv$db, qcount, return = TRUE)$count
      if (newcount == 0){ # there are no entities at this level
        arrLookup = paste(entity, "_LOOKUP", sep = "")
        qq = paste("delete(", arrLookup, ", ", base_selection_query, ")", sep = "")
        cat("Deleting entries for ids ", paste(sort(id), collapse = ", "), " from lookup array: ", arrLookup, "\n", sep = "")
        print(qq)
        iquery(.ghEnv$db, qq)
        updatedcache = entity_lookup(entityName = entity, updateCache = TRUE)
      }
    } # end of: if (is_entity_secured(entity))
  } # end of: if ( get_entity_class(entity = entity) != 'measurementdata' )
}

delete_info_fields = function(fullarrayname, id, dataset_version, delete_by_entity = NULL){
  entity = strip_namespace(fullarrayname)
  if (is.null(delete_by_entity)) {
    delete_by_entity = get_delete_by_entity(entity)
  } else {
    if (delete_by_entity != get_delete_by_entity(entity)) {
      stop("Deletion of ", entity, " can be done only via ", get_delete_by_entity(entity), " entity")
    }
  }
  arr = fullarrayname
  arrInfo = paste(arr, "_INFO", sep = "")
  entity = strip_namespace(fullarrayname)

  # Delete the mandatory fields array
  if (get_entity_class(entity = entity) == 'measurementdata') { # Special handling for measurement data class
    # -- they do not have get_<ENTITY>() calls
    # -- only allow deleting by one preferred id at a time
    cat("Deleting entries for ", get_base_idname(delete_by_entity), " = ",  
        id, " from ", arr, "_INFO\n", sep = "")
    qq = paste("delete(", arrInfo, ", ", get_base_idname(delete_by_entity), " = ",  
               id, " AND dataset_version = ", dataset_version, ")", sep = "")
    print(qq)
    iquery(.ghEnv$db, qq)
  } else {
    base_selection_query = formulate_base_selection_query(fullarrayname = fullarrayname, 
                                                          id = id)  
    versioned_selection_query = formulate_versioned_selection_query(entity = entity, 
                                                                    base_selection_query = base_selection_query, 
                                                                    dataset_version = dataset_version)
    
    qq = paste("delete(", arrInfo, ", ", versioned_selection_query, ")", sep = "")
    cat("Deleting entries for ids ", paste(sort(id), collapse = ", "), " from info array: ", arrInfo, "\n", sep = "")
    print(qq)
    iquery(.ghEnv$db, qq)
  }
}
