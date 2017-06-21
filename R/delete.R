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

formulate_base_selection_query = function(fullarrayname, ids){
  THRESH_K = 100
  sorted=sort(ids)
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
  if (dataset_versioning_exists(entity)) {
    expr_query = paste("(dataset_version = ", dataset_version, ") AND (", base_selection_query, ")", sep = "")
  } else {
    expr_query = base_selection_query
  }
  return(expr_query)
}

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

  # First check that entity exists at this id
  if (dataset_versioning_exists(entitynm = entity)){
    # entities = get_entity(entity = entity, ids = ids, dataset_version = dataset_version)
    status = try(check_entity_exists_at_id(entity = entity, id = ids, dataset_version = dataset_version, all_versions = F))
  } else {
#     entities = get_entity(entity = entity, ids = ids)
    status = try(check_entity_exists_at_id(entity = entity, id = ids))
  }
  if (class(status) == 'try-error') {
    # cat("No entries at ids: ", paste(ids, collapse = ", "), " for entity: ", entity, "\n", sep = "")
    # cat(status[1])
  } else if (status) { # if entities exist
    # Find the correct namespace
    if (lookup_exists(entity)) {
      namespaces = find_namespace(id = ids, entitynm = entity)
      nmsp = unique(namespaces)
    } else { nmsp = 'public' }
    
    if (length(nmsp) != 1) stop("Can run delete only at one namespace at a time")
    arr = paste(nmsp, entity, sep = ".")
    
    base_selection_query  = formulate_base_selection_query(fullarrayname = arr, ids = ids)
    versioned_selection_query = formulate_versioned_selection_query(entity = entity, 
                                                                    base_selection_query = base_selection_query, 
                                                                    dataset_version = dataset_version)
    ### asdf
    cat("Deleting entries for ids ", paste(sort(ids), collapse = ", "), " from ", arr, " entity\n", sep = "")
    qq = paste("delete(", arr, ", ", versioned_selection_query, ")", sep = "")
    print(qq)
    iquery(jdb$db, qq)
    
    # Clear out the info array
    infoArray = jdb$meta$L$array[[entity]]$infoArray
    if (infoArray){
      delete_info_fields(fullarrayname = arr, ids = ids, dataset_version = dataset_version)
    }
    
    # Clear out the lookup array if required
    if (lookup_exists(entity)){
      # Check if there are no remaining entities at this ID at any version
      qcount = paste("op_count(filter(", arr, ", ",
                     base_selection_query, "))" )
      newcount = iquery(jdb$db, qcount, return = TRUE)$count
      if (newcount == 0){ # there are no entities at this level
        arrLookup = paste(entity, "_LOOKUP", sep = "")
        qq = paste("delete(", arrLookup, ", ", base_selection_query, ")", sep = "")
        cat("Deleting entries for ids ", paste(sort(ids), collapse = ", "), " from lookup array: ", arrLookup, "\n", sep = "")
        print(qq)
        iquery(jdb$db, qq)
        updatedcache = entity_lookup(entityName = entity, updateCache = TRUE)
      }
    }
  } else { # end of check that some data existed in the first place
    stop("Unexpected class for try() expression")
  }
}

delete_info_fields = function(fullarrayname, ids, dataset_version){
  arr = fullarrayname
  arrInfo = paste(arr, "_INFO", sep = "")
  entity = strip_namespace(fullarrayname)

  base_selection_query = formulate_base_selection_query(fullarrayname = fullarrayname, 
                                                        ids = ids)  
  versioned_selection_query = formulate_versioned_selection_query(entity = entity, 
                                                                  base_selection_query = base_selection_query, 
                                                                  dataset_version = dataset_version)

  qq = paste("delete(", arrInfo, ", ", versioned_selection_query, ")", sep = "")
  cat("Deleting entries for ids ", paste(sort(ids), collapse = ", "), " from info array: ", arrInfo, "\n", sep = "")
  print(qq)
  iquery(jdb$db, qq)
}
