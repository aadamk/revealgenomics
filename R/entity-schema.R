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

############################################################
# Helper functions for using YAML schema object

yaml_to_dim_str = function(dims){
  dim_str = paste(
    names(dims), "=",
    sapply(dims, function(x) {paste(x$start, ":",
                                    ifelse(x$end == Inf, "*", x$end), ",", x$chunk_interval, ",",
                                    x$overlap, sep = "")}),
    sep = "", collapse = ", ")
  dim_str
}

yaml_to_attr_string = function(attributes, compression_on = FALSE){
  if (!compression_on) { 
    paste(names(attributes), ":", attributes, collapse=" , ") 
  } else {
    paste(names(attributes), ":", attributes, "COMPRESSION 'zlib'", collapse=" , ") 
  }
}

get_mandatory_fields_for_register_entity = function(arrayname){
  attrs = names(.ghEnv$meta$L$array[[strip_namespace(arrayname)]]$attributes)
  attrs = attrs[!(attrs %in% c('created', 'updated'))]
  attrs
}

#' @export
mandatory_fields = function(){
  entitynames = get_entity_names()
  l1 = sapply(entitynames, function(entitynm){get_mandatory_fields_for_register_entity(entitynm)})
  names(l1) = entitynames
  l1
}

#' @export
unique_fields = function(){
  entitynames = get_entity_names()
  l1 = sapply(entitynames, function(entitynm){
    .ghEnv$meta$L$array[[entitynm]]$unique_fields
  })
  names(l1) = entitynames
  
  # Check that all but measurement data classes have unique fields
  entity_df = get_entity_info()
  lapply(names(l1), function(entity) {
    if (is.null(l1[[entity]])) {
      entity_class = entity_df[entity_df$entity == entity, ]$class
      if (entity_class != 'measurementdata') {
        stop("unique fields were not provided for entity: ", entity)
      }
    }
  })
 
  lapply(l1, function(elem) {
    if (is.null(elem)) {
      return("MESSAGE: unique fields not relevant as metadata array")
    } else {
      return(elem)
    }
  })
}

is_entity_secured = function(entitynm){
  entitynm = strip_namespace(entitynm)
  ifelse(length(.ghEnv$meta$L$array[[entitynm]]$namespace) > 1, TRUE, FALSE)
}

is_entity_versioned = function(entitynm){
  "dataset_version" %in% get_idname(entitynm)
}

get_idname = function(arrayname){
  local_arrnm = strip_namespace(arrayname)
  idname = .ghEnv$meta$L$array[[local_arrnm]]$dims
  if (class(idname) == "character") return(idname) else return(names(idname))
}

get_base_idname = function(arrayname){
  dims = get_idname(arrayname)
  dims[!(dims %in% "dataset_version")]
}

#' @export
get_entity_names = function(data_class = NULL){
  varnames = names(.ghEnv$meta)
  varnames = varnames[varnames != "L"]
  entities = sapply(varnames, function(nm){as.character(.ghEnv$meta[nm])})
  if (!is.null(data_class)) {
    matches = sapply(entities, function(entity) {
      ifelse(get_entity_data_class(entity) == data_class, TRUE, FALSE)
    })
    entities = entities[matches]
  }
  entities
}

get_entity_class = function(entity) { 
  stopifnot(entity %in% get_entity_names())
  .ghEnv$meta$L$array[[entity]]$data_class 
}

get_search_by_entity = function(entity) { 
  entity = strip_namespace(entity)
  stopifnot(entity %in% get_entity_names())
  .ghEnv$meta$L$array[[entity]]$search_by_entity 
}

get_int64fields = function(arrayname){
  local_arrnm = strip_namespace(arrayname)
  attr_types = unlist(.ghEnv$meta$L$array[[local_arrnm]]$attributes)
  int64_fields = names(attr_types[which(!(attr_types %in% 
                                            c('string', 'datetime', 'int32', 'double', 'bool')))])
  stopifnot(all(unique(attr_types[int64_fields]) %in% c("int64", "numeric")))
  int64_fields
}

get_entity_data_class = function(entity){
  .ghEnv$meta$L$array[[entity]]$data_class
}


get_delete_by_entity = function(entity) { 
  entity = strip_namespace(entity)
  stopifnot(entity %in% get_entity_names())
  .ghEnv$meta$L$array[[entity]]$delete_by_entity 
}


#' @export
get_entity_info = function(){
  df1 = data.frame(entity = get_entity_names())
  df1$class =            sapply(get_entity_names(), function(entity) get_entity_class(entity))
  df1$measurementdata_subclass = sapply(get_entity_names(), function(entity) .ghEnv$meta$L$array[[entity]]$measurementdata_subclass)
  df1$search_by_entity = sapply(get_entity_names(), function(entity) get_search_by_entity(entity))
  df1$delete_by_entity = sapply(get_entity_names(), function(entity) get_delete_by_entity(entity))
  df1 = data.frame(apply(df1, 2, function(col) {sapply(col, function(elem) {ifelse (is.null(elem), NA, elem)})}))
  rownames(df1) = NULL
  df1
}


