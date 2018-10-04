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

yaml_to_dim_str = function(dims, for_auto_chunking=FALSE){
  if (!for_auto_chunking) {
    paste(
      names(dims), "=",
      sapply(dims, function(x) {paste(x$start, ":",
                                      ifelse(x$end == Inf, "*", x$end), ",", x$chunk_interval, ",",
                                      x$overlap, sep = "")}),
      sep = "", collapse = ", ")
  } else {
    paste0(names(dims), collapse = ",")
  }
}

yaml_to_attr_string = function(attributes, compression_on = FALSE){
  if (!compression_on) { 
    paste(names(attributes), ":", attributes, collapse=" , ") 
  } else {
    paste(names(attributes), ":", attributes, "COMPRESSION 'zlib'", collapse=" , ") 
  }
}

#' mandatory fields (internal function)
#' 
#' return mandatory fields while registering an entity
#' 
#' 'DATASET': attributes
#' other secure metadata entities (e.g. 'INDIVIDUAL', 'BIOSAMPLE', 'BIOSAMPLE'): 'dataset_id' and attributes
#' public metadata entities (e.g. 'ONTOLOGY'): attributes
#' feature entities: attributes
get_mandatory_fields_for_register_entity = function(arrayname){
  entitynm = strip_namespace(arrayname)
  attrs = names(.ghEnv$meta$L$array[[entitynm]]$attributes)
  attrs = attrs[!(attrs %in% c('created', 'updated'))]
  attrs
  
  zz = get_entity_info()
  entity_class = zz[zz$entity == entitynm, ]$class
  
  if (entity_class == 'metadata') {
    # Metadata entities that do not have to supply `dataset_id` at registration time
    # PROJECT, DATASET, ONTOLOGY, VARIANT_KEY
    if (entitynm %in% c(.ghEnv$meta$arrProject,
                          .ghEnv$meta$arrDataset,
                          .ghEnv$meta$arrOntology,
                          .ghEnv$meta$arrMetadataAttrKey, 
                          .ghEnv$meta$arrVariantKey))) {
      mandatory_fields = c('dataset_id', attrs)
    } else { # PROJECT, DATASET, ONTOLOGY, VARIANT_KEY
      mandatory_fields = attrs
    } else { 
      mandatory_fields = c('dataset_id', attrs)
    }
  } else if (entity_class == 'featuredata') {
    if (entitynm %in% c(.ghEnv$meta$arrFeature, .ghEnv$meta$arrFeatureSynonym)) {
      # arrays in which featureset_id is a dimension but also a mandatory field
      mandatory_fields = c('featureset_id', attrs) 
    } else {
      mandatory_fields = attrs
    }
  } else if (entity_class %in% c('measurementdata', 'measurementdata_cache')) {
    dims = get_idname(entitynm)
    dims = dims[!(dims %in% c('dataset_version'))]
    mandatory_fields = c(dims, attrs)
  } else {
    stop("Need to cover case for class: ", entity_class)
  }
  mandatory_fields
}

#' mandatory fields 
#' 
#' return mandatory fields while registering entities
#' 
#' 'DATASET': attributes
#' other secure metadata entities (e.g. 'INDIVIDUAL', 'BIOSAMPLE', 'BIOSAMPLE'): 'dataset_id' and attributes
#' public metadata entities (e.g. 'ONTOLOGY'): attributes
#' feature entities: attributes
#' 
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
      if ( !( entity_class %in% c('measurementdata', 'measurementdata_cache') ) ) {
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
  entitynm = strip_namespace(entitynm) # extra QC
  nmsp = find_namespace(entitynm)
  if (is.null(nmsp)) stop("unexpected namespace output")
  length(grep("public", nmsp)) == 0
}

is_entity_versioned = function(entitynm){
  "dataset_version" %in% get_idname(entitynm)
}

#' is entity cached
#' 
#' check if entity is cached
#' 
#' if no value is provided in schema file, then the entity is potentially not cached
is_entity_cached = function(entitynm) {
  val  = .ghEnv$meta$L$array[[entitynm]]$cached # read from SCHEMA file
  # if no value for cached, then entity is potentially not cached
  ifelse(is.null(val), FALSE, val) 
}

#' does infoArray exist for given entity
#' 
#' flexible fields
get_entity_infoArrayExists = function(entitynm) {
  status = .ghEnv$meta$L$array[[entitynm]]$infoArray
  if (is.null(status)) stop("infoArray status must be present for all entities")
  status
}

get_idname = function(arrayname){
  local_arrnm = strip_namespace(arrayname)
  idname = .ghEnv$meta$L$array[[local_arrnm]]$dims
  if (class(idname) == "character") return(idname) else return(names(idname))
}

get_base_idname = function(arrayname){
  entitynm = strip_namespace(arrayname)
  dims = get_idname(entitynm)
  
  if (entitynm %in% c(.ghEnv$meta$arrFeature, .ghEnv$meta$arrFeatureSynonym)) {
    # featuredata arrays that have featureset_id and/or gene_symbol_id 
    # as dimensions for faster slicing
    dims[!(dims %in% c("featureset_id", "gene_symbol_id"))] 
  } else if (entitynm != .ghEnv$meta$arrDataset) {
    dims[!(dims %in% c("dataset_id", "dataset_version"))]
  } else {
    dims[!(dims %in% "dataset_version")]
  }
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

get_entity_id = function(entity){
  .ghEnv$meta$L$array[[entity]]$entity_id
}


get_delete_by_entity = function(entity) { 
  entity = strip_namespace(entity)
  stopifnot(entity %in% get_entity_names())
  .ghEnv$meta$L$array[[entity]]$delete_by_entity 
}


#' @export
get_entity_info = function(){
  df1 = data.frame(entity = get_entity_names(), stringsAsFactors = FALSE)
  df1$entity_id =        sapply(get_entity_names(), function(entity) get_entity_id(entity))
  df1$class =            sapply(get_entity_names(), function(entity) get_entity_class(entity))
  df1$measurementdata_subclass = sapply(get_entity_names(), function(entity) .ghEnv$meta$L$array[[entity]]$measurementdata_subclass)
  df1$search_by_entity = sapply(get_entity_names(), function(entity) get_search_by_entity(entity))
  df1$delete_by_entity = sapply(get_entity_names(), function(entity) get_delete_by_entity(entity))
  df1 = data.frame(apply(df1, 2, function(col) {sapply(col, function(elem) {ifelse (is.null(elem), NA, elem)})}), 
                   stringsAsFactors = FALSE)
  df1 = df1[order(df1$entity_id), ]
  rownames(df1) = NULL
  
  df1
}


