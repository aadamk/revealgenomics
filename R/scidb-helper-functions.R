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
# Helper functions for working with scidb / scidbR package
############################################################

#' wrapper for as.scidb when supplying int64 types
#'  
#'  When type argument of as.scidb has int64-s, there are issues uploading 
#'  with both `input` and `aio_input` (see https://github.com/Paradigm4/SciDBR/issues/189)
#'  Avoid this by converting int64 fields in R before upload. 
#'    
#' @param db database connection object
#' @param df1 dataframe to be uploaded 
#' @param int64_cols columns which need to be converted to int64-s
#' 
#' @export
as.scidb_int64_cols = function(db, df1, int64_cols, ...) {
  stopifnot('data.frame' %in% class(df1))
  # if (is.null(int64_cols)) {
  #   int64_cols = names(which(sapply(df1, class) == 'numeric'))
  # }
  
  # check that all columns to be converted to int64-s are present
  stopifnot(all(int64_cols %in% colnames(df1)))
  
  # check that all columns to be converted to int64-s are integer or numeric
  stopifnot(all(sapply(df1, class)[int64_cols] %in% c('integer', 'numeric')))
  
  # Convert the user specified columns
  for (colnm in int64_cols) {
    df1[, colnm] = as.integer(df1[, colnm])
  }
  types_vec = sapply(df1, class)
  
  repl_vec = c('character' = 'string',
               'integer'   = 'int64',
               'numeric'   = 'double')
  
  # Check that the types in dataframe to be uploaded are covered by converter above
  stopifnot(all(types_vec %in% names(repl_vec)))
  
  for (idx in 1:length(repl_vec)) {
    # print(names(repl_vec)[idx])
    # print(repl_vec[idx])
    types_vec = gsub(names(repl_vec)[idx], repl_vec[idx], types_vec)
  }
  as.scidb(db, df1, type = types_vec, ...)
}

############################################################
# Helper functions for SciDB array operations
############################################################
#' @export
scidb_exists_array = function(arrayName, con = NULL) {
  con = use_ghEnv_if_null(con)
  
  !is.null(tryCatch({iquery(con$db, paste("show(", arrayName, ")", sep=""), return=TRUE, binary = FALSE)}, error = function(e) {NULL}))
}

convert_attr_double_to_int64 = function(arr, attrname, con = NULL){
  con = use_ghEnv_if_null(con)
  
  attrnames = scidb::schema(arr, "attributes")$name
  randString = "for_int64_conversion"
  arr = scidb_attribute_rename(arr, old = attrname, new = randString, con = con)
  # arr = con$db$apply(srcArray = arr, newAttr = R(attrname), expression = int64(R(randString)))
  qq = paste0("apply(", arr@name, ", ", attrname, ", int64(", randString, "))")
  # arr = con$db$project(arr, R(paste(attrnames, collapse = ", ")))
  qq = paste0("project(", qq, ", ", paste(attrnames, collapse = ","), ")")
  arr = scidb(con$db, qq)
}


#' @export
scidb_attribute_rename = function(arr, old, new, con = NULL){
  con = use_ghEnv_if_null(con)
  
  attrs = scidb::schema(arr, what = "attributes")
  attrnames = attrs$name
  stopifnot(old %in% attrnames)
  
  attrs[match(old, attrnames), "name"] = new
  # dims = scidb::schema(arr, "dimensions")
  
  attr_schema = paste(
    paste(
      paste(attrs$name, attrs$type, sep = ": "),
      ifelse(attrs$nullable, "", "NOT NULL"), sep = " "),
    collapse = ", ")
  dim_schema = gsub("<.*> *", "", scidb::schema(arr)) # TODO : build up from scratch
  newSchema = paste("<", attr_schema, ">", dim_schema)
  
  # arr = con$db$cast(srcArray = arr, schemaArray = R(newSchema))
  # arr
  scidb(con$db, paste0("cast(", arr@name, ", ",  newSchema, ")"))
}

#' @export
scidb_array_count = function(array, con = NULL){
  con = use_ghEnv_if_null(con)
  
  qq = paste("op_count(", array@name, ")", sep = "")
  iquery(con$db, qq, schema="<count:uint64> [i=0:0]", return = T)$count
}

#' @export
scidb_array_head= function(array, n = 5, con = NULL){
  con = use_ghEnv_if_null(con)
  # as.R(con$db$limit(array, R(n)))
  iquery(con$db, paste0("limit(", array@name, ", ", n, ")"), return = TRUE)
}

#' remove old array versions associated with an entity
remove_old_versions_for_entity = function(entitynm, con = NULL){
  stopifnot(entitynm %in% get_entity_names())
  
  remove_versions(arayname = full_arrayname(entitynm), con = con)
  info_array_exists = .ghEnv$meta$L$array[[entitynm]]$infoArray
  if (info_array_exists) {
    info_array_name = paste0(full_arrayname(entitynm), "_INFO")
    remove_versions(arayname = info_array_name, con = con)
  }
}

#' remove old versions associated with an array
#' 
#' Retain the last N_THRESH versions
remove_versions = function(arayname, con = NULL)
{
  N_THRESH = 5
  
  con = use_ghEnv_if_null(con)
  mv = iquery(con$db, sprintf("versions(%s)", arayname), 
              return=TRUE)
  if(nrow(mv) > N_THRESH )
  {
    mv = max(mv$version_id)
    cat("Removing versions of array:", arayname, "older than version", (mv-N_THRESH), "\n")
    iquery(con$db, sprintf("remove_versions(%s, %i)", arayname, (mv-N_THRESH)))
  }
}

drop_equi_join_dims = function(df1) {
  df1[, c('instance_id', 'value_no')] = c(NULL, NULL)
  df1
}
