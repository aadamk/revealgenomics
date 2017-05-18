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

#' Function to (potentially delete and) initialize arrays in GA4GH respository
#'
#' The function requires that you are connected to SciDB.
#' @return NULL
#' @examples
#' \dontrun{
#' init_db('FUSION')
#' init_db(c('FUSIONSET', 'FUSION'))
#' init_db(arrays = get_entity_names()) # Warning! This reinitializes all the arrays
#' }
#' @export
init_db = function(arrays_to_init){
  # if (is.null(arrays_to_init)) arrays_to_init = get_entity_names()
  db = jdb$db
  L = jdb$meta$L
  
  arrays_to_init = arrays_to_init[arrays_to_init %in% names(L$array)]
  
  if (length(arrays_to_init) == 0) {cat("ERROR: Check array names\n"); return(FALSE)}
    
  cat("CAUTION: The following arrays will be deleted and reinitialized\n", 
      paste(arrays_to_init, collapse = ", "), "\n Proceed?")  
  response <- readline("(Y)es/(N)o: ")
  if ( (tolower(response) == 'y' | tolower(response) == 'yes') & !is.na(response)) {
    cat("Proceeding with initialization of DB\n")
  } else{
    cat("Canceled initialization of DB\n")
    return(FALSE)
  }
  
  arrays = L$array

  # First clean up arrays
  for (name in arrays_to_init) {
    name = strip_namespace(name)
    arr = arrays[[name]]
    namespaces = arr$namespace
    stopifnot(!is.null(namespaces))
    dims = arr$dims
    for (namespace in namespaces){
      fullnm = paste(namespace, name, sep = ".")
      cat("Trying to remove array ", fullnm, "\n")
      tryCatch({iquery(db, paste("remove(", fullnm, ")"), force=TRUE)},
               error = function(e){cat("====Failed to remove array: ", fullnm, ",\n",sep = "")})
      info_flag = arr$infoArray
      if (!is.null(info_flag)) { if(info_flag){
        cat("Trying to remove array ", fullnm, "_INFO\n", sep = "")
        tryCatch({iquery(db, paste("remove(", fullnm, "_INFO)", sep = ""), force=TRUE)},
                 error = function(e){cat("====Failed to remove", paste("remove array: ", fullnm, "_INFO\n", sep = ""))})
      }}
    }

    ## Handle LOOKUP array
    # arrays exist in multiple namespaces ==> need a lookup array
    # also if entity is of class "data / variant_data / copynumber_data etc."
    # e.g. RNAQuantification (Expression), VARIANT, COPYNUMBER_SEG
    # do not need a LOOKUP array
    if (length(namespaces)>1 &
        length(grep("^data$|^variant_.*data$|^copynumber_.*_data$", arr$data_class)) == 0) {
      cat("Trying to remove array: public.", name, "_LOOKUP\n", sep = "")
      tryCatch({iquery(db, paste("remove(public.", name, "_LOOKUP)", sep = ""), force=TRUE)},
               error = function(e){cat("====Failed to remove", paste("remove array: public.", name, "_LOOKUP\n", sep = ""))})
    }
  }

  # Next create the arrays

  for (name in arrays_to_init) {
    name = strip_namespace(name)
    arr = arrays[[name]]
    namespaces = arr$namespace
    stopifnot(!is.null(namespaces))
    dims = arr$dims
    if (class(dims) == "character") {dim_str = dims} else if (class(dims) == "list"){
      dim_str = yaml_to_dim_str(dims)
    } else {stop("Unexpected class for dims")}
    attr_str = yaml_to_attr_string(arr$attributes, arr$compression_on)
    attr_str = paste("<", attr_str, ">")
    for (namespace in namespaces){
      fullnm = paste(namespace, name, sep = ".")
      tryCatch({
        query =       paste("create array", fullnm, attr_str, "[", dim_str, "]")
        cat("running: ", query, "\n")
        iquery(db,
               query
        )},
        error = function(e){cat("=== faced error in creating array:", fullnm, "\n")}
      )

      info_flag = arr$infoArray
      if (!is.null(info_flag)) { if(info_flag){
        if(arr$data_class == "data") {stop("array of class \"data\" cannot have INFO array")}
        tryCatch({
          # Info array
          if (is.null(arr$infoArray_max_keys)){
            key_str = "key_id"
          } else {
            key_str = paste("key_id=0:*,", arr$infoArray_max_keys, ",0", sep = "")
          }
          query = paste("create array ", fullnm, "_INFO <key: string, val: string> [", 
                        dim_str, ", ", key_str, "]",
                        sep = "")
          cat("running: ", query, "\n")
          iquery(db,
                 query
          )
        }, error = function(e){cat("=== faced error in creating array: ", fullnm, "_INFO\n", sep="")}
        )
      }}
    }
    ## Handle LOOKUP array
    # arrays exist in multiple namespaces ==> need a lookup array
    # also if entity is of class "data / variant_data / copynumber_data etc."
    # e.g. RNAQuantification (Expression), VARIANT, COPYNUMBER_SEG
    # do not need a LOOKUP array
    if (length(namespaces)>1 &
        length(grep("^data$|^variant_.*data$|^copynumber_.*_data$", arr$data_class)) == 0) {
      tryCatch({
        # Info array
        query = paste("create array public.", name, "_LOOKUP <namespace:string> [", get_base_idname(name), "]",
                      sep = "")
        cat("running: ", query, "\n")
        iquery(db,
               query
        )
      }, error = function(e){cat("=== faced error in creating array: public.", name, "_LOOKUP\n", sep="")}
      )
    }
  }
}