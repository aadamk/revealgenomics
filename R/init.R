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
#' init_db(c('MEASUREMENTSET', 'FUSION'))
#' init_db(arrays = get_entity_names()) # Warning! This reinitializes all the arrays
#' }
#' @export
init_db = function(arrays_to_init, force = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  db = con$db
  L = .ghEnv$meta$L
  
  arrays_to_init = arrays_to_init[arrays_to_init %in% names(L$array)]
  
  if (length(arrays_to_init) == 0) {cat("ERROR: Check array names\n"); return(FALSE)}
    
  cat("CAUTION: The following arrays will be deleted and reinitialized\n", 
      paste(arrays_to_init, collapse = ", "), "\n Proceed?")  
  if (!force) {
    response <- readline("(Y)es/(N)o: ")
  } else {
    response = 'yes'
  }
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
    dims = arr$dims
    
    fullnm = full_arrayname(entitynm = name)
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

  # Next create the arrays

  for (name in arrays_to_init) {
    name = strip_namespace(name)
    arr = arrays[[name]]
    dims = arr$dims
    if (class(dims) == "character") {dim_str = dims} else if (class(dims) == "list"){
      dim_str = yaml_to_dim_str(dims)
    } else {stop("Unexpected class for dims")}
    attr_str = yaml_to_attr_string(arr$attributes, arr$compression_on)
    attr_str = paste("<", attr_str, ">")

    fullnm = full_arrayname(entitynm = name)
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
#         if(arr$data_class == "data") {stop("array of class \"data\" cannot have INFO array")}
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
}