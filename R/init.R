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

#' (potentially delete and) initialize arrays managed by REVEAL/Genomics API
#'
#' The function requires that you are connected to SciDB as an user with admin privileges
#' @return NULL
#' @examples
#' \dontrun{
#' init_db('FUSION')
#' init_db(c('MEASUREMENTSET', 'FUSION'))
#' init_db(arrays = get_entity_names()) # Warning! This reinitializes all the arrays
#' }
#' @export
init_db = function(arrays_to_init = NULL, 
                   pattern_array_names = NULL,
                   force = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  db = con$db
  L = .ghEnv$meta$L
  
  if ((is.null(arrays_to_init) & is.null(pattern_array_names)) | 
      (!is.null(arrays_to_init) & !is.null(pattern_array_names))) {
    stop("Only one of the two parameters can have a non-null value:
         `arrays_to_init` or `pattern_array_names`.
         Both cannot be null.")
  }
  if (!is.null(arrays_to_init)) {
    arrays_to_init = arrays_to_init[arrays_to_init %in% names(L$array)]
  } else if (!is.null(pattern_array_names)) {
    stopifnot(length(pattern_array_names) == 1)
    arrays_to_init = names(L$array)[grep(pattern = pattern_array_names,
                                         x = names(L$array))]
  }
  
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
  
  if (as.logical(options('revealgenomics.use_scidb_ee'))) {
    if (identical(arrays_to_init, get_entity_names())) {
      cat("You asked to initialize all arrays. Should I initialize the permissions array too?\n")
      resp_perm <- readline("(Y)es/(N)o: ")
    } else { # do not reinitialize permissions array if only working on one or two arrays
      resp_perm = "n"
    }
  } else {
    # In CE mode, do not need a permissions array
    resp_perm = "n"
  }
  
  arrays = L$array

  # First clean up arrays
  for (name in arrays_to_init) {
    name = strip_namespace(name)
    arr = arrays[[name]]
    dims = arr$dims
    
    fullnm = full_arrayname(entitynm = name)
    message("Trying to remove array ", fullnm)
    tryCatch({iquery(db, paste("remove(", fullnm, ")"), force=TRUE)},
             error = function(e){cat("====Failed to remove array: ", fullnm, ",\n",sep = "")})
    info_flag = arr$infoArray
    if (!is.null(info_flag)) { if(info_flag){
      message("Trying to remove array ", fullnm, "_INFO")
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
      message("running: ", query)
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
        message("running: ", query)
        iquery(db,
               query
        )
      }, error = function(e){cat("=== faced error in creating array: ", fullnm, "_INFO\n", sep="")}
      )
    }}
  }
  
  if (.ghEnv$meta$arrOntologyCategory %in% arrays_to_init) {
    message("Initializing ontology categories array with starter value: `uncategorized`")
    query = paste0(
      "store(redimension(build(<ontology_category:string> [ontology_category_id=1:1], 'uncategorized'), ",
      revealgenomics:::full_arrayname(.ghEnv$meta$arrOntologyCategory), 
      "), ", 
      revealgenomics:::full_arrayname(.ghEnv$meta$arrOntologyCategory), 
      ")"
    )
    iquery(db, query)
    # Update cache
    invisible(revealgenomics:::get_ontology_category(updateCache = TRUE))
  }
  
  # Clean up any package cache
  message("Cleaning up any local cache values")
  .ghEnv$cache$lookup = list()
  cached_entities = get_entity_names()[sapply(get_entity_names(), revealgenomics:::is_entity_cached)]
  for (entity in cached_entities) {
    .ghEnv$cache[[entity]] = NULL
  }

  if ( (tolower(resp_perm) == 'y' | tolower(resp_perm) == 'yes') & !is.na(resp_perm)) {
    cat("Proceeding with initialization of permissions array\n")
    init_permissions_array(con = con)
  } else{
    cat("Not initalizing permissions array\n")
  }
}

init_permissions_array = function(con = NULL) {
  con = use_ghEnv_if_null(con = con)
  db = con$db
  
  cat("Try deleting permissions array (if exists)\n")
  tryCatch({
    iquery(db, paste0("remove(", PERMISSIONS_ARRAY(), ")"))
  }, 
  error = function(e) {
    cat("====Failed to remove permissions array\n")
  })
  
  cat("Create permissions array\n")
  tryCatch({
    iquery(db, paste0("create array ", PERMISSIONS_ARRAY(), " <access:bool> [user_id=0:*:0:1; dataset_id=0:*:0:256]"))
  }, 
  error = function(e) {
    cat("====Failed to create permissions array\n")
  })
  
  
}
