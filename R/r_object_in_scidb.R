#' Initialize R object cache array
#' 
#' @param con   connection object
#' @examples 
#' \dontrun{
#'   con_root = rg_connect2(username = 'scidbadmin') # has to be scidbadmin
#'   revealgenomics:::init_r_object_cache(con = con_root)
#' }
init_r_object_cache = function(con = NULL) {
  con = use_ghEnv_if_null(con = con)
  response = user_confirms_action(action = "initialize R object cache")
  if (response) {
    tryCatch({
      iquery(con$db, "remove(gh_r_object_store.R_OBJECT_CACHE)")
    }, error = function(e) {
      message("Issue deleting array. Either array did not exist before (that's perfectly OK), or you do not have permissions")
    })
    iquery(con$db, "create array gh_r_object_store.R_OBJECT_CACHE <key:string, payload:binary>[r_object_cache_id]")
  }
}

#' Upload an R object at key
#' 
#' Store an R object in scidb and associate it with user provided key
#' 
#' @param r_obj R object to be stored in scidb binary form
#' @param key   key to associate with the R object
#' @param con   connection object
#' @examples 
#' \dontrun{
#'   rg_connect(username = 'scidbadmin') # or any user with access to gh_r_object_store namespace
#'   revealgenomics:::register_r_object_at_key(r_obj = c(1:10), key = "asdf")
#'   # To retrieve data, run
#'   revealgenomics:::retrieve_r_object_at_key(key = "asdf")
#' }
register_r_object_at_key = function(r_obj, key, con = NULL) {
  con = use_ghEnv_if_null(con = con)
  keys_df = iquery(con$db, "project(gh_r_object_store.R_OBJECT_CACHE, key)", return = TRUE)
  m1 = find_matches_and_return_indices(source = key, target = keys_df$key)
  if (length(m1$source_matched_idx) == 1) {
    insert_idx = keys_df[m1$target_matched_idx, ]$r_object_cache_id
    cat("Key already exists at r_object_cache_id =", insert_idx, "\n\t",
        "Overwriting object at that id\n")
  } else if (length(m1$source_matched_idx) == 0) {
    insert_idx = ifelse(nrow(keys_df) == 0, 1, max(keys_df$r_object_cache_id) + 1)
    cat("New key being inserted at r_object_cache_id =", insert_idx, "\n")
  } else {
    stop("Unexpected")
  }
  payload = serialize(r_obj, NULL)
  payload_in_db = as.scidb(
    con$db, 
    payload
  )
  query = paste0(
    "apply(",
    payload_in_db@name, 
    ", payload, val, r_object_cache_id, ", insert_idx, 
    ", key, '", key, "')"
  )
  query = paste0(
    "insert(redimension(", 
    query, 
    ", gh_r_object_store.R_OBJECT_CACHE), gh_r_object_store.R_OBJECT_CACHE)"
  )
  iquery(con$db, query)
}

#' Retrieve R object associated with key
#' @param key   key associated with the R object
#' @param con   connection object
#' @examples 
#' \dontrun{
#'   rg_connect(username = 'scidbadmin') # or any user with access to gh_r_object_store namespace
#'   revealgenomics:::retrieve_r_object_at_key(key = "asdf")
#'   revealgenomics:::retrieve_r_object_at_key(key = "KeyDoesNotExist")
#'   # No R object found at key: KeyDoesNotExist
#' }
retrieve_r_object_at_key = function(key, con = NULL) {
  con = revealgenomics:::use_ghEnv_if_null(con = con)
  res = iquery(
    con$db, 
    paste0(
      "filter(gh_r_object_store.R_OBJECT_CACHE, key = '", key, "')"
    ),
    return = TRUE)
  if (class(res) != 'list' | length(res) != 3) {
    cat("No R object found at key:", key, "\n")
    return(invisible(NULL))
  } else if (identical(res$key, character(0))) {
    cat("No R object found at key:", key, "\n")
    return(invisible(NULL))
  } else if (length(res) == 3) {
    unserialize(res$payload[[1]])
  } else {
    stop("Unexpected")
  }
}
