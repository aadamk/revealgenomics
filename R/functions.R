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

#' @import data.table

get_revealgenomics_config = function() {
  config_file = '/etc/revealgenomics_config.yaml'
  if (file.exists(config_file)) {
    yaml.load_file(config_file)
  } else {
    NULL
  }
}

#' @export
rg_connect = function(username = NULL, password = NULL, host = NULL, port = NULL, protocol = "https"){
  # SciDB connection and R API --

  if (!is.null(get_revealgenomics_config())) {
    if (get_revealgenomics_config()$security$convert_username_to_lowercase) {
      username = tolower(username)
    }
  }

  if (is.null(username) & protocol != 'http') {
    cat("using HTTP protocol\n")
    protocol = 'http'
    unset_scidb_ee_flag = TRUE
  } else {
    unset_scidb_ee_flag = FALSE
  }

  if (!is.null(username) & protocol == 'http') {
    stop("if protocol is HTTP, cannot try authentication via HTTP")
  }

  con = NULL
  if (is.null(username)) {
    protocol = 'http'
    if (is.null(host) & is.null(port)) {
      con$db = scidbconnect(protocol = protocol)
    } else {
      con$db = scidbconnect(host = host, port = port, protocol = protocol)
    }
  } else {
    # ask for password interactively if none supplied
    # https://github.com/Paradigm4/SciDBR/issues/154#issuecomment-327989402
    if (is.null(password)) {
      if (rstudioapi::isAvailable()) { # In RStudio,
        password = rstudioapi::askForPassword(paste0("Password for ", username, ":"))
      } else { # in base R
        password = getpwd()
      } # Rscripts and knitr not yet supported
    }

    if (is.null(password)) { # if still null password
      stop("Password cannot be null")
    }
    # Attempt 1.
    err1 = tryCatch({
      if (is.null(host)& is.null(port)) {
        # If user did not specify host and port, then formulate host URL from apache config
        path1 = '/etc/httpd-default/conf.d/default-ssl.conf'
        path2 = '/opt/rh/httpd24/root/etc/httpd/conf.d/25-default_ssl.conf'
        if (file.exists(path1) & !file.exists(path2)) {
          apache_conf_file = path1
          port = NULL
          hostname = NULL
        } else if (!file.exists(path1) & file.exists(path2)) {
          apache_conf_file = path2
          port = NULL
          hostname = NULL
        } else if (!file.exists(path1) & !file.exists(path2)) {
          hostname = 'localhost'
          port = 8083
        } else {
          cat("Cannot infer hostname from apache config. Need to supply hostname as parameter to rg_connect\n")
          return(NULL)
        }
        if (is.null(hostname)) {
          hostname = tryCatch({
            system(paste0("grep ServerName ", apache_conf_file, " | awk '{print $2}'"),
                   intern = TRUE)
          },
          error = function(e) {
            cat("Could not infer hostname from apache conf\n")
            return(e)
          }
          )
          if (! "error" %in% class(hostname)) {
            hostname = paste0(hostname, '/shim/')
          } else {
            print(hostname)
            cat("Aborting rg_connect()\n")
            return(NULL)
          }
        }
        cat("hostname was not provided. Connecting to", hostname, "\n")
        con$db = scidbconnect(host = hostname, username = username, password = password,
                              port = port, protocol = protocol)
      } else {
        # If user specified host and port, try user supplied parameters
        con$db = scidbconnect(host = host, username = username, password = password, port = port, protocol = protocol)
      }
    }, error = function(e) {return(e)}
    )

    if ("error" %in% class(err1)) {
      print(err1);
      con$db = NULL
    }
  }

  if (unset_scidb_ee_flag) {
    if (!is.null(con$db)) options(revealgenomics.use_scidb_ee = FALSE)
  }
  # Store a copy of connection object in .ghEnv
  # Multi-session programs like Shiny, and the `rg_connect2` call need to explicitly delete this after rg_connect()
  .ghEnv$db = con$db
  return(con)
}

#' @export
rg_connect2 = function(username = NULL, password = NULL, host = NULL, port = NULL, protocol = "https") {
  con = rg_connect(username, password, host, port, protocol)
  .ghEnv$db = NULL
  return(con)
}

# use_ghEnv_if_null_v0 = function(con) {
#   if (is.null(con)) return(.ghEnv) else return(con)
# }
#
use_ghEnv_if_null = function(con) {
  if (is.null(con)) {
    con$db = .ghEnv$db
  }
  return(con)
}

#' Report logged in user
get_logged_in_user = function(con = NULL) {
  con = use_ghEnv_if_null(con)
  attr(con$db, "connection")$username
}

get_entity = function(entity, id, ...){
  fn_name = paste("get_", tolower(entity), sep = "")
  f = NULL
  try({f = get(fn_name)}, silent = TRUE)
  if (is.null(f)) try({f = get(paste(fn_name, "s", sep = ""))}, silent = TRUE)
  if (entity == 'ONTOLOGY') {
    f(id, updateCache = TRUE, ...)
  } else {
    f(id, ...)
  }
}

##  internal function.  It will be used in the get_dataset_subelements()
##  function as a way of programmatically identifying all metadata elements
##  within a given dataset, so that they can be deleted.  It is based on
##  the "get_entity()" function.
search_entity = function(entity, id, ...){

  ## Get the table of entities, and how to search for them.
  entity_parents_table <- get_entity_info()
  entity_idx <- match(toupper(entity),
                      entity_parents_table$entity)

  ## Verify that the given entity can be searched for.
  if (is.na(entity_parents_table$search_by_entity[entity_idx])) {
    ## If it is not a valid element to search by, stop and give an error message.
    stop(paste0("Searching for entity == '", entity, "' is not supported!"))

  } else {
    ## Find the proper search function.
    fn_name = paste("search_", tolower(entity), sep = "")
    f = NULL
    try({f = get(fn_name)}, silent = TRUE)
    if (is.null(f)) try({f = get(paste(fn_name, "s", sep = ""))}, silent = TRUE)
    f(id, ...)
  }
}

#' @export
get_ontology = function(ontology_id = NULL, updateCache = FALSE, con = NULL){
  get_ontology_from_cache(ontology_id = ontology_id,
                          updateCache = updateCache,
                          con = con)
}

get_metadata_attrkey = function(metadata_attrkey_id = NULL, updateCache = FALSE, con = NULL){
  get_metadata_attrkey_from_cache(metadata_attrkey_id = metadata_attrkey_id,
                                  updateCache = updateCache,
                                  con = con)
}

#' @export
search_attributes = function(entity, updateCache = FALSE, con = NULL) {
  entity_info = get_entity_info()
  entity_info = entity_info[
    which(sapply(entity_info$entity,
                 function(entity) .ghEnv$meta$L$array[[entity]]$infoArray)), ]

  if (! entity %in% entity_info$entity ) {
    stop("Search by attributes are available by following entities:",
         pretty_print(entity_info$entity, prettify_after = nrow(entity_info)))
  }

  search_metadata_attrkey(entity_id = get_entity_id(entity), updateCache = updateCache, con = con)$metadata_attrkey
}
#' Search metadata attributes by entity id
#'
#' @param entity_id id of API entity as assigned in \code{SCHEMA.yaml} file. You can list all
#'                  \code{entity_id}-s by running the function \code{get_entity_info()}
search_metadata_attrkey = function(entity_id, updateCache = FALSE, con = NULL){
  metadtata_attrs_in_db = get_metadata_attrkey(updateCache = updateCache, con = con)
  metadtata_attrs_in_db[metadtata_attrs_in_db$entity_id == entity_id, ]
}

#' @export
get_variant_key = function(variant_key_id = NULL, updateCache = FALSE, con = NULL){
  get_variant_key_from_cache(variant_key_id = variant_key_id,
                             updateCache = updateCache,
                             con = con)
}

get_chromosome_key = function(chromosome_key_id = NULL, updateCache = FALSE, con = NULL){
  get_chromosome_key_from_cache(chromosome_key_id = chromosome_key_id,
                                updateCache = updateCache,
                                con = con)
}

#' @export
get_definitions = function(definition_id = NULL, updateCache = FALSE, con = NULL){
  get_definition_from_cache(definition_id = definition_id,
                            updateCache = updateCache,
                            con = con)
}

get_feature_synonym = function(feature_synonym_id = NULL, updateCache = FALSE, con = NULL){
  get_feature_synonym_from_cache(feature_synonym_id = feature_synonym_id,
                                 updateCache = updateCache,
                                 con = con)
}

#' @export
get_gene_symbol = function(gene_symbol_id = NULL, updateCache = FALSE, con = NULL){
  get_gene_symbol_from_cache(gene_symbol_id = gene_symbol_id,
                             updateCache = updateCache,
                             con = con)
}

find_namespace = function(entitynm) {
  # Use secure_scan for SciDB enterprise edition only
  ifelse(options("revealgenomics.use_scidb_ee"),
        .ghEnv$meta$L$array[[entitynm]]$namespace,
        'public')
}

#' full name of array with namespace
#'
#' @export
full_arrayname = function(entitynm) {
  paste0(find_namespace(entitynm), ".", entitynm)
}

get_max_id = function(arrayname, con = NULL){
  con = use_ghEnv_if_null(con)

  max = iquery(con$db, paste("aggregate(apply(", arrayname,
                             ", temp_id, ", get_base_idname(arrayname), "), ",
                             "max(temp_id))", sep=""), return=TRUE)$temp_id_max
  if (is.na(max)) max = 0
  return(max)
}


update_tuple = function(df, ids_int64_conv, arrayname, con = NULL){
  con = use_ghEnv_if_null(con)

  if (nrow(df) < 100000) {x1 = as.scidb(con$db, df)} else {x1 = as.scidb(con$db, df, chunk_size=nrow(df))}

  x = x1
  for (idnm in ids_int64_conv){
    x = convert_attr_double_to_int64(arr = x, attrname = idnm, con = con)
  }
  if ('updated' %in% .ghEnv$meta$L$array[[strip_namespace(arrayname)]]$attributes) {
    x = scidb_attribute_rename(arr = x, old = "updated", new = "updated_old", con = con)
    qq = paste0("apply(", x@name, ", updated, string(now()))")
  } else {
    qq = x@name
  }
  qq = paste0("redimension(", qq, ", ", scidb::schema(scidb(con$db, arrayname)), ")")

  query = paste("insert(", qq, ", ", arrayname, ")", sep="")
  iquery(con$db, query)
}

register_tuple = function(df, ids_int64_conv, arrayname, con = NULL){
  con = use_ghEnv_if_null(con)

  if (nrow(df) < 100000) {
    x1 = as.scidb(con$db, df)
  }
  else {
    x1 = as.scidb(con$db, df, chunk_size=nrow(df))
  }

  x = x1
  for (idnm in ids_int64_conv){
    x = convert_attr_double_to_int64(arr = x, attrname = idnm, con = con)
  }
  qq = paste0("apply(", x@name, ", created, string(now()))")
  qq = paste0("apply(", qq, ", updated, created)")
  qq = paste0("redimension(", qq, ", ", scidb::schema(scidb(con$db, arrayname)), ")")

  query = paste("insert(", qq, ", ", arrayname, ")", sep="")
  iquery(con$db, query)
}

# finds matches of entries in dataframe to be uploaded with entries previously registered in database based on user provided unique fields
find_matches_with_db = function(df_for_upload, df_in_db, unique_fields) {
  if (length(unique_fields) == 1) {
    matches = match(df_for_upload[, unique_fields], df_in_db[, unique_fields])
  } else {
    df_for_upload2 = data.frame(lapply(df_for_upload[,unique_fields], as.character), stringsAsFactors=FALSE)
    df_for_upload2 = apply(df_for_upload2, 1, paste, collapse = "_")
    df_in_db2 = data.frame(lapply(df_in_db[,unique_fields], as.character), stringsAsFactors=FALSE)
    df_in_db2 = apply(df_in_db2, 1, paste, collapse = "_")
    matches = match(df_for_upload2, df_in_db2)
  }
  return(matches)
}

get_infoArray = function(arrayname){
  infoArray = .ghEnv$meta$L$array[[strip_namespace(arrayname)]]$infoArray
  if (is.null(infoArray)) infoArray = TRUE
  return(infoArray)
}

# Wrapper function that (1) updates mandatory fields, (2) updates flex fields
update_mandatory_and_info_fields = function(df, arrayname, con = NULL){
  con = use_ghEnv_if_null(con)

  idname = get_idname(arrayname)
  if (any(is.na(df[, idname]))) stop("Dimensions: ", paste(idname, collapse = ", "), " should not have null values at upload time!")
  int64_fields = get_int64fields(arrayname)
  infoArray = get_infoArray(arrayname)

  if (all(c('created', 'updated') %in% names(.ghEnv$meta$L$array[[strip_namespace(arrayname)]]$attributes))) {
    sel_cols = c(get_idname(arrayname),
                 mandatory_fields()[[strip_namespace(arrayname)]],
                 'created', 'updated')
  } else {
    sel_cols = c(get_idname(arrayname),
                 mandatory_fields()[[strip_namespace(arrayname)]])
  }
  update_tuple(df[, sel_cols],
               ids_int64_conv = c(idname, int64_fields),
               arrayname,
               con = con)
  if(infoArray){
    delete_info_fields(fullarrayname = arrayname,
                       id = df[, get_base_idname(arrayname)],
                       dataset_version = unique(df$dataset_version),
                       con = con)
    cat("Registering info for ", nrow(df)," entries in array: ", arrayname, "_INFO\n", sep = "")
    register_info(df = prep_df_fields(df,
                                      mandatory_fields = c(get_mandatory_fields_for_register_entity(arrayname),
                                                           get_idname(arrayname),
                                                           'created', 'updated')),
                  idname, arrayname,
                  con = con)
  }
}

#' Register the mandatory fields and update lookup
#'
#' Wrapper function that
#' (1) registers mandatory fields,
#' (2) registers flex fields
register_mandatory_and_flex_fields = function(df, arrayname, con = NULL){
  con = use_ghEnv_if_null(con)

  idname = get_idname(arrayname)
  if (any(is.na(df[, idname]))) stop("Dimensions: ", paste(idname, collapse = ", "), " should have had non null values at upload time!")
  int64_fields = get_int64fields(arrayname)
  infoArray = get_infoArray(arrayname)

  non_info_cols = c(get_idname(strip_namespace(arrayname)),
                    mandatory_fields()[[strip_namespace(arrayname)]])
  register_tuple(df = df[, non_info_cols], ids_int64_conv = c(idname, int64_fields), arrayname, con = con)
  if(infoArray){
    cat("Registering info for ", nrow(df)," entries in array: ", arrayname, "_INFO\n", sep = "")
    register_info(df, idname, arrayname, con = con)
  }
}

register_tuple_return_id = function(df,
                                    arrayname,
                                    uniq = NULL,
                                    dataset_version = NULL,
                                    con = NULL){
  con = use_ghEnv_if_null(con)

  test_unique_fields(df, uniq)         # Ideally this should have already been run earlier
  test_mandatory_fields(df, arrayname, silent = TRUE) # Ideally this should have already been run earlier

  idname = get_idname(arrayname)
  int64_fields = get_int64fields(arrayname)

  entitynm = strip_namespace(arrayname)
  mandatory_fields = mandatory_fields()[[entitynm]]
  if (entitynm == .ghEnv$meta$arrFeature) { # FEATURE entity supplies gene_symbol_id as an extra
                                            # mandatory column
    mandatory_fields = c(mandatory_fields,
                         'gene_symbol_id')
  }
  df = prep_df_fields(df, mandatory_fields)

  if (!is.null(dataset_version)) {
    df[, "dataset_version"] = dataset_version
  }

  if (is.null(uniq)){ # No need to match existing elements, just append data
    stop("Registering entity without a set of unique fields is not allowed")
  }

  # Find matches by set of unique fields provided by user
  # dimensions, and do not need to be projected
  if (entitynm == .ghEnv$meta$arrMeasurementSet) {
    do_not_project = 'dataset_id'
  } else if (entitynm == .ghEnv$meta$arrFeature) {
    do_not_project = c('gene_symbol_id', 'featureset_id')
  } else {
    do_not_project = c('dataset_id', 'featureset_id')
  }
  projected_attrs = paste0(uniq[!(uniq %in% do_not_project)], # dataset_id is a dimension, and does not need to be projected
                           collapse = ",")
  options(scidb.aio = TRUE) # try faster path: Shim uses aio_save with atts_only=0 which will include dimensions
                            # https://github.com/Paradigm4/SciDBR/commit/85895a77549a24c16766e6adf4dcc6311ee21acc
  xx = iquery(con$db, paste0("project(", arrayname, ", ",
                             projected_attrs, ")"), return = TRUE)
  options(scidb.aio = FALSE)
  matching_entity_ids = find_matches_with_db(df_for_upload = df, df_in_db = xx, unique_fields = uniq)
  nonmatching_idx = which(is.na(matching_entity_ids))
  matching_idx = which(!is.na(matching_entity_ids))

  # Find the old id-s that match
  old_id = xx[matching_entity_ids[matching_idx], get_base_idname(arrayname)]
  if (length(old_id) != 0) {
    df[matching_idx, get_base_idname(arrayname)] = old_id
  }

  if (!is.null(dataset_version) & length(old_id) != 0) {# Find maximum dataset version for the current entity at the specified dataset_id
    cat("**Versioning is ON -- must handle matching entries\n")
    if (!("dataset_version" %in% colnames(df))) stop("Field `dataset_version` must exist in dataframe to be uploaded to versioned entities")
    if (!("dataset_id" %in% colnames(df))) stop("Field `dataset_id` must exist in dataframe to be uploaded to versioned entities")
    if (entitynm != .ghEnv$meta$arrDataset) {
      cur_dataset_id = unique(df$dataset_id)
      if (length(cur_dataset_id) != 1) stop("dataset_id of df to be uploaded should be unique")
      cur_max_ver_by_entity = max(xx[xx$dataset_id == cur_dataset_id, ]$dataset_version)
    } else { # for DATASET
      cur_project_id = unique(df$project_id)
      if (length(cur_project_id) != 1) stop("project_id of dataset to be uploaded should be unique")
      cur_max_ver_by_entity = unique(xx$dataset_version)
      if (length(cur_max_ver_by_entity) != 1) stop("dataset_version of study to be uploaded should be unique")
    }
    if (dataset_version > cur_max_ver_by_entity) { # then need to register new versions for the matching entries at same entity_id
      if (entitynm == .ghEnv$meta$arrDataset) stop("use increment_dataset() for incrementing dataset versions")
      cat("Entity does not have any entry at current version number\n")
      cat("Registering new versions of", nrow(df[matching_idx, ]), "entries into", arrayname, "at version", dataset_version, "\n")
      register_mandatory_and_flex_fields(df = df[matching_idx, ], arrayname = arrayname, con = con)
    } else {
      # code to handle versioning while registering entries that have matching entries at other versions

      # Within entries matching by unique fields, find entries that do not have current version number and register those
      cat("Within entries matching by unique fields, find entries that do not have current version number and register those\n")

      dfx = df[matching_idx, ]
      matching_entity_ids_at_version = find_matches_with_db(df_for_upload = dfx, df_in_db = xx, unique_fields = c(uniq, "dataset_version"))
      matching_idx_at_version = which(!is.na(matching_entity_ids_at_version))
      nonmatching_idx_at_version = which(is.na(matching_entity_ids_at_version))
      cat("Matching entries already exist for", nrow(dfx[matching_idx_at_version, ]), "rows of",  arrayname, "at version", dataset_version, " -- returning matching ID's\n")
      if (length(nonmatching_idx_at_version) > 0) {
        cat("Registering new versions of", nrow(dfx[nonmatching_idx_at_version, ]), "entries into", arrayname, "at version", dataset_version, "\n")
        register_mandatory_and_flex_fields(df = dfx[nonmatching_idx_at_version, ], arrayname = arrayname, con = con)
      }
    }
  } else {
    if (!is.null(dataset_version) & length(old_id) == 0) cat("No matching entries for versioned entity\n")
  }

  # Now assign new id-s for entries that did not match by unique fields
  if (length(nonmatching_idx) > 0 ) {
    cat("---", length(nonmatching_idx), "rows need to be registered from total of", nrow(df), "rows provided by user\n")
    # if (length(nonmatching_idx) != nrow(df)) {stop("Need to check code here")}
    if (length(colnames(df)) > 1) { # common case
      lenToAdd = nrow(df[nonmatching_idx, ])
    } else if (length(colnames(df)) == 1) { # selection from one-column data-frame creates a vector (avoid that)
      lenToAdd = nrow(data.frame(col1 = df[nonmatching_idx, ],
                          stringsAsFactors = FALSE))
    }
    new_id = get_max_id(arrayname, con = con) + 1:lenToAdd
    df[nonmatching_idx, get_base_idname(arrayname)] = new_id

    register_mandatory_and_flex_fields(df = df[nonmatching_idx, ], arrayname = arrayname,
                                 con = con)
  } else {
    cat("--- no completely new entries to register\n")
    new_id = NULL
  }
  return(df[, idname])
}

register_versioned_secure_metadata_entity = function(entity, df,
                                                     dataset_version, only_test, con = NULL){
  con = use_ghEnv_if_null(con)

  uniq = unique_fields()[[entity]]
  if (is.null(uniq)) stop("unique fields need to be defined for entity: ",
                          entity, " in SCHEMA.yaml file")
  # Common tests
  test_register_versioned_secure_metadata_entity(entity = entity,
                                                 df, uniq,
                                                 silent = ifelse(only_test, FALSE, TRUE))

  if (!only_test) {
    if (is.null(dataset_version)) {
      dataset_version = get_dataset_max_version(dataset_id = unique(df$dataset_id), updateCache = TRUE, con = con)
      cat("dataset_version was not specified. Registering at version", dataset_version, "of dataset", unique(df$dataset_id), "\n")
    }
    arrayname = full_arrayname(entity)
    register_tuple_return_id(df, arrayname, uniq, dataset_version = dataset_version, con = con)
  } # end of if (!only_test)
}

register_info = function(df, idname, arrayname, con = NULL){
  # df[idname] = id
  info_col_pos = grep("info_", colnames(df))
  if (length(info_col_pos) > 0){
    info_col_nm = grep("info_", colnames(df), value = TRUE)
    info = df[, c(idname, info_col_nm)]
    info_col_pos = grep("info_", colnames(info))
    new_info_col_nm = sapply(strsplit(info_col_nm, "info_"), function(x){x[2]})
    colnames(info) = c(idname,
                       new_info_col_nm)
    info = info %>%
      gather(key, val, info_col_pos)

    # Check that all attribute column names are registered. If not, register
    metadata_attrs = unique(info$key)

    entity_id = get_entity_id(entity = strip_namespace(arrayname))
    metadtata_attrs_in_db = search_metadata_attrkey(entity_id = entity_id, con = con)
    if (!(all(metadata_attrs %in% metadtata_attrs_in_db$metadata_attrkey))) {
      cat("Registering new metadata attributes:",
          pretty_print(metadata_attrs[!(metadata_attrs %in% metadtata_attrs_in_db$metadata_attrkey)]),
          "\n")
      metadata_attr_id = register_metadata_attrkey(
        df1 = data.frame(metadata_attrkey = metadata_attrs,
                         entity_id = entity_id,
                         stringsAsFactors = FALSE),
        con = con
      )
      metadtata_attrs_in_db = search_metadata_attrkey(entity_id = entity_id, con = con)
      stopifnot(all(metadata_attrs %in% metadtata_attrs_in_db$metadata_attrkey))
    }

    # Assign attribute key id-s within `INFO` data.frame
    # (do not leave `key_id` generation to scidb synthetic handling)
    m1 = find_matches_and_return_indices(
      source = info$key,
      target = metadtata_attrs_in_db$metadata_attrkey
    )
    stopifnot(length(m1$source_unmatched_idx) == 0)
    info$key_id = metadtata_attrs_in_db$metadata_attrkey_id[m1$target_matched_idx]

    # Register
    register_tuple(df = info, ids_int64_conv = c(idname, 'key_id'),
                   arrayname = paste(arrayname,"_INFO",sep=""), con = con)
  }
}

count_unique_calls = function(variants){
  v = variants
  nrow(v[duplicated(v[, c('biosample_id', 'CHROM', 'POS')]), ])
}

join_ontology_terms = function(df, terms = NULL, updateCache = FALSE, con = NULL){
  if (is.null(terms)) {
    stop("This code-path should not be used after changes in
         https://github.com/Paradigm4/revealgenomics/pull/41")
    terms = grep(".*_$", colnames(df), value=TRUE)
    # if (length(terms) > 0) {
    #   stop("Will apply ontology rules for any column with trailing underscore ('_')")
    # }
  } else {
    terms = terms[terms %in% colnames(df)]
  }
  df2 = df
  for (term in terms){
    df2[, term] = get_ontology_from_cache(ontology_id = NULL,
                                          updateCache = updateCache,
                                          con = con)[df[, term], "term"]
  }
  return(df2)
}

#' @export
get_projects = function(project_id = NULL, con = NULL){
  select_from_1d_entity(entitynm = .ghEnv$meta$arrProject, id = project_id, con = con)
}

#' internal function for get_METADATA()
#'
#' function to select from metadata entities. Used by
#' `get_project()` (does not have dataset_version as a dimension), and
#' `get_dataset()`, `get_biosamples()` etc. (that have dataset_version as a dimension)
select_from_1d_entity = function(entitynm, id, dataset_version = NULL,
                                 mandatory_fields_only = FALSE,
                                 con = NULL){
  con = use_ghEnv_if_null(con)

  fullnm = full_arrayname(entitynm)
  if (is.null(id)) {
    qq = full_arrayname(entitynm)
    if (is_entity_secured(entitynm)) {
      qq = paste0(custom_scan(), "(", qq, ")")
    }
  } else {
    if (length(get_idname(entitynm)) == 1) {
      qq = form_selector_query_1d_array(arrayname = fullnm,
                                        idname = get_base_idname(fullnm),
                                        selected_ids = id,
                                        join_algorithm = 'cross_join')
    } else {
      qq = form_selector_query_secure_array(arrayname = fullnm,
                                            selected_ids = id,
                                            dataset_version = dataset_version)
    }
  }
  if (get_entity_infoArrayExists(entitynm)) {
    # join_info_unpivot(qq, arrayname = entitynm,
    #                   mandatory_fields_only = mandatory_fields_only,
    #                   con = con)
    download_unpivot_info_join(qq = qq, arrayname = entitynm,
                              mandatory_fields_only = mandatory_fields_only,
                              con = con)
  } else {
    iquery(con$db, qq, return = TRUE)
  }
}

#' Returns dataset_ids by grep on name
#'
#' @param pattern pattern to search by
#' @param con connection object
#' @param ... additional parameters to grep like \code{ignore.case}, \code{perl}. See documentation for \code{grep}
#' @export
find_dataset_id_by_grep = function(pattern, con = NULL, ...) {
  con = revealgenomics:::use_ghEnv_if_null(con = con)
  dx = iquery(con$db,
              paste0(
                "project(",
                revealgenomics:::custom_scan(), "(", revealgenomics:::full_arrayname(entitynm = .ghEnv$meta$arrDataset), "), ",
                "name)"),
              return = TRUE)
  dx[grep(pattern, dx$name, ...), ]$dataset_id
}

#' Returns project_ids by grep on name
#'
#' @param pattern pattern to search by
#' @param con connection object
#' @param ... additional parameters to grep like \code{ignore.case}, \code{perl}. See documentation for \code{grep}
#' @export
find_project_id_by_grep = function(pattern, con = NULL, ...) {
  con = revealgenomics:::use_ghEnv_if_null(con = con)
  dx = iquery(con$db,
              paste0(
                "project(",
                revealgenomics:::full_arrayname(entitynm = .ghEnv$meta$arrProject), ", ",
                "name)"),
              return = TRUE)
  dx[grep(pattern, dx$name, ...), ]$project_id
}

check_args_get = function(id, dataset_version, all_versions){
  if (is.null(id) & !is.null(dataset_version) & !all_versions) stop("null value of id is used to get all entities accessible to user. Cannot specify version")
  if (!is.null(dataset_version) & all_versions==TRUE) stop("Cannot specify specific dataset_version, and also set all_versions = TRUE")
}

get_versioned_secure_metadata_entity = function(entity, id,
                                                dataset_version,
                                                all_versions,
                                                mandatory_fields_only = FALSE,
                                                con = NULL){
  check_args_get(id = id, dataset_version, all_versions)
  df1 = select_from_1d_entity(entitynm = entity, id = id,
                             dataset_version = dataset_version,
                             mandatory_fields_only = mandatory_fields_only,
                             con = con)

  L1 = lapply(unique(df1$dataset_id),
              function(dataset_idi) {
                apply_definition_constraints(df1 = df1[df1$dataset_id == dataset_idi, ],
                                             dataset_id = dataset_idi,
                                             entity = entity,
                                             con = con)
              })
  df1 = do.call(what = "rbind",
                args = L1)

  if (!all_versions) return(latest_version(df1)) else return(df1)
}

#' @export
get_featuresets= function(featureset_id = NULL, con = NULL){
  select_from_1d_entity(entitynm = .ghEnv$meta$arrFeatureset,
                        id = featureset_id, con = con)
}

#' @export
get_referenceset = function(referenceset_id = NULL, con = NULL){
  select_from_1d_entity(entitynm = .ghEnv$meta$arrReferenceset, id =
                          referenceset_id, con = con)
}

#' @export
get_genelist = function(genelist_id = NULL, con = NULL) {
  con = use_ghEnv_if_null(con)

  left_arr = full_arrayname(.ghEnv$meta$arrGenelist)
  if (!is.null(genelist_id)) {
    condition = formulate_base_selection_query(fullarrayname = .ghEnv$meta$arrGenelist, id = genelist_id)
    left_arr = paste0("filter(", left_arr, "," , condition, ")")
  }
  right_arr = full_arrayname(.ghEnv$meta$arrGenelist_gene)
  gl = iquery(con$db, paste0("equi_join(", left_arr, " as l,
                                 grouped_aggregate(", right_arr, ", count(*) AS gene_count, genelist_id) as r,
                                 left_names: l.genelist_id, right_names:r.genelist_id,
                                 keep_dimensions:true,
                                 left_outer:true)"),
              return = T)
  gl = gl[, grep('instance_id|value_no', colnames(gl), invert = TRUE)]

  if (get_logged_in_user(con = con) %in% c('scidbadmin', 'root')) {
    return(gl)
  } else {
    return(gl[gl$public | gl$owner == get_logged_in_user(con = con), ])
  }
}

#' Gene list gene symbols visible to user
#'
#' Get data.frame containing summary of all gene-symbols (and genelist_id-s)
#' from genelist-s visible to user
#'
#' @return data.frame containing \code{genelist_id} and \code{gene_symbol}
#'
#' @export
get_genelist_gene_symbols = function(con = NULL) {
  con = use_ghEnv_if_null(con = con)
  res1 = drop_equi_join_dims(
    iquery(
      con$db,
      "project(
        filter(
          equi_join(
            grouped_aggregate(
              gh_public_rw.GENELIST_GENE,
              count(*), genelist_id, gene_symbol) as l,
            project(gh_public_rw.GENELIST, public, owner) as r,
            left_names: l.genelist_id, right_names:r.genelist_id),
        public=TRUE),
      genelist_id, gene_symbol)",
      return = TRUE
    )
  )
  res2 = drop_equi_join_dims(
    iquery(
      con$db,
      "project(
        equi_join(
          equi_join(
            grouped_aggregate(
              gh_public_rw.GENELIST_GENE,
              count(*), genelist_id, gene_symbol) as l,
            project(gh_public_rw.GENELIST, public, owner) as r,
            left_names: l.genelist_id, right_names:r.genelist_id),
          show_user(),
          left_names: owner, right_names:name),
      genelist_id,gene_symbol)",
      return = TRUE
    )
  )
  unique(rbind.fill(res1, res2))
}

#' @export
get_features = function(feature_id = NULL, mandatory_fields_only = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)

  entitynm = .ghEnv$meta$arrFeature
  arrayname = full_arrayname(entitynm)

  qq = arrayname
  if (!is.null(feature_id)) {
    qq = form_selector_query_1d_array(arrayname, get_base_idname(arrayname), as.integer(feature_id),
                                      join_algorithm = 'equi_join')

    # URL length restriction enforce by apache (see https://github.com/Paradigm4/<CUSTOMER>/issues/53)
    THRESH_query_len = 200000 # as set in /opt/rh/httpd24/root/etc/httpd/conf.d/25-default_ssl.conf

    if (stringi::stri_length(qq) >= THRESH_query_len) {
      selector = data.frame(feature_id = as.integer(feature_id),
                            val = 1,
                            stringsAsFactors = FALSE)
      xx = as.scidb(con$db, selector,
                    types = c('int64', 'int32'))

      ftr = iquery(
        con$db,
        paste0(
          "equi_join(", full_arrayname(.ghEnv$meta$arrFeature), " as l, ",
          xx@name,
          " as r, left_names:l.feature_id, right_names:r.feature_id, keep_dimensions:true)"),
        return = T)
      ftr = drop_equi_join_dims(ftr)
      if (mandatory_fields_only) {
        result = ftr
      } else {
        ftr_info = iquery(
          con$db,
          paste0(
            "equi_join(", full_arrayname(.ghEnv$meta$arrFeature), "_INFO as l, ",
            "project(", xx@name, ", feature_id) as r",
            ", left_names:l.feature_id, right_names:r.feature_id, keep_dimensions:true)"),
          return = T)
        ftr_info = drop_equi_join_dims(ftr_info)
        if (length(unique(ftr$feature_id)) < nrow(ftr)) { # feature_id-s are not unique (one feature_id mapping to multiple gene_symbol_id)
          cols_to_use = c(get_base_idname(arrayname), 'gene_symbol_id')
        } else {
          cols_to_use = get_base_idname(arrayname)
        }
        ftr_info = ftr_info[, c(cols_to_use, 'key', 'val')]
        ftr_info = ftr_info[!is.na(ftr_info$val) & ftr_info$val != "" & ftr_info$val != "NA", ]
        if (nrow(ftr) > 0 & nrow(ftr_info) > 0) {
          # Following extracted from `unpivot_key_value_pairs()`
          x2t = spread(ftr_info, "key", value = "val")
          x2t = x2t[, which(!(colnames(x2t) == "<NA>"))]
          result = merge(ftr, x2t, by = cols_to_use, all.x = T)
        } else {
          result = ftr
        }
      }
    } else {
      result = download_unpivot_info_join(
        qq = qq,
        arrayname = arrayname,
        mandatory_fields_only = mandatory_fields_only,
        con = con)
    }
  } else { # FASTER path when all data has to be downloaded
    result = iquery(con$db, qq, return = T)
    result = result[order(result$feature_id), ]
    if (!mandatory_fields_only) {
      ftr_info = iquery(con$db, paste(qq, "_INFO", sep=""), return = T)
      if (nrow(ftr_info) > 0) {
        ftr_info = ftr_info[, c('gene_symbol_id', 'feature_id', 'key', 'val')]
        ftr_info = ftr_info[!is.na(ftr_info$val) & ftr_info$val != "" & ftr_info$val != "NA", ]
        ftr_info = spread(ftr_info, "key", value = "val")
        if (length(unique(result$feature_id)) < nrow(result)) { # use basic R method because optimization in `else` clause has not been worked out for protein probes where one `feature_id` might point to multiple `gene_symbol_id`-s
          result = merge(result, ftr_info,
                         by = c(
                           get_base_idname(arrayname),
                           'gene_symbol_id'
                           ), all.x = T)
        } else {
          ftr_info = ftr_info[order(ftr_info$feature_id), ]
          result = rbind.fill(
            result[!(result$feature_id %in% ftr_info$feature_id), ],
            cbind(
              result[(result$feature_id %in% ftr_info$feature_id), ],
              ftr_info))
          result = result[order(result$feature_id), ]
        }
      }
    }
  }
  result
}

form_selector_query_secure_array = function(arrayname, selected_ids, dataset_version){
  selected_ids = unique(selected_ids)
  dataset_version = ifelse(is.null(dataset_version), "NULL", dataset_version)
  stopifnot(length(dataset_version) == 1)
  sorted=sort(selected_ids)
  breaks=c(0, which(diff(sorted)!=1), length(sorted))
  entitynm = strip_namespace(arrayname)
  if (is_entity_secured(entitynm)) {
    arrayname = paste0(custom_scan(), "(", arrayname, ")")
  }
  THRESH_K = 150  # limit at which to switch from filter to cross_join
  if (length(breaks) <= THRESH_K) { # completely contiguous set of tickers; use `between`
    subq = formulate_base_selection_query(entitynm, selected_ids)
    if (dataset_version != "NULL") {
      subq =  paste0("dataset_version=", dataset_version,
                              " AND ", subq)
    }
    query =  paste0("filter(", arrayname, ", ", subq, ")")
  } else { # mostly non-contiguous tickers, use `cross_join`
    # Formulate the cross_join query
    idname = get_base_idname(entitynm)
    diminfo = .ghEnv$meta$L$array[[entitynm]]$dims
    upload = sprintf("build(<%s:int64>[ARBITRARY_IDX=1:%d,100000,0],'[(%s)]', true)",
                     idname,
                     length(selected_ids),
                     paste(selected_ids, sep=",", collapse="),(")
    )
    apply_dataset_version = paste("apply(", upload, ", dataset_version, ", dataset_version, ")", sep = "")
    redim = paste("redimension(", apply_dataset_version, ", <ARBITRARY_IDX:int64>[", idname, "])", sep = "")

    query= paste0("cross_join(",
                 arrayname, " as A, ",
                 redim, " as B, ",
                 "A.", idname, ", " ,
                 "B.", idname,
                 ")")
    # Once project(ARRAY, -ARBITRARY_IDX) is possible (scidb 19.3), we can use that
  }
  query
}


form_selector_query_1d_array = function(arrayname, idname, selected_ids,
                                        join_algorithm = c('cross_join', 'equi_join')){
  join_algorithm = match.arg(join_algorithm)
  sorted=sort(selected_ids)
  breaks=c(0, which(diff(sorted)!=1), length(sorted))
  THRESH_K = 15  # limit at which to switch from cross_between_ to cross_join
  entitynm = strip_namespace(arrayname)
  if (is_entity_secured(entitynm)) {
    arrayname = paste0(custom_scan(), "(", arrayname, ")")
  }

  filter_string = tryCatch(expr = {
    formulate_base_selection_query(fullarrayname = arrayname,
                                    id = selected_ids)
    },
    error = function(e) {
    e
    }
  )
  if (!("error" %in% class(filter_string))) { # if we were able to create a filter string
    query =sprintf("filter(%s, %s)",
                   arrayname,
                   filter_string)
  } else { # mostly non-contiguous tickers, use `cross_join`
    upload = sprintf("build(<%s:int64>[ARBITRARY_IDX=1:%d,100000,0],'[(%s)]', true)",
                     idname,
                     length(selected_ids),
                     paste(selected_ids, sep=",", collapse="),("))
    if (join_algorithm == 'cross_join') {
      # Formulate the cross_join query
      redim = paste("redimension(", upload, ", <ARBITRARY_IDX:int64>[", idname,"])", sep = "")

      query= paste0("cross_join(",
                    arrayname, " as A, ",
                    redim, " as B, ",
                    "A.", idname, ", " ,
                    "B.", idname,
                    ")")
      # Once project(ARRAY, -ARBITRARY_IDX) is possible (scidb 19.3), we can use that
    } else if (join_algorithm == 'equi_join') {
      query= paste0("equi_join(",
                    arrayname, " as l, ",
                    upload, " as r, ",
                    "left_names:l.", idname, ", " ,
                    "right_names:r.", idname,
                    ", keep_dimensions:true)")
    }
  }
  query
}

check_args_search = function(dataset_version, all_versions){
  if (!is.null(dataset_version) & all_versions==TRUE) stop("Cannot specify specific dataset_version, and also set all_versions = TRUE")
}

latest_version = function(df){
  if (nrow(df) == 0) return(df)

  stopifnot(all(c("dataset_version", "dataset_id") %in% colnames(df)))
  df = df %>% group_by(dataset_id) %>% filter(dataset_version == max(dataset_version))
  drop_na_columns(as.data.frame(df))
}

#' internal function for `search_METADATA()``
filter_on_dataset_id_and_version = function(arrayname,
                                            dataset_id,
                                            dataset_version,
                                            con = NULL){
  con = use_ghEnv_if_null(con)

  qq = arrayname
  if (!is.null(dataset_id)) {
    fullnm = paste0(custom_scan(), "(", full_arrayname(qq), ")")
    if (is.null(dataset_version)) {
      qq = paste0("filter(", fullnm, ", ", "dataset_id = ", dataset_id, ")")
    } else {
      qq = paste0("filter(", fullnm, ", dataset_version=", dataset_version, " AND dataset_id = ", dataset_id, ")")
    }
  } else {
    stop(cat("Must specify dataset_id. To retrieve all ", tolower(arrayname), "s, use get_", tolower(arrayname), "s()", sep = ""))
  }

  download_unpivot_info_join(qq = qq, arrayname = arrayname, con = con)
}

#' internal function for \code{search_METADATA()}
#'
#' internal function for \code{search_individuals()}, \code{search_biosamples()} etc.
#' search of a metadata entry by \code{dataset_id}
search_versioned_secure_metadata_entity = function(entity,
                                                   dataset_id,
                                                   dataset_version,
                                                   all_versions,
                                                   con = NULL) {
  check_args_search(dataset_version, all_versions)
  df1 = filter_on_dataset_id_and_version(arrayname = entity, dataset_id,
                                        dataset_version = dataset_version,
                                        con = con)

  run_common_operations_on_search_metadata_output(df1 = df1,
                                                  dataset_id = dataset_id,
                                                  all_versions = all_versions,
                                                  entity = entity,
                                                  con = con)
}


run_common_operations_on_search_metadata_output = function(df1, dataset_id, entity, all_versions, con) {
  # reorder the output by the dimensions
  # from https://stackoverflow.com/questions/17310998/sort-a-dataframe-in-r-by-a-dynamic-set-of-columns-named-in-another-data-frame
  if (nrow(df1) == 0) return(df1)
  df1 = df1[do.call(order, df1[get_idname(entity)]), ]

  if (!is.null(dataset_id)) {
    df1 = apply_definition_constraints(df1 = df1,
                                       dataset_id = dataset_id,
                                       entity = entity,
                                       con = con)
  } else {
    L1 = lapply(unique(df1$dataset_id),
                function(dataset_idi) {
                  apply_definition_constraints(df1 = df1[df1$dataset_id == dataset_idi, ],
                                               dataset_id = dataset_idi,
                                               entity = entity,
                                               con = con)
                })
    df1 = do.call(what = "rbind",
                  args = L1)
  }

  if (!all_versions) return(latest_version(df1)) else return(df1)
}
#' internal function for \code{search_METADATA()} by set of requested attributes
#'
#' internal function for \code{search_individuals()}, \code{search_biosamples()} etc.
#' search of a metadata entry by \code{requested_attributes}
search_versioned_secure_metadata_entity_by_requested_attributes = function(entity,
                                                                           requested_attributes,
                                                                           dataset_id,
                                                                           dataset_version,
                                                                           all_versions,
                                                                           con) {
  con = use_ghEnv_if_null(con = con)
  idname = get_base_idname(entity)
  attrkey = search_metadata_attrkey(entity_id = get_entity_id(entity), con = con)
  m1 = find_matches_and_return_indices(attrkey$metadata_attrkey, requested_attributes)
  if (length(m1$source_matched_idx) == 0) {
    stop("None of requested attributes match attributes present in DB for requested entity:", entity,
         ". Try running the function: \n search_attributes(entity = '", entity, "')")
  }
  attrkey = attrkey[attrkey$metadata_attrkey %in% requested_attributes, ]
  selection_query = gsub(idname, "key_id",
                         formulate_base_selection_query(fullarrayname = entity, id = attrkey$metadata_attrkey_id))
  filter_info_query = paste0(
    "filter(",
    custom_scan(), "(",
    full_arrayname(entity), "_INFO),",
    selection_query, ")")
  if (!is.null(dataset_id)) {
    if (length(dataset_id) != 1) stop("Expect to search one dataset at a time")
    filter_info_query = paste0(
      "filter(",
      filter_info_query,
      ", dataset_id = ",
      dataset_id,
      ")"
    )
  }
  filter_info_df = iquery(con$db, filter_info_query, return = T)
  filter_info_df = spread(filter_info_df[, c(idname, 'key', 'val')], "key", value = "val")

  stopifnot(length(unique(filter_info_df[, idname])) == nrow(filter_info_df))
  filter_info_df = filter_info_df[order(filter_info_df[, idname]), ]

  orig_array_df = get_entity(entity = entity, id = filter_info_df[, idname], mandatory_fields_only = T, con = con)
  orig_array_df = orig_array_df[order(orig_array_df[, idname]), ]

  stopifnot(nrow(orig_array_df) == nrow(filter_info_df))

  returned_cols = requested_attributes[(requested_attributes %in% colnames(filter_info_df))]
  if (length(returned_cols) == 1) {
    filter_info_df = data.frame('val' = filter_info_df[, returned_cols],
                                stringsAsFactors = FALSE)
    filter_info_df = plyr::rename(filter_info_df, c('val' = returned_cols))
  } else {
    filter_info_df = filter_info_df[, returned_cols]
  }

  df1 = cbind(orig_array_df, filter_info_df)

  run_common_operations_on_search_metadata_output(df1 = df1, all_versions = all_versions,
                                                  dataset_id = NULL,
                                                  entity = entity,
                                                  con = con)
}

# dataset_version: can be "NULL" or any single integral value (if "NULL", then all versions would be returned back)
cross_join_select_by_two_dims = function(qq, tt, val1, val2, selected_names, dataset_version, con = NULL){
  con = use_ghEnv_if_null(con)

  selector = merge(
    data.frame(dataset_version = dataset_version),
    merge(data.frame(val1 = val1),
          data.frame(val2 = val2)))
  selected_names_all = c('dataset_version', selected_names)
  colnames(selector) = selected_names_all
  selector$flag = -1

  xx = as.scidb_int64_cols(con$db,
                           df1 = selector,
                           int64_cols = colnames(selector))
  xx1 = xx

  dims0 = scidb::schema(tt, "dimensions")$name
  selectpos = which(dims0 %in% selected_names)
  stopifnot(dims0[selectpos] == selected_names)
  cs = scidb::schema(tt, "dimensions")$chunk
  diminfo = data.frame(start = scidb::schema(tt, "dimensions")$start,
                       end = scidb::schema(tt, "dimensions")$end, stringsAsFactors = FALSE)
  ovlp = scidb::schema(tt, "dimensions")$overlap
  newdim = paste(selected_names_all, collapse = ",")
  newsch = paste("<flag:int64>[", newdim, "]", sep="")
  qq2 = paste0("redimension(", xx1@name, ", ", newsch, ")")

  subq = paste(sapply(selected_names_all, FUN=function(x) {paste(paste("A.", x, sep=""), paste("B.", x, sep=""), sep=", ")}), collapse=", ")
  qq = paste("cross_join(",
             qq, " as A,",
             qq2, " as B,", subq, ")", sep="")
  qq = paste("project(", qq, ", ", scidb::schema(tt, "attributes")$name, ")")

  iquery(con$db, qq, return = T)
}

unpivot = function(df1, arrayname) {
  unpivot = TRUE
  idname = get_idname(arrayname)

  if (nrow(df1) > 0 & sum(colnames(df1) %in% c("key", "val")) == 2 & unpivot){ # If key val pairs exist and need to be unpivoted
    if (exists('debug_trace')) {t1 = proc.time()}
    x3 = unpivot_key_value_pairs(df = df1, arrayname = arrayname)
    if (exists('debug_trace')) {cat("unpivot:\n"); print( proc.time()-t1 )}
  } else {
    if (nrow(df1) > 0) {
      cols_to_pick =  c(idname,
                        names(.ghEnv$meta$L$array[[strip_namespace(
                          arrayname)]]$attributes))
    } else {
      if (all(idname %in% colnames(df1))) {
        cols_to_pick =  c(idname,
                          names(.ghEnv$meta$L$array[[strip_namespace(
                            arrayname)]]$attributes))
      } else {
        cols_to_pick =  names(.ghEnv$meta$L$array[[strip_namespace(
                            arrayname)]]$attributes)
      }
    }
    x3 = df1[, cols_to_pick]
  }
  x3
}

#' Join flex fields
#'
#' @param replicate_query_on_info_array when joining info array, replicate query carried
#'                                      out on primary array
#'                                      e.g. `filter(gh_secure.BIOSAMPLE, dataset_id=32)`
#'                                      replicates to `filter(gh_secure.BIOSAMPLE_INFO, dataset_id=32)`.
#'                                      Turned off by default
join_info = function(qq, arrayname,
                     mandatory_fields_only = FALSE,
                     replicate_query_on_info_array = FALSE,
                     con = NULL) {
  con = use_ghEnv_if_null(con)
  # Join INFO array
  qq1 = qq
  if (!mandatory_fields_only) {
    entitynm = strip_namespace(arrayname)
    if (is_entity_secured(entitynm)) {
      info_array = paste0(custom_scan(), "(", full_arrayname(entitynm), "_INFO)")
    } else {
      info_array = paste0(                    full_arrayname(entitynm), "_INFO" )
    }
    idname = get_idname(arrayname)

    if (exists('debug_trace')) {t1 = proc.time()}
    if (FALSE){ # TODO: See why search_individuals(dataset_id = 1) is so slow; the two options here did not make a difference
      stop("have not tested secure_scan code-path yet")
      qq1 = paste("cross_join(", qq1, " as A, ", info_array, " as B, A.", idname, ", B.", idname, ")", sep = "")
    } else if (FALSE) { cat("== Swapping order of cross join between ARRAY_INFO and select(ARRAY, ..)\n")
      stop("have not tested secure_scan code-path yet")
      qq1 = paste("cross_join(", info_array, " as A, ", qq1, " as B, A.", idname, ", B.", idname, ")", sep = "")
    } else {
      if (replicate_query_on_info_array) {
        info_array_query = gsub(pattern = entitynm,
                                replacement = paste0(entitynm, "_INFO" ),
                                x = qq1)
      } else {
        info_array_query = info_array
      }
      qq1 = paste0("equi_join(", qq1, " as l, ", info_array_query,
                  " as r, left_names:(", paste(paste0('l.', idname), collapse = ","),
                  "), right_names:(", paste(paste0('r.', idname), collapse = ","),  "), left_outer:true, keep_dimensions:true)")
    }
  }
  x2 = iquery(con$db, qq1, return = TRUE)

  if (exists('debug_trace')) {cat("join with info:\n"); print( proc.time()-t1 )}
  x2
}

#' Join flex fields and unpivot
#'
#' @param replicate_query_on_info_array see description at [join_info()]
join_info_unpivot = function(qq, arrayname,
                              mandatory_fields_only = FALSE,
                              replicate_query_on_info_array = FALSE,
                              profile_timing = FALSE,
                              con = NULL) {
  if (profile_timing) {cat(paste0("Array: ", arrayname, "\n"))}
  t1 = proc.time()
  df1 = join_info(qq = qq, arrayname = arrayname,
                  mandatory_fields_only = mandatory_fields_only,
                  replicate_query_on_info_array = replicate_query_on_info_array,
                  con = con)
  if (profile_timing) {cat(paste0("join_info time: ", (proc.time()-t1)[3], "\n"))}

  t1 = proc.time()
  df2 = unpivot(df1, arrayname = arrayname)
  if (profile_timing) {cat(paste0("unpivot time: ", (proc.time()-t1)[3], "\n"))}

  # t1 = proc.time()
  # res = join_ontology_terms(df = df2, con = con)
  # if (profile_timing) {cat(paste0("join_ontology_terms time: ", (proc.time()-t1)[3], "\n"))}

  df2
}

#' Download from scidb then unpivot info and join in R
#'
#' @param qq query on primary array (allied downloaad will be from \code{_INFO} array)
#' @param arrayname primary arrayname e.g. \code{BIOSAMPLE, INDIVIDUAL}
#' @param algo_choice_project_primary_id_then_download_attrs_only flag to control iquery download mechanism
#' @param con connection object (optional when using \code{rg_connect()})
#'
#' @examples
#' \dontrun{
#' download_unpivot_info_join(qq = "filter(secure_scan(gh_secure.BIOSAMPLE), dataset_id = 11)",
#'                                 arrayname = 'BIOSAMPLE')
#' }
download_unpivot_info_join = function(qq,
                                      arrayname,
                                      mandatory_fields_only = FALSE,
                                      algo_choice_project_primary_id_then_download_attrs_only = FALSE,
                                      con = NULL) {
  con = use_ghEnv_if_null(con = con)
  res1 = iquery(con$db, qq, return = TRUE)
  res1[, 'ARBITRARY_IDX'] = NULL # in case this was introduced by the `cross_join`
                                 # Once `project(ARRAY, -ARBITRARY_IDX)` is possible (scidb 19.3), we can skip this step

  if (mandatory_fields_only | # if user requested mandatory fields only
      !.ghEnv$meta$L$array[[strip_namespace(arrayname)]]$infoArray) { # if flex fields do not exist for given entity
    res_df = res1
  } else { # try joining with INFO array
    res2 = tryCatch({ # to capture case when download of INFO array fails due to very large size
      if (algo_choice_project_primary_id_then_download_attrs_only) {
        # iquery with return attributes only
        info_query = paste0(
          "apply(",
          gsub(arrayname, paste0(arrayname, "_INFO"), qq),
          ", ", get_base_idname(arrayname), ", ", get_base_idname(arrayname), ")"
        )
        res2_schema = paste0(
          "<key:string, val:string, ", get_base_idname(arrayname), ":int64>[i]"
        )
        iquery(con$db, info_query, return = TRUE, only_attributes = TRUE,
               schema = res2_schema)
      } else {
        info_query = gsub(arrayname, paste0(arrayname, "_INFO"), qq)
        if (grepl("^equi_join", info_query)) {
          res2 = iquery(con$db, info_query, return = TRUE)
        } else {
          if (grepl("^cross_join", info_query)) {
            res2_schema = paste0(
              "<key:string, val:string, ARBITRARY_IDX:int64>[",
              paste0(pretty_print(revealgenomics:::get_idname(arrayname)), ", key_id]")
            )
          } else {
            res2_schema = paste0(
              "<key:string, val:string>[",
              paste0(pretty_print(revealgenomics:::get_idname(arrayname)), ", key_id]")
            )
          }
          res2 = iquery(con$db, info_query, return = TRUE,
                        schema = res2_schema)
        }
        # Drop the unnecessary ID-s
        if (strip_namespace(arrayname) == .ghEnv$meta$arrFeature) {
          # special handling because feature_id might not be unique when one protein probe can map to multiple genes
          cols_for_use = c(get_base_idname(arrayname), 'gene_symbol_id')
        } else {
          cols_for_use = get_base_idname(arrayname)
        }
        res2[, c(cols_for_use, 'key', 'val')]
      }
    }, error = function(e) {
      print(e)
      return(NULL)
    })

    # Merge with INFO array in R
    if (is.null(res2)) {
      res1$more_cols_in_db = TRUE
      res_df = res1
    } else if (nrow(res2) > 0) {
      res2_t = spread(res2, "key", value = "val")
      if (exists('debug_trace')) {t1 = proc.time()}
      res_df = merge(res1, res2_t, by = cols_for_use, all.x = TRUE)
      if (exists('debug_trace')) {cat("join with info in R:\n"); print( proc.time()-t1 )}
    } else {
      res_df = res1
    }
  }
  return(res_df)
}

unpivot_key_value_pairs = function(df, arrayname, key_col = "key", val = "val"){
  idname = get_idname(arrayname)

  # print("DEBUG: df")
  # print(head(df))
  # print(class(df))
  dt = data.table(df)
  setkeyv(dt, c(idname, key_col))
  # print("DEBUG: dt")
  # print(class(dt))
  # print(head(dt))
  x2s = dt[,val, by=c(idname, key_col)]
  # print("DEBUG: x2s")
  # print(head(x2s))
  # print(class(x2s))
  x2t = as.data.frame(spread(x2s, "key", value = "val"))
  # head(x2t)
  x2t = x2t[, which(!(colnames(x2t) == "<NA>"))]
  # tail(x2t)

  x4 = df[, c(get_idname(arrayname), names(.ghEnv$meta$L$array[[strip_namespace(arrayname)]]$attributes))]
  x4 = x4[!duplicated(x4[, idname]), ]

  if (is.data.frame(x2t)) x5 = merge(x4, x2t, by = idname) else x5 = x4
  return(x5)
}

# Check that parent entity exists at id-s specified in dataframe
check_entity_exists_at_id = function(entity, id, ...){
  df1 = get_entity(entity = entity, id = id, ...)
  if (nrow(df1) == 0) {
    stop("No entries for entity ", entity, " at any of the specified ids / version")
  } else if (nrow(df1) != length(id)) {
    req_ids = unique(sort(id))
    returned_ids = unique(sort(df1[, get_base_idname(entity)]))
    missing_ids = req_ids[which(!(req_ids %in% returned_ids))]
    if (is_entity_versioned(entity)) {
      stop(cat("No entries for entity", entity, "at ids:", pretty_print(missing_ids),
               "at specified version", sep = " "))
    } else {
      stop(cat("No entries for entity", entity, "at ids:", pretty_print(missing_ids),
               sep = " "))
    }
  } else {
    return(TRUE)
  }
}

# Following:
# http://www.bioconductor.org/packages/release/bioc/vignettes/Biobase/inst/doc/ExpressionSetIntroduction.pdf
convertToExpressionSet = function(expr_df, biosample_df, feature_df){

  #############################################
  ## Step 0 # Retain biosample and feature info for returned data
  feature_df =   drop_na_columns(
    feature_df[match(unique(expr_df$feature_id), feature_df$feature_id), ])
  biosample_df = drop_na_columns(
    biosample_df[match(unique(expr_df$biosample_id), biosample_df$biosample_id), ])

  #############################################
  ## Step 1 # Convert data frame to matrix
  # stopifnot(nrow(expr_df) == length(biosample_df$biosample_id) * length(feature_df$feature_id))
  exprs = acast(expr_df, feature_id~biosample_id, value.var="value")

  ##! Start fix code - Do the checking on the casted data frame instead!
  stopifnot( nrow(exprs) == nrow(feature_df) )
  stopifnot( ncol(exprs) == nrow(biosample_df) )

  ## And let's at least provide a message to the console if we encounter this in any other study, since this is relevant for debugging! (-:

  NAs.found <- apply( exprs, 1, function(x) { sum(is.na(x)) })
  if(sum(NAs.found)>0) {
    NAs.found <- NAs.found[NAs.found != 0]
    NAs.name <- feature_df %>%
      dplyr::filter(feature_id %in% names(NAs.found)) %>%
      dplyr::mutate(feature_id = factor(.data$feature_id, levels = names(NAs.found))) %>%
      dplyr::arrange(feature_id) %>%
      dplyr::pull(name) %>%
      as.character(.)
    if (getOption("revealgenomics.debug", FALSE)) {
      message(
        paste0("[convertToExpressionSet] ", NAs.found, "x empty entries found for feature_id ", names(NAs.found), " (", NAs.name, ")", sep="\n")
      )
    }
  }

  # Convert column name to biosample id name
  selected_bios = as.integer(colnames(exprs))
  pos = match(selected_bios, biosample_df$biosample_id)

  selected_bios = as.integer(colnames(exprs))
  pos = match(selected_bios, biosample_df$biosample_id)

  # In case phenotype data is a dataframe with merged INDIVIDUAL and BIOSAMPLE info
  # it is likely that `sample_name` column was used for disambiguating INDIVIDUAL.name
  # and BIOSAMPLE.name. Use that when available
  nameCol<- c("sample_name", "name")
  nameCol <- nameCol[ nameCol %in% colnames(biosample_df) ]
  if(length(nameCol) == 2) {
    nameCol <- "sample_name"
  }
  stopifnot(length(nameCol) == 1)

  colnames(exprs) = biosample_df[pos, nameCol]

  # Convert row names to feature name
  sel_fx = as.integer(rownames(exprs))
  # Get the position of the features
  fpos = match(sel_fx, feature_df$feature_id)
  fData = feature_df[fpos, ]

  if (length(unique(fData$gene_symbol)) == length(unique(fData$name))) {
    row.names(exprs) = fData$gene_symbol
    rownames(fData) = fData$gene_symbol
  } else {
    row.names(exprs) = fData$name
    rownames(fData) = fData$name
  }

  #############################################
  ## Step 2 # Get phenotype data
  pData = biosample_df[pos, ]

  # Run a check first
  # all(pData$biosample_id==colnames(exprs))
  rownames(pData) = pData[, nameCol]

  metadata = data.frame(labelDescription=colnames(pData), row.names = colnames(pData))
  phenoData <- new("AnnotatedDataFrame",
                   data=pData, varMetadata=metadata)

  #############################################
  ## Step 3 # Feature data
  metadata = data.frame(labelDescription = colnames(fData), row.names = colnames(fData))
  featureData = new("AnnotatedDataFrame",
                    data=fData, varMetadata = metadata)
  #############################################
  ## Step X # Convert to ExpressionSet
  exampleSet <- ExpressionSet(assayData=exprs,
                              phenoData=phenoData,
                              featureData=featureData)
  exampleSet
}


#' @export
update_entity = function(entity, df, con = NULL){
  update_mandatory_and_info_fields(df = df, arrayname = full_arrayname(entity), con = con)
}

get_entity_count_old = function(con = NULL){
  con = use_ghEnv_if_null(con)

  entities = c(.ghEnv$meta$arrProject,
               .ghEnv$meta$arrDataset,
               .ghEnv$meta$arrIndividuals,
               .ghEnv$meta$arrBiosample,
               .ghEnv$meta$arrMeasurementSet,
               .ghEnv$meta$arrExperimentSet)
  if (length(con$cache$nmsp_list) == 1){
    nmsp = con$cache$nmsp_list
    queries = sapply(entities, function(entity){paste("op_count(", nmsp, ".", entity, ")", sep = "")})
  }
  else if (length(con$cache$nmsp_list) == 2){
    queries = sapply(entities, FUN = function(entity) {
      paste("join(",
            paste(sapply(con$cache$nmsp_list, function(nmsp){paste("op_count(", nmsp, ".", entity, ")", sep = "")}), collapse=", "),
            ")", sep = "")
    })
  } else {
    stop("Not covered yet")
  }
  qq = queries[1]
  for (query in queries[2:length(queries)]){
    qq = paste("join(", qq, ", ", query, ")", sep = "" )
  }
  attrs = paste(
    sapply(entities, FUN=function(entity) {paste(entity, "_", con$cache$nmsp_list, sep = "")}),
    ":uint64",
    sep = "",
    collapse = ", ")
  res_schema = paste("<", attrs, ">[i=0:0,1,0]", sep = "")
  res = iquery(con$db, qq,
               schema = res_schema, return = T)
  colnames(res) = gsub("X_", "", colnames(res))

  xx = sapply(entities, FUN=function(entity) {res[, paste(entity, con$cache$nmsp_list, sep = "_")]})

  counts = data.frame(entity = entities)
  if (length(con$cache$nmsp_list) == 1){
    counts[, con$cache$nmsp_list] = as.integer(xx)
  } else {
    rownum = 1
    for (nmsp in con$cache$nmsp_list){
      counts[, nmsp] = as.integer(xx[rownum,])
      rownum = rownum + 1
    }
  }
  counts
}

#' @export
get_entity_count = function(new_function = FALSE, skip_measurement_data = TRUE, con = NULL){
  con = use_ghEnv_if_null(con)

  if (!new_function) {
    get_entity_count_old(con = con)
  } else {
    all_entities = get_entity_names()
    if (skip_measurement_data) {
      msrmnt_entities = get_entity_names(data_class = 'measurementdata')
      entities = all_entities[!(all_entities %in% msrmnt_entities)]
    } else {
      entities = all_entities
    }
    entity_arrays = unlist(
      sapply(entities, function(entity) {
        paste(.ghEnv$meta$L$array[[entity]]$namespace, entity, sep = ".")
      }))
    queries = paste("op_count(", entity_arrays, ")", sep = "")
    qq = queries[1]
    for (query in queries[2:length(queries)]){
      qq = paste("join(", qq, ", ", query, ")", sep = "" )
    }
    res = iquery(con$db, qq, return = T)

    colnames(res) = c('i', entity_arrays)

    res2 = data.frame(entity = entities)
    rownames(res2) = 1:nrow(res2)

    counts = t(sapply(res2$entity, function(entity){
      sapply(con$cache$nmsp_list, function(nmsp){
        fullarnm = paste(nmsp, entity, sep = ".")
        ifelse(fullarnm %in% colnames(res), res[, fullarnm], NA)
      })
    }))

    res2 = cbind(res2, counts)
    res2
  }
}

#' check if two API results are identical
#'
#' @param df1 data-frame 1
#' @param df2 data-frame 2
#' @param entity entity for the data-frames that are being compared (e.g. \code{BIOSAMPLE, MEASUREMENTSET} etc.)
#'
#' @examples
#' \dontrun{
#' b1 = search_biosamples(dataset_id = 1)
#' b2 = get_biosamples()
#' b2 = b2[b2$dataset_id ==1, ]
#' check_if_revealgenomics_dataframes_equal(b1, b2, 'BIOSAMPLE')
#' }
check_if_revealgenomics_dataframes_equal = function(df1, df2, entity) {
  if (identical(dim(df1), dim(df2))) {
    df1 = df1[order(df1[, revealgenomics:::get_base_idname(entity)]), ]; rownames(df1) = 1:nrow(df1);
    df2 = df2[order(df2[, revealgenomics:::get_base_idname(entity)]), ]; rownames(df2) = 1:nrow(df2); df2 = df2[, colnames(df1)]
    return(all.equal(df1, df2))
  } else {
    return(FALSE)
  }
}
