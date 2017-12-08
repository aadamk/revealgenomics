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

#' @export
gh_connect = function(username, password = NULL, host = NULL, port = NULL, protocol = "https"){
  # SciDB connection and R API --
  
  con = NULL
  
  # ask for password interactively if none supplied
  # https://github.com/Paradigm4/SciDBR/issues/154#issuecomment-327989402
  if (is.null(password)) {
    if (rstudioapi::isAvailable()) { # In RStudio, 
      password = rstudioapi::askForPassword("Password")
    } else { # in base R
      password = getpwd()
    } # Rscripts and knitr not yet supported
  }
  
  if (is.null(password)) { # if still null password
    stop("Password cannot be null")
  }
    
  # Attempt 1. 
  err1 = tryCatch({
    if (is.null(host) & is.null(port)) {
      # If user did not specify host and port, try default host for scidbconnect at port 8083
      con$db = scidbconnect(username = username, password = password, port = 8083, protocol = protocol)
    } else {
      # If user specified host and port, try user supplied parameters
      con$db = scidbconnect(host = host, username = username, password = password, port = port, protocol = protocol)
    }
  }, error = function(e) {return(e)}
  )
  
  # If previous attempts did not work, maybe port 8083 was forwarded (hard-coded to /shim below)
  if ("error" %in% class(err1) & is.null(host) & is.null(port)){
    err2 = tryCatch({ 
      con$db = scidbconnect(protocol = protocol, 
                               host = '127.0.0.1/shim/', , 
                               port = NULL,  
                               username = username, 
                               password = password) 
    }, error = function(e) {return(e)}
    )
  } else if ("error" %in% class(err1)) {
    err2 = tryCatch({stop("could not connect via user supplied parameters")}, 
                    error = function(e) {return(e)}
    )
  } else {
    err2 = FALSE
  }
  
  
  if ("error" %in% class(err2)) {
    print(err1);
    print(err2);
    con$db = NULL
  } else {
    all_nmsp = iquery(con$db, "list('namespaces')", schema = "<name:string NOT NULL> [No=0:1,2,0]", return = T)$name
  }
  
  # Store a copy of connection object in .ghEnv
  # Multi-session programs like Shiny, and the `gh_connect2` call need to explicitly delete this after gh_connect()
  if (TRUE) {
    .ghEnv$db = con$db
  }
  return(con)
}

#' @export
gh_connect2 = function(username, password = NULL, host = NULL, port = NULL, protocol = "https") {
  con = gh_connect(username, password, host, port, protocol)
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
  } else if (entity == 'FEATURE') {
    f(id, fromCache =  FALSE, ...)
  } else { 
    f(id, ...) 
  }
}

## This is a function that should go into the "functions.R" file and be an 
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

# cat("Downloading reference dataframes for fast ExpressionSet formation\n")
get_feature_from_cache = function(updateCache = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (updateCache | is.null(.ghEnv$cache$feature_ref)){
    update_feature_cache(con = con)
  }
  return(.ghEnv$cache$feature_ref)
}

update_feature_cache = function(con = NULL){
  con = use_ghEnv_if_null(con)
  
  .ghEnv$cache$feature_ref = NULL
  .ghEnv$cache$feature_ref = get_features(fromCache = FALSE, con = con)
}

get_biosamples_from_cache = function(updateCache = FALSE, con = NULL){
  if (updateCache | is.null(.ghEnv$cache$biosample_ref)){
    update_biosample_cache(con = con)
  }
  return(.ghEnv$cache$biosample_ref)
}

update_biosample_cache = function(con = NULL){
  con = use_ghEnv_if_null(con)
  .ghEnv$cache$biosample_ref = dao_get_biosample(con = con)
}

get_ontology_from_cache = function(updateCache = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (updateCache | is.null(.ghEnv$cache$dfOntology)){
    update_ontology_cache(con = con)
  }
  if (nrow(.ghEnv$cache$dfOntology) == 0) update_ontology_cache(con = con)
  return(.ghEnv$cache$dfOntology)
}

#' @export
get_ontology = function(ontology_id = NULL, updateCache = FALSE, con = NULL){
  dfOntology = get_ontology_from_cache(updateCache, con = con)
  if (!is.null(ontology_id)){
    matches = match(ontology_id, dfOntology$ontology_id)
    matches = matches[which(!is.na(matches))]
    dfOntology[matches, ]
  } else {
    dfOntology
  }
}

find_namespace = function(entitynm) {
  .ghEnv$meta$L$array[[entitynm]]$namespace
}

#' full name of array with namespace
#' 
#' @export
full_arrayname = function(entitynm) {
  paste0(find_namespace(entitynm), ".", entitynm)
}

update_ontology_cache = function(con = NULL){
  con = use_ghEnv_if_null(con)
  
  zz = iquery(con$db, full_arrayname(.ghEnv$meta$arrOntology), return = TRUE)
  if (nrow(zz) > 1) zz = zz[order(zz[, get_idname(.ghEnv$meta$arrOntology)]), ]
  .ghEnv$cache$dfOntology = zz
}

get_feature_synonym_from_cache = function(updateCache = FALSE, con = NULL){
  if (updateCache | is.null(.ghEnv$cache$dfFeatureSynonym)){
    update_feature_synonym_cache(con = con)
  }
  return(.ghEnv$cache$dfFeatureSynonym)
}
update_feature_synonym_cache = function(con = NULL){
  con = use_ghEnv_if_null(con)
  
  .ghEnv$cache$dfFeatureSynonym = iquery(con$db, .ghEnv$meta$arrFeatureSynonym, return = TRUE)
}

#' @export
scidb_exists_array = function(arrayName, con = NULL) {
  con = use_ghEnv_if_null(con)
  
  !is.null(tryCatch({iquery(con$db, paste("show(", arrayName, ")", sep=""), return=TRUE, binary = FALSE)}, error = function(e) {NULL}))
}

get_max_id = function(arrayname, con = NULL){
  con = use_ghEnv_if_null(con)
  
  max = iquery(con$db, paste("aggregate(apply(", arrayname,
                             ", id, ", get_base_idname(arrayname), "), ",
                             "max(id))", sep=""), return=TRUE)$id_max
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
  x = scidb_attribute_rename(arr = x, old = "updated", new = "updated_old", con = con)
  qq = paste0("apply(", x@name, ", updated, string(now()))")
  qq = paste0("redimension(", qq, ", ", scidb::schema(scidb(con$db, arrayname)), ")")
  
  query = paste("insert(", qq, ", ", arrayname, ")", sep="")
  iquery(con$db, query)
}

register_tuple = function(df, ids_int64_conv, arrayname, con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (nrow(df) < 100000) {x1 = as.scidb(con$db, df)} else {x1 = as.scidb(con$db, df, chunk_size=nrow(df))}
  
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

#' @export
register_project = function(df,
                            only_test = FALSE, 
                            con = NULL){
  con = use_ghEnv_if_null(con)
  
  uniq = unique_fields()[[.ghEnv$meta$arrProject]]
  test_register_project(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrProject)
    register_tuple_return_id(df, arrayname, uniq, con = con)
  } # end of if (!only_test)
}

#' @export
register_dataset = function(df,
                            public = FALSE,
                            dataset_version = 1,
                            only_test = FALSE,
                            con = NULL
){
  con = use_ghEnv_if_null(con)
  
  stopifnot(class(public) == 'logical')
  
  uniq = unique_fields()[[.ghEnv$meta$arrDataset]]
  
  df$public = public
  test_register_dataset(df, uniq, dataset_version, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrDataset)
    register_tuple_return_id(df, arrayname, uniq, dataset_version = dataset_version, con = con)
  } # end of if (!only_test)
}

#' @export
register_ontology_term = function(df, only_test = FALSE, con = NULL){
  uniq = unique_fields()[[.ghEnv$meta$arrOntology]]
  test_register_ontology(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrOntology)
    ids = register_tuple_return_id(df, arrayname, uniq, con = con)
    
    # force update the ontology
    update_ontology_cache(con = con)
    
    return(ids)
  } # end of if (!only_test)
  
  
}

#' @export
register_individual = function(df, 
                               dataset_version = NULL,
                               only_test = FALSE,
                               con = NULL){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrIndividuals, 
                                            df, dataset_version, only_test, con = con)
}

#' @export
register_referenceset = function(df, only_test = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  uniq = unique_fields()[[.ghEnv$meta$arrReferenceset]]
  test_register_referenceset(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrReferenceset)
    register_tuple_return_id(df,
                             arrayname, uniq, con = con)
  } # end of if (!only_test)
}

#' Register genelist
#' 
#' Preferred method of registering genelist-s is to 
#' provide one genelist_name and genelist_description
#' at a time.
#' 
#' Alternatively, you can supply a data.frame with [name, description] 
#' of one or more genelist(s)
#' 
#' @param genelist_name the name of the geneliest
#' @param genelist_description the description for the genelist
#' @param isPublic bool to denote if the genelist is public
#' @param df (optional) a data-frame containing [name, description] of the genelist
#' 
#' @export
register_genelist = function(genelist_name = NULL, 
                             genelist_description = NULL, 
                             isPublic = TRUE, 
                             df = NULL, 
                             only_test = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)

  if (!is.null(df) & 
      (!is.null(genelist_name) | !is.null(genelist_description))) {
    stop("Cannot supply both df and [genelist_name, genelist_description]. 
         Use one method for using this function")
  }
  
  df1 = df
  if (is.null(df1)) {
    df1 = data.frame(name = genelist_name, 
                   description = genelist_description,
                   public = isPublic,
                   stringsAsFactors = FALSE)
  }
  
  df1$owner = get_logged_in_user(con = con)
  
  uniq = unique_fields()[[.ghEnv$meta$arrGenelist]]
  test_register_genelist(df1, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrGenelist)
    register_tuple_return_id(df1,
                             arrayname, uniq, con = con)
  } # end of if (!only_test)
}

#' Register gene symbols in a gene-list
#' 
#' Preferred method of registering gene symbols in a genelist is to 
#' provide the target genelist_id and the symbols to be registered
#' (one genelist at a time)
#' 
#' Alternatively, one can supply a data.frame with [genelist_id, gene_symbol] 
#'  
#' @param genelist_id the id of the genelist (returned by `register_genelist()`)
#' @param gene_symbols the gene-symbols to be stored in a gene-list (e.g. `c('TSPAN6', 'KCNIP2', 'CFAP58', 'GOT1', 'CPN1', 'PSIP1P1')`)
#' @param df (optional) a data-frame containing [genelist_id, gene_symbol]
#'
#' @examples 
#' register_genelist_gene(genelist_id = 11, # must already exist in `genelist` table
#'                        gene_symbols = c('TSPAN6', 'KCNIP2'))
#'                        
#' @export
register_genelist_gene = function(genelist_id = NULL, 
                                  gene_symbols = NULL, 
                                  df = NULL, 
                                  only_test = FALSE, con = NULL){
  uniq = unique_fields()[[.ghEnv$meta$arrGenelist_gene]]
  if (!is.null(df) & 
      (!is.null(genelist_id) | !is.null(gene_symbols))) {
    stop("Cannot supply both df and [genelist_id, gene_symbols]. 
         Use one method for using this function")
  }

  if (is.null(df)) {
    df = data.frame(genelist_id = genelist_id, 
                    gene_symbol = gene_symbols, 
                    stringsAsFactors = FALSE)
  }
  
  test_register_genelist_gene(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrGenelist_gene)
    register_tuple_return_id(df,
                             arrayname, uniq, con = con)
  } # end of if (!only_test)
}

#' @export
register_featureset = function(df, only_test = FALSE, con = NULL){
  uniq = unique_fields()[[.ghEnv$meta$arrFeatureset]]
  test_register_featureset(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrFeatureset)
    register_tuple_return_id(df,
                             arrayname, uniq, con = con)
  } # end of if (!only_test)
}

register_feature_synonym = function(df, only_test = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  uniq = unique_fields()[[.ghEnv$meta$arrFeatureSynonym]]
  test_register_feature_synonym(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrFeatureSynonym)
    register_tuple_return_id(df, arrayname, uniq, con = con)
  } # end of if (!only_test)
}

#' @export
register_feature = function(df, register_gene_synonyms = TRUE, only_test = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  uniq = unique_fields()[[.ghEnv$meta$arrFeature]]
  test_register_feature(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrFeature)
    fid = register_tuple_return_id(df, arrayname, uniq, con = con)
    gene_ftrs = df[df$feature_type == 'gene', ]
    if (register_gene_synonyms & nrow(gene_ftrs) > 0){
      cat("Working on gene synonyms\n")
      df_syn = data.frame(synonym = gene_ftrs$name, 
                          feature_id = fid,
                          featureset_id = unique(gene_ftrs$featureset_id),
                          source = gene_ftrs$source,
                          stringsAsFactors = F)
      ftr_syn_id = register_feature_synonym(df = df_syn, con = con)
      output = list(feature_id = fid,
                    feature_synonym_id = ftr_syn_id)
    } else {
      output = fid
    }
    output
  } # end of if (!only_test)
}

#' @export
register_biosample = function(df, 
                              dataset_version = NULL,
                              only_test = FALSE,
                              con = NULL){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrBiosample, 
                                            df, dataset_version, only_test, con = con)
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
  update_tuple(df[, c(get_idname(arrayname),
                      mandatory_fields()[[strip_namespace(arrayname)]], 
                      'created', 'updated')], 
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
#' (2) updates lookup array (based on flag), [always FALSE in secure_scan branch] 
#' (3) registers flex fields
register_tuple_update_lookup = function(df, arrayname, updateLookup, con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (updateLookup) stop("unexpected in secure_scan branch")
  
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

  df = prep_df_fields(df, mandatory_fields)
  
  if (!is.null(dataset_version)) {
    df[, "dataset_version"] = dataset_version
  }
  
  if (is.null(uniq)){ # No need to match existing elements, just append data
    stop("Registering entity without a set of unique fields is not allowed")
  }
  
  # Find matches by set of unique fields provided by user
  xx = iquery(con$db, paste("project(", arrayname, ", ", 
                            paste(uniq[uniq!='dataset_id'], collapse = ", "), ")", sep = ""), return = TRUE)
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
    cur_dataset_id = unique(df$dataset_id)
    if (length(cur_dataset_id) != 1) stop("dataset_id of df to be uploaded should be unique")
    cur_max_ver_by_entity = max(xx[xx$dataset_id == cur_dataset_id, ]$dataset_version)
    if (dataset_version > cur_max_ver_by_entity) { # then need to register new versions for the matching entries at same entity_id
      cat("Entity does not have any entry at current version number\n")
      cat("Registering new versions of", nrow(df[matching_idx, ]), "entries into", arrayname, "at version", dataset_version, "\n")
      register_tuple_update_lookup(df = df[matching_idx, ], arrayname = arrayname, updateLookup = FALSE, con = con)
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
        register_tuple_update_lookup(df = dfx[nonmatching_idx_at_version, ], arrayname = arrayname, updateLookup = FALSE, con = con)
      }
    }
  } else {
    if (!is.null(dataset_version) & length(old_id) == 0) cat("No matching entries for versioned entity\n")
  }
  
  # Now assign new id-s for entries that did not match by unique fields
  if (length(nonmatching_idx) > 0 ) {
    cat("---", length(nonmatching_idx), "rows need to be registered from total of", nrow(df), "rows provided by user\n")
    # if (length(nonmatching_idx) != nrow(df)) {stop("Need to check code here")}
    new_id = get_max_id(arrayname, con = con) + 1:nrow(df[nonmatching_idx, ])
    df[nonmatching_idx, get_base_idname(arrayname)] = new_id
    
    register_tuple_update_lookup(df = df[nonmatching_idx, ], arrayname = arrayname, 
                                 updateLookup = FALSE, con = con)
  } else {
    cat("--- no completely new entries to register\n")
    new_id = NULL
  }
  return(df[, idname])
}

#' @export
register_variantset = function(df, dataset_version = NULL, only_test = FALSE, con = NULL){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrVariantset, 
                                            df, dataset_version, only_test, con = con)
}


#' @export
register_experimentset = function(df, dataset_version = NULL, only_test = FALSE, con = NULL){
  # Extra tests for ExperimentSet
  test_register_experimentset(df, silent = ifelse(only_test, FALSE, TRUE))
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrExperimentSet, 
                                            df, dataset_version, only_test, con = con)
}

#' @export
register_measurement = function(df, dataset_version = NULL, only_test = FALSE, con = NULL){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrMeasurement, 
                                            df, dataset_version, only_test, con = con)
}

#' @export
register_measurementset = function(df, dataset_version = NULL, only_test = FALSE, con = NULL){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrMeasurementSet, 
                                            df, dataset_version, only_test, con = con)
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
      dataset_version = get_dataset_max_version(dataset_id = unique(df$dataset_id), updateCache = TRUE)
      cat("dataset_version was not specified. Registering at version", dataset_version, "of dataset", unique(df$dataset_id), "\n")
    }
    arrayname = full_arrayname(entity)
    register_tuple_return_id(df, arrayname, uniq, dataset_version = dataset_version, con = con)
  } # end of if (!only_test)
}

#' @export
register_rnaquantificationset = function(df, dataset_version = NULL, only_test = FALSE, con = NULL){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrRnaquantificationset, 
                                            df, dataset_version, only_test, con = con)
}

#' @export
register_fusionset = function(df, dataset_version = NULL, only_test = FALSE, con = NULL){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrFusionset, 
                                            df, dataset_version, only_test, con = con)
}

#' @export
register_copynumberset = function(df, dataset_version = NULL, only_test = FALSE, con = NULL){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrCopyNumberSet, 
                                            df, dataset_version, only_test, con = con)
}

#' @export
register_variant = function(df, dataset_version = NULL, only_test = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  test_register_variant(df)
  if (!only_test) {
    if (!('per_gene_variant_number' %in% colnames(df))) {
      df = df %>% group_by(feature_id, biosample_id) %>% mutate(per_gene_variant_number = row_number())
    }
    df = as.data.frame(df)
    if (is.null(dataset_version)) {
      dataset_version = get_dataset_max_version(dataset_id = unique(df$dataset_id), updateCache = TRUE, con = con)
      if (is.null(dataset_version)) stop("Expected non-null dataset_version at this point")
      cat("dataset_version was not specified. Registering at version", dataset_version, "of dataset", unique(df$dataset_id), "\n")
    }
    df$dataset_version = dataset_version
    arrayname = full_arrayname(.ghEnv$meta$arrVariant)
    
    ids_int64_conv = c(get_idname(arrayname), get_int64fields(arrayname))
    
    ids_int64_conv = ids_int64_conv[(ids_int64_conv != "per_gene_variant_number")]
    cat("Uploading\n")
    non_info_cols = c(get_idname(strip_namespace(arrayname)), 
                      mandatory_fields()[[strip_namespace(arrayname)]])
    if (nrow(df) < 100000) {
      x1 = as.scidb(con$db, df[, non_info_cols])
    } else {
      x1 = as.scidb(con$db, df[, non_info_cols], chunk_size=nrow(df))
    }
    
    x = x1
    for (idnm in ids_int64_conv){
      x = convert_attr_double_to_int64(arr = x, attrname = idnm, con = con)
    }
    qq = paste0("redimension(", x@name, ", ", scidb::schema(scidb(con$db, arrayname)), ")")
    
    query = paste("insert(", qq, ", ", arrayname, ")", sep="")
    cat("Redimension and insert\n")
    iquery(con$db, query)
    
    cat("Now registering info fields\n")
    register_info(df = prep_df_fields(df,
                                      mandatory_fields = non_info_cols),
                  idname = get_idname(arrayname), arrayname = arrayname, 
                  con = con)
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
    register_tuple(df = info, ids_int64_conv = idname, arrayname = paste(arrayname,"_INFO",sep=""), con = con)
  }
}

count_unique_calls = function(variants){
  v = variants
  nrow(v[duplicated(v[, c('biosample_id', 'CHROM', 'POS')]), ])
}

join_ontology_terms = function(df, con = NULL){
  terms = grep(".*_$", colnames(df), value=TRUE)
  df2 = df
  for (term in terms){
    df2[, term] = get_ontology_from_cache(con = con)[df[, term], "term"]
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
  namespace = find_namespace(entitynm)

  fullnm = paste(namespace, ".", entitynm, sep = "")
  if (is.null(id)) {
    qq = full_arrayname(entitynm)
    if (is_entity_secured(entitynm)) qq = paste0("secure_scan(", qq, ")")
  } else {
    if (length(get_idname(entitynm)) == 1) {
      qq = form_selector_query_1d_array(arrayname = fullnm,
                                        idname = get_base_idname(fullnm),
                                        selected_ids = id)
    } else {
      qq = form_selector_query_secure_array(arrayname = fullnm,
                                            selected_ids = id,
                                            dataset_version = dataset_version)
    }
  }
  if (get_entity_infoArrayExists(entitynm)) {
    join_info_ontology_and_unpivot(qq, arrayname = entitynm, namespace=namespace, 
                                   mandatory_fields_only = mandatory_fields_only,
                                   con = con)
  } else {
    iquery(con$db, qq, return = TRUE)
  }
}


#' @export
get_datasets = function(dataset_id = NULL, dataset_version = NULL, all_versions = TRUE, mandatory_fields_only = FALSE, con = NULL){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrDataset,
                                       id = dataset_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions, 
                                       mandatory_fields_only = mandatory_fields_only, 
                                       con = con
  )
}

#' Retrieve individuals
#' 
#' get_ENTITY can be used to retrive
#' - one individual (e.g. `individual_id = 33`)
#' - more than one individual (e.g. `individual_id = c(33, 44)`)
#' - all individuals visible to user (e.g. `get_individuals()`)
#' @export
get_individuals = function(individual_id = NULL, dataset_version = NULL, all_versions = FALSE, mandatory_fields_only = FALSE, con = NULL){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrIndividuals,
                                       id = individual_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions, 
                                       mandatory_fields_only = mandatory_fields_only,
                                       con = con)
}

#' @export
get_biosamples = function(biosample_id = NULL, dataset_version = NULL, all_versions = FALSE, mandatory_fields_only = FALSE, con = NULL){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrBiosample,
                                       id = biosample_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions, 
                                       mandatory_fields_only = mandatory_fields_only,
                                       con = con)
}

check_args_get = function(id, dataset_version, all_versions){
  if (is.null(id) & !is.null(dataset_version) & !all_versions) stop("null value of id is used to get all entities accessible to user. Cannot specify version")
  if (!is.null(dataset_version) & all_versions==TRUE) stop("Cannot specify specific dataset_version, and also set all_versions = TRUE")
}

#' @export
get_rnaquantificationsets = function(rnaquantificationset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrRnaquantificationset,
                                       id = rnaquantificationset_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions,
                                       con = con)
}

#' @export
get_experimentset = function(experimentset_id = NULL, dataset_version = NULL, all_versions = FALSE, mandatory_fields_only = FALSE, con = NULL){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrExperimentSet, 
                                       id = experimentset_id, 
                                       dataset_version, all_versions, 
                                       mandatory_fields_only = mandatory_fields_only, 
                                       con = con)
}


get_versioned_secure_metadata_entity = function(entity, id, 
                                                dataset_version, 
                                                all_versions, 
                                                mandatory_fields_only = FALSE,
                                                con = NULL){
  check_args_get(id = id, dataset_version, all_versions)
  df = select_from_1d_entity(entitynm = entity, id = id, 
                             dataset_version = dataset_version, 
                             mandatory_fields_only = mandatory_fields_only,
                             con = con)
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
get_variantsets = function(variantset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrVariantset, 
                                       id = variantset_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions, 
                                       con = con)
}

#' @export
get_fusionset = function(fusionset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrFusionset, 
                                       id = fusionset_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions,
                                       con = con)
}

#' @export
get_copynumberset = function(copynumberset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrCopyNumberSet, 
                                       id = copynumberset_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions,
                                       con = con)
}

#' @export
get_featuresets= function(featureset_id = NULL, con = NULL){
  select_from_1d_entity(entitynm = .ghEnv$meta$arrFeatureset, 
                        id = featureset_id, con = con)
}

get_feature_synonym = function(feature_synonym_id = NULL, con = NULL){
  select_from_1d_entity(entitynm = .ghEnv$meta$arrFeatureSynonym, 
                        id = feature_synonym_id, con = con)
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
  gl = iquery(con$db, paste0("equi_join(", left_arr, ", 
                                 grouped_aggregate(", right_arr, ", count(*) AS gene_count, genelist_id), 
                                 'left_names=genelist_id', 'right_names=genelist_id', 
                                 'keep_dimensions=true',
                                 'left_outer=true')"), 
              return = T)
  gl = gl[, grep('instance_id|value_no', colnames(gl), invert = TRUE)]
  
  if (get_logged_in_user(con = con) %in% c('scidbadmin', 'root')) {
    return(gl)
  } else {
    return(gl[gl$public | gl$owner == get_logged_in_user(con = con), ])
  }
}


#' @export
get_features = function(feature_id = NULL, fromCache = TRUE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (!fromCache | is.null(.ghEnv$cache$feature_ref)){ # work from SciDB directly
    arrayname = full_arrayname(.ghEnv$meta$arrFeature)
    idname = get_idname(arrayname)
    
    qq = arrayname
    if (!is.null(feature_id)) {
      qq = form_selector_query_1d_array(arrayname, idname, feature_id)
      join_info_ontology_and_unpivot(qq, arrayname, namespace = find_namespace(arrayname))
    } else { # FASTER path when all data has to be downloaded
      ftr = iquery(con$db, qq, return = T)
      ftr_info = iquery(con$db, paste(qq, "_INFO", sep=""), return = T)
      ftr = merge(ftr, ftr_info, by = idname, all.x = T)
      allfeatures = unpivot_key_value_pairs(ftr, arrayname, key_col = "key", val = "val")
      if (is.null(.ghEnv$cache$feature_ref)) { # the first time (when feature cache has never been filled)
        .ghEnv$cache$feature_ref = allfeatures
      }
      allfeatures
    }
  } else { # read from cache
    feature_ref = get_feature_from_cache()
    if (is.null(feature_id)){
      return(drop_na_columns(feature_ref))
    } else {
      return(drop_na_columns(feature_ref[match(feature_id, feature_ref$feature_id),  ]))
    }
  }
}

form_selector_query_secure_array = function(arrayname, selected_ids, dataset_version){
  selected_ids = unique(selected_ids)
  dataset_version = ifelse(is.null(dataset_version), "NULL", dataset_version)
  stopifnot(length(dataset_version) == 1)
  sorted=sort(selected_ids)
  breaks=c(0, which(diff(sorted)!=1), length(sorted))
  entitynm = strip_namespace(arrayname)
  if (is_entity_secured(entitynm)) arrayname = paste0("secure_scan(", arrayname, ")")
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
    upload = sprintf("build(<%s:int64>[idx=1:%d,100000,0],'[(%s)]', true)",
                     idname, 
                     length(selected_ids), 
                     paste(selected_ids, sep=",", collapse="),(")
    )
    apply_dataset_version = paste("apply(", upload, ", dataset_version, ", dataset_version, ")", sep = "")
    redim = paste("redimension(", apply_dataset_version, ", <idx:int64>[", idname, "])", sep = "")
    
    query= paste("project(
                 cross_join(",
                 arrayname, " as A, ",
                 redim, " as B, ",
                 "A.", idname, ", " ,
                 "B.", idname,
                 "),",
                 paste(names(.ghEnv$meta$L$array[[entitynm]]$attributes), collapse = ", "),
                 ")",
                 sep = "")
  }
  query
}


form_selector_query_1d_array = function(arrayname, idname, selected_ids){
  sorted=sort(selected_ids)
  breaks=c(0, which(diff(sorted)!=1), length(sorted))
  THRESH_K = 15  # limit at which to switch from cross_between_ to cross_join
  entitynm = strip_namespace(arrayname)
  if (is_entity_secured(entitynm)) arrayname = paste0("secure_scan(", arrayname, ")")
  if (length(breaks) == 2) # completely contiguous set of tickers; use `between`
  {
    query =sprintf("between(%s, %d, %d)", arrayname, sorted[1], sorted[length(sorted)])
  }
  else if (length(breaks) >2 & length(breaks) <= THRESH_K + 2) # few sets of contiguous tickers; use `cross_between`
  {
    cb_pts =  paste( sapply(seq(length(breaks)-1), function(i) sprintf("%d, %d", sorted[breaks[i]+1], sorted[breaks[i+1]])), collapse=" , ")
    query=sprintf("cross_between_(%s, %s)", arrayname, cb_pts)
  }
  else # mostly non-contiguous tickers, use `cross_join`
  {
    # Formulate the cross_join query
    diminfo = .ghEnv$meta$L$array[[entitynm]]$dims
    if (class(diminfo) == "character") chunksize = 1000000 else stop("code not covered")
    upload = sprintf("build(<%s:int64>[idx=1:%d,100000,0],'[(%s)]', true)",
                     idname, 
                     length(selected_ids), 
                     paste(selected_ids, sep=",", collapse="),("))
    redim = paste("redimension(", upload, ", <idx:int64>[", idname,"=0:*,", as.integer(chunksize), ",0])", sep = "")
    
    query= paste("project(cross_join(",
                 arrayname, " as A, ",
                 redim, "as B, ",
                 "A.", idname, ", " ,
                 "B.", idname,
                 "),",
                 paste(names(.ghEnv$meta$L$array[[entitynm]]$attributes), collapse = ", "),
                 ")",
                 sep = "")
  }
  query
}

#' Search features by synonym
#' 
#' @param synonym: A name for a gene by any convention e.g. ensembl_gene_id, entrez_id, vega_id
#' @param id_type: (Optional) The id type by which to search e.g. ensembl_gene_id, entrez_id, vega_id
#' @param featureset_id: (Optional) The featureset within which to search
#' @return feature(s) associated with provided synonym
#' @export
search_feature_by_synonym = function(synonym, id_type = NULL, featureset_id = NULL, updateCache = FALSE, con = NULL){
  syn = get_feature_synonym_from_cache(updateCache = updateCache, con = con)
  f1 = syn[syn$synonym %in% synonym, ]
  if (!is.null(id_type)) {f1 = f1[f1$source == id_type, ]}
  if (!is.null(featureset_id)) {f1 = f1[f1$featureset_id == f1$featureset_id, ]}
  get_features(feature_id = f1$feature_id, fromCache = !updateCache, con = con)
}

#' @export
search_features = function(gene_symbol = NULL, feature_type = NULL, featureset_id = NULL, con = NULL){
  arrayname = full_arrayname(.ghEnv$meta$arrFeature)
  
  qq = arrayname
  if (!is.null(featureset_id)){
    if (length(featureset_id)==1){
      qq = paste("filter(", qq, ", featureset_id = ", featureset_id, ")", sep="")
    } else if (length(featureset_id)==2){
      qq = paste("filter(", qq, ", featureset_id = '", featureset_id[1], "' OR featureset_id = '", featureset_id[2], "')", sep="")
    } else {stop("Not covered yet")}
  }
  
  if (!is.null(feature_type)){
    if (length(feature_type)==1){
      qq = paste("filter(", qq, ", feature_type = '", feature_type, "')", sep="")
    } else if (length(feature_type)==2){
      qq = paste("filter(", qq, ", feature_type = '", feature_type[1], "' OR feature_type = '", feature_type[2], "')", sep="")
    } else {stop("Not covered yet")}
  }
  
  if (!is.null(gene_symbol)) {
    subq = paste(sapply(gene_symbol, FUN = function(x) {paste("gene_symbol = '", x, "'", sep = "")}), collapse = " OR ")
    qq = paste("filter(", qq, ", ", subq, ")", sep="")
  }
  
  join_info_ontology_and_unpivot(qq, arrayname, 
                                 namespace = get_namespace(arrayname), con = con)
}

#' @export
search_genelist_gene = function(genelist = NULL, 
                                genelist_id = NULL, con = NULL){
  con = use_ghEnv_if_null(con)
  
  arrayname = full_arrayname(.ghEnv$meta$arrGenelist_gene)
  
  if (!is.null(genelist) & !is.null(genelist_id)) {
    stop("Use only one method for searching. Preferred method is using genelist")
  }
  
  # API level security (TODO: replace with pscan() operator)
  if (is.null(genelist_id)) {
    genelist_id = genelist$genelist_id
  } else {
    genelist = get_genelist(genelist_id = genelist_id)
  }
  if (!genelist$public & 
      !(get_logged_in_user(con = con) %in% c('root', 'scidbadmin', genelist$owner))) {
    stop("Do not have permissions to search genelist_id: ", genelist_id)
  }
  
  qq = arrayname
  if (length(genelist_id)==1){
    qq = paste("filter(", qq, ", genelist_id = ", genelist_id, ")", sep="")
  } else {stop("Not covered yet")}
  
  res = iquery(con$db, qq, return = TRUE)
  
  
}

#' @export
search_datasets = function(project_id = NULL, dataset_version = NULL, all_versions = TRUE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  check_args_search(dataset_version, all_versions)
  arrayname = .ghEnv$meta$arrDataset
  
  qq = arrayname
  if (!is.null(project_id)) {
    namespace = find_namespace(id = project_id, entitynm = .ghEnv$meta$arrProject, dflookup = get_project_lookup(), con = con)
    if (is.null(namespace)) namespace = NA # to handle the case of no entry in lookup array at all
    if (is.na(namespace)) {
      cat("trying to download project lookup array once again, to see if there has been a recent update")
      namespace = find_namespace(id = project_id, entitynm = .ghEnv$meta$arrProject, dflookup = get_project_lookup(updateCache = TRUE),
                                 con = con)
    }
    if (!(namespace %in% con$cache$nmsp_list)) {stop("user does not have permission to access data for project_id: ", project_id,
                                                        "or project does not exist")}
    fullnm = paste(namespace, ".", qq, sep = "")
    if (is.null(dataset_version)) {
      qq = paste("filter(", fullnm, ", ", "project_id = ", project_id, ")", sep="")
    } else {
      # TODO: remove condition check after the ordering of dimensions is fixed [entity_id, dataset_version] or [dataset_version, entity_id]
      if (grep("dataset_version", get_idname(arrayname)) == 2) { # datset_version is 2nd dimension
        qq = paste("filter(between(", fullnm, ", null,", dataset_version, ", null, ", dataset_version,  "), ", "project_id = ", project_id, ")", sep="")
      } else if (grep("dataset_version", get_idname(arrayname)) == 1) { # datset_version is 1st dimension
        qq = paste("filter(between(", fullnm, ", ", dataset_version, ", null,", dataset_version,  ", null), ", "project_id = ", project_id, ")", sep="")
      }
    }
  } else {
    stop(cat("Must specify project_id To retrieve all datasets, use get_datasets()", sep = ""))
  }
  
  df = join_info_ontology_and_unpivot(qq,
                                      arrayname,
                                      namespace = namespace,
                                      con = con)
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' Search individuals by dataset
#' 
#' `search_individuals()` can be used to retrive
#' all individuals in a particular dataset
#' @export
search_individuals = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrIndividuals, 
                                          dataset_id, dataset_version, all_versions,
                                          con = con)
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

# For legacy reasons, this function is named find_nmsp_...
# Before secure_scan(), the namespace was found by a lookup by dataset_id
# Currently, this function knows the namespace for an entity by entity-type alone
find_nmsp_filter_on_dataset_id_and_version = function(arrayname, 
                                                      dataset_id, 
                                                      dataset_version, 
                                                      con = NULL){
  con = use_ghEnv_if_null(con)
  
  qq = arrayname
  if (!is.null(dataset_id)) {
    fullnm = paste0("secure_scan(", full_arrayname(qq), ")")
    if (is.null(dataset_version)) {
      qq = paste0("filter(", fullnm, ", ", "dataset_id = ", dataset_id, ")")
    } else {
      qq = paste0("filter(", fullnm, ", dataset_version=", dataset_version, " AND dataset_id = ", dataset_id, ")")
    }
  } else {
    stop(cat("Must specify dataset_id. To retrieve all ", tolower(arrayname), "s, use get_", tolower(arrayname), "s()", sep = ""))
  }
  
  join_info_ontology_and_unpivot(qq,
                                 arrayname,
                                 namespace = find_namespace(arrayname),
                                 con = con)
}

#' @export
search_biosamples = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrBiosample, 
                                          dataset_id, dataset_version, all_versions,
                                          con = con)
  
}

#' @export
search_ontology = function(terms, exact_match = TRUE, updateCache = FALSE, con = NULL){
  ont = get_ontology(updateCache = updateCache, con = con)
  if (nrow(ont) == 0) return(NA)
  ont_ids = ont$ontology_id
  names(ont_ids) = ont$term
  if (exact_match){
    res = ont_ids[terms]
    if (any(is.na(res)) & !updateCache) {
      cat("Updating ontology cache\n")
      search_ontology(terms, exact_match = exact_match, updateCache = TRUE, con = con)
    }
    names(res) = terms
    as.integer(res)
  } else {
    if (length(terms) != 1) {
      # do a recursive call
      if (updateCache) stop("cannot do inexact searching on multiple terms with updateCache set to TRUE")
      lapply(terms, FUN = function(term) {search_ontology(terms = term, exact_match = FALSE, con = con)})
    } else {
      ont[grep(terms, ignore.case = TRUE, ont$term), ]
    }
  }
}

#' @export
search_rnaquantificationset = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrRnaquantificationset, 
                                          dataset_id, dataset_version, all_versions, con = con)
}

#' @export
search_variantsets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrVariantset, 
                                          dataset_id, dataset_version, all_versions, con = con)
  
}

#' @export
search_fusionsets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrFusionset, 
                                          dataset_id, dataset_version, all_versions, con = con)
  
}

#' @export
search_copynumbersets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrCopyNumberSet, 
                                          dataset_id, dataset_version, all_versions, con = con)
}

#' @export
search_experimentsets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrExperimentSet, 
                                          dataset_id, dataset_version, all_versions, con = con)
}

#' @export
search_measurements = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrMeasurement, 
                                          dataset_id, dataset_version, all_versions, con = con)
}

#' @export
search_measurementsets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  # TODO: use generic function search_versioned_secure_metadata_entity()
  # after adding a flag to only download mandatory fields
  con = use_ghEnv_if_null(con)
  arr = paste0(con$cache$nmsp_list, ".MEASUREMENTSET")
  if (length(arr) == 2) arr = paste0("merge(", 
                                               arr[1], ",", 
                                               arr[2], ")")
  
  qq = paste0("filter(", arr, ", dataset_id=", dataset_id, ")")
  if (!is.null(dataset_version)) qq = paste0("filter(", qq, ", dataset_version=", dataset_version, ")")
  iquery(con$db, qq, return = TRUE)
}

#' internal function for search_METADATA()
#' 
#' internal function for `search_individuals()`, `search_biosamples()` etc.
#' search of a metadata entry by `dataset_id`
search_versioned_secure_metadata_entity = function(entity, 
                                                   dataset_id, 
                                                   dataset_version, 
                                                   all_versions, 
                                                   con = NULL) {
  check_args_search(dataset_version, all_versions)
  df = find_nmsp_filter_on_dataset_id_and_version(arrayname = entity, dataset_id, 
                                                  dataset_version = dataset_version, 
                                                  con = con)
  
  # reorder the output by the dimensions
  # from https://stackoverflow.com/questions/17310998/sort-a-dataframe-in-r-by-a-dynamic-set-of-columns-named-in-another-data-frame
  df = df[do.call(order, df[get_idname(entity)]), ] 
  if (!all_versions) return(latest_version(df)) else return(df)
}


# dataset_version: can be "NULL" or any single integral value (if "NULL", then all versions would be returned back)
cross_between_select_on_two = function(qq, tt, val1, val2, selected_names, dataset_version, con = NULL){
  con = use_ghEnv_if_null(con)
  
  selector = merge(
    data.frame(dataset_version = dataset_version),
    merge(data.frame(val1 = val1),
          data.frame(val2 = val2)))
  selected_names_all = c('dataset_version', selected_names)
  colnames(selector) = selected_names_all
  selector$flag = TRUE
  
  xx = as.scidb(con$db, selector, 
                types = c(rep("int64", length(selected_names_all)), 'bool'))
  xx1 = xx
  # for (attr in selected_names_all){
  #   xx1 = convert_attr_double_to_int64(arr = xx1, attrname = attr, con = con)
  # }
  # xx1
  
  dims0 = scidb::schema(tt, "dimensions")$name
  selectpos = which(dims0 %in% selected_names)
  stopifnot(dims0[selectpos] == selected_names)
  # dims0[selectpos]
  cs = scidb::schema(tt, "dimensions")$chunk
  diminfo = data.frame(start = scidb::schema(tt, "dimensions")$start,
                       end = scidb::schema(tt, "dimensions")$end, stringsAsFactors = FALSE)
  ovlp = scidb::schema(tt, "dimensions")$overlap
  # fn = function(dimname) {yaml_to_dim_str(.ghEnv$meta$L$array[[.ghEnv$meta$arrRnaquantification]]$dims[dimname])}
  # newdim = paste(sapply(selected_names_all, FUN = fn), collapse = ",")
  newdim = paste(selected_names_all, collapse = ",")
  newsch = paste("<flag:bool>[", newdim, "]", sep="")
  # xx1 = con$db$redimension(xx1, R(newsch))
  qq2 = paste0("redimension(", xx1@name, ", ", newsch, ")") 
  
  subq = paste(sapply(selected_names_all, FUN=function(x) {paste(paste("A.", x, sep=""), paste("B.", x, sep=""), sep=", ")}), collapse=", ")
  qq = paste("cross_join(",
             qq, " as A,",
             qq2, " as B,", subq, ")", sep="")
  qq = paste("project(", qq, ", ", scidb::schema(tt, "attributes")$name, ")")
  
  iquery(con$db, qq, return = T)
}

#' @export
get_rnaquantification_counts = function(rnaquantificationset_id = NULL, con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (is.null(rnaquantificationset_id)){
    # public first
    x = scidb(con$db, .ghEnv$meta$arrRnaquantification)
    # c = as.R(con$db$aggregate(srcArray = x, AGGREGATE_CALL = "count(*)", groupbyDim = 'rnaquantificationset_id, dataset_version'))
    c = iquery(con$db, paste0("aggregate(", x@name, ", count(*), rnaquantificationset_id, dataset_version"), return = TRUE)
    for (nmsp in con$cache$nmsp_list){
      if (nmsp != 'public'){
        x = scidb(con$db, paste(nmsp, .ghEnv$meta$arrRnaquantification, sep = "."))
        # c1 = as.R(con$db$aggregate(srcArray = x, AGGREGATE_CALL = "count(*)", groupbyDim = 'rnaquantificationset_id, dataset_version'))
        c1 = iquery(con$db, paste0("aggregate(", x@name, ", count(*), rnaquantificationset_id, dataset_version"), return = TRUE)
        c = rbind(c, c1)
      }
    }
  } else { #specific rnaquantificationset_id is specified
    nmsp = find_namespace(id = rnaquantificationset_id, entitynm = .ghEnv$meta$arrRnaquantificationset, 
                          dflookup = get_rnaquantificationset_lookup(),
                          con = con)
    qq = paste("between(", nmsp, ".", .ghEnv$meta$arrRnaquantification, ", null, ", rnaquantificationset_id, ", null, null, null, ", rnaquantificationset_id, ", null, null)", sep = "" )
    qq = paste("aggregate(", qq, ", count(*), rnaquantificationset_id, dataset_version)")
    c = iquery(con$db, qq, return = T)
  }
  c = c[order(c$rnaquantificationset_id, c$dataset_version), ]
  return(c)
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
      x3 = df1[, c(idname,
                  names(.ghEnv$meta$L$array[[strip_namespace(arrayname)]]$attributes))]
    } else {
      x3 = df1[, names(.ghEnv$meta$L$array[[strip_namespace(arrayname)]]$attributes)]
    }
  }
  x3
}

join_info = function(qq, arrayname, namespace = 'public', mandatory_fields_only = FALSE, con = NULL) {
  con = use_ghEnv_if_null(con)
  entitynm = strip_namespace(arrayname)
  if (is_entity_secured(entitynm)) {
    info_array = paste0("secure_scan(", namespace, ".", entitynm, "_INFO)")
  } else {
    info_array = paste0(                namespace, ".", entitynm, "_INFO" )
  }
  qq1 = qq
  idname = get_idname(arrayname)
  # Join INFO array
  if (!mandatory_fields_only) {
    if (exists('debug_trace')) {t1 = proc.time()}
    if (FALSE){ # TODO: See why search_individuals(dataset_id = 1) is so slow; the two options here did not make a difference
      stop("have not tested secure_scan code-path yet")
      qq1 = paste("cross_join(", qq1, " as A, ", info_array, " as B, A.", idname, ", B.", idname, ")", sep = "")
    } else if (FALSE) { cat("== Swapping order of cross join between ARRAY_INFO and select(ARRAY, ..)\n")
      stop("have not tested secure_scan code-path yet")
      qq1 = paste("cross_join(", info_array, " as A, ", qq1, " as B, A.", idname, ", B.", idname, ")", sep = "")
    } else {
      qq1 = paste0("equi_join(", qq1, ", ", info_array, 
                  ", 'left_names=", paste(idname, collapse = ","), 
                  "', 'right_names=", paste(idname, collapse = ","),  "', 'left_outer=true', 'keep_dimensions=true')")
    }
  }
  x2 = iquery(con$db, qq1, return = TRUE)
  
  if (exists('debug_trace')) {cat("join with info:\n"); print( proc.time()-t1 )}
  x2
}

join_info_ontology_and_unpivot = function(qq, arrayname, namespace, mandatory_fields_only = FALSE, con = NULL) {
  df1 = join_info(qq, arrayname, namespace, mandatory_fields_only, con)
  df2 = unpivot(df1, arrayname = arrayname)
  join_ontology_terms(df = df2, con = con)
}

unpivot_key_value_pairs = function(df, arrayname, key_col = "key", val = "val"){
  idname = get_idname(arrayname)
  
  dt = data.table(df)
  setkeyv(dt, c(idname, key_col))
  x2s = dt[,val, by=c(idname, key_col)]
  head(x2s)
  
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

#' @export
register_expression_dataframe = function(df1, dataset_version, con = NULL){
  con = use_ghEnv_if_null(con)
  
  test_register_expression_dataframe(df1)
  
  # df1 = df1[, mandatory_fields()[[.ghEnv$meta$arrRnaquantification]]]
  df1 = df1[, c('dataset_id', 'rnaquantificationset_id', 'biosample_id', 
                'feature_id', 'value')]
  adf_expr0 = as.scidb(con$db, 
                       df1, 
                       chunk_size=nrow(df1), 
                       name = "temp_df", 
                       types = c('int64', 'int64', 'int64', 'int64', 'float'))
  
  qq2 = paste0("apply(", 
               adf_expr0@name, 
                    ", dataset_version, ", dataset_version, ")")
  
  fullnm = full_arrayname(.ghEnv$meta$arrRnaquantification)
  qq2 = paste0("redimension(", qq2, ", ", fullnm, ")")
  
  cat("inserting data for", nrow(df1), "expression values into", fullnm, "array \n")
  iquery(con$db, paste("insert(", qq2, ", ", fullnm, ")"))
  # con$db$remove("temp_df")
  iquery(con$db, "remove(temp_df)")
}

#' @export
register_expression_matrix = function(filepath,
                                      rnaquantificationset_id,
                                      featureset_id,
                                      feature_type,
                                      dataset_version = NULL,
                                      only_test = FALSE,
                                      con = NULL){
  con = use_ghEnv_if_null(con)
  
  test_register_expression_matrix(filepath,
                                  rnaquantificationset_id,
                                  featureset_id,
                                  feature_type,
                                  dataset_version)
  if (!only_test) {
    if (is.null(dataset_version)) {
      rqset = get_rnaquantificationsets(rnaquantificationset_id = rnaquantificationset_id, con = con) # finds the latest version
      dataset_version = rqset$dataset_version
      cat("Dataset version not specified. Inferred version from rnaquantificationset_id as version:", dataset_version, "\n")
    } else {
      stopifnot(length(dataset_version) == 1)
      rqset = get_rnaquantificationsets(rnaquantificationset_id = rnaquantificationset_id,
                                        dataset_version = dataset_version, con = con)
    }
    dataset_id = rqset$dataset_id
    cat("Specified rnaquantificationset_id belongs to dataset:", dataset_id, "\n")
    
    arr_feature = full_arrayname(.ghEnv$meta$arrFeature)
    arr_biosample = full_arrayname(.ghEnv$meta$arrBiosample)
    arrayname = full_arrayname(.ghEnv$meta$arrRnaquantification)
    cat("======\n")
    cat(paste("Working on expression file:\n\t", filepath, "\n"))
    x = read.delim(file = filepath, nrows = 10)
    ncol(x)
    
    
    ##################
    # Do some simple checks on the column-names (only for debug purposes)
    length(colnames(x))
    length(grep(".*_BM", tail(colnames(x),-1) ))
    length(grep(".*_PB", colnames(x) ))
    
    colnames(x)[grep(".*_2_.*", colnames(x) )]
    
    colnames(x)[grep(".*2087.*", colnames(x) )]
    colnames(x)[grep(".*1179.*", colnames(x) )]
    ##################
    
    
    ############# START LOADING INTO SCIDB ########
    
    query = paste("aio_input('", filepath, "',
                  'num_attributes=" , length(colnames(x)), "',
                  'split_on_dimension=1')")
    # t1 = scidb(con$db, query)
    # t1 = con$db$between(srcArray = t1, lowCoord = "NULL, NULL, NULL, NULL", highCoord = R(paste("NULL, NULL, NULL,", length(colnames(x))-1)))
    t0 = scidb(con$db, paste0("between(", query, ", NULL, NULL, NULL, NULL, NULL, NULL, NULL, ", length(colnames(x))-1, ")")) 
    t1 = store(con$db, t0, temp=TRUE)
    
    # ================================
    ## Step 1. Join the SciDB feature ID
    
    # first form the list of features in current table
    # featurelist_curr = con$db$between(srcArray = t1, lowCoord = "NULL, NULL, NULL, 0", highCoord = "NULL, NULL, NULL, 0")
    featurelist_curr = scidb(con$db, paste0("filter(", t1@name, ", attribute_no = 0)"))
    cat(paste("number of feature_id-s in the current expression count table:", scidb_array_count(featurelist_curr, con = con)-1, "\n"))
    FEATUREKEY = scidb(con$db, arr_feature)
    cat(paste("number of feature_id-s in the SciDB feature ID list:", scidb_array_count(FEATUREKEY, con = con), "\n"))
    
    # sel_features = con$db$filter(FEATUREKEY, R(paste("feature_type='", feature_type, "' AND featureset_id = ", featureset_id, sep = "")))
    sel_features = scidb(con$db, paste0("filter(", FEATUREKEY@name, ", ", 
                                                "feature_type='", feature_type, "' AND featureset_id = ", featureset_id, ")"))
    cat(paste("number of feature_id-s in the SciDB feature ID list for transcript type: '", feature_type,
              "' and featureset_id: '", featureset_id, "' is:", scidb_array_count(sel_features, con = con), "\n", sep = ""))
    
    # ff = con$db$project(srcArray = sel_features, selectedAttr = "name")
    ff = scidb(con$db, paste0("project(", sel_features@name, ", name)"))
    
    qq2 = paste0("equi_join(",
                      ff@name, ", ",
                      featurelist_curr@name, ", ",
                      "'left_names=name', 'right_names=a', 'keep_dimensions=1')")
    
    qq2 = paste0("redimension(", qq2, 
                              ", <feature_id :int64>[tuple_no=0:*,10000000,0,dst_instance_id=0:63,1,0,src_instance_id=0:63,1,0])")
    
    joinBack1 = scidb(con$db,
                      paste("cross_join(",
                            t1@name, " as X, ",
                            qq2, " as Y, ",
                            "X.tuple_no, Y.tuple_no, X.dst_instance_id, Y.dst_instance_id, X.src_instance_id, Y.src_instance_id)", sep = ""))
    joinBack1@name
    
    joinBack1 = store(con$db, joinBack1, temp=TRUE)
    
    # Verify with
    # scidb_array_head(con$db$between(srcArray = joinBack1, lowCoord = "0, NULL, NULL, NULL", highCoord = "0, NULL, NULL, NULL"))
    
    cat("Number of features in study that matched with SciDB ID:\n")
    countFeatures = scidb_array_count(joinBack1) / ncol(x)
    print(countFeatures)
    
    stopifnot(countFeatures == (scidb_array_count(featurelist_curr, con = con)-1))
    # ================================
    ## Step 2. Join the SciDB patient ID
    # first form the list of patients in current table
    
    # patientlist_curr = con$db$between(t1, lowCoord = "0, 0, NULL, 1", highCoord = "0, 0, NULL, NULL")
    patientlist_curr = scidb(con$db, paste0("between(", t1@name, ", 0, 0, NULL, 1, 0, 0, NULL, NULL)"))
    # Check that the "public_id"_"spectrum_id" is unique enough, otherwise we have to consider the suffix "BM", "PB"
    stopifnot(length(unique(as.R(patientlist_curr)$a)) == (ncol(x)-1))
    
    scidb_array_head(patientlist_curr, con = con)
    
    cat(paste("number of biosamples in the expression count table:", scidb_array_count(patientlist_curr, con = con), "\n"))
    # PATIENTKEY = scidb(con$db, arr_biosample)
    # PATIENTKEY = con$db$filter(PATIENTKEY, R(paste('dataset_id=', dataset_id)))
    PATIENTKEY = scidb(con$db, paste0("filter(", arr_biosample, ", dataset_id = ", dataset_id, ")"))
    cat(paste("number of biosamples registered in database in selected namespace:" , scidb_array_count(PATIENTKEY), "\n"))
    
    
    # now do the joining
    qq = paste("equi_join(",
               patientlist_curr@name, ", ",
               "project(filter(", PATIENTKEY@name, ", dataset_version = ", dataset_version, "), name), ",
               "'left_names=a', 'right_names=name', 'keep_dimensions=1')")
    joinPatientName = scidb(con$db, qq)
    
    cat("number of matching public_id-s between expression-count table and PER_PATIENT csv file:\n")
    countMatches = scidb_array_count(joinPatientName)
    print(countMatches)
    
    
    
    # Verify
    # x1 = as.R(con$db$project(PATIENTKEY, "name"))
    x1 = iquery(con$db, paste0("project(", PATIENTKEY@name, ", name)"), return = T)
    x2 = as.R(patientlist_curr)
    
    stopifnot(countMatches == sum(x2$a %in% x1$name))
    
    # cat("The expression count table public_id-s that are not present in PER_PATIENT csv file: \n")
    # tt = x2$public_id %in% x1$PUBLIC_ID
    # print(x2$public_id[which(!tt)])
    
    # joinPatientName = con$db$redimension(joinPatientName,
    #                                      R(paste("<biosample_id:int64>
    #                                                 [attribute_no=0:", countMatches+1, ",",countMatches+2, ",0]", sep = "")))
    qq3 = paste0("redimension(", joinPatientName@name,
                              ", <biosample_id:int64>[attribute_no=0:", countMatches+1, ",",countMatches+2, ",0])")
    
    joinBack2 = scidb(con$db,
                      paste0("cross_join(",
                            joinBack1@name, " as X, ",
                            qq3, "as Y, ",
                            "X.attribute_no, Y.attribute_no)"))
    joinBack2@name
    
    joinBack2 = store(con$db, joinBack2, temp=TRUE)
    
    # Verify with
    # scidb_array_head(con$db$filter(joinBack2, "tuple_no = 0"))
    
    cat("Number of expression level values in current array:\n")
    countExpressions = scidb_array_count(joinBack2)
    print(countExpressions)
    stopifnot(countExpressions == countMatches*countFeatures)
    
    ####################
    # Redimension the expression level array
    gct_table = scidb(con$db,
                      paste("apply(",
                            joinBack2@name, ", ",
                            "value, dcast(a, float(null)), ",
                            "rnaquantificationset_id, ", rnaquantificationset_id,
                            ", dataset_id, ", dataset_id, 
                            ", dataset_version, ", dataset_version,
                            ")", sep = "")
    )
    
    # Need to insert the expression matrix table into one big array
    insertable_qq = paste0("redimension(", gct_table@name, ", ",
                                    arrayname, ")") # TODO: would be good to not have this resolution clash
    
    if (scidb_exists_array(arrayname, con = con)) {
      cat(paste("Inserting expression matrix data into", arrayname, "at dataset_version", dataset_version, "\n"))
      iquery(con$db, paste("insert(", insertable_qq, ", ", arrayname, ")"))
    } else {
      stop("expression array does not exist")
    }
    
    return(rnaquantificationset_id)
  } # end of if (!only_test)
}

#' @export
register_copynumber_seg = function(experimentset, only_test = FALSE, con = NULL){
  test_register_copynumber_seg(experimentset)
  if (!only_test) {
    dataset_id = experimentset$dataset_id
    b = search_biosamples(dataset_id = dataset_id, dataset_version = experimentset$dataset_version, con = con)
    
    xx = read.delim(experimentset$file_path)
    xx$biosample_id = b[match(xx$ID, b$name), ]$biosample_id
    stopifnot(!any(is.na(xx$biosample_id)))
    xx$ID = NULL
    colnames(xx) = gsub("chrom", "reference_name", colnames(xx))
    colnames(xx) = gsub("loc.start", "start", colnames(xx))
    colnames(xx) = gsub("loc.end",   "end",   colnames(xx))
    colnames(xx) = gsub("[.]", "_", colnames(xx))
    
    xx$experimentset_id = experimentset$experimentset_id
    xx$dataset_version = experimentset$dataset_version
    
    arrayname = paste(find_namespace(id = experimentset$experimentset_id, 
                                     entitynm = .ghEnv$meta$arrExperimentSet, con = con),
                      .ghEnv$meta$arrCopynumber_seg, sep = ".")
    
    cat("Inserting", nrow(xx), "entries into array:", arrayname, "at version", experimentset$dataset_version, "\n")
    register_tuple(df = xx,
                   ids_int64_conv = c('start', 'end', 'dataset_version', 'experimentset_id', 'biosample_id'),
                   arrayname = arrayname, con = con)
  } # end of if (!only_test)
}

#' @export
register_copynumber_matrix_file = function(copynumberset, dataset_version, featureset_id, 
                                           only_test = FALSE, con = NULL){
  test_register_copnyumber_matrix_file(copynumberset, dataset_version)
  if (!only_test) {
    dataset_id = copynumberset$dataset_id
    dataset_version = copynumberset$dataset_version
    b = search_biosamples(dataset_id = dataset_id, dataset_version = dataset_version, con = con)
    
    xx = read.delim(copynumberset$file_path)
    matched_biosample_id = b[match(tail(colnames(xx), -1), b$name), ]$biosample_id
    if (!(all(!is.na(matched_biosample_id)))) {
      stop("Found biosamples in .mat file that are not previously registered on system")
    }
    colnames(xx) = c(colnames(xx)[1], matched_biosample_id)
    
    ff = search_features(featureset_id = featureset_id, feature_type = 'gene', con = con)
    
    matched_feature_id = ff[match(xx$Gene, ff$name), ]$feature_id
    if (!(all(!is.na(matched_feature_id)))) {
      stop("Found features in .mat file that are not previously 
           registered within specified featureset_id: ", 
           featureset_id)
    }
    xx$Gene = matched_feature_id
    colnames(xx) = gsub("^Gene$", "feature_id", colnames(xx))
    head(xx[1:5, 1:5])
    
    xx2 = tidyr::gather(xx, "biosample_id", "copynumber_val", 2:ncol(xx))
    stopifnot(nrow(xx2) == nrow(xx)*(ncol(xx)-1))
    
    xx2$dataset_version = dataset_version
    xx2$copynumberset_id = copynumberset$copynumberset_id
    
    namespace = find_namespace(id = copynumberset$copynumberset_id, 
                               entitynm = .ghEnv$meta$arrCopyNumberSet, con = con)
    arrayname = paste(namespace, .ghEnv$meta$arrCopynumber_mat, sep = ".")
    cat("Inserting", nrow(xx2), "entries into", arrayname, "at version", dataset_version, "\n")
    register_tuple(df = xx2, 
                   ids_int64_conv = get_idname(.ghEnv$meta$arrCopynumber_mat), 
                   arrayname = arrayname, con = con)
  } # end of if (!only_test)
  }

#' @export
register_fusion_data = function(df, fusionset, only_test = FALSE, con = NULL){
  test_register_fusion_data(df, fusionset)
  if (!only_test) {
    dataset_id = fusionset$dataset_id
    dataset_version = fusionset$dataset_version
    
    xx = df
    
    update_feature_synonym_cache(con = con)
    syn = get_feature_synonym_from_cache(con = con)
    
    # Now register the left and right genes with system feature_id-s
    xx$feature_id_left = syn[match(xx$gene_left, syn$synonym), ]$feature_id
    xx$feature_id_right = syn[match(xx$gene_right, syn$synonym), ]$feature_id
    stopifnot(!any(is.na(xx$feature_id_left)))
    stopifnot(!any(is.na(xx$feature_id_right)))
    
    # Biosamples
    b = search_biosamples(dataset_id = dataset_id, dataset_version = dataset_version, con = con)
    xx$biosample_id = b[match(xx$biosample_name, b$name), ]$biosample_id
    stopifnot(!any(is.na(xx$biosample_id)))
    
    # Rename some columns
    colnames(xx)[colnames(xx) == 'Sample'] = 'sample_name_unabbreviated'
    colnames(xx)[colnames(xx) == 'chrom_left'] = 'reference_name_left'
    colnames(xx)[colnames(xx) == 'chrom_right'] = 'reference_name_right'
    
    xx$fusionset_id = fusionset_record$fusionset_id
    xx$dataset_version = dataset_version
    
    nmsp = find_namespace(id = fusionset_record$fusionset_id, entitynm = .ghEnv$meta$arrFusionset, con = con)
    arrayname = paste(nmsp, .ghEnv$meta$arrFusion, sep = ".")
    
    xx = xx %>% group_by(biosample_id) %>% mutate(fusion_id = row_number())
    xx = as.data.frame(xx)
    
    cat("registering", nrow(xx), "entries of fusion data into array", arrayname, "\n")
    register_tuple(df = xx,
                   ids_int64_conv = c(
                     get_idname(arrayname), get_int64fields(arrayname)),
                   arrayname = arrayname, 
                   con = con)
  } # end of if (!only_test)
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
  stopifnot(nrow(expr_df) == length(biosample_df$biosample_id) * length(feature_df$feature_id))
  exprs = acast(expr_df, feature_id~biosample_id, value.var="value")
  
  # Convert column name to biosample id name
  selected_bios = as.integer(colnames(exprs))
  pos = match(selected_bios, biosample_df$biosample_id)
  colnames(exprs) = biosample_df[pos, "name"]
  
  # Convert row names to feature name
  sel_fx = as.integer(rownames(exprs))
  # Get the position of the features
  fpos = match(sel_fx, feature_df$feature_id)
  fData = feature_df[fpos, ]
  
  row.names(exprs) = fData$name
  rownames(fData) = fData$name
  
  #############################################
  ## Step 2 # Get phenotype data
  pData = biosample_df[pos, ]
  
  # Run a check first
  # all(pData$biosample_id==colnames(exprs))
  rownames(pData) = pData$name
  
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
  if (is_entity_secured(entity)){
    namespaces = find_namespace(id = df[, get_base_idname(entity)], entitynm = entity, con = con)
    nmsp = unique(namespaces)
    if (length(nmsp) != 1) stop("entity to be updated must belong to one namespace only")
  } else {
    nmsp = "public"
  }
  fullarraynm = paste(nmsp, entity, sep = ".")
  update_mandatory_and_info_fields(df = df, arrayname = fullarraynm, con = con)
}

get_entity_count_old = function(con = NULL){
  con = use_ghEnv_if_null(con)
  
  entities = c(.ghEnv$meta$arrProject, .ghEnv$meta$arrDataset, 
               .ghEnv$meta$arrIndividuals, .ghEnv$meta$arrBiosample, 
               .ghEnv$meta$arrRnaquantificationset, 
               .ghEnv$meta$arrVariantset,
               .ghEnv$meta$arrFusionset,
               .ghEnv$meta$arrCopyNumberSet, 
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
