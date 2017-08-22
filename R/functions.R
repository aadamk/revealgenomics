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
gh_connect = function(username, password, host = NULL, port = NULL, protocol = "https"){
  # SciDB connection and R API --
  
  # Attempt 1. 
  err1 = tryCatch({
    if (is.null(host) & is.null(port)) {
      # If user did not specify host and port, try default host for scidbconnect at port 8083
      .ghEnv$db = scidbconnect(username = username, password = password, port = 8083, protocol = protocol)
    } else {
      # If user specified host and port, try user supplied parameters
      .ghEnv$db = scidbconnect(host = host, username = username, password = password, port = port, protocol = protocol)
    }
  }, error = function(e) {return(e)}
  )
  
  # If previous attempts did not work, maybe port 8083 was forwarded (hard-coded to /shim below)
  if ("error" %in% class(err1) & is.null(host) & is.null(port)){
    err2 = tryCatch({ 
      .ghEnv$db = scidbconnect(protocol = protocol, 
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
    .ghEnv$db = NULL
  } else {
    .ghEnv$cache$nmsp_list = iquery(.ghEnv$db, "list('namespaces')", schema = "<name:string NOT NULL> [No=0:1,2,0]", return = T)$name
    .ghEnv$cache$nmsp_list = .ghEnv$cache$nmsp_list[.ghEnv$cache$nmsp_list %in% c('public', 'clinical', 'collaboration')]
  }
}

entity_lookup = function(entityName, updateCache = FALSE){
  if (updateCache | is.null(.ghEnv$cache$lookup[[entityName]])){
    .ghEnv$cache$lookup[[entityName]] = iquery(.ghEnv$db, paste(entityName, "_LOOKUP", sep = ""),
                                               schema = paste("<namespace:string> [", get_idname(entityName), "=0:*]", sep = ""),
                                               return = T)
  }
  return(.ghEnv$cache$lookup[[entityName]])
}

get_project_lookup = function(updateCache = FALSE){
  entity_lookup(.ghEnv$meta$arrProject, updateCache = updateCache)
}
get_dataset_lookup = function(updateCache = FALSE){
  entity_lookup(.ghEnv$meta$arrDataset, updateCache = updateCache)
}
get_individuals_lookup = function(updateCache = FALSE){
  entity_lookup(.ghEnv$meta$arrIndividuals, updateCache = updateCache)
}
get_biosample_lookup = function(updateCache = FALSE){
  entity_lookup(.ghEnv$meta$arrBiosample, updateCache = updateCache)
}
get_rnaquantificationset_lookup = function(updateCache = FALSE){
  entity_lookup(.ghEnv$meta$arrRnaquantificationset, updateCache = updateCache)
}
get_variantset_lookup = function(updateCache = FALSE){
  entity_lookup(.ghEnv$meta$arrVariantset, updateCache = updateCache)
}
get_fusionset_lookup = function(updateCache = FALSE){
  entity_lookup(.ghEnv$meta$arrFusionset, updateCache = updateCache)
}
get_experimentset_lookup = function(updateCache = FALSE){
  entity_lookup(.ghEnv$meta$arrExperimentSet, updateCache = updateCache)
}
get_copynumberset_lookup = function(updateCache = FALSE){
  entity_lookup(.ghEnv$meta$arrCopyNumberSet, updateCache = updateCache)
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

# cat("Downloading reference dataframes for fast ExpressionSet formation\n")
get_feature_from_cache = function(updateCache = FALSE){
  if (updateCache | is.null(.ghEnv$cache$feature_ref)){
    update_feature_cache()
  }
  return(.ghEnv$cache$feature_ref)
}
update_feature_cache = function(){
  .ghEnv$cache$feature_ref = NULL
  .ghEnv$cache$feature_ref = get_features(fromCache = FALSE)
}

get_biosamples_from_cache = function(updateCache = FALSE){
  if (updateCache | is.null(.ghEnv$cache$biosample_ref)){
    update_biosample_cache()
  }
  return(.ghEnv$cache$biosample_ref)
}

update_biosample_cache = function(){
  .ghEnv$cache$biosample_ref = get_biosamples()
}

get_ontology_from_cache = function(updateCache = FALSE){
  if (updateCache | is.null(.ghEnv$cache$dfOntology)){
    update_ontology_cache()
  }
  if (nrow(.ghEnv$cache$dfOntology) == 0) update_ontology_cache()
  return(.ghEnv$cache$dfOntology)
}

#' @export
get_ontology = function(ontology_id = NULL, updateCache = FALSE){
  dfOntology = get_ontology_from_cache(updateCache)
  if (!is.null(ontology_id)){
    matches = match(ontology_id, dfOntology$ontology_id)
    matches = matches[which(!is.na(matches))]
    dfOntology[matches, ]
  } else {
    dfOntology
  }
}

update_ontology_cache = function(){
  .ghEnv$cache$dfOntology = iquery(.ghEnv$db, .ghEnv$meta$arrOntology, return = TRUE)
}

get_feature_synonym_from_cache = function(updateCache = FALSE){
  if (updateCache | is.null(.ghEnv$cache$dfFeatureSynonym)){
    update_feature_synonym_cache()
  }
  return(.ghEnv$cache$dfFeatureSynonym)
}
update_feature_synonym_cache = function(){
  .ghEnv$cache$dfFeatureSynonym = iquery(.ghEnv$db, .ghEnv$meta$arrFeatureSynonym, return = TRUE)
}

#' @export
scidb_exists_array = function(arrayName) {
  !is.null(tryCatch({iquery(.ghEnv$db, paste("show(", arrayName, ")", sep=""), return=TRUE, binary = FALSE)}, error = function(e) {NULL}))
}

get_max_id = function(arrayname){
  if (is_entity_secured(arrayname)) { # Lookup array must exist
    max = iquery(.ghEnv$db,
                 paste("aggregate(apply(public.",
                       strip_namespace(arrayname), "_LOOKUP, id, ",
                       get_base_idname(arrayname), "), max(id))", sep = ""
                 ),
                 return=TRUE)$id_max
  } else { # Only exists in public namespaces
    max = iquery(.ghEnv$db, paste("aggregate(apply(", arrayname,
                                  ", id, ", get_base_idname(arrayname), "), ",
                                  "max(id))", sep=""), return=TRUE)$id_max
  }
  if (is.na(max)) max = 0
  return(max)
}

convert_attr_double_to_int64 = function(arr, attrname){
  attrnames = schema(arr, "attributes")$name
  randString = "for_int64_conversion"
  arr = scidb_attribute_rename(arr, old = attrname, new = randString)
  arr = .ghEnv$db$apply(srcArray = arr, newAttr = R(attrname), expression = int64(R(randString)))
  arr = .ghEnv$db$project(arr, R(paste(attrnames, collapse = ", ")))
  arr
}


update_tuple = function(df, ids_int64_conv, arrayname){
  if (nrow(df) < 100000) {x1 = as.scidb(.ghEnv$db, df)} else {x1 = as.scidb(.ghEnv$db, df, chunk_size=nrow(df))}
  
  x = x1
  for (idnm in ids_int64_conv){
    x = convert_attr_double_to_int64(arr = x, attrname = idnm)
  }
  x = scidb_attribute_rename(arr = x, old = "updated", new = "updated_old")
  x = .ghEnv$db$apply(srcArray = x, newAttr = "updated", expression = "string(now())")
  x = .ghEnv$db$redimension(srcArray = x, schemaArray = R(schema(scidb(.ghEnv$db, arrayname))))
  
  query = paste("insert(", x@name, ", ", arrayname, ")", sep="")
  iquery(.ghEnv$db, query)
}

register_tuple = function(df, ids_int64_conv, arrayname){
  if (nrow(df) < 100000) {x1 = as.scidb(.ghEnv$db, df)} else {x1 = as.scidb(.ghEnv$db, df, chunk_size=nrow(df))}
  
  x = x1
  for (idnm in ids_int64_conv){
    x = convert_attr_double_to_int64(arr = x, attrname = idnm)
  }
  x = .ghEnv$db$apply(srcArray = x, newAttr = "created", expression = "string(now())")
  x = .ghEnv$db$apply(srcArray = x, newAttr = "updated", expression = "created")
  x = .ghEnv$db$redimension(srcArray = x, schemaArray = R(schema(scidb(.ghEnv$db, arrayname))))
  
  query = paste("insert(", x@name, ", ", arrayname, ")", sep="")
  iquery(.ghEnv$db, query)
}

#' @export
register_project = function(df,
                            namespace,
                            only_test = FALSE){
  uniq = unique_fields()[[.ghEnv$meta$arrProject]]
  test_register_project(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = paste(namespace, ".", .ghEnv$meta$arrProject, sep = "")
    register_tuple_return_id(df, arrayname, uniq)
  } # end of if (!only_test)
}

#' @export
register_dataset = function(df,
                            dataset_version = 1,
                            only_test = FALSE
){
  uniq = unique_fields()[[.ghEnv$meta$arrDataset]]
  
  test_register_dataset(df, uniq, dataset_version, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    namespace = find_namespace(id = unique(df$project_id), entitynm=.ghEnv$meta$arrProject)
    arrayname = paste(namespace, ".", .ghEnv$meta$arrDataset, sep = "")
    register_tuple_return_id(df, arrayname, uniq, dataset_version = dataset_version)
  } # end of if (!only_test)
}

find_namespace = function(id, entitynm, dflookup = NULL){
  if (is.null(dflookup)) { # if need to download the lookup array from scidb
    lookuparr = paste(entitynm, "_LOOKUP", sep = "")
    dflookup = iquery(.ghEnv$db, lookuparr, return = T)
    # dflookup[, get_idname(entitynm)] = as.integer(dflookup[, get_idname(entitynm)])
  }
  dflookup[match(id, dflookup[, get_base_idname(entitynm)]), ]$namespace
}

#' @export
register_ontology_term = function(df, only_test = FALSE){
  uniq = unique_fields()[[.ghEnv$meta$arrOntology]]
  test_register_ontology(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = .ghEnv$meta$arrOntology
    register_tuple_return_id(df, arrayname, uniq)
  } # end of if (!only_test)
}

#' @export
register_individual = function(df,
                               dataset_version = NULL,
                               only_test = FALSE){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrIndividuals, 
                                            df, dataset_version, only_test)
}

#' @export
register_referenceset = function(df, only_test = FALSE){
  uniq = unique_fields()[[.ghEnv$meta$arrReferenceset]]
  test_register_referenceset(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = .ghEnv$meta$arrReferenceset
    register_tuple_return_id(df,
                             arrayname, uniq)
  } # end of if (!only_test)
}

#' @export
register_genelist = function(df, only_test = FALSE){
  uniq = unique_fields()[[.ghEnv$meta$arrGenelist]]
  df$owner = iquery(.ghEnv$db, query = "show_user()", return = TRUE)$name
  test_register_genelist(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = .ghEnv$meta$arrGenelist
    register_tuple_return_id(df,
                             arrayname, uniq)
  } # end of if (!only_test)
}

#' @export
register_genelist_gene = function(df, only_test = FALSE){
  uniq = unique_fields()[[.ghEnv$meta$arrGenelist_gene]]
  test_register_genelist_gene(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = .ghEnv$meta$arrGenelist_gene
    register_tuple_return_id(df,
                             arrayname, uniq)
  } # end of if (!only_test)
}

#' @export
register_featureset = function(df, only_test = FALSE){
  uniq = unique_fields()[[.ghEnv$meta$arrFeatureset]]
  test_register_featureset(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = .ghEnv$meta$arrFeatureset
    register_tuple_return_id(df,
                             arrayname, uniq)
  } # end of if (!only_test)
}

register_feature_synonym = function(df, only_test = FALSE){
  uniq = unique_fields()[[.ghEnv$meta$arrFeatureSynonym]]
  test_register_feature_synonym(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = .ghEnv$meta$arrFeatureSynonym
    register_tuple_return_id(df, arrayname, uniq)
  } # end of if (!only_test)
}

#' @export
register_feature = function(df, register_gene_synonyms = TRUE, only_test = FALSE){
  uniq = unique_fields()[[.ghEnv$meta$arrFeature]]
  test_register_feature(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = .ghEnv$meta$arrFeature
    fid = register_tuple_return_id(df, arrayname, uniq)
    gene_ftrs = df[df$feature_type == 'gene', ]
    if (register_gene_synonyms & nrow(gene_ftrs) > 0){
      cat("Working on gene synonyms\n")
      df_syn = data.frame(synonym = gene_ftrs$name, 
                          feature_id = fid,
                          featureset_id = unique(gene_ftrs$featureset_id),
                          source = gene_ftrs$source,
                          stringsAsFactors = F)
      ftr_syn_id = register_feature_synonym(df = df_syn)
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
                              only_test = FALSE){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrBiosample, 
                                            df, dataset_version, only_test)
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
update_mandatory_and_info_fields = function(df, arrayname){
  idname = get_idname(arrayname)
  if (any(is.na(df[, idname]))) stop("Dimensions: ", paste(idname, collapse = ", "), " should not have null values at upload time!")
  int64_fields = get_int64fields(arrayname)
  infoArray = get_infoArray(arrayname)
  update_tuple(df[, c(get_idname(arrayname),
                      mandatory_fields()[[strip_namespace(arrayname)]], 
                      'created', 'updated')], 
               ids_int64_conv = c(idname, int64_fields), 
               arrayname)
  if(infoArray){
    delete_info_fields(fullarrayname = arrayname,
                       id = df[, get_base_idname(arrayname)],
                       dataset_version = unique(df$dataset_version))
    cat("Registering info for ", nrow(df)," entries in array: ", arrayname, "_INFO\n", sep = "")
    register_info(df = prep_df_fields(df,
                                      mandatory_fields = c(get_mandatory_fields_for_register_entity(arrayname),
                                                           get_idname(arrayname),
                                                           'created', 'updated')),
                  idname, arrayname)
  }
}

# Wrapper function that (1) registers mandatory fields, (2) updates lookup array (based on flag), and (3) registers flex fields
register_tuple_update_lookup = function(df, arrayname, updateLookup){
  idname = get_idname(arrayname)
  if (any(is.na(df[, idname]))) stop("Dimensions: ", paste(idname, collapse = ", "), " should have had non null values at upload time!")
  int64_fields = get_int64fields(arrayname)
  infoArray = get_infoArray(arrayname)
  
  non_info_cols = c(get_idname(strip_namespace(arrayname)), 
                    mandatory_fields()[[strip_namespace(arrayname)]])
  register_tuple(df = df[, non_info_cols], ids_int64_conv = c(idname, int64_fields), arrayname)
  if (updateLookup){
    new_id = df[, get_base_idname(arrayname)]
    update_lookup_array(new_id, arrayname)
  }
  if(infoArray){
    cat("Registering info for ", nrow(df)," entries in array: ", arrayname, "_INFO\n", sep = "")
    register_info(df, idname, arrayname)
  }
}

register_tuple_return_id = function(df,
                                    arrayname,
                                    uniq = NULL,
                                    dataset_version = NULL){
  test_unique_fields(df, uniq)         # Ideally this should have already been run earlier
  test_mandatory_fields(df, arrayname, silent = TRUE) # Ideally this should have already been run earlier
  
  idname = get_idname(arrayname)
  int64_fields = get_int64fields(arrayname)
  
  mandatory_fields = names(.ghEnv$meta$L$array[[strip_namespace(arrayname)]]$attributes)
  namespaces_for_entity = .ghEnv$meta$L$array[[strip_namespace(arrayname)]]$namespace
  
  df = prep_df_fields(df, mandatory_fields)
  
  if (!is.null(dataset_version)) {
    df[, "dataset_version"] = dataset_version
  }
  
  if (is.null(uniq)){ # No need to match existing elements, just append data
    stop("Registering entity without a set of unique fields is not allowed")
  }
  
  # Find matches by set of unique fields provided by user
  xx = iquery(.ghEnv$db, paste("project(", arrayname, ", ", paste(uniq, collapse = ", "), ")", sep = ""), return = TRUE)
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
      register_tuple_update_lookup(df = df[matching_idx, ], arrayname = arrayname, updateLookup = FALSE)
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
        register_tuple_update_lookup(df = dfx[nonmatching_idx_at_version, ], arrayname = arrayname, updateLookup = FALSE)
      }
    }
  } else {
    if (!is.null(dataset_version) & length(old_id) == 0) cat("No matching entries for versioned entity\n")
  }
  
  # Now assign new id-s for entries that did not match by unique fields
  if (length(nonmatching_idx) > 0 ) {
    cat("---", length(nonmatching_idx), "rows need to be registered from total of", nrow(df), "rows provided by user\n")
    # if (length(nonmatching_idx) != nrow(df)) {stop("Need to check code here")}
    new_id = get_max_id(arrayname) + 1:nrow(df[nonmatching_idx, ])
    df[nonmatching_idx, get_base_idname(arrayname)] = new_id
    
    updateLookup = ifelse(length(namespaces_for_entity) > 1, TRUE, FALSE) # Lookup array must exist if entity exists in multiple namespaces
    register_tuple_update_lookup(df = df[nonmatching_idx, ], arrayname = arrayname, updateLookup = updateLookup)
  } else {
    cat("--- no completely new entries to register\n")
    new_id = NULL
  }
  return(df[, idname])
}

update_lookup_array = function(new_id, arrayname){
  URI_414_ERROR_THRESH = 400
  name = strip_namespace(arrayname)
  namespace = get_namespace(arrayname)
  
  lookuparr = paste(name, "_LOOKUP", sep = "")
  a = scidb(.ghEnv$db, lookuparr)
  
  if (length(new_id) <= URI_414_ERROR_THRESH) {
    cat("inserting id-s:", pretty_print(new_id), "into array", lookuparr, "\n")
    
    # qq = sprintf("build(<%s:int64, namespace: string>[idx=1:%d,100000,0],'{1}[(%s)]', true)",
    #              get_base_idname(arrayname), length(new_id), 
    #               paste(new_id, namespace, sep=",", collapse="), ("))
    # qq = sprintf("build(<%s:int64, namespace: string>[idx=1:%d,100000,0],'[(%s)]', true)",
    #              get_base_idname(arrayname), length(new_id), 
    #              paste(new_id, 
    #                    paste("\\'", namespace, "\\'", sep = ""), 
    #                    sep=",", collapse="),("))
    qq = sprintf("apply(build(<%s:int64>[idx=1:%d,100000,0],'[(%s)]', true), namespace, %s)",
                 get_base_idname(arrayname), 
                 length(new_id), 
                 paste(new_id, collapse="), ("), 
                 paste("'", namespace, "'", sep = ""))
    qq = paste("redimension(", qq, ", ", schema(a), ")", sep = "")
    qq = paste("insert(", qq, ", ", lookuparr, ")", sep = "")
    # cat("query:", qq, "\n")
    iquery(.ghEnv$db, qq)
  } else { # alternate upload path when URI can be too long
    cat("inserting id-s:", pretty_print(new_id), "into array", lookuparr, " - via upload\n")
    
    new_id_df = data.frame(new_id = new_id,
                           namespace = namespace, 
                           stringsAsFactors = FALSE)
    colnames(new_id_df)[which(colnames(new_id_df) == 'new_id')] = 
      get_base_idname(arrayname)
    uploaded = as.scidb(.ghEnv$db, new_id_df)
    uploaded = convert_attr_double_to_int64(arr = uploaded, attrname = get_base_idname(arrayname))
    qq = paste("redimension(", uploaded@name, ", ", schema(a), ")", sep = "")
    qq = paste("insert(", qq, ", ", lookuparr, ")", sep = "")
    # cat("query:", qq, "\n")
    iquery(.ghEnv$db, qq)
  }
}

#' @export
register_variantset = function(df, dataset_version = NULL, only_test = FALSE){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrVariantset, 
                                            df, dataset_version, only_test)
}


#' @export
register_experimentset = function(df, dataset_version = NULL, only_test = FALSE){
  test_register_experimentset(df, silent = ifelse(only_test, FALSE, TRUE))
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrExperimentSet, 
                                            df, dataset_version, only_test)
}

#' @export
register_measurement = function(df, dataset_version = NULL, only_test = FALSE){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrMeasurement, 
                                            df, dataset_version, only_test)
}

register_versioned_secure_metadata_entity = function(entity, df, 
                                                     dataset_version, only_test){
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
    namespace = find_namespace(unique(df$dataset_id), entitynm=.ghEnv$meta$arrDataset)
    arrayname = paste(namespace, ".", entity, sep = "")
    register_tuple_return_id(df, arrayname, uniq, dataset_version = dataset_version)
  } # end of if (!only_test)
}

#' @export
register_rnaquantificationset = function(df, dataset_version = NULL, only_test = FALSE){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrRnaquantificationset, 
                                            df, dataset_version, only_test)
}

#' @export
register_fusionset = function(df, dataset_version = NULL, only_test = FALSE){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrFusionset, 
                                            df, dataset_version, only_test)
}

#' @export
register_copynumberset = function(df, dataset_version = NULL, only_test = FALSE){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrCopyNumberSet, 
                                            df, dataset_version, only_test)
}

#' @export
register_variant = function(df, dataset_version = NULL, only_test = FALSE){
  test_register_variant(df)
  if (!only_test) {
    df = df %>% group_by(feature_id, biosample_id) %>% mutate(variant_id = row_number())
    df = as.data.frame(df)
    if (is.null(dataset_version)) {
      dataset_version = get_dataset_max_version(dataset_id = unique(df$dataset_id), updateCache = TRUE)
      if (is.null(dataset_version)) stop("Expected non-null dataset_version at this point")
      cat("dataset_version was not specified. Registering at version", dataset_version, "of dataset", unique(df$dataset_id), "\n")
    }
    df$dataset_version = dataset_version
    namespace = find_namespace(unique(df$dataset_id), entitynm=.ghEnv$meta$arrDataset)
    arrayname = paste(namespace, ".", .ghEnv$meta$arrVariant, sep = "")
    
    ids_int64_conv = c(get_idname(arrayname), get_int64fields(arrayname))
    
    ids_int64_conv = ids_int64_conv[(ids_int64_conv != "variant_id")]
    cat("Uploading\n")
    non_info_cols = c(get_idname(strip_namespace(arrayname)), 
                      mandatory_fields()[[strip_namespace(arrayname)]])
    if (nrow(df) < 100000) {
      x1 = as.scidb(.ghEnv$db, df[, non_info_cols])
    } else {
      x1 = as.scidb(.ghEnv$db, df[, non_info_cols], chunk_size=nrow(df))
    }
    
    x = x1
    for (idnm in ids_int64_conv){
      x = convert_attr_double_to_int64(arr = x, attrname = idnm)
    }
    x = .ghEnv$db$apply(srcArray = x, newAttr = "created", expression = "string(now())")
    x = .ghEnv$db$apply(srcArray = x, newAttr = "updated", expression = "created")
    x = .ghEnv$db$redimension(srcArray = x, schemaArray = R(schema(scidb(.ghEnv$db, arrayname))))
    
    query = paste("insert(", x@name, ", ", arrayname, ")", sep="")
    cat("Redimension and insert\n")
    iquery(.ghEnv$db, query)
    
    df2 = df
    df2$dataset_id = NULL
    
    cat("Now registering info fields\n")
    register_info(df = prep_df_fields(df2,
                                      mandatory_fields = non_info_cols),
                  idname = get_idname(arrayname), arrayname = arrayname)
  } # end of if (!only_test)
}

register_info = function(df, idname, arrayname){
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
    register_tuple(df = info, ids_int64_conv = idname, arrayname = paste(arrayname,"_INFO",sep=""))
  }
}

count_unique_calls = function(variants){
  v = variants
  nrow(v[duplicated(v[, c('biosample_id', 'CHROM', 'POS')]), ])
}

join_ontology_terms = function(df){
  terms = grep(".*_$", colnames(df), value=TRUE)
  df2 = df
  for (term in terms){
    df2[, term] = get_ontology_from_cache()[df[, term], "term"]
  }
  return(df2)
}

#' @export
get_projects = function(project_id = NULL){
  if (!is.null(project_id)) { # Need to look up specific individual ID
    select_from_1d_entity(entitynm = .ghEnv$meta$arrProject, id = project_id)
  } else { # Need to gather info across namespaces
    merge_across_namespaces(arrayname = .ghEnv$meta$arrProject)
  }
}

select_from_1d_entity = function(entitynm, id, dataset_version = NULL){
  namespace = find_namespace(id, entitynm, entity_lookup(entityName = entitynm))
  if (is.null(namespace)) namespace = NA # to handle the case of no entry in lookup array at all
  if (any(is.na(namespace))) {
    cat("trying to download lookup array once again, to see if there has been a recent update\n")
    namespace = update_lookup_and_find_namespace_again(entitynm, id)
  }
  if (any(!(namespace %in% .ghEnv$cache$nmsp_list))) stop("user probably does have acecss to id-s: ", paste(id[which(!(namespace %in% .ghEnv$cache$nmsp_list))], collapse = ", "))
  names(id) = namespace
  df0 = data.frame()
  for (nmsp in unique(namespace)){
    cat("--DEBUG--: retrieving entities from namespace:", nmsp, "\n")
    df1 = select_from_1d_entity_by_namespace(namespace = nmsp, entitynm, id = id[which(names(id) == nmsp)], dataset_version = dataset_version)
    if (nrow(df1) > 0){
      l = list(df0,
               df1)
      df0 = rbindlist(l, use.names=TRUE, fill=TRUE)
    }
  }
  df0 = data.frame(df0)
  # return(df0[match(id, df0[, get_base_idname(entitynm)]), ])
  return(df0)
}

update_lookup_and_find_namespace_again = function(entitynm, id){
  cat("updating lookup array for entity:", entitynm, "\n")
  .ghEnv$cache$lookup[[entitynm]] = entity_lookup(entityName = entitynm, updateCache = TRUE)
  namespace = find_namespace(id, entitynm, .ghEnv$cache$lookup[[entitynm]])
  if (any(is.na(namespace))) stop(entitynm, " at id: ",
                                  paste(id[which(is.na(namespace))], collapse = ", "),
                                  " does not exist\n")
  return(namespace)
}

select_from_1d_entity_by_namespace = function(namespace, entitynm, id, dataset_version){
  fullnm = paste(namespace, ".", entitynm, sep = "")
  if (length(get_idname(entitynm)) == 1) {
    qq = form_selector_query_1d_array(arrayname = fullnm,
                                      idname = get_base_idname(fullnm),
                                      selected_ids = id)
  } else {
    qq = form_selector_query_2d_array(arrayname = fullnm,
                                      dim1 = get_base_idname(fullnm), dim1_selected_ids = id,
                                      dim2 = "dataset_version", dim2_selected_ids = dataset_version)
  }
  join_info_ontology_and_unpivot(qq, arrayname = entitynm, namespace=namespace)
}

merge_across_namespaces = function(arrayname, mandatory_fields_only = FALSE){
  arrayname = strip_namespace(arrayname)
  
  # public namespace first
  qq = arrayname
  df = join_info_ontology_and_unpivot(qq, arrayname, mandatory_fields_only = mandatory_fields_only)
  for (namespace in .ghEnv$cache$nmsp_list){
    if (namespace != 'public'){
      cat("--DEBUG--: joining with data from namespace:", namespace, "\n")
      fullnm = paste(namespace, ".", arrayname, sep = "")
      dfx = join_info_ontology_and_unpivot(fullnm, arrayname, namespace = namespace, 
                                           mandatory_fields_only = mandatory_fields_only)
      if (nrow(dfx) > 0) {
        l = list(df,
                 dfx)
        df = rbindlist(l, use.names=TRUE, fill=TRUE)
      }
    }
  }
  df = as.data.frame(df)
  if (nrow(df) > 0) {return(df[order(df[, get_base_idname(arrayname)]), ])} else {return(df)}
}

#' @export
get_datasets = function(dataset_id = NULL, dataset_version = NULL, all_versions = TRUE, mandatory_fields_only = FALSE){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrDataset,
                                       id = dataset_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions, 
                                       mandatory_fields_only = mandatory_fields_only)
}

#' @export
get_individuals = function(individual_id = NULL, dataset_version = NULL, all_versions = FALSE, mandatory_fields_only = FALSE){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrIndividuals,
                                       id = individual_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions, 
                                       mandatory_fields_only = mandatory_fields_only)
}

#' @export
get_biosamples = function(biosample_id = NULL, dataset_version = NULL, all_versions = FALSE, mandatory_fields_only = FALSE){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrBiosample,
                                       id = biosample_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions, 
                                       mandatory_fields_only = mandatory_fields_only)
}

check_args_get = function(id, dataset_version, all_versions){
  if (is.null(id) & !is.null(dataset_version) & !all_versions) stop("null value of id is used to get all entities accessible to user. Cannot specify version")
  if (!is.null(dataset_version) & all_versions==TRUE) stop("Cannot specify specific dataset_version, and also set all_versions = TRUE")
}

#' @export
get_rnaquantificationsets = function(rnaquantificationset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrRnaquantificationset,
                                       id = rnaquantificationset_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions)
}

#' @export
get_experimentset = function(experimentset_id = NULL, dataset_version = NULL, all_versions = FALSE, mandatory_fields_only = FALSE){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrExperimentSet, 
                                       id = experimentset_id, 
                                       dataset_version, all_versions, 
                                       mandatory_fields_only = mandatory_fields_only)
}


get_versioned_secure_metadata_entity = function(entity, id, 
                                                dataset_version, 
                                                all_versions, 
                                                mandatory_fields_only = FALSE){
  check_args_get(id = id, dataset_version, all_versions)
  if (!is.null(id)) { # Need to look up specific ID
    df = select_from_1d_entity(entitynm = entity, id = id, 
                               dataset_version = dataset_version)
  } else { # Need to gather info across namespaces
    df = merge_across_namespaces(arrayname = entity, mandatory_fields_only = mandatory_fields_only)
  }
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
get_variantsets = function(variantset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrVariantset, 
                                       id = variantset_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions)
}

#' @export
get_fusionset = function(fusionset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrFusionset, 
                                       id = fusionset_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions)
}

#' @export
get_copynumberset = function(copynumberset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrCopyNumberSet, 
                                       id = copynumberset_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions)
}

#' @export
get_featuresets= function(featureset_id = NULL){
  get_unversioned_public_metadata_entity(arrayname = .ghEnv$meta$arrFeatureset, 
                                         id = featureset_id)
}

get_feature_synonym = function(feature_synonym_id = NULL){
  get_unversioned_public_metadata_entity(arrayname = .ghEnv$meta$arrFeatureSynonym, 
                                         id = feature_synonym_id, 
                                         infoArray = FALSE)
}

get_unversioned_public_metadata_entity = function(arrayname, id, infoArray = TRUE){
  local_arrnm = strip_namespace(arrayname)
  idname = get_idname(arrayname)
  stopifnot(class(idname) == "character")
  
  qq = arrayname
  if (!is.null(id)) {
    qq = form_selector_query_1d_array(arrayname, idname, id)
  }
  if (infoArray) {
    join_info_ontology_and_unpivot(qq, arrayname)
  } else {
    iquery(.ghEnv$db, qq, return = TRUE)
  }
}

#' @export
get_referenceset = function(referenceset_id = NULL){
  get_unversioned_public_metadata_entity(arrayname = .ghEnv$meta$arrReferenceset, 
                                         id = referenceset_id)
}

#' @export
get_genelist = function(genelist_id = NULL){
  get_unversioned_public_metadata_entity(arrayname = .ghEnv$meta$arrGenelist, 
                                         id = genelist_id,
                                         infoArray = FALSE)
}


#' @export
get_features = function(feature_id = NULL, fromCache = TRUE){
  if (!fromCache | is.null(.ghEnv$cache$feature_ref)){ # work from SciDB directly
    arrayname = .ghEnv$meta$arrFeature
    idname = get_idname(arrayname)
    
    qq = arrayname
    if (!is.null(feature_id)) {
      qq = form_selector_query_1d_array(arrayname, idname, feature_id)
      join_info_ontology_and_unpivot(qq, arrayname)
    } else { # FASTER path when all data has to be downloaded
      ftr = iquery(.ghEnv$db, qq, return = T)
      ftr_info = iquery(.ghEnv$db, paste(qq, "_INFO", sep=""), return = T)
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

form_selector_query_2d_array = function(arrayname, dim1, dim1_selected_ids, dim2, dim2_selected_ids){
  dim1_selected_ids = unique(dim1_selected_ids)
  dim2_selected_ids = ifelse(is.null(dim2_selected_ids), "NULL", dim2_selected_ids)
  stopifnot(dim2 == "dataset_version" & length(dim2_selected_ids) == 1)
  sorted=sort(dim1_selected_ids)
  breaks=c(0, which(diff(sorted)!=1), length(sorted))
  THRESH_K = 15  # limit at which to switch from cross_between_ to cross_join
  if (length(breaks) == 2) # completely contiguous set of tickers; use `between`
  {
    query =sprintf("between(%s, %d, %s, %d, %s)", arrayname, sorted[1], dim2_selected_ids,
                   sorted[length(sorted)], dim2_selected_ids)
  }
  else if (length(breaks) >2 & length(breaks) <= THRESH_K + 2) # few sets of contiguous tickers; use `cross_between`
  {
    cb_pts =  paste( sapply(seq(length(breaks)-1), function(i) sprintf("%d, %s, %d, %s", sorted[breaks[i]+1], dim2_selected_ids,
                                                                       sorted[breaks[i+1]], dim2_selected_ids)), collapse=" , ")
    #    rt_array=sprintf("build(<dateLo:int64, rowLo: int64, tickerLo: int64, dateHi: int64, rowHi: int64, tickerHi: int64>[idx=1:%d,100000,0],'{1}[%s]',true)", length(breaks)-1, cb_pts)
    query=sprintf("cross_between_(%s, %s)", arrayname, cb_pts)
  }
  else # mostly non-contiguous tickers, use `cross_join`
  {
    # Formulate the cross_join query
    idname = get_base_idname(arrayname)
    diminfo = .ghEnv$meta$L$array[[strip_namespace(arrayname)]]$dims
    upload = sprintf("build(<%s:int64>[idx=1:%d,100000,0],'{1}[(%s)]', true)",
                     idname, length(dim1_selected_ids), paste(dim1_selected_ids, sep=",", collapse="), ("))
    apply_dim2 = paste("apply(", upload, ", ", dim2, ", ", dim2_selected_ids, ")", sep = "")
    redim = paste("redimension(", apply_dim2, ", <idx:int64>[", idname, "])", sep = "")
    
    query= paste("project(
                 cross_join(",
                 arrayname, " as A, ",
                 redim, " as B, ",
                 "A.", idname, ", " ,
                 "B.", idname,
                 "),",
                 paste(names(.ghEnv$meta$L$array[[strip_namespace(arrayname)]]$attributes), collapse = ", "),
                 ")",
                 sep = "")
  }
  query
}


form_selector_query_1d_array = function(arrayname, idname, selected_ids){
  sorted=sort(selected_ids)
  breaks=c(0, which(diff(sorted)!=1), length(sorted))
  THRESH_K = 15  # limit at which to switch from cross_between_ to cross_join
  if (length(breaks) == 2) # completely contiguous set of tickers; use `between`
  {
    query =sprintf("between(%s, %d, %d)", arrayname, sorted[1], sorted[length(sorted)])
  }
  else if (length(breaks) >2 & length(breaks) <= THRESH_K + 2) # few sets of contiguous tickers; use `cross_between`
  {
    cb_pts =  paste( sapply(seq(length(breaks)-1), function(i) sprintf("%d, %d", sorted[breaks[i]+1], sorted[breaks[i+1]])), collapse=" , ")
    #    rt_array=sprintf("build(<dateLo:int64, rowLo: int64, tickerLo: int64, dateHi: int64, rowHi: int64, tickerHi: int64>[idx=1:%d,100000,0],'{1}[%s]',true)", length(breaks)-1, cb_pts)
    query=sprintf("cross_between_(%s, %s)", arrayname, cb_pts)
  }
  else # mostly non-contiguous tickers, use `cross_join`
  {
    # Formulate the cross_join query
    diminfo = .ghEnv$meta$L$array[[strip_namespace(arrayname)]]$dims
    if (class(diminfo) == "character") chunksize = 1000000 else stop("code not covered")
    upload = sprintf("build(<%s:int64>[idx=1:%d,100000,0],'{1}[(%s)]', true)",
                     idname, length(selected_ids), paste(selected_ids, sep=",", collapse="), ("))
    redim = paste("redimension(", upload, ", <idx:int64>[", idname,"=0:*,", as.integer(chunksize), ",0])", sep = "")
    
    query= paste("project(cross_join(",
                 arrayname, " as A, ",
                 redim, "as B, ",
                 "A.", idname, ", " ,
                 "B.", idname,
                 "),",
                 paste(names(.ghEnv$meta$L$array[[strip_namespace(arrayname)]]$attributes), collapse = ", "),
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
search_feature_by_synonym = function(synonym, id_type = NULL, featureset_id = NULL, updateCache = FALSE){
  syn = get_feature_synonym_from_cache(updateCache = updateCache)
  f1 = syn[syn$synonym %in% synonym, ]
  if (!is.null(id_type)) {f1 = f1[f1$source == id_type, ]}
  if (!is.null(featureset_id)) {f1 = f1[f1$featureset_id == f1$featureset_id, ]}
  get_features(feature_id = f1$feature_id, fromCache = !updateCache)
}

#' @export
search_features = function(gene_symbol = NULL, feature_type = NULL, featureset_id = NULL){
  arrayname = .ghEnv$meta$arrFeature
  
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
  
  join_info_ontology_and_unpivot(qq, arrayname)
  # s_join_ontology_terms(qq)[]
}

#' @export
search_genelist_gene = function(genelist_id){
  arrayname = .ghEnv$meta$arrGenelist_gene
  
  qq = arrayname
  if (length(genelist_id)==1){
    qq = paste("filter(", qq, ", genelist_id = ", genelist_id, ")", sep="")
  } else {stop("Not covered yet")}
  
  iquery(.ghEnv$db, qq, return = TRUE)
}

#' @export
search_datasets = function(project_id = NULL, dataset_version = NULL, all_versions = TRUE){
  check_args_search(dataset_version, all_versions)
  arrayname = .ghEnv$meta$arrDataset
  
  qq = arrayname
  if (!is.null(project_id)) {
    namespace = find_namespace(id = project_id, entitynm = .ghEnv$meta$arrProject, dflookup = get_project_lookup())
    if (is.null(namespace)) namespace = NA # to handle the case of no entry in lookup array at all
    if (is.na(namespace)) {
      cat("trying to download project lookup array once again, to see if there has been a recent update")
      namespace = find_namespace(id = project_id, entitynm = .ghEnv$meta$arrProject, dflookup = get_project_lookup(updateCache = TRUE))
    }
    if (!(namespace %in% .ghEnv$cache$nmsp_list)) {stop("user does not have permission to access data for project_id: ", project_id)}
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
                                      namespace = namespace)
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
search_individuals = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_search(dataset_version, all_versions)
  i = find_nmsp_filter_on_dataset_id_and_version(arrayname = .ghEnv$meta$arrIndividuals, dataset_id, dataset_version = dataset_version)
  if (!all_versions) return(latest_version(i)) else return(i)
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

find_nmsp_filter_on_dataset_id_and_version = function(arrayname, dataset_id, dataset_version){
  qq = arrayname
  if (!is.null(dataset_id)) {
    namespace = find_namespace(id = dataset_id, entitynm = .ghEnv$meta$arrDataset, dflookup = get_dataset_lookup())
    if (is.null(namespace)) namespace = NA # to handle the case of no entry in lookup array at all
    if (any(is.na(namespace))) {
      cat("trying to download lookup array once again, to see if there has been a recent update")
      namespace = update_lookup_and_find_namespace_again(entitynm = .ghEnv$meta$arrDataset, id = dataset_id)
    }
    if (!(namespace %in% .ghEnv$cache$nmsp_list)) {stop("user does not have permission to access data for dataset_id: ", dataset_id)}
    fullnm = paste(namespace, ".", qq, sep = "")
    if (is.null(dataset_version)) {
      qq = paste("filter(", fullnm, ", ", "dataset_id = ", dataset_id, ")", sep="")
    } else {
      # TODO: remove condition check after the ordering of dimensions is fixed [entity_id, dataset_version] or [dataset_version, entity_id]
      if (grep("dataset_version", get_idname(arrayname)) == 2) { # datset_version is 2nd dimension
        qq = paste("filter(between(", fullnm, ", null,", dataset_version, ", null, ", dataset_version,  "), ", "dataset_id = ", dataset_id, ")", sep="")
      } else if (grep("dataset_version", get_idname(arrayname)) == 1) { # datset_version is 1st dimension
        qq = paste("filter(between(", fullnm, ", ", dataset_version, ", null,", dataset_version,  ", null), ", "dataset_id = ", dataset_id, ")", sep="")
      }
    }
  } else {
    stop(cat("Must specify dataset_id. To retrieve all ", tolower(arrayname), "s, use get_", tolower(arrayname), "s()", sep = ""))
  }
  
  join_info_ontology_and_unpivot(qq,
                                 arrayname,
                                 namespace = namespace)
}

#' @export
search_biosamples = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_search(dataset_version, all_versions)
  df = find_nmsp_filter_on_dataset_id_and_version(arrayname = .ghEnv$meta$arrBiosample, dataset_id, dataset_version = dataset_version)
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
search_ontology = function(terms, exact_match = TRUE, updateCache = FALSE){
  ont = get_ontology(updateCache = updateCache)
  if (nrow(ont) == 0) return(NA)
  ont_ids = ont$ontology_id
  names(ont_ids) = ont$term
  if (exact_match){
    res = ont_ids[terms]
    if (any(is.na(res)) & !updateCache) {
      cat("Updating ontology cache\n")
      search_ontology(terms, exact_match = exact_match, updateCache = TRUE)
    }
    names(res) = terms
    as.integer(res)
  } else {
    if (length(terms) != 1) {
      # do a recursive call
      if (updateCache) stop("cannot do inexact searching on multiple terms with updateCache set to TRUE")
      lapply(terms, FUN = function(term) {search_ontology(terms = term, exact_match = FALSE)})
    } else {
      ont[grep(terms, ignore.case = TRUE, ont$term), ]
    }
  }
}

#' @export
search_rnaquantificationset = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_search(dataset_version, all_versions)
  df = find_nmsp_filter_on_dataset_id_and_version(arrayname = .ghEnv$meta$arrRnaquantificationset, dataset_id, dataset_version = dataset_version)
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
search_variantsets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_search(dataset_version, all_versions)
  df = find_nmsp_filter_on_dataset_id_and_version(arrayname = .ghEnv$meta$arrVariantset, dataset_id, dataset_version = dataset_version)
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
search_fusionsets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_search(dataset_version, all_versions)
  df = find_nmsp_filter_on_dataset_id_and_version(arrayname = .ghEnv$meta$arrFusionset, dataset_id, dataset_version = dataset_version)
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
search_copynumbersets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrCopyNumberSet, 
                                          dataset_id, dataset_version, all_versions)
}

#' @export
search_experimentsets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  search_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrExperimentSet, 
                                          dataset_id, dataset_version, all_versions)
}

search_versioned_secure_metadata_entity = function(entity, dataset_id, dataset_version, all_versions) {
  check_args_search(dataset_version, all_versions)
  df = find_nmsp_filter_on_dataset_id_and_version(arrayname = entity, dataset_id, 
                                                  dataset_version = dataset_version)
  if (!all_versions) return(latest_version(df)) else return(df)
}


# dataset_version: can be "NULL" or any single integral value (if "NULL", then all versions would be returned back)
cross_between_select_on_two = function(qq, tt, val1, val2, selected_names, dataset_version){
  selector = merge(
    data.frame(dataset_version = dataset_version),
    merge(data.frame(val1 = val1),
          data.frame(val2 = val2)))
  selected_names_all = c('dataset_version', selected_names)
  colnames(selector) = selected_names_all
  selector$flag = TRUE
  
  xx = as.scidb(.ghEnv$db, selector)
  xx1 = xx
  for (attr in selected_names_all){
    xx1 = convert_attr_double_to_int64(arr = xx1, attrname = attr)
  }
  xx1
  
  dims0 = schema(tt, "dimensions")$name
  selectpos = which(dims0 %in% selected_names)
  stopifnot(dims0[selectpos] == selected_names)
  # dims0[selectpos]
  cs = schema(tt, "dimensions")$chunk
  diminfo = data.frame(start = schema(tt, "dimensions")$start,
                       end = schema(tt, "dimensions")$end, stringsAsFactors = FALSE)
  ovlp = schema(tt, "dimensions")$overlap
  # selected_names
  # selectpos
  # fn = function(i) {paste(dims0[i], "=", diminfo$start[i], ":", diminfo$end[i], ",", cs[i], ",", ovlp[i], sep = "" )}
  fn = function(dimname) {yaml_to_dim_str(.ghEnv$meta$L$array[[.ghEnv$meta$arrRnaquantification]]$dims[dimname])}
  newdim = paste(sapply(selected_names_all, FUN = fn), collapse = ",")
  newsch = paste("<flag:bool>[", newdim, "]", sep="")
  xx1 = .ghEnv$db$redimension(xx1, R(newsch))
  
  subq = paste(sapply(selected_names_all, FUN=function(x) {paste(paste("A.", x, sep=""), paste("B.", x, sep=""), sep=", ")}), collapse=", ")
  qq = paste("cross_join(",
             qq, " as A,",
             xx1@name, " as B,", subq, ")", sep="")
  qq = paste("project(", qq, ", ", schema(tt, "attributes")$name, ")")
  
  iquery(.ghEnv$db, qq, return = T)
}

#' @export
get_rnaquantification_counts = function(rnaquantificationset_id = NULL){
  if (is.null(rnaquantificationset_id)){
    x = scidb(.ghEnv$db, .ghEnv$meta$arrRnaquantification)
    c = as.R(.ghEnv$db$aggregate(srcArray = x, AGGREGATE_CALL = "count(*)", groupbyDim = 'rnaquantificationset_id, dataset_version'))
    for (nmsp in .ghEnv$cache$nmsp_list){
      if (nmsp != 'public'){
        x = scidb(.ghEnv$db, paste(nmsp, .ghEnv$meta$arrRnaquantification, sep = "."))
        c1 = as.R(.ghEnv$db$aggregate(srcArray = x, AGGREGATE_CALL = "count(*)", groupbyDim = 'rnaquantificationset_id, dataset_version'))
        c = rbind(c, c1)
      }
    }
  } else { #specific rnaquantificationset_id is specified
    nmsp = find_namespace(id = rnaquantificationset_id, entitynm = .ghEnv$meta$arrRnaquantificationset, dflookup = get_rnaquantificationset_lookup())
    qq = paste("between(", nmsp, ".", .ghEnv$meta$arrRnaquantification, ", null, ", rnaquantificationset_id, ", null, null, null, ", rnaquantificationset_id, ", null, null)", sep = "" )
    qq = paste("aggregate(", qq, ", count(*), rnaquantificationset_id, dataset_version)")
    c = iquery(.ghEnv$db, qq, return = T)
  }
  c = c[order(c$rnaquantificationset_id, c$dataset_version), ]
  return(c)
}

join_info_ontology_and_unpivot = function(qq, arrayname, namespace = 'public', mandatory_fields_only = FALSE){
  unpivot = TRUE
  idname = get_idname(arrayname)
  
  qq1 = qq
  # Join INFO array
  if (!mandatory_fields_only) {
    if (exists('debug_trace')) {t1 = proc.time()}
    if (FALSE){ # TODO: See why search_individuals(dataset_id = 1) is so slow; the two options here did not make a difference
      qq1 = paste("cross_join(", qq1, " as A, ", namespace, ".", arrayname, "_INFO as B, A.", idname, ", B.", idname, ")", sep = "")
    } else if (FALSE) { cat("== Swapping order of cross join between ARRAY_INFO and select(ARRAY, ..)\n")
      qq1 = paste("cross_join(", namespace, ".", arrayname, "_INFO as A, ", qq1, " as B, A.", idname, ", B.", idname, ")", sep = "")
    } else {
      qq1 = paste("equi_join(", qq1, ", ", namespace, ".", arrayname, "_INFO, 'left_names=", paste(idname, collapse = ","), "', 'right_names=", paste(idname, collapse = ","),  "', 'left_outer=true', 'keep_dimensions=true')", sep = "")
    }
  }
  x2 = iquery(.ghEnv$db, qq1, return = TRUE)

  if (exists('debug_trace')) {cat("join with info:\n"); print( proc.time()-t1 )}
  
  if (nrow(x2) > 0 & sum(colnames(x2) %in% c("key", "val")) == 2 & unpivot){ # If key val pairs exist and need to be unpivoted
    if (exists('debug_trace')) {t1 = proc.time()}
    x3 = unpivot_key_value_pairs(df = x2, arrayname = arrayname)
    if (exists('debug_trace')) {cat("unpivot:\n"); print( proc.time()-t1 )}
  } else {
    if (nrow(x2) > 0) {
      x3 = x2[, c(idname,
                  names(.ghEnv$meta$L$array[[strip_namespace(arrayname)]]$attributes))]
    } else {
      x3 = x2[, names(.ghEnv$meta$L$array[[strip_namespace(arrayname)]]$attributes)]
    }
  }
  
  # Join ontology terms
  #   if (is.null(.ghEnv$cache$dfOntology)) { # when ontology has not been downloaded
  #     if (exists('debug_trace')) {t1 = proc.time()}
  #     .ghEnv$cache$dfOntology = iquery(.ghEnv$db, .ghEnv$meta$arrOntology, return = TRUE)
  #     if (exists('debug_trace')) {cat("download arrOntology:\n"); print( proc.time()-t1 )}
  #   }
  join_ontology_terms(df = x3)
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
      stop(cat("No entries for entity", entity, "at ids:", paste(missing_ids, collapse = ", "), 
               "at specified version", sep = " "))
    } else {
      stop(cat("No entries for entity", entity, "at ids:", paste(missing_ids, collapse = ", "),
               sep = " ")) 
    }
  } else {
    return(TRUE)
  }
}

#' @export
register_expression_dataframe = function(df1, dataset_version){
  stopifnot(c('rnaquantificationset_id', 'biosample_id', 'feature_id', 'expression')
            %in% colnames(df1))
  
  check_entity_exists_at_id(entity = 'RNAQUANTIFICATIONSET',
                            id = sort(unique(df1$rnaquantificationset_id)))
  check_entity_exists_at_id(entity = 'BIOSAMPLE',
                            id = sort(unique(df1$rnaquantificationset_id)))
  #   check_entity_exists_at_id(entity = 'FEATURE',
  #                             id = sort(unique(df1$feature_id)))
  
  
  namespace_to_insert = unique(find_namespace(id = unique(df_expr$rnaquantificationset_id), 
                                              entitynm = .ghEnv$meta$arrRnaquantificationset))
  if (length(namespace_to_insert) != 1) stop("Error: Trying to insert expression data into multiple namespaces")
  
  adf_expr0 = as.scidb(.ghEnv$db, df_expr, chunk_size=nrow(df_expr), name = "temp_df")
  
  adf_expr = convert_attr_double_to_int64(arr = adf_expr0, attrname = "rnaquantificationset_id")
  adf_expr = convert_attr_double_to_int64(arr = adf_expr, attrname = "biosample_id")
  adf_expr = convert_attr_double_to_int64(arr = adf_expr, attrname = "feature_id")
  adf_expr = scidb(.ghEnv$db, paste("apply(", 
                                    adf_expr@name, 
                                    ", expression_count, float(expression),
                                    dataset_version, ", dataset_version, ")", 
                                    sep = ""))
  
  subarr = .ghEnv$db$redimension(adf_expr, R(schema(scidb(.ghEnv$db, .ghEnv$meta$arrRnaquantification))))
  
  fullnm = paste(namespace_to_insert, .ghEnv$meta$arrRnaquantification, sep = ".")
  cat("inserting data for", nrow(df_expr), "expression values into", fullnm, "array \n")
  iquery(.ghEnv$db, paste("insert(", subarr@name, ", ", fullnm, ")"))
  .ghEnv$db$remove("temp_df")
}

#' @export
register_expression_matrix = function(filepath,
                                      rnaquantificationset_id,
                                      featureset_id,
                                      feature_type,
                                      dataset_version = NULL,
                                      only_test = FALSE){
  test_register_expression_matrix(filepath,
                                  rnaquantificationset_id,
                                  featureset_id,
                                  feature_type,
                                  dataset_version)
  if (!only_test) {
    if (is.null(dataset_version)) {
      rqset = get_rnaquantificationsets(rnaquantificationset_id = rnaquantificationset_id) # finds the latest version
      dataset_version = rqset$dataset_version
      cat("Dataset version not specified. Inferred version from rnaquantificationset_id as version:", dataset_version, "\n")
    } else {
      stopifnot(length(dataset_version) == 1)
      rqset = get_rnaquantificationsets(rnaquantificationset_id = rnaquantificationset_id,
                                        dataset_version = dataset_version)
    }
    dataset_id = rqset$dataset_id
    cat("Specified rnaquantificationset_id belongs to dataset:", dataset_id, "\n")
    
    arr_feature = .ghEnv$meta$arrFeature
    namespace = find_namespace(id = rnaquantificationset_id, entitynm = .ghEnv$meta$arrRnaquantificationset, dflookup = get_rnaquantificationset_lookup())
    arr_biosample = paste(namespace, .ghEnv$meta$arrBiosample, sep = ".")
    arrayname = paste(namespace, .ghEnv$meta$arrRnaquantification, sep = ".")
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
    t1 = scidb(.ghEnv$db, query)
    t1 = .ghEnv$db$between(srcArray = t1, lowCoord = "NULL, NULL, NULL, NULL", highCoord = R(paste("NULL, NULL, NULL,", length(colnames(x))-1)))
    t1 = store(.ghEnv$db, t1, temp=TRUE)
    
    # ================================
    ## Step 1. Join the SciDB feature ID
    
    # first form the list of features in current table
    # featurelist_curr = subset(t1, attribute_no == 0)
    featurelist_curr = .ghEnv$db$between(srcArray = t1, lowCoord = "NULL, NULL, NULL, 0", highCoord = "NULL, NULL, NULL, 0")
    cat(paste("number of feature_id-s in the current expression count table:", scidb_array_count(featurelist_curr)-1, "\n"))
    FEATUREKEY = scidb(.ghEnv$db, arr_feature)
    cat(paste("number of feature_id-s in the SciDB feature ID list:", scidb_array_count(FEATUREKEY), "\n"))
    
    sel_features = .ghEnv$db$filter(FEATUREKEY, R(paste("feature_type='", feature_type, "' AND featureset_id = ", featureset_id, sep = "")))
    cat(paste("number of feature_id-s in the SciDB feature ID list for transcript type: '", feature_type,
              "' and featureset_id: '", featureset_id, "' is:", scidb_array_count(sel_features), "\n", sep = ""))
    
    ff = .ghEnv$db$project(srcArray = sel_features, selectedAttr = "name")
    
    joinFeatureName = scidb(.ghEnv$db,
                            paste("equi_join(",
                                  ff@name, ", ",
                                  featurelist_curr@name, ", ",
                                  "'left_names=name', 'right_names=a', 'keep_dimensions=1')")
    )
    joinFeatureName = .ghEnv$db$redimension(srcArray = joinFeatureName,
                                            "<feature_id :int64>
                                            [tuple_no=0:*,10000000,0,dst_instance_id=0:63,1,0,src_instance_id=0:63,1,0]")
    
    joinBack1 = scidb(.ghEnv$db,
                      paste("cross_join(",
                            t1@name, " as X, ",
                            joinFeatureName@name, " as Y, ",
                            "X.tuple_no, Y.tuple_no, X.dst_instance_id, Y.dst_instance_id, X.src_instance_id, Y.src_instance_id)", sep = ""))
    joinBack1@name
    
    joinBack1 = store(.ghEnv$db, joinBack1, temp=TRUE)
    
    # Verify with
    scidb_array_head(.ghEnv$db$between(srcArray = joinBack1, lowCoord = "0, NULL, NULL, NULL", highCoord = "0, NULL, NULL, NULL"))
    
    cat("Number of features in study that matched with SciDB ID:\n")
    countFeatures = scidb_array_count(joinBack1) / ncol(x)
    print(countFeatures)
    
    stopifnot(countFeatures == (scidb_array_count(featurelist_curr)-1))
    # ================================
    ## Step 2. Join the SciDB patient ID
    # first form the list of patients in current table
    
    patientlist_curr = .ghEnv$db$between(t1, lowCoord = "0, 0, NULL, 1", highCoord = "0, 0, NULL, NULL")
    # Check that the "public_id"_"spectrum_id" is unique enough, otherwise we have to consider the suffix "BM", "PB"
    stopifnot(length(unique(as.R(patientlist_curr)$a)) == (ncol(x)-1))
    
    scidb_array_head(patientlist_curr)
    
    cat(paste("number of biosamples in the expression count table:", scidb_array_count(patientlist_curr), "\n"))
    PATIENTKEY = scidb(.ghEnv$db, arr_biosample)
    PATIENTKEY = .ghEnv$db$filter(PATIENTKEY, R(paste('dataset_id=', dataset_id)))
    cat(paste("number of biosamples registered in database in selected namespace:" , scidb_array_count(PATIENTKEY), "\n"))
    
    
    # now do the joining
    qq = paste("equi_join(",
               patientlist_curr@name, ", ",
               "project(filter(", PATIENTKEY@name, ", dataset_version = ", dataset_version, "), name), ",
               "'left_names=a', 'right_names=name', 'keep_dimensions=1')")
    joinPatientName = scidb(.ghEnv$db, qq)
    
    cat("number of matching public_id-s between expression-count table and PER_PATIENT csv file:\n")
    countMatches = scidb_array_count(joinPatientName)
    print(countMatches)
    
    
    
    # Verify
    x1 = as.R(.ghEnv$db$project(PATIENTKEY, "name"))
    x2 = as.R(patientlist_curr)
    
    stopifnot(countMatches == sum(x2$a %in% x1$name))
    
    # cat("The expression count table public_id-s that are not present in PER_PATIENT csv file: \n")
    # tt = x2$public_id %in% x1$PUBLIC_ID
    # print(x2$public_id[which(!tt)])
    
    joinPatientName = .ghEnv$db$redimension(joinPatientName,
                                            R(paste("<biosample_id:int64>
                                                    [attribute_no=0:", countMatches+1, ",",countMatches+2, ",0]", sep = "")))
    
    joinBack2 = scidb(.ghEnv$db,
                      paste("cross_join(",
                            joinBack1@name, " as X, ",
                            joinPatientName@name, "as Y, ",
                            "X.attribute_no, Y.attribute_no)", sep = ""))
    joinBack2@name
    
    joinBack2 = store(.ghEnv$db, joinBack2, temp=TRUE)
    
    # Verify with
    scidb_array_head(.ghEnv$db$filter(joinBack2, "tuple_no = 0"))
    
    cat("Number of expression level values in current array:\n")
    countExpressions = scidb_array_count(joinBack2)
    print(countExpressions)
    stopifnot(countExpressions == countMatches*countFeatures)
    
    ####################
    # Redimension the expression level array
    gct_table = scidb(.ghEnv$db,
                      paste("apply(",
                            joinBack2@name, ", ",
                            "expression_count, dcast(a, float(null)), ",
                            "rnaquantificationset_id, ", rnaquantificationset_id,
                            ", dataset_version, ", dataset_version,
                            ")", sep = "")
    )
    
    # Need to insert the expression matrix table into one big array
    insertable = .ghEnv$db$redimension(gct_table,
                                       R(scidb::schema(scidb(.ghEnv$db, arrayname)))) # TODO: would be good to not have this resolution clash
    
    if (scidb_exists_array(arrayname)) {
      cat(paste("Inserting expression matrix data into", arrayname, "at dataset_version", dataset_version, "\n"))
      iquery(.ghEnv$db, paste("insert(", insertable@name, ", ", arrayname, ")"))
    } else {
      stop("expression array does not exist")
    }
    
    return(rnaquantificationset_id)
  } # end of if (!only_test)
}

#' @export
register_copynumber_seg = function(experimentset, only_test = FALSE){
  test_register_copynumber_seg(experimentset)
  if (!only_test) {
    dataset_id = experimentset$dataset_id
    b = search_biosamples(dataset_id = dataset_id, dataset_version = experimentset$dataset_version)
    
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
                                     entitynm = .ghEnv$meta$arrExperimentSet),
                      .ghEnv$meta$arrCopynumber_seg, sep = ".")
    
    cat("Inserting", nrow(xx), "entries into array:", arrayname, "at version", experimentset$dataset_version, "\n")
    register_tuple(df = xx,
                   ids_int64_conv = c('start', 'end', 'dataset_version', 'experimentset_id', 'biosample_id'),
                   arrayname = arrayname)
  } # end of if (!only_test)
}

#' @export
register_copynumber_matrix_file = function(copynumberset, dataset_version, featureset_id, 
                                           only_test = FALSE){
  test_register_copnyumber_matrix_file(copynumberset, dataset_version)
  if (!only_test) {
    dataset_id = copynumberset$dataset_id
    dataset_version = copynumberset$dataset_version
    b = search_biosamples(dataset_id = dataset_id, dataset_version = dataset_version)
    
    xx = read.delim(copynumberset$file_path)
    matched_biosample_id = b[match(tail(colnames(xx), -1), b$name), ]$biosample_id
    if (!(all(!is.na(matched_biosample_id)))) {
      stop("Found biosamples in .mat file that are not previously registered on system")
    }
    colnames(xx) = c(colnames(xx)[1], matched_biosample_id)
    
    ff = search_features(featureset_id = featureset_id, feature_type = 'gene')
    
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
                               entitynm = .ghEnv$meta$arrCopyNumberSet)
    arrayname = paste(namespace, .ghEnv$meta$arrCopynumber_mat, sep = ".")
    cat("Inserting", nrow(xx2), "entries into", arrayname, "at version", dataset_version, "\n")
    register_tuple(df = xx2, 
                   ids_int64_conv = get_idname(.ghEnv$meta$arrCopynumber_mat), 
                   arrayname = arrayname)
  } # end of if (!only_test)
  }

#' @export
register_fusion_data = function(df, fusionset, only_test = FALSE){
  test_register_fusion_data(df, fusionset)
  if (!only_test) {
    dataset_id = fusionset$dataset_id
    dataset_version = fusionset$dataset_version
    
    xx = df
    
    update_feature_synonym_cache()
    syn = get_feature_synonym_from_cache()
    
    # Now register the left and right genes with system feature_id-s
    xx$feature_id_left = syn[match(xx$gene_left, syn$synonym), ]$feature_id
    xx$feature_id_right = syn[match(xx$gene_right, syn$synonym), ]$feature_id
    stopifnot(!any(is.na(xx$feature_id_left)))
    stopifnot(!any(is.na(xx$feature_id_right)))
    
    # Biosamples
    b = search_biosamples(dataset_id = dataset_id, dataset_version = dataset_version)
    xx$biosample_id = b[match(xx$biosample_name, b$name), ]$biosample_id
    stopifnot(!any(is.na(xx$biosample_id)))
    
    # Rename some columns
    colnames(xx)[colnames(xx) == 'Sample'] = 'sample_name_unabbreviated'
    colnames(xx)[colnames(xx) == 'chrom_left'] = 'reference_name_left'
    colnames(xx)[colnames(xx) == 'chrom_right'] = 'reference_name_right'
    
    xx$fusionset_id = fusionset_record$fusionset_id
    xx$dataset_version = dataset_version
    
    nmsp = find_namespace(id = fusionset_record$fusionset_id, entitynm = .ghEnv$meta$arrFusionset)
    arrayname = paste(nmsp, .ghEnv$meta$arrFusion, sep = ".")
    
    xx = xx %>% group_by(biosample_id) %>% mutate(fusion_id = row_number())
    xx = as.data.frame(xx)
    
    cat("registering", nrow(xx), "entries of fusion data into array", arrayname, "\n")
    register_tuple(df = xx,
                   ids_int64_conv = c(
                     get_idname(arrayname), get_int64fields(arrayname)),
                   arrayname = arrayname
    )
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
  exprs = acast(expr_df, feature_id~biosample_id, value.var="expression_count")
  
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
update_entity = function(entity, df){
  if (is_entity_secured(entity)){
    namespaces = find_namespace(id = df[, get_base_idname(entity)], entitynm = entity)
    nmsp = unique(namespaces)
    if (length(nmsp) != 1) stop("entity to be updated must belong to one namespace only")
  } else {
    nmsp = "public"
  }
  fullarraynm = paste(nmsp, entity, sep = ".")
  update_mandatory_and_info_fields(df = df, arrayname = fullarraynm)
}

get_entity_count_old = function(){
  entities = c(.ghEnv$meta$arrProject, .ghEnv$meta$arrDataset, 
               .ghEnv$meta$arrIndividuals, .ghEnv$meta$arrBiosample, 
               .ghEnv$meta$arrRnaquantificationset, 
               .ghEnv$meta$arrVariantset,
               .ghEnv$meta$arrFusionset,
               .ghEnv$meta$arrCopyNumberSet, 
               .ghEnv$meta$arrExperimentSet)
  if (length(.ghEnv$cache$nmsp_list) == 1){
    nmsp = .ghEnv$cache$nmsp_list
    queries = sapply(entities, function(entity){paste("op_count(", nmsp, ".", entity, ")", sep = "")})
  }
  else if (length(.ghEnv$cache$nmsp_list) == 2){
    queries = sapply(entities, FUN = function(entity) {
      paste("join(",
            paste(sapply(.ghEnv$cache$nmsp_list, function(nmsp){paste("op_count(", nmsp, ".", entity, ")", sep = "")}), collapse=", "),
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
    sapply(entities, FUN=function(entity) {paste(entity, "_", .ghEnv$cache$nmsp_list, sep = "")}),
    ":uint64",
    sep = "",
    collapse = ", ")
  res_schema = paste("<", attrs, ">[i=0:0,1,0]", sep = "")
  res = iquery(.ghEnv$db, qq,
               schema = res_schema, return = T)
  colnames(res) = gsub("X_", "", colnames(res))
  
  xx = sapply(entities, FUN=function(entity) {res[, paste(entity, .ghEnv$cache$nmsp_list, sep = "_")]})
  
  counts = data.frame(entity = entities)
  if (length(.ghEnv$cache$nmsp_list) == 1){
    counts[, .ghEnv$cache$nmsp_list] = as.integer(xx)
  } else {
    rownum = 1
    for (nmsp in .ghEnv$cache$nmsp_list){
      counts[, nmsp] = as.integer(xx[rownum,])
      rownum = rownum + 1
    }
  }
  counts
}

#' @export
get_entity_count = function(new_function = FALSE, skip_measurement_data = TRUE){
  if (!new_function) {
    get_entity_count_old()
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
    res = iquery(.ghEnv$db, qq, return = T)
    
    colnames(res) = c('i', entity_arrays)
    
    res2 = data.frame(entity = entities)
    rownames(res2) = 1:nrow(res2)
    
    counts = t(sapply(res2$entity, function(entity){
      sapply(.ghEnv$cache$nmsp_list, function(nmsp){
        fullarnm = paste(nmsp, entity, sep = ".")
        ifelse(fullarnm %in% colnames(res), res[, fullarnm], NA)
      })
    }))
    
    res2 = cbind(res2, counts)  
    res2
  }
}
