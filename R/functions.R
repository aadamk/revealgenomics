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
gh_connect = function(username, password, host = NULL, port = 8083, protocol = "https"){
  jdb$meta$L = yaml.load_file(system.file("data", "SCHEMA.yaml", package="scidb4gh"))

  jdb$meta$arrProject = 'PROJECT'
  jdb$meta$arrDataset = 'DATASET'
  jdb$meta$arrIndividuals = 'INDIVIDUAL'
  jdb$meta$arrOntology = 'ONTOLOGY'
  jdb$meta$arrBiosample = 'BIOSAMPLE'
  jdb$meta$arrRnaquantificationset = 'RNAQUANTIFICATIONSET'
  jdb$meta$arrRnaquantification = 'RNAQUANTIFICATION'
  jdb$meta$arrFeature = 'FEATURE'
  jdb$meta$arrFeatureSynonym = 'FEATURE_SYNONYM'
  jdb$meta$arrFeatureset = 'FEATURESET'
  jdb$meta$arrVariantset = 'VARIANTSET'
  jdb$meta$arrVariant = 'VARIANT'
  jdb$meta$arrFusionset = 'FUSIONSET'
  jdb$meta$arrFusion = 'FUSION'
  jdb$meta$arrCopyNumberSet = 'COPYNUMBERSET'
  jdb$meta$arrCopyNumberSubSet = 'COPYNUMBERSUBSET'
  jdb$meta$arrCopynumber_seg = 'COPYNUMBER_SEG'
  jdb$meta$arrCopynumber_mat = 'COPYNUMBER_MAT'

  # Prepare variables for the cache
  jdb$cache$ontology_ref = NULL
  jdb$cache$lookup = list()
  jdb$cache$lookup[[jdb$meta$arrProject]] = NULL
  jdb$cache$lookup[[jdb$meta$arrDataset]] = NULL
  jdb$cache$lookup[[jdb$meta$arrIndividuals]] = NULL
  jdb$cache$lookup[[jdb$meta$arrBiosample]] = NULL
  jdb$cache$lookup[[jdb$meta$arrRnaquantificationset]] = NULL
  jdb$cache$lookup[[jdb$meta$arrVariantset]] = NULL
  jdb$cache$lookup[[jdb$meta$arrFusionset]] = NULL
  jdb$cache$feature_ref = NULL
  jdb$cache$dfFeatureSynonym = NULL
  jdb$cache$biosample_ref = NULL

  # SciDB connection and R API
  if (is.null(host)) {
    jdb$db = scidbconnect(username = username, password = password, port = port, protocol = protocol)
  } else {
    jdb$db = scidbconnect(host = host, username = username, password = password, port = port, protocol = protocol)
  }

  jdb$cache$nmsp_list = iquery(jdb$db, "list('namespaces')", schema = "<name:string NOT NULL> [No=0:1,2,0]", return = T)$name
  jdb$cache$nmsp_list = jdb$cache$nmsp_list[jdb$cache$nmsp_list %in% c('public', 'clinical', 'collaboration')]

  # return(jdb)
}

entity_lookup = function(entityName, updateCache = FALSE){
  if (updateCache | is.null(jdb$cache$lookup[[entityName]])){
    jdb$cache$lookup[[entityName]] = iquery(jdb$db, paste(entityName, "_LOOKUP", sep = ""),
                                            schema = paste("<namespace:string> [", get_idname(entityName), "=0:*]", sep = ""),
                                            return = T)
  }
  return(jdb$cache$lookup[[entityName]])
}

get_project_lookup = function(updateCache = FALSE){
  entity_lookup(jdb$meta$arrProject, updateCache = updateCache)
}
get_dataset_lookup = function(updateCache = FALSE){
  entity_lookup(jdb$meta$arrDataset, updateCache = updateCache)
}
get_individuals_lookup = function(updateCache = FALSE){
  entity_lookup(jdb$meta$arrIndividuals, updateCache = updateCache)
}
get_biosample_lookup = function(updateCache = FALSE){
  entity_lookup(jdb$meta$arrBiosample, updateCache = updateCache)
}
get_rnaquantificationset_lookup = function(updateCache = FALSE){
  entity_lookup(jdb$meta$arrRnaquantificationset, updateCache = updateCache)
}
get_variantset_lookup = function(updateCache = FALSE){
  entity_lookup(jdb$meta$arrVariantset, updateCache = updateCache)
}
get_fusionset_lookup = function(updateCache = FALSE){
  entity_lookup(jdb$meta$arrFusionset, updateCache = updateCache)
}

get_entity = function(entity, ids){
  fn_name = paste("get_", tolower(entity), sep = "")
  f = NULL
  try({f = get(fn_name)}, silent = TRUE)
  if (is.null(f)) try({f = get(paste(fn_name, "s", sep = ""))}, silent = TRUE)
  if (entity == 'ONTOLOGY') {
    f(ids, updateCache = TRUE)
  } else if (entity == 'FEATURE') {
    f(ids, fromCache =  FALSE)
  } else { 
    f(ids) 
  }
}

# cat("Downloading reference dataframes for fast ExpressionSet formation\n")
get_feature_from_cache = function(updateCache = FALSE){
  if (updateCache | is.null(jdb$cache$feature_ref)){
    update_feature_cache()
  }
  return(jdb$cache$feature_ref)
}
update_feature_cache = function(){
  jdb$cache$feature_ref = NULL
  jdb$cache$feature_ref = get_features(fromCache = FALSE)
}

get_biosamples_from_cache = function(updateCache = FALSE){
  if (updateCache | is.null(jdb$cache$biosample_ref)){
    update_biosample_cache()
  }
  return(jdb$cache$biosample_ref)
}

update_biosample_cache = function(){
  jdb$cache$biosample_ref = get_biosamples()
}

get_ontology_from_cache = function(updateCache = FALSE){
  if (updateCache | is.null(jdb$cache$dfOntology)){
    update_ontology_cache()
  }
  if (nrow(jdb$cache$dfOntology) == 0) update_ontology_cache()
  return(jdb$cache$dfOntology)
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
  jdb$cache$dfOntology = iquery(jdb$db, jdb$meta$arrOntology, return = TRUE)
}

get_feature_synonym_from_cache = function(updateCache = FALSE){
  if (updateCache | is.null(jdb$cache$dfFeatureSynonym)){
    update_feature_synonym_cache()
  }
  return(jdb$cache$dfFeatureSynonym)
}
update_feature_synonym_cache = function(){
  jdb$cache$dfFeatureSynonym = iquery(jdb$db, jdb$meta$arrFeatureSynonym, return = TRUE)
}

#' @export
scidb_exists_array = function(arrayName) {
  !is.null(tryCatch({iquery(jdb$db, paste("show(", arrayName, ")", sep=""), return=TRUE, binary = FALSE)}, error = function(e) {NULL}))
}

lookup_exists = function(entitynm){
  entitynm = strip_namespace(entitynm)
  ifelse(length(jdb$meta$L$array[[entitynm]]$namespace) > 1, TRUE, FALSE)
}

dataset_versioning_exists = function(entitynm){
  "dataset_version" %in% get_idname(entitynm)
}

get_idname = function(arrayname){
  local_arrnm = strip_namespace(arrayname)
  idname = jdb$meta$L$array[[local_arrnm]]$dims
  if (class(idname) == "character") return(idname) else return(names(idname))
}

get_base_idname = function(arrayname){
  dims = get_idname(arrayname)
  dims[!(dims %in% "dataset_version")]
}

get_int64fields = function(arrayname){
  local_arrnm = strip_namespace(arrayname)
  attr_types = unlist(jdb$meta$L$array[[local_arrnm]]$attributes)
  int64_fields = names(which(attr_types != "string" &
                               attr_types != "datetime" &
                               attr_types != "int32" &
                               attr_types != "double"))
  stopifnot(all(unique(attr_types[int64_fields]) %in% c("int64", "numeric")))
  int64_fields
}

#' @export
get_entity_names = function(){
  varnames = names(jdb$meta)
  varnames = varnames[varnames != "L"]
  sapply(varnames, function(nm){as.character(jdb$meta[nm])})
}

get_max_id = function(arrayname){
  if (lookup_exists(arrayname)) { # Lookup array must exist
    max = iquery(jdb$db,
                 paste("aggregate(apply(public.",
                       strip_namespace(arrayname), "_LOOKUP, id, ",
                       get_base_idname(arrayname), "), max(id))", sep = ""
                 ),
                 return=TRUE)$id_max
  } else { # Only exists in public namespaces
    max = iquery(jdb$db, paste("aggregate(apply(", arrayname,
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
  arr = jdb$db$apply(srcArray = arr, newAttr = R(attrname), expression = int64(R(randString)))
  arr = jdb$db$project(arr, R(paste(attrnames, collapse = ", ")))
  arr
}


update_tuple = function(df, ids_int64_conv, arrayname){
  if (nrow(df) < 100000) {x1 = as.scidb(jdb$db, df)} else {x1 = as.scidb(jdb$db, df, chunk_size=nrow(df))}

  x = x1
  for (idnm in ids_int64_conv){
    x = convert_attr_double_to_int64(arr = x, attrname = idnm)
  }
  x = scidb_attribute_rename(arr = x, old = "updated", new = "updated_old")
  x = jdb$db$apply(srcArray = x, newAttr = "updated", expression = "string(now())")
  x = jdb$db$redimension(srcArray = x, schemaArray = R(schema(scidb(jdb$db, arrayname))))

  query = paste("insert(", x@name, ", ", arrayname, ")", sep="")
  iquery(jdb$db, query)
}

register_tuple = function(df, ids_int64_conv, arrayname){
  if (nrow(df) < 100000) {x1 = as.scidb(jdb$db, df)} else {x1 = as.scidb(jdb$db, df, chunk_size=nrow(df))}

  x = x1
  for (idnm in ids_int64_conv){
    x = convert_attr_double_to_int64(arr = x, attrname = idnm)
  }
  x = jdb$db$apply(srcArray = x, newAttr = "created", expression = "string(now())")
  x = jdb$db$apply(srcArray = x, newAttr = "updated", expression = "created")
  x = jdb$db$redimension(srcArray = x, schemaArray = R(schema(scidb(jdb$db, arrayname))))

  query = paste("insert(", x@name, ", ", arrayname, ")", sep="")
  iquery(jdb$db, query)
}

#' @export
register_project = function(df,
                            namespace,
                            only_test = FALSE){
  uniq = 'name'
  test_register_project(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = paste(namespace, ".", jdb$meta$arrProject, sep = "")
    register_tuple_return_id(df, arrayname, uniq)
  } # end of if (!only_test)
}

#' @export
register_dataset = function(df,
                            dataset_version = 1,
                            only_test = FALSE
){
  uniq = c('project_id', 'name')

  test_register_dataset(df, uniq, dataset_version, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    namespace = find_namespace(id = unique(df$project_id), entitynm=jdb$meta$arrProject)
    arrayname = paste(namespace, ".", jdb$meta$arrDataset, sep = "")
    register_tuple_return_id(df, arrayname, uniq, dataset_version = dataset_version)
  } # end of if (!only_test)
}

find_namespace = function(id, entitynm, dflookup = NULL){
  if (is.null(dflookup)) { # if need to download the lookup array from scidb
    lookuparr = paste(entitynm, "_LOOKUP", sep = "")
    dflookup = iquery(jdb$db, lookuparr, return = T)
    # dflookup[, get_idname(entitynm)] = as.integer(dflookup[, get_idname(entitynm)])
  }
  dflookup[match(id, dflookup[, get_base_idname(entitynm)]), ]$namespace
}

#' @export
register_ontology_term = function(df, only_test = FALSE){
  uniq = "term"
  test_register_ontology(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = jdb$meta$arrOntology
    register_tuple_return_id(df, arrayname, uniq)
  } # end of if (!only_test)
}

#' @export
register_individual = function(df,
                               dataset_version = NULL,
                               only_test = FALSE){
  uniq = c('dataset_id', 'name')
  test_register_individual(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    if (is.null(dataset_version)) {
      dataset_version = get_dataset_max_version(dataset_id = unique(df$dataset_id), updateCache = TRUE)
      cat("dataset_version was not specified. Registering at version", dataset_version, "of dataset", unique(df$dataset_id), "\n")
    }
    namespace = find_namespace(id = unique(df$dataset_id), entitynm=jdb$meta$arrDataset)
    arrayname = paste(namespace, ".", jdb$meta$arrIndividuals, sep = "")
    register_tuple_return_id(df, arrayname, uniq, dataset_version = dataset_version)
  } # end of if (!only_test)
}

#' @export
register_featureset = function(df, only_test = FALSE){
  uniq = 'name'
  test_register_featureset(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = jdb$meta$arrFeatureset
    register_tuple_return_id(df,
                           arrayname, uniq)
  } # end of if (!only_test)
}

#' @export
register_feature = function(df, only_test = FALSE){
  uniq = c("name", "featureset_id", "feature_type")
  test_register_feature(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = jdb$meta$arrFeature
    register_tuple_return_id(df, arrayname, uniq)
  } # end of if (!only_test)
}

#' @export
register_biosample = function(df,
                              dataset_version = NULL,
                              only_test = FALSE){
  uniq = c('dataset_id', 'name')
  test_register_biosample(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    if (is.null(dataset_version)) {
      dataset_version = get_dataset_max_version(dataset_id = unique(df$dataset_id), updateCache = TRUE)
      cat("dataset_version was not specified. Registering at version", dataset_version, "of dataset", unique(df$dataset_id), "\n")
    }
    namespace = find_namespace(unique(df$dataset_id), entitynm=jdb$meta$arrDataset)
    arrayname = paste(namespace, ".", jdb$meta$arrBiosample, sep = "")
    register_tuple_return_id(df, arrayname, uniq, dataset_version)
  } # end of if (!only_test)
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
  infoArray = jdb$meta$L$array[[strip_namespace(arrayname)]]$infoArray
  if (is.null(infoArray)) infoArray = TRUE
  return(infoArray)
}

# Wrapper function that (1) updates mandatory fields, (2) updates flex fields
update_mandatory_and_info_fields = function(df, arrayname){
  idname = get_idname(arrayname)
  if (any(is.na(df[, idname]))) stop("Dimensions: ", paste(idname, collapse = ", "), " should not have null values at upload time!")
  int64_fields = get_int64fields(arrayname)
  infoArray = get_infoArray(arrayname)
  update_tuple(df, ids_int64_conv = c(idname, int64_fields), arrayname)
  if(infoArray){
    delete_info_fields(fullarrayname = arrayname,
                       ids = df[, get_base_idname(arrayname)],
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

  mandatory_fields = names(jdb$meta$L$array[[strip_namespace(arrayname)]]$attributes)
  namespaces_for_entity = jdb$meta$L$array[[strip_namespace(arrayname)]]$namespace

  df = prep_df_fields(df, mandatory_fields)

  if (!is.null(dataset_version)) {
    df[, "dataset_version"] = dataset_version
  }

  if (is.null(uniq)){ # No need to match existing elements, just append data
    stop("Registering entity without a set of unique fields is not allowed")
  }

  # Find matches by set of unique fields provided by user
  xx = iquery(jdb$db, paste("project(", arrayname, ", ", paste(uniq, collapse = ", "), ")", sep = ""), return = TRUE)
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

update_lookup_array = function(id, arrayname){
  name = strip_namespace(arrayname)
  namespace = get_namespace(arrayname)

  lookuparr = paste(name, "_LOOKUP", sep = "")
  a = scidb(jdb$db, lookuparr)

  qq = sprintf("build(<%s:int64, namespace: string>[idx=1:%d,100000,0],'{1}[(%s)]', true)",
               get_base_idname(arrayname), length(id), paste(id, namespace, sep=",", collapse="), ("))
  qq = paste("redimension(", qq, ", ", schema(a), ")", sep = "")
  qq = paste("insert(", qq, ", ", lookuparr, ")", sep = "")
  cat("inserting id-s:", id, "into array", lookuparr, "\n")
  # cat("query:", qq, "\n")
  iquery(jdb$db, qq)
}

#' @export
register_variantset = function(df, dataset_version = NULL, only_test = FALSE){
  uniq = c('dataset_id', 'name')
  test_register_variantset(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    if (is.null(dataset_version)) {
      dataset_version = get_dataset_max_version(dataset_id = unique(df$dataset_id), updateCache = TRUE)
      cat("dataset_version was not specified. Registering at version", dataset_version, "of dataset", unique(df$dataset_id), "\n")
    }
    namespace = find_namespace(unique(df$dataset_id), entitynm=jdb$meta$arrDataset)
    arrayname = paste(namespace, ".", jdb$meta$arrVariantset, sep = "")
    register_tuple_return_id(df, arrayname, uniq, dataset_version = dataset_version)
  } # end of if (!only_test)
}

#' @export
register_rnaquantificationset = function(df, dataset_version = NULL, only_test = FALSE){
  uniq = c("dataset_id", "name")
  test_register_rnaquantificationset(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    if (is.null(dataset_version)) {
      dataset_version = get_dataset_max_version(dataset_id = unique(df$dataset_id), updateCache = TRUE)
      cat("dataset_version was not specified. Registering at version", dataset_version, "of dataset", unique(df$dataset_id), "\n")
    }
    namespace = find_namespace(unique(df$dataset_id), entitynm=jdb$meta$arrDataset)
    arrayname = paste(namespace, ".", jdb$meta$arrRnaquantificationset, sep = "")
    register_tuple_return_id(df, arrayname, uniq, dataset_version = dataset_version)
  } # end of if (!only_test)
}

#' @export
register_fusionset = function(df, dataset_version = NULL, only_test = FALSE){
  uniq = c('dataset_id', 'name')
  test_register_fusionset(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    if (is.null(dataset_version)) {
      dataset_version = get_dataset_max_version(dataset_id = unique(df$dataset_id), updateCache = TRUE)
      cat("dataset_version was not specified. Registering at version", dataset_version, "of dataset", unique(df$dataset_id), "\n")
    }
    namespace = find_namespace(unique(df$dataset_id), entitynm=jdb$meta$arrDataset)
    arrayname = paste(namespace, ".", jdb$meta$arrFusionset, sep = "")
    register_tuple_return_id(df, arrayname, uniq, dataset_version = dataset_version)
  } # end of if (!only_test)
}

#' @export
register_copynumberset = function(df, dataset_version = NULL, only_test = FALSE){
  uniq = c('dataset_id', 'name')
  test_register_copynumberset(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    if (is.null(dataset_version)) {
      dataset_version = get_dataset_max_version(dataset_id = unique(df$dataset_id), updateCache = TRUE)
      cat("dataset_version was not specified. Registering at version", dataset_version, "of dataset", unique(df$dataset_id), "\n")
    }
    namespace = find_namespace(unique(df$dataset_id), entitynm=jdb$meta$arrDataset)
    arrayname = paste(namespace, ".", jdb$meta$arrCopyNumberSet, sep = "")
    register_tuple_return_id(df, arrayname, uniq, dataset_version = dataset_version)
  } # end of if (!only_test)
}

#' @export
register_copynumbersubset = function(df, dataset_version = NULL, only_test = FALSE){
  uniq = c('dataset_id', 'copynumberset_id', 'name')
  test_register_copynumbersubset(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    if (is.null(dataset_version)) {
      dataset_version = get_dataset_max_version(dataset_id = unique(df$dataset_id), updateCache = TRUE)
      cat("dataset_version was not specified. Registering at version", dataset_version, "of dataset", unique(df$dataset_id), "\n")
    }
    namespace = find_namespace(unique(df$dataset_id), entitynm=jdb$meta$arrDataset)
    arrayname = paste(namespace, ".", jdb$meta$arrCopyNumberSubSet, sep = "")
    register_tuple_return_id(df, arrayname, uniq, dataset_version = dataset_version)
  } # end of if (!only_test)
}

#' @export
register_variant = function(df, dataset_version = NULL, only_test = FALSE){
  test_register_variant(df)
  if (!only_test) {
    df = df %>% group_by(feature_id, biosample_id) %>% mutate(variant_id = row_number())
    df = as.data.frame(df)
    if (is.null(dataset_version)) {
      dataset_version = get_dataset_max_version(dataset_id = unique(df$dataset_id), updateCache = TRUE)
      cat("dataset_version was not specified. Registering at version", dataset_version, "of dataset", unique(df$dataset_id), "\n")
    }
    namespace = find_namespace(unique(df$dataset_id), entitynm=jdb$meta$arrDataset)
    arrayname = paste(namespace, ".", jdb$meta$arrVariant, sep = "")

    ids_int64_conv = c(get_idname(arrayname), get_int64fields(arrayname))

    ids_int64_conv = ids_int64_conv[(ids_int64_conv != "variant_id")]
    cat("Uploading\n")
    if (nrow(df) < 100000) {x1 = as.scidb(jdb$db, df)} else {x1 = as.scidb(jdb$db, df, chunk_size=nrow(df))}

    x = x1
    for (idnm in ids_int64_conv){
      x = convert_attr_double_to_int64(arr = x, attrname = idnm)
    }
    x = jdb$db$apply(srcArray = x, newAttr = "created", expression = "string(now())")
    x = jdb$db$apply(srcArray = x, newAttr = "updated", expression = "created")
    x = jdb$db$redimension(srcArray = x, schemaArray = R(schema(scidb(jdb$db, arrayname))))

    query = paste("insert(", x@name, ", ", arrayname, ")", sep="")
    cat("Redimension and insert\n")
    iquery(jdb$db, query)

    df2 = df
    df2$dataset_id = NULL

    cat("Now registering info fields\n")
    register_info(df = prep_df_fields(df2,
                                      mandatory_fields = c(get_mandatory_fields_for_register_entity(arrayname),
                                                           get_idname(arrayname))),
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

register_feature_synonym = function(df, uniq, only_test = FALSE){
  test_register_feature_synonym(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = jdb$meta$arrFeatureSynonym
    register_tuple_return_id(df, arrayname, uniq)
  } # end of if (!only_test)
}


#' @export
search_variants = function(variantset, biosample = NULL, feature = NULL){
  if (!is.null(variantset)) {variantset_id = variantset$variantset_id} else {
    stop("variantset must be supplied"); variantset_id = NULL
  }
  if (length(unique(variantset$dataset_version)) != 1) {
    stop("multiple dataset versions in supplied variantset");
  }
  dataset_version = unique(variantset$dataset_version)
  if (!is.null(biosample)) {
    stopifnot(length(unique(biosample$dataset_version))==1)
    if (!(unique(biosample$dataset_version)==dataset_version)) stop("dataset_version-s of variantset and biosample must be same")
  }
  namespace = find_namespace(id = variantset_id,
                             entitynm = jdb$meta$arrVariantset,
                             dflookup = get_variantset_lookup())
  cat("Found namespace: ", namespace, "\n")
  arrayname = paste(namespace, jdb$meta$arrVariant, sep = ".")
  if (!is.null(biosample))            {biosample_id = biosample$biosample_id}                                  else {biosample_id = NULL}
  if (!is.null(feature))              {feature_id = feature$feature_id}                                        else {feature_id = NULL}

  if (exists('debug_trace')) cat("retrieving expression data from server\n")
  res = search_variants_scidb(arrayname,
                              variantset_id,
                              biosample_id,
                              feature_id,
                              dataset_version = dataset_version)
  res
}

count_unique_calls = function(variants){
  v = variants
  nrow(v[duplicated(v[, c('biosample_id', 'CHROM', 'POS')]), ])
}

search_variants_scidb = function(arrayname, variantset_id, biosample_id = NULL, feature_id = NULL, dataset_version){
  if (is.null(dataset_version)) stop("dataset_version must be supplied")
  if (length(dataset_version) != 1) stop("can handle only one dataset_version at a time")

  if (is.null(variantset_id)) stop("variantset_id must be supplied")
  if (length(variantset_id) != 1) stop("can handle only one variantset_id at a time")

  left_query = paste("between(", arrayname,
                     ", ", dataset_version, ", ", variantset_id, ", null, null, null",
                     ", ", dataset_version, ", ", variantset_id, ", null, null, null)", sep = "")
  right_query = paste("between(", arrayname, "_INFO",
                      ", ", dataset_version, ", ", variantset_id, ", null, null, null, null",
                      ", ", dataset_version, ", ", variantset_id, ", null, null, null, null)", sep = "")

  if (!is.null(biosample_id)){
    if (length(biosample_id) == 1) {
      left_query = paste("between(", left_query,
                         ", null, null, ", biosample_id, ", null, null",
                         ", null, null, ", biosample_id, ", null, null)", sep = "")
      right_query = paste("between(", right_query,
                          ", null, null, ", biosample_id, ", null, null, null",
                          ", null, null, ", biosample_id, ", null, null, null)", sep = "")
    }
  }

  if (!is.null(feature_id)){
    if (length(feature_id) == 1) {
      left_query = paste("between(", left_query,
                         ", null, null, null, ", feature_id, ", null",
                         ", null, null, null, ", feature_id, ", null)", sep = "")
      right_query = paste("between(", right_query,
                          ", null, null, null, ", feature_id, ", null, null",
                          ", null, null, null, ", feature_id, ", null, null)", sep = "")
    }
  }

  xx = join_info_ontology_and_unpivot(qq = left_query, arrayname = strip_namespace(arrayname), namespace = get_namespace(arrayname))
  xx
}

#' @export
search_fusion = function(fusionset, biosample = NULL, feature = NULL){
  if (!is.null(fusionset)) {fusionset_id = fusionset$fusionset_id} else {
    stop("fusionset must be supplied"); fusionset_id = NULL
  }
  if (length(unique(fusionset$dataset_version)) != 1) {
    stop("multiple dataset versions in supplied fusionset");
  }
  dataset_version = unique(fusionset$dataset_version)
  if (!is.null(biosample)) {
    stopifnot(length(unique(biosample$dataset_version))==1)
    if (!(unique(biosample$dataset_version)==dataset_version)) stop("dataset_version-s of fusionset and biosample must be same")
  }
  namespace = find_namespace(id = fusionset_id,
                             entitynm = jdb$meta$arrFusionset,
                             dflookup = get_fusionset_lookup())
  cat("Found namespace: ", namespace, "\n")
  arrayname = paste(namespace, jdb$meta$arrFusion, sep = ".")
  if (!is.null(biosample))            {biosample_id = biosample$biosample_id}                                  else {biosample_id = NULL}
  if (!is.null(feature))              {feature_id = feature$feature_id}                                        else {feature_id = NULL}

  if (exists('debug_trace')) cat("retrieving fusion data from server\n")
  res = search_fusions_scidb(arrayname,
                             fusionset_id,
                             biosample_id,
                             feature_id,
                             dataset_version = dataset_version)
  res
}

search_fusions_scidb = function(arrayname, fusionset_id, biosample_id = NULL, feature_id = NULL, dataset_version){
  if (is.null(dataset_version)) stop("dataset_version must be supplied")
  if (length(dataset_version) != 1) stop("can handle only one dataset_version at a time")

  if (is.null(fusionset_id)) stop("fusionset_id must be supplied")
  if (length(fusionset_id) != 1) stop("can handle only one fusionset_id at a time")

  left_query = paste("between(", arrayname,
                     ", ", dataset_version, ", ", fusionset_id, ", null, null",
                     ", ", dataset_version, ", ", fusionset_id, ", null, null)", sep = "")

  if (!is.null(biosample_id)){
    if (length(biosample_id) == 1) {
      left_query = paste("between(", left_query,
                         ", null, null, ", biosample_id, ", null",
                         ", null, null, ", biosample_id, ", null)", sep = "")
    }
  }

  if (!is.null(feature_id)){
    if (length(feature_id) == 1) {
      left_query = paste("filter(", left_query,
                         ", feature_id_left = ", feature_id, " OR feature_id_right = ", feature_id, ")", sep = "")
    } else {
      stop("not yet covered")
    }
  }

  iquery(jdb$db, left_query, return = TRUE)
}

get_mandatory_fields_for_register_entity = function(arrayname){
  attrs = names(jdb$meta$L$array[[strip_namespace(arrayname)]]$attributes)
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

report_missing_mandatory_fields = function(df, arrayname){
  fields = get_mandatory_fields_for_register_entity(strip_namespace(arrayname))
  fields[which(!(fields %in% colnames(df)))]
}

test_mandatory_fields = function(df, arrayname, silent = TRUE){
  mandatory_fields = get_mandatory_fields_for_register_entity(arrayname)
  missing_fields =  report_missing_mandatory_fields(df, arrayname = arrayname)
  if (length(missing_fields) == 0){
    if (!silent){
      cat("Looks OK\n---Preview of mandatory fields:---\n")
      print(head(df[, mandatory_fields]))
      cat("---Flexible fields:\n")
      print(colnames(df)[!(colnames(df) %in% mandatory_fields)])
    }
  } else {
    stop("Following fields are missing: \n", paste(missing_fields, collapse = ", "))
  }
}

test_unique_fields = function(df, uniq){
  if(any(duplicated(df[, uniq]))) {
    stop("duplicate entries exist in data to be uploaded need to be removed. Check fields: ",
         paste(uniq, collapse = ", "))
  }
}

test_register_project = function(df, uniq, silent = TRUE){
  test_mandatory_fields(df, arrayname = jdb$meta$arrProject, silent = silent)
  test_unique_fields(df, uniq)
}

test_register_dataset = function(df, uniq, dataset_version, silent = TRUE){
  test_mandatory_fields(df, arrayname = jdb$meta$arrDataset, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$project_id))!=1) stop("Datasets to be registered must belong to a single project")
  if (dataset_version != 1) stop("to increment dataset versions, use the function `increment_dataset_version()`")
}

test_register_individual = function(df, uniq, silent = TRUE){
  test_mandatory_fields(df, arrayname = jdb$meta$arrIndividuals, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$dataset_id))!=1) stop("Individuals to be registered must belong to a single dataset/study")
}

test_register_biosample = function(df, uniq, silent = TRUE){
  test_mandatory_fields(df, arrayname = jdb$meta$arrBiosample, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$dataset_id))!=1) stop("Biosamples to be registered must belong to a single dataset/study")
}

test_register_rnaquantificationset = function(df, uniq, silent = TRUE){
  test_mandatory_fields(df, arrayname = jdb$meta$arrRnaquantificationset, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$dataset_id))!=1) stop("Rnaquantificationset to be registered must belong to a single dataset/study")
}

test_register_ontology = function(df, uniq, silent = TRUE){
  test_mandatory_fields(df, arrayname = jdb$meta$arrOntology, silent = silent)
  test_unique_fields(df, uniq)
}

test_register_featureset = function(df, uniq, silent = TRUE){
  test_mandatory_fields(df, arrayname = jdb$meta$arrFeatureset, silent = silent)
  test_unique_fields(df, uniq)
}

test_register_feature = function(df, uniq, silent = TRUE){
  test_mandatory_fields(df, arrayname = jdb$meta$arrFeature, silent = silent)
  test_unique_fields(df, uniq)
}

test_register_feature_synonym = function(df, uniq, silent = TRUE){
  test_mandatory_fields(df, arrayname = jdb$meta$arrFeatureSynonym, silent = silent)
  test_unique_fields(df, uniq)
}

test_register_variantset = function(df, uniq, silent = TRUE){
  test_mandatory_fields(df, arrayname = jdb$meta$arrVariantset, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$dataset_id))!=1) stop("VariantSet to be registered must belong to a single dataset/study")
}

test_register_fusionset = function(df, uniq, silent = TRUE){
  test_mandatory_fields(df, arrayname = jdb$meta$arrFusionset, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$dataset_id))!=1) stop("FusionSet to be registered must belong to a single dataset/study")
}

test_register_copynumberset = function(df, uniq, silent = TRUE){
  test_mandatory_fields(df, arrayname = jdb$meta$arrCopyNumberSet, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$dataset_id))!=1) stop("CopyNumberSet to be registered must belong to a single dataset/study")
}

test_register_copynumbersubset = function(df, uniq, silent = TRUE){
  test_mandatory_fields(df, arrayname = jdb$meta$arrCopyNumberSubSet, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$dataset_id))!=1) stop("CopyNumberSubset to be registered must belong to a single dataset/study")
}

test_register_expression_matrix = function(filepath,
                                             rnaquantificationset_id,
                                             featureset_id,
                                             feature_type,
                                             dataset_version){
  stopifnot(length(rnaquantificationset_id) == 1)
  stopifnot(length(featureset_id) == 1)
  stopifnot(feature_type == 'gene' | feature_type == 'transcript')
}

test_register_variant = function(df){
  if(length(unique(df$dataset_id))!=1) stop("Variants to be registered must belong to a single dataset/study")
  test_mandatory_fields(df, arrayname = jdb$meta$arrVariant)
}

test_register_copynumber_seg = function(copynumberset){
  stopifnot(nrow(copynumberset) == 1)
  stopifnot("filepath" %in% colnames(copynumberset))
}

test_register_copnyumber_matrix_file = function(copynumberSubSet, dataset_version){
  stopifnot(nrow(copynumberSubSet) == 1)
  stopifnot("filepath" %in% colnames(copynumberSubSet))
}

test_register_fusion_data = function(df, fusionset){
  stopifnot(nrow(fusionset) == 1)
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
    select_from_1d_entity(entitynm = jdb$meta$arrProject, id = project_id)
  } else { # Need to gather info across namespaces
    merge_across_namespaces(arrayname = jdb$meta$arrProject)
  }
}

select_from_1d_entity = function(entitynm, id, dataset_version = NULL){
  namespace = find_namespace(id, entitynm, entity_lookup(entityName = entitynm))
  if (is.null(namespace)) namespace = NA # to handle the case of no entry in lookup array at all
  if (any(is.na(namespace))) {
    cat("trying to download lookup array once again, to see if there has been a recent update\n")
    namespace = update_lookup_and_find_namespace_again(entitynm, id)
  }
  if (any(!(namespace %in% jdb$cache$nmsp_list))) stop("user probably does have acecss to id-s: ", paste(id[which(!(namespace %in% jdb$cache$nmsp_list))], collapse = ", "))
  names(id) = namespace
  df = data.frame()
  for (nmsp in unique(namespace)){
    cat("--DEBUG--: retrieving entities from namespace:", nmsp, "\n")
    l = list(df,
             select_from_1d_entity_by_namespace(namespace = nmsp, entitynm, id = id[which(names(id) == nmsp)], dataset_version = dataset_version))
    df = rbindlist(l, use.names=TRUE, fill=TRUE)
  }
  df = data.frame(df)
  # return(df[match(id, df[, get_base_idname(entitynm)]), ])
  return(df)
}

update_lookup_and_find_namespace_again = function(entitynm, id){
  cat("updating lookup array for entity:", entitynm, "\n")
  jdb$cache$lookup[[entitynm]] = entity_lookup(entityName = entitynm, updateCache = TRUE)
  namespace = find_namespace(id, entitynm, jdb$cache$lookup[[entitynm]])
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

merge_across_namespaces = function(arrayname){
  arrayname = strip_namespace(arrayname)

  # public namespace first
  qq = arrayname
  df = join_info_ontology_and_unpivot(qq, arrayname)
  for (namespace in jdb$cache$nmsp_list){
    if (namespace != 'public'){
      cat("--DEBUG--: joining with data from namespace:", namespace, "\n")
      fullnm = paste(namespace, ".", arrayname, sep = "")
      dfx = join_info_ontology_and_unpivot(fullnm, arrayname, namespace = namespace)
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
get_datasets = function(dataset_id = NULL, dataset_version = NULL, all_versions = TRUE){
  check_args_get(id = dataset_id, dataset_version, all_versions)
  if (!is.null(dataset_id)) { # Need to look up specific project ID
    df = select_from_1d_entity(entitynm = jdb$meta$arrDataset, id = dataset_id, dataset_version = dataset_version)
  } else { # Need to gather info across namespaces
    df = merge_across_namespaces(arrayname = jdb$meta$arrDataset)
  }
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
get_individuals = function(individual_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_get(id = individual_id, dataset_version, all_versions)
  if (!is.null(individual_id)) { # Need to look up specific individual ID
    df = select_from_1d_entity(entitynm = jdb$meta$arrIndividuals, id = individual_id, dataset_version = dataset_version)
  } else { # Need to gather info across namespaces
    df = merge_across_namespaces(arrayname = jdb$meta$arrIndividuals)
  }
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
get_biosamples = function(biosample_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_get(id = biosample_id, dataset_version, all_versions)

  if (!is.null(biosample_id)) { # Need to look up specific biosample ID
    df = select_from_1d_entity(entitynm = jdb$meta$arrBiosample, id = biosample_id, dataset_version = dataset_version)
  } else { # Need to gather info across namespaces
    df = merge_across_namespaces(arrayname = jdb$meta$arrBiosample)
  }
  if (!all_versions) return(latest_version(df)) else return(df)
}

check_args_get = function(id, dataset_version, all_versions){
  if (is.null(id) & !is.null(dataset_version) & !all_versions) stop("null value of id is used to get all entities accessible to user. Cannot specify version")
  if (!is.null(dataset_version) & all_versions==TRUE) stop("Cannot specify specific dataset_version, and also set all_versions = TRUE")
}

#' @export
get_rnaquantificationsets = function(rnaquantificationset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_get(id = rnaquantificationset_id, dataset_version, all_versions)
  if (!is.null(rnaquantificationset_id)) { # Need to look up specific ID
    df = select_from_1d_entity(entitynm = jdb$meta$arrRnaquantificationset, id = rnaquantificationset_id, dataset_version = dataset_version)
  } else { # Need to gather info across namespaces
    df = merge_across_namespaces(arrayname = jdb$meta$arrRnaquantificationset)
  }
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
get_variantsets = function(variantset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_get(id = variantset_id, dataset_version, all_versions)
  if (!is.null(variantset_id)) { # Need to look up specific ID
    df = select_from_1d_entity(entitynm = jdb$meta$arrVariantset, id = variantset_id, dataset_version = dataset_version)
  } else { # Need to gather info across namespaces
    df = merge_across_namespaces(arrayname = jdb$meta$arrVariantset)
  }
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
get_fusionset = function(fusionset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_get(id = fusionset_id, dataset_version, all_versions)
  if (!is.null(fusionset_id)) { # Need to look up specific ID
    df = select_from_1d_entity(entitynm = jdb$meta$arrFusionset, id = fusionset_id, dataset_version = dataset_version)
  } else { # Need to gather info across namespaces
    df = merge_across_namespaces(arrayname = jdb$meta$arrFusionset)
  }
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
get_copynumberset = function(copynumberset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_get(id = copynumberset_id, dataset_version, all_versions)
  if (!is.null(copynumberset_id)) { # Need to look up specific ID
    df = select_from_1d_entity(entitynm = jdb$meta$arrCopyNumberSet, id = copynumberset_id, dataset_version = dataset_version)
  } else { # Need to gather info across namespaces
    df = merge_across_namespaces(arrayname = jdb$meta$arrCopyNumberSet)
  }
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
get_copynumbersubset = function(copynumbersubset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_get(id = copynumbersubset_id, dataset_version, all_versions)
  if (!is.null(copynumbersubset_id)) { # Need to look up specific ID
    df = select_from_1d_entity(entitynm = jdb$meta$arrCopyNumberSubSet, id = copynumbersubset_id, dataset_version = dataset_version)
  } else { # Need to gather info across namespaces
    df = merge_across_namespaces(arrayname = jdb$meta$arrCopyNumberSubSet)
  }
  if (!all_versions) return(latest_version(df)) else return(df)
}


#' @export
get_featuresets= function(id = NULL){
  arrayname = jdb$meta$arrFeatureset

  local_arrnm = strip_namespace(arrayname)
  idname = get_idname(arrayname)
  stopifnot(class(idname) == "character")

  qq = arrayname
  if (!is.null(id)) {qq = paste("filter(", arrayname, ", ", idname, " = ", id, ")", sep="")}
  join_info_ontology_and_unpivot(qq, arrayname)
}


#' @export
get_features = function(feature_id = NULL, fromCache = TRUE){
  if (!fromCache | is.null(jdb$cache$feature_ref)){ # work from SciDB directly
    arrayname = jdb$meta$arrFeature
    idname = get_idname(arrayname)

    qq = arrayname
    if (!is.null(feature_id)) {
      qq = form_selector_query_1d_array(arrayname, idname, feature_id)
      join_info_ontology_and_unpivot(qq, arrayname)
    } else { # FASTER path when all data has to be downloaded
      ftr = iquery(jdb$db, qq, return = T)
      ftr_info = iquery(jdb$db, paste(qq, "_INFO", sep=""), return = T)
      ftr = merge(ftr, ftr_info, by = idname, all.x = T)
      allfeatures = unpivot_key_value_pairs(ftr, arrayname, key_col = "key", val = "val")
      if (is.null(jdb$cache$feature_ref)) { # the first time (when feature cache has never been filled)
        jdb$cache$feature_ref = allfeatures
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
    diminfo = jdb$meta$L$array[[strip_namespace(arrayname)]]$dims
    upload = sprintf("build(<%s:int64>[idx=1:%d,100000,0],'{1}[(%s)]', true)",
                     idname, length(dim1_selected_ids), paste(dim1_selected_ids, sep=",", collapse="), ("))
    apply_dim2 = paste("apply(", upload, ", ", dim2, ", ", dim2_selected_ids, ")", sep = "")
    redim = paste("redimension(", apply_dim2, ", <idx:int64>[", get_idname(arrayname), "])", sep = "")

    query= paste("project(
                 cross_join(",
                 arrayname, " as A, ",
                 redim, "as B, ",
                 "A.", idname, ", " ,
                 "B.", idname,
                 "),",
                 paste(names(jdb$meta$L$array[[strip_namespace(arrayname)]]$attributes), collapse = ", "),
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
    diminfo = jdb$meta$L$array[[strip_namespace(arrayname)]]$dims
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
                 paste(names(jdb$meta$L$array[[strip_namespace(arrayname)]]$attributes), collapse = ", "),
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
  f1 = syn[syn$synonym == synonym, ]
  if (!is.null(id_type)) {f1 = f1[f1$source == id_type, ]}
  if (!is.null(featureset_id)) {f1 = f1[f1$featureset_id == f1$featureset_id, ]}
  get_features(feature_id = f1$feature_id, fromCache = !updateCache)
}

#' @export
search_features = function(gene_symbol = NULL, feature_type = NULL, featureset_id = NULL){
  arrayname = jdb$meta$arrFeature

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
search_datasets = function(project_id = NULL, dataset_version = NULL, all_versions = TRUE){
  check_args_search(dataset_version, all_versions)
  arrayname = jdb$meta$arrDataset

  qq = arrayname
  if (!is.null(project_id)) {
    namespace = find_namespace(id = project_id, entitynm = jdb$meta$arrProject, dflookup = get_project_lookup())
    if (!(namespace %in% jdb$cache$nmsp_list)) {stop("user does not have permission to access data for project_id: ", project_id)}
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
  i = find_nmsp_filter_on_dataset_id_and_version(arrayname = jdb$meta$arrIndividuals, dataset_id, dataset_version = dataset_version)
  if (!all_versions) return(latest_version(i)) else return(i)
}

check_args_search = function(dataset_version, all_versions){
  if (!is.null(dataset_version) & all_versions==TRUE) stop("Cannot specify specific dataset_version, and also set all_versions = TRUE")
}

latest_version = function(df){
  stopifnot(all(c("dataset_version", "dataset_id") %in% colnames(df)))

  df = df %>% group_by(dataset_id) %>% filter(dataset_version == max(dataset_version))

  drop_na_columns(as.data.frame(df))
}

find_nmsp_filter_on_dataset_id_and_version = function(arrayname, dataset_id, dataset_version){
  qq = arrayname
  if (!is.null(dataset_id)) {
    namespace = find_namespace(id = dataset_id, entitynm = jdb$meta$arrDataset, dflookup = get_dataset_lookup())
    if (is.null(namespace)) namespace = NA # to handle the case of no entry in lookup array at all
    if (any(is.na(namespace))) {
      cat("trying to download lookup array once again, to see if there has been a recent update")
      namespace = update_lookup_and_find_namespace_again(entitynm = jdb$meta$arrDataset, id = dataset_id)
    }
    if (!(namespace %in% jdb$cache$nmsp_list)) {stop("user does not have permission to access data for dataset_id: ", dataset_id)}
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
  df = find_nmsp_filter_on_dataset_id_and_version(arrayname = jdb$meta$arrBiosample, dataset_id, dataset_version = dataset_version)
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
search_ontology = function(terms, exact_match = TRUE, updateCache = FALSE){
  ont = get_ontology(updateCache = updateCache)
  ont_ids = ont$ontology_id
  names(ont_ids) = ont$term
  if (exact_match){
    res = ont_ids[terms]
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
  df = find_nmsp_filter_on_dataset_id_and_version(arrayname = jdb$meta$arrRnaquantificationset, dataset_id, dataset_version = dataset_version)
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
search_variantsets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_search(dataset_version, all_versions)
  df = find_nmsp_filter_on_dataset_id_and_version(arrayname = jdb$meta$arrVariantset, dataset_id, dataset_version = dataset_version)
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
search_fusionsets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_search(dataset_version, all_versions)
  df = find_nmsp_filter_on_dataset_id_and_version(arrayname = jdb$meta$arrFusionset, dataset_id, dataset_version = dataset_version)
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
search_copynumbersets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_search(dataset_version, all_versions)
  df = find_nmsp_filter_on_dataset_id_and_version(arrayname = jdb$meta$arrCopyNumberSet, dataset_id, dataset_version = dataset_version)
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
search_copynumbersubsets = function(dataset_id = NULL, dataset_version = NULL, all_versions = FALSE){
  check_args_search(dataset_version, all_versions)
  df = find_nmsp_filter_on_dataset_id_and_version(arrayname = jdb$meta$arrCopyNumberSubSet, dataset_id, dataset_version = dataset_version)
  if (!all_versions) return(latest_version(df)) else return(df)
}

#' @export
search_rnaquantification = function(rnaquantificationset = NULL,
                                    biosample = NULL,
                                    feature = NULL,
                                    formExpressionSet = TRUE){
  if (!is.null(rnaquantificationset)) {rnaquantificationset_id = rnaquantificationset$rnaquantificationset_id} else {
    stop("rnaquantificationset must be supplied"); rnaquantificationset_id = NULL
  }
  if (length(unique(rnaquantificationset$dataset_version)) != 1) {
    stop("multiple dataset versions in supplied rnaquantificationset");
  }
  dataset_version = unique(rnaquantificationset$dataset_version)
  if (!is.null(biosample)) {
    stopifnot(length(unique(biosample$dataset_version))==1)
    stopifnot(unique(biosample$dataset_version)==dataset_version)
  }
  namespace = find_namespace(id = rnaquantificationset_id,
                             entitynm = jdb$meta$arrRnaquantificationset,
                             dflookup = get_rnaquantificationset_lookup())
  arrayname = paste(namespace, jdb$meta$arrRnaquantification, sep = ".")
  if (!is.null(biosample))            {biosample_id = biosample$biosample_id}                                  else {biosample_id = NULL}
  if (!is.null(feature))              {feature_id = feature$feature_id}                                        else {feature_id = NULL}

  if (exists('debug_trace')) cat("retrieving expression data from server\n")
  res = search_rnaquantification_scidb(arrayname,
                                       rnaquantificationset_id,
                                       biosample_id,
                                       feature_id,
                                       dataset_version = dataset_version)
  if (nrow(res) == 0) return(NULL)
  if (!formExpressionSet) return(res)

  # If user did not provide biosample, then query the server for it, or retrieve from global biosample list
  if (is.null(biosample)) {
    biosample_id = unique(res$biosample_id)
    if (FALSE) { # avoiding this path right now (might be useful when the download of all accessible biosamples is prohibitive)
      cat("query the server for matching biosamples\n")
      biosample = get_biosamples(biosample_id)
    } else{
      biosample_ref = get_biosamples_from_cache()
      biosample = biosample_ref[biosample_ref$biosample_id %in% biosample_id, ]
      biosample = drop_na_columns(biosample)
    }
  }

  # If user did not provide feature, then query the server for it, or retrieve from global feature list
  if (is.null(feature)) {
    feature_id = unique(res$feature_id)
    if (FALSE) { # avoiding this path for now (the download of all features registered on the system is not expected to be too time-consuming)
      cat("query the server for matching features\n")
      feature = get_features(feature_id)
    } else{
      feature_ref = get_feature_from_cache()

      feature = feature_ref[feature_ref$feature_id %in% feature_id, ]
      feature = drop_na_columns(feature)
    }
  }

  return(get_list_expression_set(expr_df = res, dataset_version, rnaquantificationset, biosample, feature))
}

get_list_expression_set = function(expr_df, dataset_version, rnaquantificationset, biosample, feature){
  if (nrow(rnaquantificationset) > 1) {stop("currently does not support returning expressionSets for multiple rnaquantification sets")}
  if (length(dataset_version) != 1) {stop("currently does not support returning expressionSets for multiple dataset_verions")}

  convertToExpressionSet(expr_df, biosample_df = biosample, feature_df = feature)
}

search_rnaquantification_scidb = function(arrayname,
                                          rnaquantificationset_id,
                                          biosample_id,
                                          feature_id,
                                          dataset_version){
  tt = scidb(jdb$db, arrayname)

  if (is.null(dataset_version)) dataset_version = "NULL"
  if (length(dataset_version) != 1) {stop("cannot specify one dataset_version at a time")}

  qq = arrayname
  if (!is.null(rnaquantificationset_id) & !is.null(biosample_id) & !is.null(feature_id)) { # all 3 selections made by user
    if (length(rnaquantificationset_id) == 1 & length(biosample_id) == 1 & length(feature_id) == 1) {
      qq = paste("between(",
                 qq,
                 ", ", dataset_version,
                 ", ", rnaquantificationset_id,
                 ", ", biosample_id,
                 ", ", feature_id,
                 ", ", dataset_version,
                 ", ", rnaquantificationset_id,
                 ", ", biosample_id,
                 ", ", feature_id,
                 ")", sep="")
      res = iquery(jdb$db, qq, return = T)
    }
    else {
      selector = merge(
        merge(
          merge(data.frame(dataset_version = as.integer(dataset_version)),
                data.frame(rnaquantificationset_id = as.integer(rnaquantificationset_id))),
          data.frame(biosample_id = as.integer(biosample_id))),
        data.frame(feature_id = as.integer(feature_id)))
      if (TRUE){ # Return data using join
        selector$flag = TRUE
        # t1 = proc.time()
        xx = as.scidb(jdb$db, selector)
        # print(proc.time()-t1)
        qq2 = paste("apply(", xx@name, ",
                    dataset_version_, int64(dataset_version),
                    rnaquantificationset_id_, int64(rnaquantificationset_id),
                    biosample_id_, int64(biosample_id),
                    feature_id_, int64(feature_id))")
        qq2 = paste("cast(", qq2, ", <
                    dataset_version_old:int32,
                    rnaquantificationset_id_old:int32,
                    biosample_id_old:int32,
                    feature_id_old:int32,
                    flag:bool,
                    dataset_version:int64,
                    rnaquantificationset_id:int64,
                    biosample_id:int64,
                    feature_id:int64
                    >
                    [tuple_no,dst_instance_id,src_instance_id] )")
        qq2 = paste("redimension(", qq2, ", <flag:bool>[", yaml_to_dim_str(jdb$meta$L$array[[jdb$meta$arrRnaquantification]]$dims), "])")
        # head(scidb(qq2))
        # dimschema = gsub("<.*>([A-Z]*)", "\\1", schema(tt))
        # newschema = paste("<flag:bool>", dimschema)
        # xx = redimension(xx, newschema)

        qq = paste("join(",
                   qq, ",",
                   qq2, ")")
        qq = paste("project(", qq, ", expression_count)")
        res = iquery(jdb$db, qq, return = T)
      } else { # Return data using cross_between_
        stop("Code needs to be added here to support inclusion of `dataset_version` in expression matrix schema")
        df = selector[order(selector$rnaquantificationset_id, selector$biosample_id, selector$feature_id), ]
        sqq = NULL
        for (nn in 1:nrow(df)){
          row = df[nn, ]
          si = paste(row, collapse = ", ")
          si = paste(si, si, sep = ", ")
          sqq = paste(sqq, si, sep=ifelse(is.null(sqq), "", ", "))
        }
        qq = paste("cross_between_(", qq, ", ", sqq, ")")
        res = iquery(jdb$db, qq, return = T)
      }
    }
  } else if (!is.null(rnaquantificationset_id) & !is.null(biosample_id) & is.null(feature_id)) { # user selected rqs and bs, not f
    selected_names = c('rnaquantificationset_id', 'biosample_id')
    val1 = rnaquantificationset_id
    val2 = biosample_id
    res = cross_between_select_on_two(qq, tt, val1, val2, selected_names, dataset_version = dataset_version)
  } else if (is.null(rnaquantificationset_id) & !is.null(biosample_id) & !is.null(feature_id)) { # user selected bs and f, not rqs
    selected_names = c('biosample_id', 'feature_id')
    val1 = biosample_id
    val2 = feature_id
    res = cross_between_select_on_two(qq, tt, val1, val2, selected_names, dataset_version = dataset_version)
  } else if (!is.null(rnaquantificationset_id) & is.null(biosample_id) & !is.null(feature_id)) { # user selected rqs and f, not bs
    selected_names = c('rnaquantificationset_id', 'feature_id')
    val1 = rnaquantificationset_id
    val2 = feature_id
    res = cross_between_select_on_two(qq, tt, val1, val2, selected_names, dataset_version = dataset_version)
  } else if (!is.null(rnaquantificationset_id) & is.null(biosample_id) & is.null(feature_id)) { # user selected rqs only
    if (exists('debug_trace')) cat("Only RNAQuantificationSet is selected.\n")
    if (length(rnaquantificationset_id) == 1){
      qq = paste("between(", qq, ",NULL,", rnaquantificationset_id, ",NULL,NULL,NULL,", rnaquantificationset_id, ",NULL,NULL)", sep = "")
    } else {
      stop("code for multiple rnaquantificationset_id to be added. Alternatively, call the search function by individual rnaquantificationset_id.")
    }
    res = iquery(jdb$db, qq, return = TRUE)
  } else if (is.null(rnaquantificationset_id) & !is.null(biosample_id) & is.null(feature_id)) { # user selected bs only
    stop("Only biosample is selected. Downloaded data could be quite large. Consider downselecting by RNAQuantificationSet_id or feature_id. ")
  }
  res
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

  xx = as.scidb(jdb$db, selector)
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
  fn = function(dimname) {yaml_to_dim_str(jdb$meta$L$array[[jdb$meta$arrRnaquantification]]$dims[dimname])}
  newdim = paste(sapply(selected_names_all, FUN = fn), collapse = ",")
  newsch = paste("<flag:bool>[", newdim, "]", sep="")
  xx1 = jdb$db$redimension(xx1, R(newsch))

  subq = paste(sapply(selected_names_all, FUN=function(x) {paste(paste("A.", x, sep=""), paste("B.", x, sep=""), sep=", ")}), collapse=", ")
  qq = paste("cross_join(",
             qq, " as A,",
             xx1@name, " as B,", subq, ")", sep="")
  qq = paste("project(", qq, ", ", schema(tt, "attributes")$name, ")")

  iquery(jdb$db, qq, return = T)
}

#' @export
get_rnaquantification_counts = function(rnaquantificationset_id = NULL){
  if (is.null(rnaquantificationset_id)){
    x = scidb(jdb$db, jdb$meta$arrRnaquantification)
    c = as.R(jdb$db$aggregate(srcArray = x, AGGREGATE_CALL = "count(*)", groupbyDim = 'rnaquantificationset_id, dataset_version'))
    for (nmsp in jdb$cache$nmsp_list){
      if (nmsp != 'public'){
        x = scidb(jdb$db, paste(nmsp, jdb$meta$arrRnaquantification, sep = "."))
        c1 = as.R(jdb$db$aggregate(srcArray = x, AGGREGATE_CALL = "count(*)", groupbyDim = 'rnaquantificationset_id, dataset_version'))
        c = rbind(c, c1)
      }
    }
  } else { #specific rnaquantificationset_id is specified
    nmsp = find_namespace(id = rnaquantificationset_id, entitynm = jdb$meta$arrRnaquantificationset, dflookup = get_rnaquantificationset_lookup())
    qq = paste("between(", nmsp, ".", jdb$meta$arrRnaquantification, ", null, ", rnaquantificationset_id, ", null, null, null, ", rnaquantificationset_id, ", null, null)", sep = "" )
    qq = paste("aggregate(", qq, ", count(*), rnaquantificationset_id, dataset_version)")
    c = iquery(jdb$db, qq, return = T)
  }
  c = c[order(c$rnaquantificationset_id, c$dataset_version), ]
  return(c)
}

join_info_ontology_and_unpivot = function(qq, arrayname, namespace = 'public'){
  unpivot = TRUE
  idname = get_idname(arrayname)

  # Join INFO array
  if (exists('debug_trace')) {t1 = proc.time()}
  if (FALSE){ # TODO: See why search_individuals(dataset_id = 1) is so slow; the two options here did not make a difference
    qq1 = paste("cross_join(", qq, " as A, ", namespace, ".", arrayname, "_INFO as B, A.", idname, ", B.", idname, ")", sep = "")
  } else if (FALSE) { cat("== Swapping order of cross join between ARRAY_INFO and select(ARRAY, ..)\n")
    qq1 = paste("cross_join(", namespace, ".", arrayname, "_INFO as A, ", qq, " as B, A.", idname, ", B.", idname, ")", sep = "")
  } else {
    qq1 = paste("equi_join(", qq, ", ", namespace, ".", arrayname, "_INFO, 'left_names=", paste(idname, collapse = ","), "', 'right_names=", paste(idname, collapse = ","),  "', 'left_outer=true', 'keep_dimensions=true')", sep = "")
  }
  x2 = iquery(jdb$db, qq1, return = TRUE)
  # # TODO: BEGIN: Remove this when bit64 integration is removed from SciDBR package
  # int64_cols = which(sapply(x2, class) == "integer64")
  # x2[, int64_cols] = sapply(int64_cols, FUN=function(i){as.integer(x2[, i])})
  # # TODO: END: Remove this when bit64 integration is removed from SciDBR package

  if (exists('debug_trace')) {cat("join with info:\n"); print( proc.time()-t1 )}

  if (nrow(x2) > 0 & sum(colnames(x2) %in% c("key", "val")) == 2 & unpivot){ # If key val pairs exist and need to be unpivoted
    if (exists('debug_trace')) {t1 = proc.time()}
    x3 = unpivot_key_value_pairs(df = x2, arrayname = arrayname)
    if (exists('debug_trace')) {cat("unpivot:\n"); print( proc.time()-t1 )}
  } else {
    if (nrow(x2) > 0) {
      x3 = x2[, c(jdb$meta$L$array[[strip_namespace(arrayname)]]$dims,
                  names(jdb$meta$L$array[[strip_namespace(arrayname)]]$attributes))]
    } else {
      x3 = x2[, names(jdb$meta$L$array[[strip_namespace(arrayname)]]$attributes)]
    }
  }

  # Join ontology terms
  #   if (is.null(jdb$cache$dfOntology)) { # when ontology has not been downloaded
  #     if (exists('debug_trace')) {t1 = proc.time()}
  #     jdb$cache$dfOntology = iquery(jdb$db, jdb$meta$arrOntology, return = TRUE)
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

  x4 = df[, c(get_idname(arrayname), names(jdb$meta$L$array[[strip_namespace(arrayname)]]$attributes))]
  x4 = x4[!duplicated(x4[, idname]), ]

  if (is.data.frame(x2t)) x5 = merge(x4, x2t, by = idname) else x5 = x4
  return(x5)
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

    arr_feature = jdb$meta$arrFeature
    namespace = find_namespace(id = rnaquantificationset_id, entitynm = jdb$meta$arrRnaquantificationset, dflookup = get_rnaquantificationset_lookup())
    arr_biosample = paste(namespace, jdb$meta$arrBiosample, sep = ".")
    arrayname = paste(namespace, jdb$meta$arrRnaquantification, sep = ".")
    cat("======\n")
    cat(paste("Working on expression file:\n\t", filepath, "\n"))
    x = read.delim(file = filepath, nrows = 10)
    ncol(x)


    ##################
    # Do some simple checks on the column-names
    length(colnames(x))
    length(grep(".*_BM", tail(colnames(x),-1) ))
    length(grep(".*_PB", colnames(x) ))

    colnames(x)[grep(".*_2_.*", colnames(x) )]

    colnames(x)[grep(".*2087.*", colnames(x) )]
    colnames(x)[grep(".*1179.*", colnames(x) )]
    ##################

    xx = tail(colnames(x), -1)
    # public_id = str_extract(xx, "MMRF_[0-9]+")
    #
    # spectrum_suffix = str_extract(
    #   str_extract(xx, "_[0-9]_"),
    #   "[0-9]")
    #
    # locations = str_locate(xx, "MMRF_[0-9]+_[0-9]_")
    # suffix2 = NULL
    # for (i in 1: length(xx)) {
    #   suffix2[i] = substr(xx[i], locations[i,2]+1, str_length(xx[i]))
    # }


    ############# START LOADING INTO SCIDB ########

    query = paste("aio_input('", filepath, "',
                  'num_attributes=" , length(colnames(x)), "',
                  'split_on_dimension=1')")
    t1 = scidb(jdb$db, query)
    t1 = jdb$db$between(srcArray = t1, lowCoord = "NULL, NULL, NULL, NULL", highCoord = R(paste("NULL, NULL, NULL,", length(colnames(x))-1)))
    t1 = store(jdb$db, t1, temp=TRUE)

    # ================================
    ## Step 1. Join the SciDB feature ID

    # first form the list of features in current table
    # featurelist_curr = subset(t1, attribute_no == 0)
    featurelist_curr = jdb$db$between(srcArray = t1, lowCoord = "NULL, NULL, NULL, 0", highCoord = "NULL, NULL, NULL, 0")
    cat(paste("number of feature_id-s in the current expression count table:", scidb_array_count(featurelist_curr)-1, "\n"))
    FEATUREKEY = scidb(jdb$db, arr_feature)
    cat(paste("number of feature_id-s in the SciDB feature ID list:", scidb_array_count(FEATUREKEY), "\n"))

    sel_features = jdb$db$filter(FEATUREKEY, R(paste("feature_type='", feature_type, "' AND featureset_id = ", featureset_id, sep = "")))
    cat(paste("number of feature_id-s in the SciDB feature ID list for transcript type: '", feature_type,
              "' and featureset_id: '", featureset_id, "' is:", scidb_array_count(sel_features), "\n", sep = ""))

    ff = jdb$db$project(srcArray = sel_features, selectedAttr = "name")
    # ff = scidb(jdb$db, paste("apply(", ff@name, ", feature_id, feature_id)", sep=""))
    # now do the joining
    # temp_arr = jdb$db$apply(srcArray = featurelist_curr, newAttr = "tuple_no", expression = "tuple_no")
    #                      tuple_no = tuple_no,
    #                      dst_instance_id = dst_instance_id,
    #                      src_instance_id = src_instance_id)
    joinFeatureName = scidb(jdb$db,
                            paste("equi_join(",
                                  ff@name, ", ",
                                  featurelist_curr@name, ", ",
                                  "'left_names=name', 'right_names=a', 'keep_dimensions=1')")
    )
    joinFeatureName = jdb$db$redimension(srcArray = joinFeatureName,
                                         "<feature_id :int64>
                                         [tuple_no=0:*,10000000,0,dst_instance_id=0:63,1,0,src_instance_id=0:63,1,0]")

    joinBack1 = scidb(jdb$db,
                      paste("cross_join(",
                            t1@name, " as X, ",
                            joinFeatureName@name, " as Y, ",
                            "X.tuple_no, Y.tuple_no, X.dst_instance_id, Y.dst_instance_id, X.src_instance_id, Y.src_instance_id)", sep = ""))
    joinBack1@name

    joinBack1 = store(jdb$db, joinBack1, temp=TRUE)

    # Verify with
    # head(subset(joinBack1, tuple_no == 0))
    scidb_array_head(jdb$db$between(srcArray = joinBack1, lowCoord = "0, NULL, NULL, NULL", highCoord = "0, NULL, NULL, NULL"))

    cat("Number of features in study that matched with SciDB ID:\n")
    countFeatures = scidb_array_count(joinBack1) / ncol(x)
    print(countFeatures)

    stopifnot(countFeatures == (scidb_array_count(featurelist_curr)-1))
    # ================================
    ## Step 2. Join the SciDB patient ID
    # first form the list of patients in current table

    # patientlist_curr = subset(t1, tuple_no == 0 & dst_instance_id == 0 & attribute_no > 0)
    patientlist_curr = jdb$db$between(t1, lowCoord = "0, 0, NULL, 1", highCoord = "0, 0, NULL, NULL")
    # Check that the "public_id"_"spectrum_id" is unique enough, otherwise we have to consider the suffix "BM", "PB"
    stopifnot(length(unique(as.R(patientlist_curr)$a)) == (ncol(x)-1))

    scidb_array_head(patientlist_curr)

    cat(paste("number of biosamples in the expression count table:", scidb_array_count(patientlist_curr), "\n"))
    PATIENTKEY = scidb(jdb$db, arr_biosample)
    PATIENTKEY = jdb$db$filter(PATIENTKEY, R(paste('dataset_id=', dataset_id)))
    cat(paste("number of biosamples registered in database in selected namespace:" , scidb_array_count(PATIENTKEY), "\n"))


    # now do the joining
    # joinPatientName = merge(transform(patientlist_curr,
    #                                   attribute_no = attribute_no),
    #                         project(
    #                           scidb(paste("apply(", PATIENTKEY@name, ", biosample_id, biosample_id)", sep="")),
    #                           c("name", "biosample_id")),
    #                         by.x = "a", by.y = "name")
    qq = paste("equi_join(",
               patientlist_curr@name, ", ",
               "project(filter(", PATIENTKEY@name, ", dataset_version = ", dataset_version, "), name), ",
               "'left_names=a', 'right_names=name', 'keep_dimensions=1')")
    joinPatientName = scidb(jdb$db, qq)

    cat("number of matching public_id-s between expression-count table and PER_PATIENT csv file:\n")
    countMatches = scidb_array_count(joinPatientName)
    print(countMatches)



    # Verify
    x1 = as.R(jdb$db$project(PATIENTKEY, "name"))
    x2 = as.R(patientlist_curr)

    stopifnot(countMatches == sum(x2$a %in% x1$name))

    # cat("The expression count table public_id-s that are not present in PER_PATIENT csv file: \n")
    # tt = x2$public_id %in% x1$PUBLIC_ID
    # print(x2$public_id[which(!tt)])

    joinPatientName = jdb$db$redimension(joinPatientName,
                                         R(paste("<biosample_id:int64>
                                                 [attribute_no=0:", countMatches+1, ",",countMatches+2, ",0]", sep = "")))

    joinBack2 = scidb(jdb$db,
                      paste("cross_join(",
                            joinBack1@name, " as X, ",
                            joinPatientName@name, "as Y, ",
                            "X.attribute_no, Y.attribute_no)", sep = ""))
    joinBack2@name

    joinBack2 = store(jdb$db, joinBack2, temp=TRUE)

    # Verify with
    scidb_array_head(jdb$db$filter(joinBack2, "tuple_no = 0"))

    cat("Number of expression level values in current array:\n")
    countExpressions = scidb_array_count(joinBack2)
    print(countExpressions)
    stopifnot(countExpressions == countMatches*countFeatures)

    ####################
    # Redimension the expression level array
    gct_table = scidb(jdb$db,
                      paste("apply(",
                            joinBack2@name, ", ",
                            "expression_count, dcast(a, float(null)), ",
                            "rnaquantificationset_id, ", rnaquantificationset_id,
                            ", dataset_version, ", dataset_version,
                            ")", sep = "")
    )
    # gct_table = attribute_rename(gct_table, "sdb_feature_no", "sdb_feature_no_old")
    # gct_table = attribute_rename(gct_table, "sdb_feature_no_dim", "sdb_feature_no")

    # Need to insert the expression matrix table into one big array
    # insertable = transform(gct_table, sdb_file_id = file_id)
    insertable = jdb$db$redimension(gct_table,
                                    R(schema(scidb(jdb$db, arrayname))))

    if (scidb_exists_array(arrayname)) {
      cat(paste("Inserting expression matrix data into", arrayname, "at dataset_version", dataset_version, "\n"))
      iquery(jdb$db, paste("insert(", insertable@name, ", ", arrayname, ")"))
    } else {
      stop("expression array does not exist")
    }

    return(rnaquantificationset_id)
  } # end of if (!only_test)
}

#' @export
register_copynumber_seg = function(copynumberset, only_test = FALSE){
  test_register_copynumber_seg(copynumberset)
  if (!only_test) {
    dataset_id = copynumberset$dataset_id
    b = search_biosamples(dataset_id = dataset_id, dataset_version = copynumberset$dataset_version)

    xx = read.delim(copynumberset$filepath)
    xx$biosample_id = b[match(xx$ID, b$name), ]$biosample_id
    stopifnot(!any(is.na(xx$biosample_id)))
    xx$ID = NULL
    colnames(xx) = gsub("chrom", "reference_name", colnames(xx))
    colnames(xx) = gsub("loc.start", "start", colnames(xx))
    colnames(xx) = gsub("loc.end",   "end",   colnames(xx))
    colnames(xx) = gsub("[.]", "_", colnames(xx))

    xx$copynumberset_id = copynumberset$copynumberset_id
    xx$dataset_version = copynumberset$dataset_version

    arrayname = paste(find_namespace(id = copynumberset$copynumberset_id, entitynm = jdb$meta$arrCopyNumberSet),
                      jdb$meta$arrCopynumber_seg, sep = ".")

    cat("Inserting", nrow(xx), "entries into array:", arrayname, "at version", copynumberset$dataset_version, "\n")
    register_tuple(df = xx,
                   ids_int64_conv = c('start', 'end', 'dataset_version', 'copynumberset_id', 'biosample_id'),
                   arrayname = arrayname)
  } # end of if (!only_test)
}

#' @export
register_copynumber_matrix_file = function(copynumberSubSet, dataset_version, only_test = FALSE){
  test_register_copnyumber_matrix_file(copynumberSubSet, dataset_version)
  if (!only_test) {
    dataset_id = copynumberSubSet$dataset_id
    dataset_version = copynumberSubSet$dataset_version
    b = search_biosamples(dataset_id = dataset_id, dataset_version = dataset_version)

    xx = read.delim(copynumberSubSet$filepath)
    matched_biosample_id = b[match(tail(colnames(xx), -1), b$name), ]$biosample_id
    stopifnot(all(!is.na(matched_biosample_id)))
    colnames(xx) = c(colnames(xx)[1], matched_biosample_id)

    ff = search_features(featureset_id = 1) # TODO: Assign automatically
    ff_genes = ff[ff$feature_type == 'gene', ]

    matched_feature_id = ff_genes[match(xx$Gene, ff_genes$name), ]$feature_id
    stopifnot(all(!is.na(matched_feature_id)))
    xx$Gene = matched_feature_id
    colnames(xx) = gsub("^Gene$", "feature_id", colnames(xx))
    head(xx[1:5, 1:5])

    xx2 = tidyr::gather(xx, "biosample_id", "copynumber_val", 2:ncol(xx))
    stopifnot(nrow(xx2) == nrow(xx)*(ncol(xx)-1))

    xx2$dataset_version = dataset_version
    xx2$copynumbersubset_id = copynumberSubSet$copynumbersubset_id

    namespace = find_namespace(id = copynumberSubSet$copynumbersubset_id, entitynm = jdb$meta$arrCopyNumberSubSet)
    arrayname = paste(namespace, jdb$meta$arrCopynumber_mat, sep = ".")
    cat("Inserting", nrow(xx2), "entries into", arrayname, "at version", dataset_version, "\n")
    register_tuple(df = xx2, ids_int64_conv = get_idname(jdb$meta$arrCopynumber_mat), arrayname = arrayname)
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

    nmsp = find_namespace(id = fusionset_record$fusionset_id, entitynm = jdb$meta$arrFusionset)
    arrayname = paste(nmsp, jdb$meta$arrFusion, sep = ".")

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
  feature_df = feature_df[match(unique(expr_df$feature_id), feature_df$feature_id), ]
  biosample_df = biosample_df[match(unique(expr_df$biosample_id), biosample_df$biosample_id), ]

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

delete_info_fields = function(fullarrayname, ids, dataset_version){
  # So far, have coded the case where consecutive ids is provided
  if ( !( identical( as.integer(sort(ids)), 
                     (min(ids): max(ids)) ) ) ) stop("Delete only works on consecutive set of entity_id-s")
  
  arr = fullarrayname
  entity = strip_namespace(fullarrayname)
  arrInfo = paste(arr, "_INFO", sep = "")
  if (dataset_versioning_exists(entity)) {
    stopifnot(length(dataset_version) == 1)
    qq = paste("filter(", arrInfo, ", (",  get_base_idname(arr), " < ", min(ids), " OR ",
               get_base_idname(arr), " > ", max(ids), ") OR (dataset_version != ", dataset_version, "))", sep = "")
  } else { # Entity does not have dataset_version-s
    qq = paste("filter(", arrInfo, ", ",  get_base_idname(arr), " < ", min(ids), " OR ",
               get_base_idname(arr), " > ", max(ids), ")", sep = "")
  }
  qq = paste("store(", qq, ", ", arrInfo, ")", sep = "")
  cat("Deleting entries for ids ", paste(sort(ids), collapse = ", "), " from info array: ", arrInfo, "\n", sep = "")
  iquery(jdb$db, qq)
}
#' @export
delete_entity = function(entity, ids, dataset_version = NULL){
  if (!(entity %in% get_entity_names())) stop("Entity '", entity, "' does not exist")
  if (is.null(ids)) return()
  if (dataset_versioning_exists(entity)) {
    if (is.null(dataset_version)) {
      stop("must supply dataset_version for versioned entities.
             Use dataset_version = 1 if only one version exists for relevant clinical study")
    } else {
      stopifnot(length(dataset_version) == 1)
    }
  }
  # So far, have coded the case where consecutive ids is provided
  if ( !( identical( as.integer(sort(ids)), 
                    (min(ids): max(ids)) ) ) ) stop("Delete only works on consecutive set of entity_id-s")

  # First check that entity exists at this id
  entities = get_entity(entity = entity, ids = ids)
  if (nrow(entities) == 0) {
    cat("No entries at ids: ", paste(ids, collapse = ", "), " for entity: ", entity, "\n", sep = "")
  } else { # if entities exist
    # Find the correct namespace
    if (lookup_exists(entity)) {
      namespaces = find_namespace(id = ids, entitynm = entity)
      nmsp = unique(namespaces)
    } else { nmsp = 'public' }

    if (length(nmsp) != 1) stop("Can run delete only at one namespace at a time")
    arr = paste(nmsp, entity, sep = ".")

    # Clear out the array
    if (dataset_versioning_exists(entity)) {
      qq = paste("filter(", arr, ", (",  get_base_idname(arr), " < ", min(ids), " OR ",
                 get_base_idname(arr), " > ", max(ids), ") OR (dataset_version != ", dataset_version, "))", sep = "")
    } else { # Entity does not have dataset_version-s
      qq = paste("filter(", arr, ", ",  get_base_idname(arr), " < ", min(ids), " OR ",
                 get_base_idname(arr), " > ", max(ids), ")", sep = "")
    }
    qq = paste("store(", qq, ", ", arr, ")", sep = "")
    cat("Deleting entries for ids ", paste(sort(ids), collapse = ", "), " from ", arr, " entity\n", sep = "")
    iquery(jdb$db, qq)

    # Clear out the info array
    infoArray = jdb$meta$L$array[[entity]]$infoArray
    if (infoArray){
      delete_info_fields(fullarrayname = arr, ids = ids, dataset_version = dataset_version)
    }

    # Clear out the lookup array if required
    if (lookup_exists(entity)){
      # Check if there are no entities at this ID
      qcount = paste("op_count(filter(", arr, ", ",
                     get_base_idname(entity), " >= ", min(ids), " AND ",
                     get_base_idname(entity), " <= ", max(ids), "))" )
      newcount = iquery(jdb$db, qcount, return = TRUE)$count
      if (newcount == 0){ # there are no entities at this level
        arrLookup = paste(entity, "_LOOKUP", sep = "")
        qq = paste("filter(", arrLookup, ", ",  get_base_idname(arr), " < ", min(ids), " OR ",
                   get_base_idname(arr), " > ", max(ids), ")", sep = "")
        qq = paste("store(", qq, ", ", arrLookup, ")", sep = "")
        cat("Deleting entries for ids ", paste(sort(ids), collapse = ", "), " from lookup array: ", arrLookup, "\n", sep = "")
        iquery(jdb$db, qq)
        updatedcache = entity_lookup(entityName = entity, updateCache = TRUE)
      }
    }
  } # end of check that some data existed in the first place
}

#' @export
update_entity = function(entity, df){
  if (lookup_exists(entity)){
    namespaces = find_namespace(id = df[, get_base_idname(entity)], entitynm = entity)
    nmsp = unique(namespaces)
    if (length(nmsp) != 1) stop("entity to be updated must belong to one namespace only")
  } else {
    nmsp = "public"
  }
  fullarraynm = paste(nmsp, entity, sep = ".")
  update_mandatory_and_info_fields(df = df, arrayname = fullarraynm)
}

#' @export
get_entity_count = function(){
  entities = c(jdb$meta$arrProject, jdb$meta$arrDataset, jdb$meta$arrIndividuals, jdb$meta$arrBiosample, jdb$meta$arrRnaquantificationset)
  if (length(jdb$cache$nmsp_list) == 1){
    nmsp = jdb$cache$nmsp_list
    queries = sapply(entities, function(entity){paste("op_count(", nmsp, ".", entity, ")", sep = "")})
  }
  else if (length(jdb$cache$nmsp_list) == 2){
    queries = sapply(entities, FUN = function(entity) {
      paste("join(",
            paste(sapply(jdb$cache$nmsp_list, function(nmsp){paste("op_count(", nmsp, ".", entity, ")", sep = "")}), collapse=", "),
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
    sapply(entities, FUN=function(entity) {paste(entity, "_", jdb$cache$nmsp_list, sep = "")}),
    ":uint64",
    sep = "",
    collapse = ", ")
  res_schema = paste("<", attrs, ">[i=0:0,1,0]", sep = "")
  res = iquery(jdb$db, qq,
               schema = res_schema, return = T)
  colnames(res) = gsub("X_", "", colnames(res))

  xx = sapply(entities, FUN=function(entity) {res[, paste(entity, jdb$cache$nmsp_list, sep = "_")]})

  counts = data.frame(entity = entities)
  if (length(jdb$cache$nmsp_list) == 1){
    counts[, jdb$cache$nmsp_list] = as.integer(xx)
  } else {
    rownum = 1
    for (nmsp in jdb$cache$nmsp_list){
      counts[, nmsp] = as.integer(xx[rownum,])
      rownum = rownum + 1
    }
  }
  counts
}

