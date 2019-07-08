rm(list=ls())
library(revealgenomics)
options(scidb.result_size_limit = 4096)
if (FALSE) {
  con_admin = rg_connect2(username = 'scidbadmin')
  # if (scidb_exists_array('gh_secure.ENTITY_INFO', con = con_admin)) {
  #   iquery(con_admin$db, "remove(gh_secure.ENTITY_INFO)")
  #   iquery(con_admin$db, "create array gh_secure.ENTITY_INFO <value_id:int64>[entity_id, dataset_id, dataset_version, metadata_attrkey_id, entity_base_id]")
  # }
  # if (scidb_exists_array('gh_public.VALUE_STORE', con = con_admin)) {
  #   iquery(con_admin$db, "remove(gh_public.VALUE_STORE)")
  #   iquery(con_admin$db, "create array gh_public.VALUE_STORE <value:string>[value_id]")
  # }
  init_db(arrays_to_init = c(
    .ghEnv$meta$arrMetadataValue, 
    .ghEnv$meta$arrEntityFlexFields
  ), con = con_admin)
}

creds_file = '~/.rg_config_secure-rw.json'
rg_connect(username = read_json(creds_file)$`user-name`, password = read_json(creds_file)$`user-password`)

ei = get_entity_info()
ei = drop_na_columns(ei[ei$class == 'metadata', ])

metadata_entities = c(.ghEnv$meta$arrDataset, 
                      .ghEnv$meta$arrIndividuals,
                      .ghEnv$meta$arrBiosample, 
                      .ghEnv$meta$arrExperimentSet, 
                      .ghEnv$meta$arrMeasurementSet)

ei$entity[! ei$entity %in% metadata_entities]

L1 = lapply(metadata_entities, function(entity) {
  message("Downloading INFO array for entity: ", entity)
  if (revealgenomics:::get_infoArray(entity)) {
    res_flex = iquery(.ghEnv$db, 
                      paste0(
                        revealgenomics:::custom_scan(), "(", 
                        revealgenomics:::full_arrayname(entity), "_INFO)"
                      ),
                      return = T)
  } else {
    res_flex = data.frame()
  }

  message("Downloading mandatory fields array for entity: ", entity)
  res_man = iquery(.ghEnv$db, 
                   paste0(
                     revealgenomics:::custom_scan(), "(", 
                     revealgenomics:::full_arrayname(entity), ")"
                   ),
                   return = T)
  fields_to_skip = switch(
      entity,
      'DATASET' = c('project_id', 'public'),
      'INDIVIDUAL' = c('name', 'description'),
      'BIOSAMPLE' = c('name', 'description', 'individual_id'),
      'EXPERIMENTSET' = c('experiment_type_API'),
      'MEASUREMENTSET' = c('entity', 'featureset_id'),
      stop("Not covered case for entity:", entity))
  fields_to_skip = unique(c(fields_to_skip, c('created', 'updated')))
  message("--- skipping fields: ", pretty_print(fields_to_skip))
  res_man = res_man[, !(colnames(res_man) %in% fields_to_skip)]
  
  idnames = revealgenomics:::get_idname(entity)
  if (identical(sort(idnames), sort(colnames(res_man)))) {
    message("No fields worth indexing from within mandatory fields")
    res_man_as_flex = data.frame()
  } else {
    message("--- reformatting as a flex fields data.frame")
    res_man_as_flex = tidyr::gather_(
      data = res_man, 
      key_col = 'key', value_col = 'val', 
      gather_cols = colnames(res_man) [ ! (colnames(res_man) %in% idnames)])
    res_man_as_flex$key = paste0(entity, "__", res_man_as_flex$key)
    
    metadata_attrkeys = unique(res_man_as_flex$key)
    metadata_attrkey_idx = register_metadata_attrkey(
      df1 = data.frame(
        metadata_attrkey = metadata_attrkeys,
        entity_id = as.integer(revealgenomics:::get_entity_id(entity)),
        stringsAsFactors = FALSE
      ))
    metadata_attrkey_df = revealgenomics:::get_metadata_attrkey(metadata_attrkey_id = metadata_attrkey_idx)
    lookup_vec = metadata_attrkey_df$metadata_attrkey_id
    names(lookup_vec) = metadata_attrkey_df$metadata_attrkey
    res_man_as_flex$key_id = lookup_vec[res_man_as_flex$key]
    if (length(colnames(res_flex)) > 0) {
      res_man_as_flex = res_man_as_flex[, colnames(res_flex)]
    }
  }
  
  
  # Merge the two
  rbind(
    res_flex, 
    res_man_as_flex
  )
})
names(L1) = metadata_entities
format(object.size(L1), units="Mb")
# [1] "116.1 Mb" # scidb storage will be much lesser as dimensions are inferred
# [1] "465.1 Mb" # TCGA + scrnaseq machine

# dim(entity_info)
# # [1] 1233060       6
# length(unique(entity_info$val))
# # [1] 38401

uniq_vals = sort(unique(unlist(sapply(L1, function(elem) unique(elem$val)))))
length(uniq_vals)
# [1] 146411
# [1] 1062032 # TCGA + scrnaseq machine
# [1] 1062058 # TCGA + scrnaseq machine (with mandatory fields being indexed)


uniq_vals_df = data.frame(
  metadata_value  = uniq_vals, 
  ontology_category_id = revealgenomics:::search_ontology_category(ontology_category = 'uncategorized')$ontology_category_id,
  stringsAsFactors = FALSE
)
newline_match_pos = grep("\n", uniq_vals_df$metadata_value)
if (length(newline_match_pos) > 0) {
  message("Replacing ", length(newline_match_pos), " of 'newline' characters with '---'")
  message("Found in: ", pretty_print(uniq_vals_df$metadata_value[newline_match_pos]))
  uniq_vals_df$metadata_value = gsub("\n", " --- ", uniq_vals_df$metadata_value)
}
mv_idx  = revealgenomics:::register_metadata_value(
  df1 = uniq_vals_df
)

uniq_vals_df = revealgenomics:::get_metadata_value()
uniq_vals_df = uniq_vals_df[, !(colnames(uniq_vals_df) %in% 
                                  c('created', 'updated'))]
if (length(which(duplicated(uniq_vals_df$metadata_value))) > 0) {
  message("TODO: Some duplicates found in metadata value, Should not ", 
          "have occurred. Skippling for now")
  uniq_vals_df = uniq_vals_df[!duplicated(uniq_vals_df$metadata_value), ]
}
format(object.size(uniq_vals_df), units="Mb")
# [1] "11 Mb"
# [1] "123.9 Mb" # TCGA + scrnaseq machine
# [1] "200.9 Mb" # TCGA + scrnaseq machine (mandatory fields + extra col for ontology category id)

head(grep("lymphoma", uniq_vals_df$metadata_value, value = T))

for (entity in metadata_entities) {
  message("Assigning value id for entity: ", entity)
  if (any(is.na(L1[[entity]]$val))) {
    message("Dropping NA values")
    L1[[entity]] = L1[[entity]][which(!is.na(L1[[entity]]$val)), ]
  }
  
  m1 = find_matches_and_return_indices(
    source = L1[[entity]]$val,
    target = uniq_vals_df$metadata_value)
  if (length(m1$source_unmatched_idx) > 0) {
    message("Dropping problematic entries that are somehow not matching with search index")
    L1[[entity]] = L1[[entity]][m1$source_matched_idx, ]
    m1 = find_matches_and_return_indices(
      source = L1[[entity]]$val,
      target = uniq_vals_df$metadata_value)
    stopifnot(length(m1$source_unmatched_idx) == 0)
  }
  
  L1[[entity]]$metadata_value_id = NA
  L1[[entity]]$metadata_value_id = uniq_vals_df$metadata_value_id[m1$target_matched_idx]
  
  L1[[entity]] = plyr::rename(
    L1[[entity]],
    c('key_id' = 'metadata_attrkey_id')
  )
  if (entity == 'DATASET') {
    L1[[entity]][, 'entity_base_id'] = L1[[entity]][, 'dataset_id'] # create duplicate column
  } else { # rename the base id of each entity into a generic name
    colnames(L1[[entity]])[
      which(colnames(L1[[entity]]) == revealgenomics:::get_base_idname(entity))] = 'entity_base_id'
  }
  
  L1[[entity]]$entity_id = .ghEnv$meta$L$array[[entity]]$entity_id
  L1[[entity]] = L1[[entity]][, 
                              c('entity_id',
                                'dataset_id', 'dataset_version', 
                                'entity_base_id',
                                'metadata_attrkey_id',
                                'metadata_value_id'
                                )]
}

df_info_combined = rbind.fill(L1)
head(df_info_combined)
tail(df_info_combined)

dfx = as.scidb_int64_cols(
  .ghEnv$db, 
  df_info_combined,
  int64_cols = c('entity_id', 'dataset_id',
                 'dataset_version', 'entity_base_id',
                 'metadata_attrkey_id', 'metadata_value_id'))

iquery(
  .ghEnv$db,
  paste0(
    "store(redimension(",
    dfx@name, 
    ", ", revealgenomics:::full_arrayname(.ghEnv$meta$arrEntityFlexFields), 
    "), ", revealgenomics:::full_arrayname(.ghEnv$meta$arrEntityFlexFields), 
    ")"
  )
)

# Defunct code (usable as prototype)

# newline_in_data = grep("\n", uniq_vals_df$value)
# if (length(newline_in_data) > 0) {
#   message("Replacing ", length(newline_in_data), " newline characters with '--'")
#   uniq_vals_df$value = gsub("\n", "--", uniq_vals_df$value)
# }
# uniq_vals_arr = as.scidb_int64_cols(.ghEnv$db, uniq_vals_df, int64_cols = 'value_id')
# iquery(.ghEnv$db, 
#        paste0(
#          "store(redimension(", 
#          uniq_vals_arr@name, 
#          ", <value:string>[value_id])", 
#          ", gh_public.VALUE_STORE)"))
# rm(uniq_vals_df)
# dataset_id = find_dataset_id_by_grep("CASTOR")
# dataset_id = find_dataset_id_by_grep("neuron_9k")
search_biosamples_v2 = function(dataset_id) {
  system.time({uniq_vals_df = iquery(.ghEnv$db, "gh_public.VALUE_STORE", return = TRUE)})
  b1 = iquery(
    .ghEnv$db, 
    paste0(
      "filter(", 
      revealgenomics:::custom_scan(), 
      "(", revealgenomics:::full_arrayname(.ghEnv$meta$arrBiosample), 
      "), dataset_id = ", dataset_id, ")"),
    return = T)
  b2 = iquery(
    .ghEnv$db, 
    paste0(
      "filter(", 
      revealgenomics:::custom_scan(), 
      "(", 
      "gh_secure.ENTITY_INFO", 
      "), dataset_id = ", dataset_id, " AND entity_id = 1005)"),
    return = T)
  
  metadata_keys = revealgenomics:::get_metadata_attrkey()
  head(b2)
  m3 = find_matches_and_return_indices(
    source = b2$value_id, target = uniq_vals_df$value_id
  )
  stopifnot(length(m3$source_unmatched_idx) == 0)
  b2$val = uniq_vals_df$value[m3$target_matched_idx]
  
  b2$value_id = NULL
  b2$entity_id = NULL
  b2 = plyr::rename(b2, c('entity_base_id' = 'biosample_id'))
  
  b2s = tidyr::spread(b2, key = "metadata_attrkey_id", value = "val")
  
  posn_metadata_attrkeys = which(! colnames(b2s) %in% colnames(b2) )
  m4 = find_matches_and_return_indices(
    source = colnames(b2s)[posn_metadata_attrkeys],
    target = metadata_keys[metadata_keys$entity_id == 1005, ]$metadata_attrkey_id
  )
  stopifnot(length(m4$source_unmatched_idx) == 0)
  colnames(b2s)[posn_metadata_attrkeys] = metadata_keys$metadata_attrkey[m4$target_matched_idx]
  
  # Apply ontology values
  revealgenomics:::run_common_operations_on_search_metadata_output(
    df1 = b2s, dataset_id = dataset_id, 
    entity = .ghEnv$meta$arrBiosample, all_versions = TRUE, con = NULL)
}
