rm(list=ls())
library(revealgenomics)

if (FALSE) {
  con_admin = rg_connect2(username = 'scidbadmin')
  iquery(con_admin$db, "create array gh_secure.ENTITY_INFO <value_id:int64>[entity_id, dataset_id, dataset_version, metadata_attrkey_id, entity_base_id]")
  iquery(con_admin$db, "create array gh_public.VALUE_STORE <value:string>[value_id]")
}

creds_file = '~/.rg_config_secure-rw.json'
rg_connect(username = read_json(creds_file)$`user-name`, password = read_json(creds_file)$`user-password`)

ei = get_entity_info()
ei = drop_na_columns(ei[ei$class == 'metadata', ])

metadata_entities = c(.ghEnv$meta$arrDataset, 
                      .ghEnv$meta$arrIndividuals,
                      .ghEnv$meta$arrBiosample)

ei$entity[! ei$entity %in% metadata_entities]

L1 = lapply(metadata_entities, function(entity) {
  message("Downloading INFO array for entity: ", entity)
  iquery(.ghEnv$db, 
                       paste0(
                         revealgenomics:::custom_scan(), "(", 
                         revealgenomics:::full_arrayname(entity), "_INFO)"
                       ),
                       return = T
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
uniq_vals_df = data.frame(value_id = 1:length(uniq_vals), value = uniq_vals,
                          stringsAsFactors = FALSE)
format(object.size(uniq_vals_df), units="Mb")
# [1] "11 Mb"
# [1] "123.9 Mb" # TCGA + scrnaseq machine
head(uniq_vals_df)
tail(uniq_vals_df)

grep("lymphoma", uniq_vals_df$value, value = T)

for (entity in metadata_entities) {
  message("Assigning value id for entity: ", entity)
  if (any(is.na(L1[[entity]]$val))) {
    message("Dropping NA values")
    L1[[entity]] = L1[[entity]][which(!is.na(L1[[entity]]$val)), ]
  }
  
  m1 = find_matches_and_return_indices(
    source = L1[[entity]]$val,
    target = uniq_vals_df$value)
  stopifnot(length(m1$source_unmatched_idx) == 0)  
  
  L1[[entity]]$value_id = NA
  L1[[entity]]$value_id = uniq_vals_df$value_id[m1$target_matched_idx]
  
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
                                'value_id'
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
                 'metadata_attrkey_id', 'value_id'))

iquery(
  .ghEnv$db,
  paste0(
    "store(redimension(",
    dfx@name, 
    ", <value_id:int64>[entity_id, dataset_id, dataset_version, metadata_attrkey_id, entity_base_id])",
    ", gh_secure.ENTITY_INFO)"
  )
)

# Value store
newline_in_data = grep("\n", uniq_vals_df$value)
if (length(newline_in_data) > 0) {
  message("Replacing ", length(newline_in_data), " newline characters with '--'")
  uniq_vals_df$value = gsub("\n", "--", uniq_vals_df$value)
}
uniq_vals_arr = as.scidb_int64_cols(.ghEnv$db, uniq_vals_df, int64_cols = 'value_id')
iquery(.ghEnv$db, 
       paste0(
         "store(redimension(", 
         uniq_vals_arr@name, 
         ", <value:string>[value_id])", 
         ", gh_public.VALUE_STORE)"))
rm(uniq_vals_df)
dataset_id = find_dataset_id_by_grep("CASTOR")
dataset_id = find_dataset_id_by_grep("neuron_9k")
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
