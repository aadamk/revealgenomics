# Earlier INFO fields were assigned `key_id` as synthetic dimensions so were all over the place
# This script collects all keys into the array METADATA_ATTR_KEY and then uses the `key_id` assigned
# by the API to transform all <METADATA>_INFO arrays

rm(list=ls())
library(revealgenomics)

if (!revealgenomics:::scidb_exists_array(revealgenomics:::full_arrayname(.ghEnv$meta$arrMetadataAttrKey))) {  # one time only
  rg_connect(username = 'scidbadmin')
  init_db(arrays_to_init = .ghEnv$meta$arrMetadataAttrKey)
}
creds_file = '~/.rg_config_secure-rw.json'
rg_connect(username = read_json(creds_file)$`user-name`, password = read_json(creds_file)$`user-password`)

######### Get all fields for metadata entities ###########
entity_info = get_entity_info()
entity_info = entity_info[
  which(sapply(entity_info$entity, 
               function(entity) .ghEnv$meta$L$array[[entity]]$infoArray)), ]

for (entity in entity_info$entity) {
  cat("===============================\n")
  message("Working on entity: ", entity)
  entity_array_name = paste0(revealgenomics:::full_arrayname(entity), "_INFO")
  entity_id = revealgenomics:::get_entity_id(entity)
  
  # Check if entity already transformed  
  entity_info_df = iquery(.ghEnv$db, entity_array_name, return = T)
  if (nrow(entity_info_df) == 0) {
    cat("No INFO entries for entity: ", entity, " -- skipping\n")
    next
  }
  attrkey = revealgenomics:::search_metadata_attrkey(entity_id = revealgenomics:::get_entity_id(entity))
  if (nrow(attrkey) > 0) {
    if (length(setdiff(entity_info_df$key_id, attrkey$metadata_attrkey_id)) == 0) {
      cat("Already transformed entity:", entity, " -- skipping\n")
      next
    }
  }

  allkeys = tryCatch({
    iquery(.ghEnv$db, 
           paste0("grouped_aggregate(", 
                  entity_array_name, 
                  ", count(*), key)"), 
           return = TRUE
           )
  }, error = function(e) {
    return(NA)
  })
  if (class(allkeys) == 'data.frame') {
    if (nrow(allkeys) > 0) {
      df_metadata_attrkey = data.frame(
        entity_id = entity_id, 
        metadata_attrkey = allkeys$key, 
        stringsAsFactors = FALSE
      )
      key_idx = revealgenomics:::register_metadata_attrkey(df1 = df_metadata_attrkey)
    } else {
      browser()
    }
  } else {
    browser()
  }
  
  # Running some verification
  attrkey = revealgenomics:::search_metadata_attrkey(entity_id = revealgenomics:::get_entity_id(entity))
  
  # Make sure that all keys have already been registered
  m1 = find_matches_and_return_indices(source = entity_info_df$key, target = attrkey$metadata_attrkey)
  stopifnot(length(m1$source_unmatched_idx) == 0)
  
  if (!all(entity_info_df$key_id %in% attrkey$metadata_attrkey_id)) { # i.e. transformation has not already been carried out
    query = paste0(
      "equi_join(", 
      entity_array_name, 
      ", filter(", 
      revealgenomics:::full_arrayname(.ghEnv$meta$arrMetadataAttrKey), 
      ", entity_id = ", 
      revealgenomics:::get_entity_id(entity), 
      "), 'left_names=key', 'right_names=metadata_attrkey', 'keep_dimensions=1')")
    
    if (entity == 'FEATURE') { # Extra dimensions for FEATURE_INFO array (filling in manually)
      new_schema = "<key:string,val:string,featureset_id:int64 NOT NULL,gene_symbol_id:int64 NOT NULL,feature_id:int64 NOT NULL,key_id:int64 NOT NULL,entity_id:int64,metadata_attrkey_id:int64 NOT NULL> [instance_id; value_no]"
    } else {
      new_schema = paste0(
        "<key:string,val:string,", 
        ifelse(revealgenomics:::is_entity_versioned(entity), "dataset_version:int64 NOT NULL," , ""),  
        ifelse(
          'dataset_id' %in% names(.ghEnv$meta$L$array[[entity]]$dims), 
          "dataset_id:int64 NOT NULL,", ""), 
        ifelse(entity != .ghEnv$meta$arrDataset, 
               paste0(revealgenomics:::get_base_idname(entity), ":int64 NOT NULL, "),
               ""), 
        "OLD__key_id:int64 NOT NULL,entity_id:int64,key_id:int64 NOT NULL> [instance_id; value_no]"
      )
    }
    query1 = paste0(
      "redimension(cast(", query, ", ",  new_schema, "), ", 
      entity_array_name, ")"
    )
    count_new = iquery(.ghEnv$db, paste0("op_count(", query1, ")"), return = T)
    stopifnot(count_new$count == nrow(entity_info_df))
    query2 = paste0(
      "store(", query1, ", ", 
      entity_array_name, ")"
    )
    
    cat("Running the transformation and insertion\n")
    iquery(.ghEnv$db, query2)
  } else {
    cat("Skipping entity:", entity, ". Already covered. \n")
  }
  
  # Run verification one last time
  entity_info_df = iquery(.ghEnv$db, entity_array_name, return = T)
  stopifnot(
    identical(
      setdiff(attrkey$metadata_attrkey_id, attrkey$metadata_attrkey_id), 
      numeric(0)
    )
  )
}
