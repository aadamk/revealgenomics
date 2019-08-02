# FEATURE SUMMARY

rm(list=ls())
library(revealgenomics)

if (FALSE) { # one time only
  # Initialize the FeatureSummary array
  con_root = rg_connect2(username = 'scidbadmin')
  init_db(arrays_to_init = .ghEnv$meta$arrFeatureSummary, con = con_root)
}

# Now start loading FeatureSummary entries
creds_file = '~/.rg_config_secure-rw.json'
rg_connect(username = read_json(creds_file)$`user-name`, password = read_json(creds_file)$`user-password`)

ms = get_measurementsets(mandatory_fields_only = T)
ms = ms[order(ms$entity,  ms$measurementset_id), ]

d_all = get_datasets(mandatory_fields_only = T)

# skip for now
ms = ms[!ms$entity %in% c('FUSION', 'MEASUREMENT'), ]
for (idx in 1:nrow(ms)) {
  ms_idx = ms[idx, ]
  measurementset_idx = ms_idx$measurementset_id
  dataset_versionx = ms_idx$dataset_version
  entity = ms_idx$entity
  
  df_idx = d_all[d_all$dataset_id == ms_idx$dataset_id, ]
  
  message(
    "idx: ", idx, " of ", nrow(ms), "\n",
    "dataset_id: ", df_idx$dataset_id, 
    ", dataset_name: ", df_idx$name, 
    "\nentity: ", ms_idx$entity,
    "\n\tpipeline_id: ", ms_idx$measurementset_id, 
    ", pipeline_name: ", ms_idx$name
    )
  
  revealgenomics:::register_feature_summary_at_measurementset(measurementset_df = ms_idx)
}
