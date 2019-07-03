# 1. Run script `integerize_info_values.R`
# --- this places the index in named arrays not maintained by the API

# 2. Initialize the search by values index arrays
rm(list=ls())
if (FALSE) {
  rg_connect(username = 'scidbadmin')
    init_db(
    arrays_to_init = c(.ghEnv$meta$arrEntityFlexFields, 
                       .ghEnv$meta$arrMetadataValue))
}

# 3. Move the named arrays from step (1) into API maintained arrays

# 3a. Metadata values
nExisting = iquery(
  .ghEnv$db, 
  "op_count(gh_public.METADATA_VALUE)", 
  return = T
)$count
if (nExisting == 0) {
  iquery(
    .ghEnv$db,
    paste0(
      "insert(",
      "redimension(apply(", 
      "gh_public.VALUE_STORE, metadata_value, value, ", 
      "metadata_value_id, value_id, ontology_category_id, 1, ", 
      "created, '2019-06-15 xx:xx:xx', updated, '2019-06-15 xx:xx:xx'), ", 
      "gh_public.METADATA_VALUE), ", 
      "gh_public.METADATA_VALUE)"
    )
  )
}

# 3b. Entity flex fields
nExisting = iquery(
  .ghEnv$db, 
  "op_count(gh_secure.ENTITY_FLEX_FIELDS)", 
  return = T
)$count
if (nExisting == 0) {
  iquery(
    .ghEnv$db,
    "insert(project(apply(gh_secure.ENTITY_INFO, metadata_value_id, value_id), metadata_value_id), gh_secure.ENTITY_FLEX_FIELDS)"
  )
}

# 4. Now use loader to load new data (copied from script `enable_search_by_vals.R`)

creds_file = '~/.scidb_secure_user_auth'
rg_connect(username = read_json(creds_file)$`user-name`, password = read_json(creds_file)$`user-password`)
project_id = register_project(
  df = data.frame(name = 'TestSearchByVals',
                  description = 'To test that ENTITY_INFO array is filled properly during entity registration',
                  stringsAsFactors = FALSE)
)
dataset_idx = register_dataset(
  df = data.frame(
    name = c('TestSearchByValsDataset1', 'TestSearchByValsDataset2'), 
    project_id = project_id, 
    description = 'To test that ENTITY_INFO array is filled properly during entity registration: randomString: xkcd39',
    flexField1 = 32,
    flexField2 = 'randomString: DennisTheMenace45',
    flexField3 = 'randomString: HagarTheHorrible',
    stringsAsFactors = FALSE
  ))
individual_idx = register_individual(
  df = data.frame(
    name = c('Alec Baldwin', 'Leonardo diCaprio', 'Robert Downey Jr'),
    dataset_id = dataset_idx$dataset_id[1], 
    description = c('30 Rock', 'Inception', 'IronMan'), 
    flexField4 = 'randomString: Avengers and others',
    flexField5 = 'randomString: Titanic and others', 
    stringsAsFactors = F
  )
)
