register_feature_summary_at_measurementset = function(measurementset_df, dataset_version = NULL, con = NULL) {
  con = use_ghEnv_if_null(con = con)
  entity = measurementset_df$entity
  if (nrow(measurementset_df) != 1) stop("Expect to register feature summary one measurementset at a time. ", 
                                           "Supplied, nrow(measurementset_df) = ", nrow(measurementset_df))
  
  if (is.null(dataset_version)) {
    dataset_version = measurementset_df$dataset_version
  } else {
    if (dataset_version != measurementset_df$dataset_version) {
      stop("User supplied dataset_version = ", dataset_version, 
           " but measurementset has dataset_version = ", measurementset_df$dataset_version)
    }
  }
  if (measurementset_df$entity != 'FUSION') {
    qq = paste0(
      "aggregate(
      filter(", 
      revealgenomics:::custom_scan(), "(",
      full_arrayname(entity), 
      "), 
      measurementset_id = ", measurementset_df$measurementset_id, 
      " AND dataset_version = ", dataset_version, "), ",
      "count(*), measurementset_id, feature_id)")
    res = iquery(con$db, qq, return = TRUE)
  } else { # special handling for fusion
    qq = paste0(
      "aggregate(
      filter(", 
      revealgenomics:::custom_scan(), "(",
      full_arrayname(entity), 
      "), 
        measurementset_id = ", measurementset_df$measurementset_id, 
      " AND dataset_version = ", dataset_version, "), ",
      "count(*), measurementset_id, feature_id_left, feature_id_right)")
    res = iquery(con$db, qq, return = TRUE)
    message("Need to mangle feature_id_left and feature_id_right into common feature_id for FUSION case")
    browser()
  }
  
  stopifnot(length(unique(res$feature_id)) == nrow(res))
  
  message("\t# features:", nrow(res), 
      "\n\tpreview of feature_id-s:", pretty_print(res$feature_id))
  if (nrow(res) > 0) {
    message("Uploading feature summary at pipeline")
    resX = as.scidb_int64_cols(db = con$db, df1 = res, 
                               int64_cols = c('measurementset_id', 
                                              'feature_id', 
                                              'count'))
    qq0 = paste0(
      "apply(", 
      resX@name, 
      ", biosample_count, int32(count)", 
      ", dataset_id, int64(", measurementset_df$dataset_id, ")",  
      ", dataset_version, int64(", dataset_version, ")", 
      ")"
    )
    qq1 = paste0(
      "insert(redimension(", 
      qq0, ", ", 
      full_arrayname(.ghEnv$meta$arrFeatureSummary), "), ", 
      full_arrayname(.ghEnv$meta$arrFeatureSummary), ")"
    )
    message("Inserting")
    iquery(con$db, qq1)
  } else {
    message("No entries for this pipeline")
  }
}

find_entities_that_have_features = function(feature_df, entity = c('DATASET', 'MEASUREMENTSET'), con = NULL) {
  entity = match.arg(entity)
  con = revealgenomics:::use_ghEnv_if_null(con = con)
  
  if (nrow(feature_df) == 0) return(data.frame())
  
  arrnm = 'gh_secure.FEATURE_SUMMARY'
  right_arr = revealgenomics:::formulate_build_literal_query(as.integer(feature_df$feature_id), value_name = 'feature_id')
  qq_join = paste0(
    "equi_join(", 
    revealgenomics:::custom_scan(), "(", arrnm, ")", 
    ", ", 
    right_arr, 
    ", 'left_names=feature_id', 'right_names=feature_id', 'keep_dimensions=1')"
  )
  
  idname = revealgenomics:::get_base_idname(entity)
  if (entity == .ghEnv$meta$arrDataset) {
    qq_aggr = paste0(
      "grouped_aggregate(", 
      qq_join, 
      ", count(*), ", 
      idname, ", feature_id)"
    )
  } else {
    qq_aggr = paste0(
      "grouped_aggregate(", 
      qq_join, 
      ", count(*), ", 
      "dataset_id, ", idname, ", feature_id)"
    )
  }
  
  qq_res = paste0(
    "equi_join(", 
    qq_aggr, 
    ", ", 
    "project(", 
    revealgenomics:::custom_scan(), "(", 
    revealgenomics:::full_arrayname(.ghEnv$meta$arrDataset), ")", 
    ", name)", 
    ", 'left_names=dataset_id', 'right_names=dataset_id', 'keep_dimensions=0')"
  )
  if (entity == .ghEnv$meta$arrMeasurementSet) {
    qq_res =  paste0(
      "equi_join(", 
      qq_res, 
      ", ", 
      "project(apply(", 
      revealgenomics:::custom_scan(), "(", 
      revealgenomics:::full_arrayname(.ghEnv$meta$arrMeasurementSet), ")", 
      ", measurementset_name, name), measurementset_name, entity)", 
      ", 'left_names=measurementset_id', 'right_names=measurementset_id', 'keep_dimensions=0')"
    )
  }
  res_df = revealgenomics:::drop_equi_join_dims(iquery(con$db, qq_res, return = T))
  
  # Plumb in the gene symbol names
  
  feature_id_lookup = feature_df$gene_symbol
  names(feature_id_lookup) = as.character(feature_df$feature_id)
  
  res_df$gene_symbol = feature_id_lookup[as.character(res_df$feature_id)]
  
  if (entity == .ghEnv$meta$arrDataset) {
    return(
      res_df[, c(idname, 'name', 'feature_id', 'gene_symbol')]
    )  
  } else {
    res_df[, c('dataset_id', 'name', idname, 'entity', 'measurementset_name', 'feature_id', 'gene_symbol')]
  }
}

if (FALSE) { # sample usage shown here
  feature_df = search_features(gene_symbol = c('EGFR', 'KRAS', 'TP53', 'MYC', 'CD276'), mandatory_fields_only = T)
  res = find_entities_that_have_features(
    feature_df = feature_df,
    entity = 'DATASET')
  head(res)
  tail(res)
  table(res$gene_symbol)
  xx = res %>% select(-feature_id) %>% distinct() %>% group_by(name) %>% summarise(count = n()) %>% filter(count == length(unique(feature_df$gene_symbol)))
  table(xx$count)
  
  res = find_entities_that_have_features(
    feature_df = feature_df,
    entity = 'MEASUREMENTSET')
}
