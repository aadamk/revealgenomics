search_entity_flex_fields = function(dataset_id, dataset_version = 1, con = NULL) {
  con = use_ghEnv_if_null(con = con)
  stopifnot(length(dataset_id) == 1)
  stopifnot(length(dataset_version) == 1)
  arrnm = full_arrayname(.ghEnv$meta$arrEntityFlexFields)
  res = iquery(
    con$db, 
    paste0(
      "filter(", arrnm, 
      ", dataset_id = ", dataset_id, ")"
    ),
    return = T
  )
  res[res$dataset_version == dataset_version, ]
}

#' Find matching datasets by searching across entities
#' 
#' @param metadata_value_df portion of \code{get_metadata_value()}, must contain the column \code{metadata_value_id}
#' 
#' @export
find_dataset_id_by_metadata_value = function(metadata_value_df, con = NULL) {
  con = use_ghEnv_if_null(con = con)
  metadata_value_id = metadata_value_df$metadata_value_id
  metadata_value_id = as.integer(sort(metadata_value_id))
  if (length(metadata_value_id) == 0) {
    return(data.frame(
      dataset_id = integer(),
      dataset_name = character(),
      entity_id = integer(), 
      entity = character(), 
      count = integer(),
      stringsAsFactors = F
    ))
  }
  if (length(metadata_value_id) <= 20) {
    message("Searching by following metadata_value id-s: (", 
            pretty_print(metadata_value_id, prettify_after = 20), ")")
  } else {
    message("Searching by following metadata_value id-s: ", 
            pretty_print(metadata_value_id))
  }
  build_literal_query = paste0(
    "build(<metadata_value_id:int64>[idx=0:", 
    length(metadata_value_id)-1, 
    "], '[", 
    paste0(metadata_value_id, collapse = ", "),
    "]', true)"
  )
  eq_join_query = paste0(
    "equi_join(", 
    revealgenomics:::custom_scan(), "(", 
    revealgenomics:::full_arrayname(.ghEnv$meta$arrEntityFlexFields), 
    "), ",
    build_literal_query,
    ", 'left_names=metadata_value_id', 'right_names=metadata_value_id'",
    ", 'keep_dimensions=true')"
  )
  aggregate_query = paste0(
    "grouped_aggregate(",
    eq_join_query, 
    ", count(*), dataset_id, entity_id)")
  join_dataset_name = paste0(
    "equi_join(",
    "project(",
    revealgenomics:::custom_scan(), "(", 
    revealgenomics:::full_arrayname(.ghEnv$meta$arrDataset), 
    ")", 
    ", name) ",
    "as X, ",
    aggregate_query, " as Y, ",
    "'left_names=dataset_id', 'right_names=dataset_id')"
  )
  res = revealgenomics:::drop_equi_join_dims(iquery(con$db, join_dataset_name, return = T))
  res$entity = revealgenomics:::get_entity_from_entity_id(entity_id = res$entity_id)
  res = plyr::rename(res, c('name' = 'dataset_name'))
  res = res[, c('dataset_id', 'dataset_name', 'entity_id', 'entity', 'count')]
  res[order(res$dataset_id, res$entity_id), ]
}
