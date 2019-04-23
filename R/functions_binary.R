#' @export
register_measurementdata_cache = function(
  measurementset, 
  pipeline_data, 
  con = NULL
) {
  con = revealgenomics:::use_ghEnv_if_null(con = con)
  fullnm = full_arrayname(.ghEnv$meta$arrMeasurementDataCache)
  
  # Begin uploading
  payoad_class = as.character(class(pipeline_data))
  pipeline_data = serialize(pipeline_data, NULL)
  payload_size = as.numeric(object.size(pipeline_data))
  THRESH_SCIDB_BINARY_UPLOAD = 500*1024*1024 # MB at a time to upload
  factor = ceiling(payload_size / THRESH_SCIDB_BINARY_UPLOAD)
  
  payload_length = length(pipeline_data) 
  stepsize = ceiling(payload_length/factor)
  for (idx in 1:factor) {
    cat("idx:", idx, "of", factor, "\n")
    
    if (idx < factor) {
      range_idx = c((stepsize*(idx-1) + 1): (stepsize*idx))
    } else { # remember to include till the very end 
      range_idx = c((stepsize*(idx-1) + 1): payload_length)
    }
    payload_size_at_idx = as.numeric(object.size(pipeline_data[range_idx]))
    pipeline_data_db = as.scidb(
      con$db, 
      pipeline_data[range_idx])
    # Rename as binary `payload`
    qq1 = paste0("cast(", pipeline_data_db@name, ", <payload:binary>[i])")
    # Add other attribute values
    qq1 = paste0(
      "apply(", 
      qq1, ", ", 
      "measurementset_id, ", measurementset$measurementset_id, ", ", 
      "dataset_id, ", measurementset$dataset_id, ", ", 
      "dataset_version, ", measurementset$dataset_version, ", ", 
      "subpart_id, ", idx, ", ", 
      "payload_size_bytes, int64(", payload_size_at_idx, "), ", 
      "cache_valid, TRUE, ", 
      "cache_mark_timestamp, string(now()), ", 
      "payload_class, '", payoad_class, "')"
    )
    # Redimension into target schema and insert
    qq1 = paste0(
      "redimension(", 
      qq1, 
      ", ", fullnm, ")"
    )
    qq1 = paste0(
      "insert(", 
      qq1, ", ", fullnm, ")"
    )
    cat("inserting data of size", payload_size_at_idx/factor/1024/1024, "MB into", fullnm, 
        "array at measurementset_id =", unique(measurementset$measurementset_id), "subpart:", idx, "\n")
    
    iquery(con$db, qq1)
  }
}

#' @export
search_measurementdata_cache = function(measurementset, con = NULL) {
  if (measurementdata_cache_is_cache_valid(measurementset = measurementset, con = con)) {
    num_subparts = measurementdata_cache_get_num_subparts(measurementset = measurementset, con = con)
    if (num_subparts == 1) {
      measurementdata_cache_download_subpart(measurementset = measurementset, 
                                             subpartidx = 1, con = con)
    } else {
      message("Multiple subparts")
      L1 = lapply(1:num_subparts, function(idx) {
        cat("Downloading", idx, "of", num_subparts, "\n\t")
        t1 = proc.time();
        res = revealgenomics:::measurementdata_cache_download_subpart(measurementset = measurementset, subpartidx = idx, convert_to_R = FALSE)
        cat((proc.time()-t1)[3], "seconds\n")
        res
      })
      vec = L1[[1]]$payload[[1]]
      for (idx in 2:num_subparts) {
        vec = c(vec, L1[[idx]]$payload[[1]])
      }
      unserialize(vec)
    }
  } else {
    message("Cache is not valid")
    return(NULL)
  }
}

measurementdata_cache_is_cache_valid = function(measurementset, con = NULL) {
  con = use_ghEnv_if_null(con = con)
  stopifnot(nrow(measurementset) == 1)
  arraynm = full_arrayname(.ghEnv$meta$arrMeasurementDataCache)
  filter_string = paste0(
    "filter(", 
    custom_scan(), "(", 
    arraynm, "), measurementset_id = ", measurementset$measurementset_id, 
    ")"
  )
  is_cache_valid = iquery(
    con$db, 
    paste0(
      "project(", 
      filter_string, 
      ", cache_valid)"
    ),
    return = TRUE)
  all(is_cache_valid$cache_valid)
}

measurementdata_cache_get_num_subparts = function(measurementset, con = NULL) {
  con = use_ghEnv_if_null(con = con)
  stopifnot(nrow(measurementset) == 1)
  arraynm = full_arrayname(.ghEnv$meta$arrMeasurementDataCache)
  filter_string = paste0(
    "filter(", 
    custom_scan(), "(", 
    arraynm, "), measurementset_id = ", measurementset$measurementset_id, 
    ")"
  )
  sum(iquery(
    con$db, 
    paste0(
      "aggregate(project(",
      filter_string, 
      ", payload_size_bytes), count(*), subpart_id)"
    ),
    return = TRUE
  )$count)
}

measurementdata_cache_get_download_size = function(measurementset, con = NULL) {
  con = use_ghEnv_if_null(con = con)
  stopifnot(nrow(measurementset) == 1)
  arraynm = full_arrayname(.ghEnv$meta$arrMeasurementDataCache)
  filter_string = paste0(
    "filter(", 
    custom_scan(), "(", 
    arraynm, "), measurementset_id = ", measurementset$measurementset_id, 
    ")"
  )
  sum(iquery(
    con$db, 
    paste0(
      "project(",
      filter_string, 
      ", payload_size_bytes)"
    ),
    return = TRUE
  )$payload_size_bytes)
}


measurementdata_cache_download_subpart = function(measurementset, subpartidx, convert_to_R = TRUE, con = NULL) {
  con = use_ghEnv_if_null(con = con)
  arraynm = full_arrayname(.ghEnv$meta$arrMeasurementDataCache)
  filter_string = paste0(
    "filter(", 
    custom_scan(), "(", 
    arraynm, "), measurementset_id = ", measurementset$measurementset_id, 
    " AND subpart_id = ", subpartidx, 
    ")"
  )
  res_df = iquery(
    con$db, 
    filter_string, 
    return = TRUE
  )
  if (convert_to_R) {
    unserialize(res_df$payload[[1]])
  } else {
    res_df
  }
}

measurementdata_cache_return_metadata = function(con = NULL) {
  con = use_ghEnv_if_null(con = con)
  arraynm = full_arrayname(.ghEnv$meta$arrMeasurementDataCache)
  iquery(
    con$db, 
    paste0(
      "aggregate(",
      custom_scan(), "(", 
      arraynm, 
      "), ", 
      "sum(payload_size_bytes) AS payload_size_bytes, ", 
      "min(payload_class) AS payload_class,", 
      "min(cache_valid) AS cache_valid,", 
      "min(cache_mark_timestamp) AS cache_mark_timestamp, ", 
      "dataset_version, dataset_id, measurementset_id)"
    ),
    return = TRUE
  )
}
