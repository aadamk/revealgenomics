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
