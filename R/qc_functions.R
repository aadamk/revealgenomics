#' Run QC checks on data loaded via Excel file
#' 
#' Run quality control checks on data loaded via Excel file. Make sure Measurement array is linked 
#' properly to (1) corresponding entries in Biosample entity, (2) corresponding entries
#' in MeasurementImportFile entity
#' 
#' @param measurementset_id one / set of \code{measurementset_id}-s to restrict QC checking to.
#'                          If not supplied, then QC checks will be carried out on all 
#'                          \code{measurementset_id}-s that are available in MeasurementSet entity
#'                          
#' @export
run_quality_control_checks = function(measurementset_id = NULL) {
  if (is.null(measurementset_id)) {
    measurementset_df = get_measurementsets() # retrieve all measurementsets
    measurementset_df = measurementset_df[order(measurementset_df$measurementset_id), ]
    measurementset_id = measurementset_df$measurementset_id
    d_all = get_datasets()
  } else {
    measurementset_id = sort(measurementset_id)
    measurementset_df = get_measurementsets(measurementset_id = measurementset_id)
    d_all = get_datasets(dataset_id = sort(unique(measurementset_df$dataset_id)))
  }
  
  L0 = lapply(measurementset_df$measurementset_id, function (id) {
    pipeline_df = measurementset_df[measurementset_df$measurementset_id == id, ]
    d = d_all[d_all$dataset_id == pipeline_df$dataset_id, ]
    
    name_str = paste0(
      "dataset_id: ", d$dataset_id, 
      ", study name: ", d$name, 
      " -- measurementset_id: ", id,
      ", pipeline name: ", pipeline_df$name
    )
    message(name_str)
    cat("Downloading counts of data at given pipeline\n")
    sample_counts_df = tryCatch({
      iquery(
        .ghEnv$db,
        paste0("aggregate(filter(", 
               full_arrayname(pipeline_df$entity), 
               ", measurementset_id=", 
               pipeline_df$measurementset_id, 
               "), count(*), biosample_id)"),
        return = TRUE
      )
    }, error = function(e) {
      return(NULL)
    }
    ) 
    
    cat("Downloading samples at given pipeline\n")
    bios = NULL
    if (!is.null(sample_counts_df)) {
      if (nrow(sample_counts_df) > 0) {
        bios = get_biosamples(biosample_id = sort(unique(sample_counts_df$biosample_id)))
      } 
    } 
    
    cat("Unique molecule types: ", 
        pretty_print(unique(bios$sample_molecule_type)), "\n")
    
    cat("Downloading measurement import files for current pipeline\n")
    filelinks_df = search_measurements(measurementset_id = unique(pipeline_df$measurementset_id))
    
    elem = list(
      measurementset_id = id, 
      name_str = name_str,
      dataset = d,
      measurementImportFile = filelinks_df,
      measurementset = pipeline_df,
      biosample = bios,
      measurement_counts = sample_counts_df
    )
    if (is.null(elem$biosample)) {
      elem$condition1_failed = NA
      elem$condition2_failed = NA
    } else {
      elem$condition1_failed = !all(elem$biosample$biosample_id %in% elem$measurementImportFile$biosample_id)
      # Note: More import files might exist than linked biosamples -- so the opposite check is not done
      
      elem$condition2_failed = !all(elem$measurement_counts$biosample_id %in% elem$biosample$biosample_id)
      # Note: More biosamples might exist than biosamples in a given measurement array at
      # a given pipeline -- so the opposite check is not done
    }
    if (!is.na(elem$condition1_failed) & elem$condition1_failed) message("*** Failed condition 1")
    if (!is.na(elem$condition2_failed) & elem$condition2_failed) message("*** Failed condition 2")
    
    elem
  })
  
  names(L0) = sapply(L0, function(elem) elem$name_str)
  
  ###########################################################################
  condn1 = paste0(
    "Condition 1: samples in measurement array link correctly to measurementImportFile array", 
    " (is a check that correct `sample_molecule_type` were mapped)")
  condition1_failure_loc = sapply(L0, function(elem) {
    !is.na(elem$condition1_failed) & elem$condition1_failed
  })
  condition1_failure_loc = condition1_failure_loc[condition1_failure_loc]
  
  ###########################################################################
  condn2 = paste0(
    "Condition 2: samples in measurement array link correctly to biosamples array"
  )
  condition2_failure_loc = sapply(L0, function(elem) {
    !is.na(elem$condition2_failed) & elem$condition2_failed
  })
  condition2_failure_loc = condition2_failure_loc[condition2_failure_loc]
  
  ###########################################################################
  na_loc = sapply(L0, function(elem) {
    (is.na(elem$condition1_failed) | is.na(elem$condition2_failed)) &
     !grepl("file link|mutations - unfiltered", elem$measurementset$name)
  })
  na_loc = na_loc[na_loc]
  
  res_list = list(
    condition1 = condn1, 
    condition1_failures = names(condition1_failure_loc),
    condition2 = condn2, 
    condition2_failures = names(condition2_failure_loc),
    unexpected_na_result_at = names(na_loc)
  )
  if (length(na_loc) > 0) {
    message("Unexpected NA result at:\n\t",
            pretty_print(names(na_loc)))
  }
  if (length(condition1_failure_loc) > 0 | length(condition2_failure_loc) > 0) {
    message("QC failed", 
            "\n\tCondition 1:", condn1, " failed at: \n\t",
            pretty_print(names(condition1_failure_loc)), 
            "\n\tCondition2: ", condn2, " failed at: \n\t", 
            pretty_print(names(condition2_failure_loc)))

  } else {
    message("QC passed at measurementset_id(s): ", 
            pretty_print(measurementset_id, prettify_after = length(measurementset_id)))
  }
  res_list
}
