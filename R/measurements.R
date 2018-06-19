#' register all measurements for a specific dataset
#' 
#' The function should be called after loading the `MeasurementData` arrays` (e.g. Variant, RNA-seq etc.).
#' The `MEASUREMENT` entity records one entry for each entry in a `MeasurementData` array per biosample,
#' per pipeline
#' 
#' (A `MEASUREMENT` combines both Experiment and Pipeline information) 
#' @export
populate_measurements = function(dataset_id, dataset_version, con = NULL) {
  con = use_ghEnv_if_null(con)
  db = con$db
  df_info = get_entity_info()
  df_info_msrmt = df_info[df_info$class == 'measurementdata',]
  df_info_msrmt$entity = as.character(df_info_msrmt$entity)
  
  for (idx in which(df_info_msrmt$entity != .ghEnv$meta$arrCopynumber_seg)){
    msrmnt_entity = df_info_msrmt[idx, ]$entity
    # stopifnot(is_entity_secured(msrmnt_entity))
    cat("Measurement entity: ", msrmnt_entity, "\n")
    
    msrmt_array = full_arrayname(msrmnt_entity)
    msrmt_set_nm = df_info_msrmt[idx, ]$search_by_entity
    msrmt_set_idnm = get_base_idname(msrmt_set_nm)
    t1 = proc.time()
    res = iquery(db,
                 paste("aggregate(", custom_scan(), "(", msrmt_array, 
                       "), count(*), biosample_id, ", 
                       msrmt_set_idnm, 
                       ", dataset_version)"), 
                 return = T)
    proc.time()-t1
    if (nrow(res) > 0) {
      cat("----Number of rows: ", nrow(res), "\n")
      
      stopifnot(all(res$count > 0))
      res$count = NULL
      res$measurement_entity = msrmnt_entity
      head(res)
      
      msrmt_set_DF = get_entity(entity = msrmt_set_nm, 
                                           id = sort(unique(res[, msrmt_set_idnm])),  
                                           all_versions = TRUE)
      
      res2 = merge(msrmt_set_DF[, c('dataset_id', 'dataset_version', msrmt_set_idnm, 'experimentset_id', 'name')], 
                   res, 
                   by = c(msrmt_set_idnm, 'dataset_version'),
                   all.x = TRUE)
      if (nrow(res) != nrow(res2)) stop("Data for entity: ", msrmnt_entity, " was registered without corresponding measurement set!")
      colnames(res2)[which(colnames(res2) == msrmt_set_idnm)] = 'measurementset_id'
      colnames(res2)[which(colnames(res2) == 'name')] = 'measurementset_name'
      # res2$dataset_id = 1
      # if (length(unique(res2$dataset_version)) != 1 |
      #             unique(res2$dataset_version) != 1) stop("This script has not been checked for multiple dataset versions at a time")
      # res2$dataset_version = NULL
      
      cat("======\n")
      cat("Registering", nrow(res2), "experiment-pipeline entries\n")
      for (dataset_idi in sort(unique(res2$dataset_id))) {
        res2_sel1 = res2[res2$dataset_id == dataset_idi, ]
        for (dataset_version in sort(unique(res2_sel1$dataset_version))) {
          res2_sel2 = res2_sel1[res2_sel1$dataset_version == dataset_version, ]
          res2_sel2$dataset_version = NULL
          cat("======------======\n")
          cat("Registering", nrow(res2_sel2), 
              "experiment-pipeline entries for dataset_id:", dataset_idi, "at version:", dataset_version, "\n")
          measurement_record = register_measurement(df = res2_sel2, 
                                                    dataset_version = dataset_version)
        }
      }
    }
  }
}

#' @export
get_measurements = function(measurement_id = NULL, dataset_version = NULL, 
                           all_versions = TRUE, mandatory_fields_only = FALSE, con = NULL){
  msrmt = get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrMeasurement, 
                                               id = measurement_id, 
                                               dataset_version, all_versions, 
                                               mandatory_fields_only = mandatory_fields_only, 
                                               con = con)
  
  # Merge with datasets info to join in study category
  d = get_datasets(con = con)
  if (!('study category' %in% colnames(d))) {
    d$`study category` = NA
  }
  if (any(is.na(d$`study category`))) {
    d[which(is.na(d$`study category`)), ]$`study category` = NA
  }
  msrmt2 = merge(msrmt, 
                 d[, c('dataset_id', 'dataset_version', 'study category')], 
                 by = c('dataset_id', 'dataset_version'))
  if (nrow(msrmt2) != nrow(msrmt)) stop("Some measurements did not belong to specific dataset_id-s")
  msrmt2
}

#' retrieve all the experiments available to logged in user
#' 
#' joins `Measurement`, `Dataset` (for `study category` field if available), and `ExperimentSet` arrays
#' to return experiment information to user
#' 
#' @examples 
#' experiments = get_experiments()
#' cat("Categorization of experiments by major type\n")
#' table(experiments$measurement_entity)
#' cat("Categorization of experiments by sub type\n")
#' table(paste(experiments$measurement_entity, experiments$name, sep = ": "))
#' cat("Categorization of experiments by sub type\n")
#' table(experiments$`study category`)
#' 
#' @export
get_experiments = function(con = NULL) {
  con = use_ghEnv_if_null(con)
  
  info_key = 'study category'
  con = use_ghEnv_if_null(con)
  
  qq = paste("equi_join(", 
             "grouped_aggregate(", custom_scan(), "(", full_arrayname(.ghEnv$meta$arrMeasurement), "), ", 
             "count(*),", 
             "dataset_id, dataset_version, experimentset_id, measurement_entity, biosample_id) as X, ", 
             "filter(", custom_scan(), "(", full_arrayname(.ghEnv$meta$arrDataset), "_INFO), key='", info_key, "'),",
             "'left_names=dataset_id,dataset_version',",
             "'right_names=dataset_id,dataset_version', 'left_outer=true')", 
             sep = "")
  
  qq2 = paste("equi_join(", 
              qq, ", ", 
              "project(
              apply(", custom_scan(), "(", full_arrayname(.ghEnv$meta$arrExperimentSet), "), experimentset_id_, experimentset_id), 
              experimentset_id_, name), ", 
              "'left_names=experimentset_id,dataset_version', ", 
              "'right_names=experimentset_id_,dataset_version')")
  
  xx = iquery(con$db, 
              qq2, 
              return = T, only_attributes = T)
  stopifnot(unique(xx$key) == info_key | is.na(unique(xx$key)))
  xx$key = NULL
  colnames(xx)[which(colnames(xx) == 'val')] = info_key
  xx
}


