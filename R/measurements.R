#' register all measurements for a specific dataset
#' 
#' Work in progress
#' Once finished, this function should eventually be used instead of the script <load-experiment.R>
#' The function would be called after loading the data for each dataset
#' It compiles information for all measerement entities, and inserts the relevant
#' information into the MEASUREMENT entity (at this point, this is called the EXPERIMENT entity,
#' but the name should be changed. A MEASUREMENT combines both the Experiment and the 
#' Pipeline information) 
register_measurements = function(dataset_id, dataset_version, con = NULL) {
  con = use_ghEnv_if_null(con)
  
  df_info = get_entity_info()
  df_info_msrmt = df_info[df_info$class == 'measurementdata',]
  df_info_msrmt$entity = as.character(df_info_msrmt$entity)
  
  nmsp = scidb4gh:::find_namespace(id = dataset_id, entitynm = 'DATASET')
  cat("--Namespace: ", nmsp, "\n")
  for (idx in c(1:3,5)){
    msrmnt_entity = df_info_msrmt[idx, ]$entity
    stopifnot(scidb4gh:::is_entity_secured(msrmnt_entity) & 
                length(.ghEnv$meta$L$array[[msrmnt_entity]]$namespace) > 1)
    cat("Measurement entity: ", msrmnt_entity, "\n")
    
    msrmt_array = paste(nmsp, msrmnt_entity, sep = ".")
    msrmt_set_nm = df_info_msrmt[idx, ]$search_by_entity
    msrmt_set_idnm = scidb4gh:::get_base_idname(msrmt_set_nm)
    t1 = proc.time()
    res = iquery(con$db,
                 paste("aggregate(filter(", msrmt_array, 
                                  ", dataset_id = ", dataset_id, " AND dataset_version = ", dataset_version, ")", 
                       ", count(*), biosample_id, ", 
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
      
      msrmt_set_DF = scidb4gh:::get_entity(entity = msrmt_set_nm, 
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
      if (length(unique(res2$dataset_version)) != 1 |
          unique(res2$dataset_version) != 1) stop("This script has not been checked for multiple dataset versions at a time")
      # res2$dataset_version = NULL
      
      cat("======\n")
      cat("Registering", nrow(res2), "experiment-pipeline entries\n")
      for (dataset_idi in unique(res2$dataset_id)) {
        res2_sel1 = res2[res2$dataset_id == dataset_idi, ]
        for (dataset_version in unique(res2_sel1$dataset_version)) {
          res2_sel2 = res2_sel1[res2_sel1$dataset_version == dataset_version, ]
          res2_sel2$dataset_version = NULL
          cat("======------======\n")
          cat("Registering", nrow(res2_sel2), 
              "experiment-pipeline entries for dataset_id:", dataset_idi, "at version:", dataset_version, "\n")
          experiment_record = register_experiment(df = res2_sel2, 
                                                  dataset_version = dataset_version)
        }
      }
    }
  }
}

#' @export
get_measurement = function(measurement_id = NULL, dataset_version = NULL, 
                           all_versions = TRUE, mandatory_fields_only = FALSE, con = NULL){
  msrmt = get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrMeasurement, 
                                               id = measurement_id, 
                                               dataset_version, all_versions, 
                                               mandatory_fields_only = mandatory_fields_only, 
                                               con = con)
  
  # Merge with datasets info to join in study category
  d = get_datasets(con = con)
  if (any(is.na(d$`study category`))) {
    d[which(is.na(d$`study category`)), ]$`study category` = "unknown"
  }
  msrmt2 = merge(msrmt, 
                 d[, c('dataset_id', 'dataset_version', 'study category')], 
                 by = c('dataset_id', 'dataset_version'))
  if (nrow(msrmt2) != nrow(msrmt)) stop("Some measurements did not belong to specific dataset_id-s")
  msrmt2
}

get_experiment_v1 = function(mandatory_fields_only = FALSE) {
  msrmt = get_measurement(mandatory_fields_only = mandatory_fields_only)
  
  zz = msrmt[, c('dataset_id', 'dataset_version', 'experimentset_id', 'measurement_entity', 'biosample_id', 'study category')]
  head(zz)
  zz = zz[!duplicated(zz), ]
  
  # Merge with ExperimentSet info to join in experiment sub-type
  expsets = get_experimentset(mandatory_fields_only = mandatory_fields_only)
  experiments = merge(zz, 
                      expsets[, c('experimentset_id', 'name')], 
                      by = 'experimentset_id')
  
  experiments
}

#' retrieve all the experiments available to logged in user
#' 
#' joins Measurement, Dataset (for study category field), and ExperimentSet arrays
#' to return experiment information to user
#' 
#' @examples 
#' experiments = get_experiment()
#' cat("Categorization of experiments by major type\n")
#' table(experiments$measurement_entity)
#' cat("Categorization of experiments by sub type\n")
#' table(paste(experiments$measurement_entity, experiments$name, sep = ": "))
#' cat("Categorization of experiments by sub type\n")
#' table(experiments$`study category`)
#' 
#' @export
get_experiment = function(con = NULL) {
  con = use_ghEnv_if_null(con)
  
  info_key = 'study category'
  con = use_ghEnv_if_null(con)
  
  qq = paste("equi_join(", 
             "grouped_aggregate(secure_scan(", full_arrayname(.ghEnv$meta$arrMeasurement), "), ", 
             "count(*),", 
             "dataset_id, dataset_version, experimentset_id, measurement_entity, biosample_id) as X, ", 
             "filter(secure_scan(", full_arrayname(.ghEnv$meta$arrDataset), "_INFO), key='", info_key, "'),",
             "'left_names=dataset_id,dataset_version',",
             "'right_names=dataset_id,dataset_version')", 
             sep = "")
  
  qq2 = paste("equi_join(", 
              qq, ", ", 
              "project(
              apply(secure_scan(", full_arrayname(.ghEnv$meta$arrExperimentSet), "), experimentset_id_, experimentset_id), 
              experimentset_id_, name), ", 
              "'left_names=experimentset_id,dataset_version', ", 
              "'right_names=experimentset_id_,dataset_version')")
  
  xx = iquery(con$db, 
              qq2, 
              return = T)
  stopifnot(unique(xx$key) == info_key)
  xx$key = NULL
  colnames(xx)[which(colnames(xx) == 'val')] = info_key
  xx
}


