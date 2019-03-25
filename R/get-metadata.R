#
# BEGIN_COPYRIGHT
#
# PARADIGM4 INC.
# This file is part of the Paradigm4 Enterprise SciDB distribution kit
# and may only be used with a valid Paradigm4 contract and in accord
# with the terms and conditions specified by that contract.
#
# Copyright (C) 2011 - 2017 Paradigm4 Inc.
# All Rights Reserved.
#
# END_COPYRIGHT
#

#' @export
get_measurementsets = function(measurementset_id = NULL, dataset_version = NULL, all_versions = FALSE, mandatory_fields_only = FALSE, con = NULL){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrMeasurementSet,
                                       id = measurementset_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions,
                                       mandatory_fields_only = mandatory_fields_only,
                                       con = con)
}

#' @export
get_biosamples = function(biosample_id = NULL, dataset_version = NULL, all_versions = FALSE, mandatory_fields_only = FALSE, con = NULL){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrBiosample,
                                       id = biosample_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions, 
                                       mandatory_fields_only = mandatory_fields_only,
                                       con = con)
}

#' @export
get_experimentset = function(experimentset_id = NULL, dataset_version = NULL, all_versions = FALSE, mandatory_fields_only = FALSE, con = NULL){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrExperimentSet, 
                                       id = experimentset_id, 
                                       dataset_version, all_versions, 
                                       mandatory_fields_only = mandatory_fields_only, 
                                       con = con)
}

#' @export
get_datasets = function(dataset_id = NULL, dataset_version = NULL, 
                        all_versions = TRUE, mandatory_fields_only = FALSE, 
                        merge_project_info = FALSE, 
                        con = NULL){
  if (!merge_project_info) {
    get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrDataset,
                                         id = dataset_id, 
                                         dataset_version = dataset_version, 
                                         all_versions = all_versions, 
                                         mandatory_fields_only = mandatory_fields_only, 
                                         con = con)
  } else { # merge project info
    con = use_ghEnv_if_null(con = con)
    if (is.null(dataset_id)) {
      left_arr = paste0(
        custom_scan(), "(", full_arrayname(.ghEnv$meta$arrDataset), ")")
    } else {
      left_arr = form_selector_query_secure_array(arrayname = full_arrayname(.ghEnv$meta$arrDataset),
                                                  selected_ids = dataset_id,
                                                  dataset_version = dataset_version)
    }
    query = paste0(
      "equi_join(", 
      left_arr, ", ", 
      full_arrayname(.ghEnv$meta$arrProject), ", ", 
      "'left_names=project_id', 'right_names=project_id', 'keep_dimensions=TRUE')"
    )
    query = paste0(
      "equi_join(", 
      query, 
      ", ",
      gsub(.ghEnv$meta$arrDataset, 
           paste0(.ghEnv$meta$arrDataset, "_INFO"),
           left_arr),
      ", 'left_names=dataset_id,dataset_version',",
      "'right_names=dataset_id,dataset_version', ",
      "'left_outer=true')"
    )
    res = iquery(con$db, query, return=T)
    # equi_join introduces columns called '_1' for commonly named columns of second array (PROJECT here)
    colnames(res)[grep("_1$", colnames(res))] = 
      gsub("_1", "", 
           paste0("project_", colnames(res)[grep("_1$", colnames(res))]))
    drop_equi_join_dims(df1 = res)
  }
}

#' Retrieve individuals
#' 
#' get_ENTITY can be used to retrive
#' - one individual (e.g. `individual_id = 33`)
#' - more than one individual (e.g. `individual_id = c(33, 44)`)
#' - all individuals visible to user (e.g. `get_individuals()`)
#' @export
get_individuals = function(individual_id = NULL, dataset_version = NULL, all_versions = FALSE, mandatory_fields_only = FALSE, con = NULL){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrIndividuals,
                                       id = individual_id, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions, 
                                       mandatory_fields_only = mandatory_fields_only,
                                       con = con)
}
