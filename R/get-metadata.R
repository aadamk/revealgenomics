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
get_measurementsets = function(measurementset_arr_index = NULL, dataset_version = NULL, all_versions = FALSE, con = NULL){
  get_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrMeasurementSet,
                                       id = measurementset_arr_index, 
                                       dataset_version = dataset_version, 
                                       all_versions = all_versions,
                                       mandatory_fields_only = TRUE,
                                       con = con)
}

