#
# BEGIN_COPYRIGHT
#
# PARADIGM4 INC.
# This file is part of the Paradigm4 Enterprise SciDB distribution kit
# and may only be used with a valid Paradigm4 contract and in accord
# with the terms and conditions specified by that contract.
#
# Copyright (C) 2011 - 2018 Paradigm4 Inc.
# All Rights Reserved.
#
# END_COPYRIGHT
#

# HELPER FUNCTIONS functions specifically for interpreting / parsing the Excel template sheet

# note usage of the useCachedValues parameter
# otherwise, was throwing warnings about the concatenation field
myExcelReader = function(workbook, sheet_name) {
  readWorksheet(object = workbook, 
                sheet = sheet_name, 
                check.names = FALSE, 
                useCachedValues = TRUE)
}

#' Enforce that columns in any data sheet have definitions in Definitions sheet
#' 
template_helper_enforce_columns_defined = function(data_df, definitions) {
  attrs_defi = sort(definitions$attribute_name)
  attrs_data = sort(colnames(data_df))
  unmatched1 = attrs_defi[!(attrs_defi %in% attrs_data)]
  unmatched2 = attrs_data[!(attrs_data %in% attrs_defi)]
  if (length(unmatched1) !=0 | length(unmatched2) != 0) {
    cat("Following items exist in definitions sheet, but not in data sheet:\n\t")
    cat(pretty_print(unmatched1))
    cat("\n\nFollowing items exist in data sheet, but not in definitions sheet:\n\t")
    cat(pretty_print(unmatched2))
    cat("\n")
    stop("Above columns in data sheet not defined in Definitions seet")
  }
}


#' extract definitions corresponding to a specific sheet
#' 
#' Find the definitions for a specific sheet (e.g. Subjects, Samples)
#' by consulting the relevant column (e.g. attribute_in_Subjects, attribute_in_Samples)
template_helper_extract_definitions = function(sheetName, def) {
  col_for_pick = paste0("attribute_in_", sheetName)
  defi = def[def[, col_for_pick], ]
  cat("From", nrow(def), "rows of definitions sheet, picked out", nrow(defi), "rows for current entity\n")
  defi
}

#' Enforce that mandatory columns listed in Definitions sheet are present in data
template_helper_enforce_mandatory_columns_present = function(data_df, definitions) {
  mandatory_columns = definitions[definitions$importance == 1, ]$attribute_name
  stopifnot(mandatory_columns %in% colnames(data_df))
}

#' Extract pipeline meta information 
#' 
#' Given rows from Pipelines sheet, extract the unique values in `pipeline_information`
#' column, and find the matching rows in `pipeline-choices` sheeet
#' 
#' @param pipelines_df data-frame containing rows in Pipelines sheet corresponding to a 
#'        `[project_id, study_id, study_version]` record
template_helper_extract_pipeline_meta_info = function(pipelines_df, workbook) {
  # note usage of the useCachedValues parameter
  # otherwise, was throwing warnings about the concatenation field
  pipmeta = readWorksheet(object = workbook, sheet = 'pipeline-choices', 
                          check.names = FALSE, 
                          useCachedValues = TRUE)
  
  pipeline_selector_col = "pipeline_information (Data-type / Sub-type / Pipeline-name)"
  pipeline_list = unique(pipelines_df[, pipeline_selector_col])
  
  matchL = find_matches_and_return_indices(pipeline_list, pipmeta$concat)
  if (length(matchL$source_unmatched_idx) > 0) {
    cat("Following pipeline entries do not have matches in `pipeline-choices` sheet:\n\t", 
        pretty_print(unmatched), "\n")
  }
  pipeline_meta_df = pipmeta[matchL$target_matched_idx, ]
  # pipeline_meta_df = drop_na_columns(pipeline_meta_df)
  pipeline_meta_df$measurement_entity = toupper(pipeline_meta_df$measurement_entity)
  
  #############
  # Merge in featureset information
  # Create dataframe containing pipeline and featureset
  xx = pipelines_df[, c(pipeline_selector_col, 'featureset_name')]
  pip_fset_uniq_df = xx[!duplicated(xx), ]
  # Confirm that each pipeline has only one featureset assigned to it
  stopifnot(nrow(pip_fset_uniq_df) == length(pipeline_list))
  # Merge 
  pipeline_meta_df = merge(pipeline_meta_df, pip_fset_uniq_df, 
                           by.x = 'concat',
                           by.y = pipeline_selector_col)
  
  pipeline_meta_df
}

#' extract information pertaining to a project-study record
#' 
#' Helper function for template Excel sheet. 
#' 
#' Given a project-study record [project_id, dataset_id, dataset_version]
#' find the information from a target sheet (e.g. Subjects, Samples, Pipelines) pertaining
#' to that record
template_helper_extract_record_related_rows = function(workbook, sheetName, record) {
  masterSheet = 'Studies'
  stopifnot(nrow(record) == 1)
  
  project_id = record$project_id
  dataset_id = record$dataset_id
  dataset_version = record$dataset_version
  
  dataset = get_datasets(dataset_id = dataset_id, dataset_version = dataset_version, 
                         all_versions = FALSE)
  
  stopifnot(nrow(dataset) == 1)
  stopifnot(project_id == dataset$project_id)
  
  data0 = readWorksheet(workbook, sheet = sheetName,
                        check.names = FALSE,
                        useCachedValues = TRUE)
  data0 = data0[!duplicated(data0), ]
  study = readWorksheet(workbook, sheet = masterSheet,
                        check.names = FALSE,
                        useCachedValues = TRUE)
  
  study_loc = study[study[, 'study_name'] == dataset$name &
                      study[, 'study_version'] == dataset_version, ]
  study_id_loc = study_loc$study_id
  proj_id_loc = study_loc$project_id
  
  data = data0[data0$study_id == study_id_loc &
                 data0$project_id == proj_id_loc &
                 data0$study_version == dataset_version, ]
  cat("From", nrow(data0), tolower(sheetName), "-- working on", nrow(data), tolower(sheetName), 
      "belonging to \n\t project_id:", proj_id_loc, "(local), \n\t study_id:", study_id_loc, "(local id), ",
      dataset_id, "(scidb) \n\t at version", dataset_version, "\n")
  
  data
}

