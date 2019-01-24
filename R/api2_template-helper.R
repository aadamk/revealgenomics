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

#' Storing columns that link different sheets
#' 
#' what columns link FeatureSets between featureset_choices sheet and Pipelines sheet
#' `api_choices_col` is same as `choices_col` if not specified
template_linker = list(
    featureset = list(
      choices_sheet      = 'featureset_choices',
      choices_col = 'featureset_name',
      api_choices_col    = 'featureset_scidb',
      pipelines_sel_col  = 'featureset_choice'
    ),
    filter = list(
      choices_sheet      = 'filter_choices',
      choices_col        = 'filter_name',
      pipelines_sel_col  = 'filter_choice'
    ),
    pipeline = list(
      choices_sheet      = 'pipeline_choices',
      choices_col        = 'pipeline_scidb',
      pipelines_sel_col  = 'pipeline_choice'
    )
  )

#' Load Excel workbook into memory
#' 
#' To be used in conjunction with `myExcelReader()`
#' `
#' @param filename path to Excel workbook
#' @param use_readxl use readxl package (default), otherwise XLConnect package
#' @param silently spew log messages while loading workbook (logging turned on by default)
#' 
#' @export
myExcelLoader = function(filename, use_readxl=TRUE, silently=FALSE) {
  tryCatch({
    if (use_readxl) {
      readxl::excel_sheets(path = filename)
    } else {
      workbook = XLConnect::loadWorkbook(filename = filename)
    }
  },
  error = function(e) {
    cat("Could not open file at file path:", filename)
    return(NULL)
  })
  
  required_sheets = c("Definitions",           "Studies",               "Subjects",             
                      "Samples",               "Pipelines",             "Contrasts",            
                      "pipeline_choices",      "featureset_choices",    "filter_choices" )
  
  if (use_readxl) {
    sheets_in_file = readxl::excel_sheets(path = filename)
  } else {
    sheets_in_file = XLConnect::getSheets(workbook)
  }
  if (! all(required_sheets %in% sheets_in_file) ) {
    stop("Following required sheet(s) not present: ", 
         paste0(required_sheets[!(required_sheets %in% sheets_in_file)],
                collapse = ", "))
  }
  
  wb = list()
  for (sheet_nm in required_sheets) {
    if (!silently) cat("Reading sheet: ", sheet_nm, "\n")
    if (use_readxl) {
      wb[[sheet_nm]] = as.data.frame(readxl::read_excel(path = filename, ## file name + location
                                          sheet = sheet_nm, ## Which sheet name to parse 
                                          trim_ws = TRUE, ## Trim trailing or leading whitespaces?
                                          guess_max = 21474836))  # guess_max default at 1000 throws read errors at CASTOR / POLLUX sheet
                                                                 # leaving at Inf throws warning messages.
                                                                 # warning messages prompted the value used here.
    } else {
      wb[[sheet_nm]] = XLConnect::readWorksheet(object = workbook, sheet = sheet_nm, 
                                                check.names = FALSE, 
                                                useCachedValues = TRUE)
    }
    # cleanup string literals 
    # 
    # convert columns in a dataframe from string vector of 'TRUE'/'FALSE' to logicals
    convert_string_columns_to_logicals = function(dfx, colnms) {
      # custom converter from TRUE or FALSE strings to logical
      # 
      # param vec character vector containing 'TRUE' or 'FALSE'
      as.logical_custom = function(vec) {
        cat("Converting string TRUE or FALSE to logical\n")
        res = sapply(vec, function(val) {
          switch (val,
                  'TRUE' = TRUE,
                  'FALSE' = FALSE,
                  stop("value must be 'TRUE' or 'FALSE'")
          )}
        )
        names(res) = NULL
        res
      }
      
      if (!all(sapply(dfx, class)[colnms] == 'logical')) {
        colnms_x = colnms[which(sapply(dfx, class)[colnms] != 'logical')]
        for (colnm in colnms_x) {
          cat("column:", colnm, "\n\t")
          dfx[, colnm] = as.logical_custom(dfx[, colnm])
        }
      }
      dfx
    }
    if (sheet_nm == 'Definitions') {
      wb[[sheet_nm]] = convert_string_columns_to_logicals(dfx = wb[[sheet_nm]],
                                  colnms = grep("attribute_in_", colnames(wb[[sheet_nm]]), value = TRUE)
                                                          )
    } else if (sheet_nm == 'Studies') {
      wb[[sheet_nm]] = convert_string_columns_to_logicals(dfx = wb[[sheet_nm]],
                                                          colnms = 'is_study_public')
    }
  }
  wb
}

#' Read sheet from Excel workbook
#' 
#' To be used in conjunction with `myExcelLoader()`
#' 
#' Note usage of the useCachedValues parameter
#' otherwise, was throwing warnings about the concatenation field
#' 
#' @param workbook Workbook must be of type `XLConnect::workbook` (loaded by `XLConnect::loadWorkbook()`) 
#'                 or of type `list` (loaded by `revealgenomics::myExcelLoader()`)
#' @param sheet_name name of sheet to read from workbook
myExcelReader = function(workbook, sheet_name) {
  if (class(workbook) == 'workbook') {
    readWorksheet(object = workbook, 
                  sheet = sheet_name, 
                  check.names = FALSE, 
                  useCachedValues = TRUE)
  } else if (class(workbook) == 'list') {
    if (sheet_name %in% names(workbook)) {
      workbook[[sheet_name]]
    } else {
      stop("No sheet by name: ", sheet_name, " in workbook. \n\nAvailable sheets: ",
           paste0(names(workbook), collapse = ", "))
    }
  } else {
    stop("Workbook must be of type `XLConnect::workbook` (loaded by `XLConnect::loadWorkbook()`)\n 
         or of type `list` (loaded by `revealgenomics::myExcelLoader()`)")
  }
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
  defi = def[def[, col_for_pick] & 
               !is.na(def[, col_for_pick]), ]
  if (getOption("revealgenomics.debug", FALSE)) {
    cat("From", nrow(def), "rows of definitions sheet, picked out", nrow(defi), "rows for current entity\n")
  }
  defi
}

#' Enforce that mandatory columns listed in Definitions sheet are present in data
template_helper_enforce_mandatory_columns_present = function(data_df, definitions) {
  mandatory_columns = definitions[definitions$importance == 1, ]$attribute_name
  if (!all(mandatory_columns %in% colnames(data_df))) {
    stop("Following attributes defined as mandatory in Definitions sheet, but not present in data:\n\t",
        pretty_print(mandatory_columns[!(mandatory_columns %in% colnames(data_df))]), "\n")
  }
}

#' Extract pipeline meta information 
#' 
#' Given rows from Pipelines sheet, extract the unique keys for pipeline, filter and featurset-s.
#' Then find the matching and relevant rows in `pipeline_choices`, `filter_choices` and 
#' `featureset_choices`
#' 
#' @param pipelines_df data-frame containing rows in Pipelines sheet corresponding to a 
#'                     `[project_id, study_id, study_version]` record
#' @param choicesObj list containing instantiated objects of PipelineChoices, FilterChoices
#'                   and FeaturesetChoices classes
template_helper_extract_pipeline_meta_info = function(pipelines_df, choicesObj, record) {
  selector_col_pipeline_choice = template_linker$pipeline$pipelines_sel_col
  selector_col_filter_choice   = template_linker$filter$pipelines_sel_col
  selector_col_featureset_choice = template_linker$featureset$pipelines_sel_col
  
  msmtset_selector = unique(
    pipelines_df[, c(selector_col_pipeline_choice,
                     selector_col_filter_choice,
                     selector_col_featureset_choice)])
  
  pipeline_df =
    drop_na_columns(
      do.call(what = 'rbind',
              args = lapply(msmtset_selector[, selector_col_pipeline_choice],
                            function(choice) {
                              choicesObj$pipelineChoicesObj$get_pipeline_metadata(keys = choice)
                            })
      )
    )
  
  filter_df =
    drop_na_columns(
      do.call(what = 'rbind',
              args = lapply(msmtset_selector[, selector_col_filter_choice],
                            function(choice) {
                              choicesObj$filterChoicesObj$get_filter_metadata(keys = choice)
                            })
      )
    )
  
  featureset_df =
    drop_na_columns(
      do.call(what = 'rbind',
              args = lapply(msmtset_selector[, selector_col_featureset_choice],
                            function(choice) {
                              choicesObj$featuresetChoicesObj$get_featureset_metadata(keys = choice)
                            })
      )
    )
  
  # drop some local information
  filter_df$filter_id = NULL 
  filter_df$measurement_entity = NULL 
  
  msmtset_df = cbind(pipeline_df,
                     filter_df,
                     featureset_df)
  msmtset_df$dataset_id = record$dataset_id
  msmtset_df$measurement_entity = 
    template_helper_assign_measurement_entity(pipeline_df = msmtset_df)
  
  msmtset_df
}

#' extract information pertaining to a project-study record
#' 
#' Helper function for template Excel sheet. 
#' 
#' Given a project-study record \code{project_id, dataset_id, dataset_version}
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
  
  # data0 = readWorksheet(workbook, sheet = sheetName,
  #                       check.names = FALSE,
  #                       useCachedValues = TRUE)
  data0 = myExcelReader(workbook = workbook, sheet_name = sheetName)
  data0 = data0[!duplicated(data0), ]
  # study = readWorksheet(workbook, sheet = masterSheet,
  #                       check.names = FALSE,
  #                       useCachedValues = TRUE)
  study = myExcelReader(workbook = workbook, sheet_name = masterSheet)
  
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

#' Convert external (human-readable names) to API internal names
#' 
#' external                   API
#' Gene Expression            RNAQUANTIFICATION
#' Variant                    VARIANT
#' Rearrangement              FUSION
template_helper_convert_names = function(api_name = NULL, external_name = NULL) {
  if (is.null(api_name) & is.null(external_name)) {
    stop("Must supply at least one parameter")
  } else if (!is.null(api_name) & !is.null(external_name)) {
    stop("Must supply only one parameter")
  }
  df1 = data.frame(
    api_names = c(.ghEnv$meta$arrRnaquantification,
                  .ghEnv$meta$arrVariant, 
                  .ghEnv$meta$arrFusion,
                  .ghEnv$meta$arrProteomics),
    external_name = c('Gene Expression',
                      'Variant',
                      'Rearrangement',
                      'Proteomics'),
    stringsAsFactors = FALSE
  )
  if (!is.null(api_name)) {
    m1 = find_matches_and_return_indices(api_name, df1$api_name)
    if (length(m1$source_unmatched_idx) != 0) {
      stop("Unexpected internal names provided for conversion: ",
           pretty_print(api_name[m1$source_unmatched_idx]))
    }
    df1$external_name[m1$target_matched_idx]
  } else if (!is.null(external_name)) {
    m1 = find_matches_and_return_indices(external_name, df1$external_name)
    if (length(m1$source_unmatched_idx) != 0) {
      stop("Unexpected external names provided for conversion: ",
           pretty_print(external_name[m1$source_unmatched_idx]))
    }
    df1$api_name[m1$target_matched_idx]
  }
}

#' Formulate a file path from Excel template
template_helper_formulate_file_path = function(pipeline_df, local_path = TRUE) {
  if (local_path) {
    if (! ('local_project_folder_prefix' %in% colnames(pipeline_df)) ) {
      pipeline_df$local_project_folder_prefix = NA
    }
    file.path(na_to_blank(pipeline_df$local_project_folder_prefix), 
              na_to_blank(pipeline_df$project_folder),
              na_to_blank(pipeline_df$project_subfolder),
              pipeline_df$filename)
  } else {
    if (! ('cloud_project_folder_prefix' %in% colnames(pipeline_df)) ) {
      cat("Warning: cloud folder prefix not provided\n")
      pipeline_df$cloud_project_folder_prefix = NA
    }
    file.path(na_to_blank(pipeline_df$cloud_project_folder_prefix), 
              na_to_blank(pipeline_df$project_folder),
              na_to_blank(pipeline_df$project_subfolder),
              pipeline_df$filename)  
    }
}

#' Assign measurement entity
#' 
#' Assign measurement entity based on pipeline and filter choice selection in Pipelines sheet of Excel sheet
template_helper_assign_measurement_entity = function(pipeline_df) {
  stopifnot(all(c('measurement_entity', 'filter_name') %in% colnames(pipeline_df)))
  pipeline_names = pipeline_df$measurement_entity
  filter_names   = pipeline_df$filter_name
  converted = rep(NA, length(pipeline_names))
  # Positions where one must only consdier pipeline to choose scidb entity
  condn_match = (pipeline_names != "Copy Number Variation") & 
    (!grepl("file link", filter_names))
  pos_pipeline_only = which(condn_match)
  # Positions where one must consider both pipeline and filter
  pos_pipeline_filter = which(!condn_match)
  
  # Now handle accordingly
  if (length(pos_pipeline_only) > 0) {
    converted[pos_pipeline_only] = 
      template_helper_convert_names(external_name = pipeline_names[pos_pipeline_only])
  }
  if (length(pos_pipeline_filter) > 0) {
    converted[pos_pipeline_filter] = sapply(
      pos_pipeline_filter, 
      function(idx) {
        pipeline = pipeline_df$pipeline_scidb[idx]
        filter = pipeline_df$filter_name[idx]
        if (length(grep("file link", filter)) > 0) {
          result = .ghEnv$meta$arrMeasurement
        } else if (grepl("FMI|Personalis", pipeline)) {
          result = .ghEnv$meta$arrCopynumber_variant
        } else if (length(grep("log2", filter)) > 0) {
          result = .ghEnv$meta$arrCopynumber_mat
        } else if (length(grep("state call", filter)) > 0) {
          result = .ghEnv$meta$arrCopynumber_mat_string
        } else {
          stop("Case not covered -- pipeline: ", pipeline, " filter: ", filter)
        }
        return(result)
      }
    )   
  }
  return(converted)
}

mappings_experimentset = 
  c( 'Single Nucleotide Variant' = 'VARIANT',
     'RNA-seq'                   = 'RNAQUANTIFICATION_RNASEQ', 
     'Microarray'                = 'RNAQUANTIFICATION_MICROARRAY',
     'Fusion'                    = 'FUSION',
     'Exome CNV'                 = 'COPYNUMBERVARIANT_EXOME', 
     'Whole Genome CNV'          = 'COPYNUMBERVARIANT_WHOLE_GENOME',
     'Targeted Region CNV'       = 'COPYNUMBERVARIANT_TARGETED_REGION')

#' Assign experiment entity
#' 
#' @param experiment_name from \code{data_subtype} column from Excel template
template_helper_assign_experiment_entity = function(experiment_name) {
  
  mapped = mappings_experimentset[experiment_name]
  stopifnot(all(!is.na(mapped)))
  mapped
}
#' Convert entity to suffix
#' 
#' === Entity-name        Suffix ===
#' RNAQUANTIFICATION      RNA
#' VARIANT                DNA
#' FUSION                 DNA / RNA (cannot provide suffix)
#' COPYNUMBER_MAT         DNA / RNA (cannot provide suffix)
#' PROTEOMICS             Protein
template_helper_suffix_by_entity = function(entity) {
  names_suffixes = c(.ghEnv$meta$arrRnaquantification, 
                     .ghEnv$meta$arrProteomics,
                     .ghEnv$meta$arrVariant
  )
  suffixes = c('RNA', 'Protein', 'DNA')
               # rep('DNA', length(names_suffixes)-2))
  names(suffixes) = names_suffixes
  if (!all(entity %in% names(suffixes))) {
    stop("Assigning suffix 'DNA', 'RNA', 'Protein' based on entity type.
                   Suffix needs to be assigned for entity:\n\t", 
         entity[which(!(entity %in% names(suffixes)))])
  }
  return(suffixes[entity])
}

