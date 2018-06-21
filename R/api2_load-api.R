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
#####################################

# File structure is as follows
# - <load-api.R> is a collection of high-level functions called in a loader script
# - <load-helper.R, load-helper-filetypes.R> contain lower level functions for 
#                   interpreting data in worksheet or specific file-types 
#                   (e.g. Cufflinks RNASeq files, Gemine DNASeq files) before ingesting into SciDB
# - <template-helper.R> contains functions specifically for interpreting / parsing the Excel template sheet

#' @export
api_register_project_datasets = function(workbook_path = NULL, workbook = NULL, only_test = FALSE, con = NULL) {
  con = use_ghEnv_if_null(con)
  if (is.null(workbook_path) & is.null(workbook)) {
    stop("must supply path to workbook, or workbook opened by XLConnect::loadWorkbook")
  }
  if (is.null(workbook) & !is.null(workbook_path)){
    workbook = loadWorkbook(workbook_path)
  }
  df0 = myExcelReader(workbook, sheet_name = 'Studies')

  proj_study_ids_summ = data.frame(project_id = numeric(0),
                                   dataset_id = numeric(0),
                                   dataset_version = numeric(0))
  # Work on one project at a time
  for (proj_idx in df0$project_id) {
    cat("====Working on project idx", proj_idx, "of worksheet\n")
    dfi = df0[df0$project_id == proj_idx, ]

    # --------------------------    
    # PROJECT
    # Register the project first
    wksht_fields_proj = c('project_name', 'project_description')
    dfi_proj = dfi[, wksht_fields_proj]
    dfi_proj = dfi_proj[!duplicated(dfi_proj), ]
    
    if (nrow(dfi_proj) != 1) {
      stop("Expected one row project dataframe here. 
           Project name and description should be consistent across rows of worksheet Studies sheet")
    }
    
    dfi_proj = load_helper_column_rename(dfx = dfi_proj,
                                 scidb4gh_fields = mandatory_fields()[[.ghEnv$meta$arrProject]],
                                 worksheet_fields = wksht_fields_proj
                                )
    
    project_id = register_project(df = dfi_proj, 
                                  only_test = only_test, 
                                  con = con)
    rm(dfi_proj)
    
    # IMPORTANT: remove the columns that will no longer be needed
    dfi[, wksht_fields_proj] = NULL
    
    # --------------------------    
    # DATASET  
    dfi$project_id = project_id
    scidb4gh_fields = mandatory_fields()[[.ghEnv$meta$arrDataset]]
    wksht_fields_study = c('study_name', 'study_description', 'project_id', 'is_study_public')
    stopifnot(all(c(wksht_fields_study, 'study_version') %in% colnames(df0)))
    
    dfi_st = load_helper_column_rename(dfx = dfi, 
                               scidb4gh_fields = mandatory_fields()[[.ghEnv$meta$arrDataset]], 
                               worksheet_fields = wksht_fields_study)
    
    # if (!(length(unique(dfi_st$name)) == 1 &
    #       length(unique(dfi_st$description)) == 1)) {
    #   stop("Currently loader handles one study per project.
    #        Need to add code for handling multiple studies per project (using field 'study_id'")
    # }
    # IMPORTANT: remove the columns that will no longer be needed
    dfi_st$study_id = NULL
    
    if (length(unique(dfi_st$public)) != 1) {
      stop("Study versions should either be public or not")
    } else {
      # df1$public = NULL
    }
    
    dataset_version = unique(dfi_st$study_version)
    if (length(dataset_version) != 1) {
      stop("must write code to ingest multiple study_version-s")
    }
    # IMPORTANT: remove the columns that will no longer be needed
    dfi_st$study_version = NULL
    dataset_record = register_dataset(df = dfi_st, public = unique(dfi_st$public), 
                                      dataset_version = dataset_version,
                                      only_test = only_test, 
                                      con = con)
    
    proj_study_ids = dataset_record
    proj_study_ids$project_id = project_id
    proj_study_ids
    proj_study_ids = proj_study_ids[, 
                                    c('project_id', 'dataset_id', 'dataset_version')]
    
    proj_study_ids_summ = rbind(proj_study_ids_summ, proj_study_ids)
    cat("----\n")
  } # <end of> for (proj_idx in df0$project_id)

  proj_study_ids_summ
}

#' register definitions for dataset
#' 
#' 
api_register_definitions = function(df_definitions, record, con = NULL) {
  stopifnot(nrow(record) == 1)
  
  df_definitions$dataset_id = record$dataset_id
  definition_record = register_definitions(df = as.data.frame(df_definitions), 
                                           con = con)
}

#' register individuals
#' 
#' @param workbook workbook object returned by XLConnect:loadWorbook
#' @param record record of scidb project_id, dataset_id and dataset_version at which to register individuals
#' @export
api_register_individuals = function(workbook, record, def, con = NULL) {
  stopifnot(nrow(record) == 1)
  
  data_df = load_helper_prepare_dataframe(workbook = workbook,
                                         record = record, 
                                         def = def, 
                                         sheetName = 'Subjects',
                                         entityName = .ghEnv$meta$arrIndividuals,
                                         worksheet_fields = 
                                           c('dataset_id', 'subject_id', 'description', 
                                             'species_', 'SEX'),
                                         con = con)
  
  
  data_df_record = register_individual(df = data_df, dataset_version = record$dataset_version, 
                                     con = con)

}

#' Samples / Biosample
#' @export
api_register_biosamples = function(workbook, record, def, con = NULL) {
  stopifnot(nrow(record) == 1)
  
  data_df = load_helper_prepare_dataframe(workbook = workbook,
                                         record = record, 
                                         def = def, 
                                         sheetName = 'Samples',
                                         entityName = .ghEnv$meta$arrBiosample,
                                         worksheet_fields = 
                                           c('dataset_id', 'sample_name', 
                                             'description', 'disease_', 'individual_id'),
                                         con = con)
  
  data_df_record = register_biosample(df = data_df, 
                                      dataset_version = record$dataset_version, 
                                      con = con)
}

#' Register FeatureSets, ExperimentSets and MeasurementSets
#' 
#' This function differs from api_register_indiv/bios/measurements
#' that work row by row on Subjects/Samples/Pipelines sheets respectively.
#' This function needs to find unique set of Experiments from the unique value of `concat` column
#' in Pipelines sheet, and then uses info in `pipeline_choices` sheet to fill up the 
#' other necessary information
#' @export
api_register_featuresets_experimentsets_measurementsets = function(workbook, record, def, con = NULL) {
  stopifnot(nrow(record) == 1)
  
  # Create choices objects from metadata sheet
  choicesObj = list(
    pipelineChoicesObj = PipelineChoices$new(
      pipeline_choices_df = myExcelReader(workbook = workbook, 
                                          sheet_name = template_linker$pipeline$choices_sheet)),
    filterChoicesObj = FilterChoices$new(
      filter_choices_df = myExcelReader(workbook = workbook, 
                                        sheet_name = template_linker$filter$choices_sheet)),
    featuresetChoicesObj = FeaturesetChoices$new(
      featureset_choices_df = myExcelReader(workbook = workbook, 
                                            sheet_name = template_linker$featureset$choices_sheet))
  )
  
  pipelines_df = template_helper_extract_record_related_rows(workbook = workbook,
                                                             sheetName = 'Pipelines', 
                                                             record = record)

  ######################################  
  # EXTRACT ALL RELEVANT INFORMATION
  msmtset_df = template_helper_extract_pipeline_meta_info(pipelines_df = pipelines_df, 
                                                          choicesObj = choicesObj,
                                                          record = record)
  if (nrow(msmtset_df) == 0) {
    cat("No ExperimentSets that match pipeline_choices description\n")
    return(NULL)
  }

  # ====================================
  # some parsing on the data
  # Extract relevant definitions 
  defi = rbind(template_helper_extract_definitions(sheetName = 'pipeline_choices', def = def), 
               template_helper_extract_definitions(sheetName = 'filter_choices', def = def),
               template_helper_extract_definitions(sheetName = 'featureset_choices', def = def))
  defi = defi[!(defi$attribute_name %in% c('filter_id')), ]
  
  # Enforce that columns in data are defined in Definitions sheet
  cat("Suppressing this check as unique case here\n")
  try({template_helper_enforce_columns_defined(data_df = msmtset_df, 
                                               definitions = defi)})
  
  
  # Enforce that mandatory columns listed in Definitions sheet are present in data
  cat("Suppressing this check as unique case here\n")
  try({template_helper_enforce_mandatory_columns_present(data_df = msmtset_df,
                                                    definitions = defi)})
  
  
  ######################################  
  # EXPERIMENTSET
  # Formulate ExperimentSet
  columns_experimentSet = c('dataset_id', 
                            'measurement_entity',
                            'data_subtype')
  expset_df = unique(msmtset_df[, columns_experimentSet])
  expset_df$name = expset_df$data_subtype
  expset_df$description = paste0(expset_df$name, " experiments")
  expset_df$molecule = '...'
  
  expset_record = register_experimentset(df = expset_df, dataset_version = record$dataset_version, 
                                         con = con)
  
  ######################################  
  # FEATURESET
  # Formulate FeatureSet
  columns_featureSet = c('featureset_name', 
                         'featureset_scidb',
                         'featureset_source', 
                         'featureset_source_version',
                         'featureset_species')
  ftrset_df = unique(msmtset_df[, columns_featureSet])
  
  new_colnames_featureset = c('featureset_name', 'name', 'source', 'source_version', 'species')
  names(new_colnames_featureset) = columns_featureSet
  ftrset_df = plyr::rename(ftrset_df, new_colnames_featureset)
  ftrset_df$description = "..."
  ftrset_df$source_uri = "..."
  
  refSets = get_referenceset()
  if (nrow(refSets) > 2) {
    stop("In following piece of code referenceSet linking logic is implemented assuming only two referenceSets: GRCh37 and GRCh38")
  }
  refset37_id = refSets[grep("37", refSets$name), ]$referenceset_id
  refset38_id = refSets[grep("38", refSets$name), ]$referenceset_id
  stopifnot(length(refset37_id) == 1 &
              length(refset38_id) == 1)
  
  ftrset_df$referenceset_id = -1
  if (length(grep("37", ftrset_df$source)) > 0) {
    ftrset_df[grep("37", ftrset_df$source), ]$referenceset_id = refset37_id
  }
  if (length(grep("38", ftrset_df$source)) > 0) {
    ftrset_df[grep("38", ftrset_df$source), ]$referenceset_id = refset38_id
  }
  cat("==== Registering FEATURESET ====\n")
  ftrset_record = register_featureset(df = ftrset_df, 
                          con = con)  
  
  # ====================================
  # MEASUREMENTSET
  
  # experimentset_id
  expset_df = get_experimentset(experimentset_id = expset_record$experimentset_id, 
                                dataset_version = record$dataset_version)
  msmtset_df = merge(msmtset_df, 
                     expset_df[, c(get_base_idname(.ghEnv$meta$arrExperimentSet),
                                   columns_experimentSet)],
                     by = columns_experimentSet)
  
  # featureset_id
  fsets = get_featuresets(con = con)
  matchL = find_matches_and_return_indices(msmtset_df$featureset_name, 
                                           fsets[, template_linker$featureset$choices_col])
  if (length(matchL$source_unmatched_idx) > 0){
    cat("Following pipelines do not have featuresets defined yet -- skipping them:\n")
    print(msmtset_df[matchL$source_unmatched_idx, c(1:5)])
    msmtset_df = msmtset_df[matchL$source_matched_idx, ]
    stop("Should not have occurred")
  }
  msmtset_df$featureset_id = fsets$featureset_id[matchL$target_matched_idx]
  
  # Rename columns from external custom fields to scidb4gh fields
  msmtset_df = plyr::rename(msmtset_df, 
                            c('measurement_entity' = 'entity'))
  msmtset_df$name = paste0(msmtset_df$pipeline_source_title, ": ", msmtset_df$filter_name)
  msmtset_df = drop_na_columns(msmtset_df)
  
  # description
  if ('pipeline_source_title' %in% colnames(msmtset_df)) {
    msmtset_df$description = msmtset_df$pipeline_source_title
  } else {
    msmtset_df$description = '...'
  }
  
  msmtset_record = register_measurementset(df = msmtset_df, 
                                              dataset_version = record$dataset_version, 
                                              con = con)
  return(list(ExperimentSetRecord = expset_record,
              MeasurementSetRecord = msmtset_record))
}

#' automatically register ontology terms
#' 
#' gather the terms from controlled_vocabulary column, 
#' categorize by attribute_name, and
#' assign them ontology_id-s
#' @export
api_register_ontology_from_definition_sheet = function(workbook = NULL, 
                                                       def = NULL,
                                                       con = NULL) {
  con = use_ghEnv_if_null(con=con)
  
  if (is.null(workbook) & is.null(def)) stop("must supply at least master workbook
                                       or definitions worksheet")
  
  if (is.null(def)) def = myExcelReader(workbook = workbook, 
                                        sheet_name = 'Definitions')
  
  defx = def[!is.na(def$controlled_vocabulary), ]
  defx = defx[, c('attribute_name', 'controlled_vocabulary')]
  
  L1 = lapply(1:nrow(defx),
        FUN = function(idx) {
                  term_list = defx$controlled_vocabulary[idx]
                  vec = trimws(
                    unlist(stringi::stri_split(str = term_list, 
                                               regex = "//")), 
                         which = 'both')
                  data.frame(
                    category = defx$attribute_name[idx],
                    term = c(vec, "NA"), stringsAsFactors = FALSE)
  })
  
  ont_df = do.call(what = "rbind", 
                   args = L1)
  
  ont_df$source_name = "..."
  ont_df$source_version = "..."
  
  ont_NA = data.frame(term = "NA", 
                      category = 'uncategorized',
                      source_name = "...", source_version = "...", 
                      stringsAsFactors = FALSE)
  ont_df = rbind(ont_df, ont_NA)
  register_ontology_term(df = ont_df, con = con)
}

