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

#' register entities via Excel sheet
#' 
#' Register entities using Excel sheet. 
#' Excel sheet has complete data about metadata (`PROJECT`, `DATASET`, `INDIVIDUAL`, ...) ,
#' and links to files storing the `MeasurementData` entities 
#' (e.g. `RNASeq`, `Variant` etc.)
#' 
#' @param study_worksheet path to Excel worksheet
#' @param ... refer parameters for function \code{\link{register_entities_workbook}}
#' @export
register_entities_excel = function(study_worksheet, 
                                   ...) {
  workbook = myExcelLoader(filename = study_worksheet)
  register_entities_workbook(workbook = workbook, ...)
}

#' register entities via list of R data-frames
#' 
#' Register entities using list of R data-frames
#' list of R data-frames has complete data about metadata (`PROJECT`, `DATASET`, `INDIVIDUAL`, ...) ,
#' and links to files storing the `MeasurementData` entities 
#' (e.g. `RNASeq`, `Variant` etc.)
#' 
#' @param register_upto_entity use this parameter to restrict load up to an user-specified entity.  
#'                         e.g. you can load up to `Ontology`, `Dataset` (and `Project`), 
#'                         `Definition` and not load the rest. The ordering is same
#'                         as in the parameter definition below.
#'                         Default value is `all` i.e. all entities are loaded
#' @param register_measurement_entity use this parameter to restrict which measurement-data types 
#'                                    should be loaded by the function. 
#'                                    Default value is `all` i.e. all measurement-types are loaded
#' @param pipeline_name_filter use this filter to restrict data-loading to pipelines with fully/partially
#'                             matched names. Default value is `NULL` (i.e. no checking by name done)
#' @export
register_entities_workbook = function(workbook, 
                                  register_upto_entity = c('all', 'ONTOLOGY', 'DATASET', 'DEFINITION',
                                                           'INDIVIDUAL', 'BIOSAMPLE', 'MEASUREMENTSET'),
                                  register_measurement_entity = c('all', 'RNAQUANTIFICATION', 'VARIANT',
                                                                  'FUSION', 'PROTEOMICS', 
                                                                  'COPYNUMBER_SEG', 'COPYNUMBER_MAT'),
                                  pipeline_name_filter = NULL,
                                  con = NULL) {
  register_upto_entity = match.arg(register_upto_entity)                                          
  register_measurement_entity = match.arg(register_measurement_entity)                                          
  if (!({zz = get_entity_info(); 
             register_measurement_entity %in% 
               c('all', zz[zz$class == "measurementdata", ]$entity)})) {
    stop("Invalid register_measurement_entity definition in function")
  }
  abort_condition_met = function(register_upto_entity, check_with_entity) {
    if (register_upto_entity == check_with_entity) {
      cat(paste0("... Aborting after loading up to entity: ", check_with_entity, "\n"))
      return(TRUE)
    } else {
      return(FALSE)
    }
  }
  def = myExcelReader(workbook, sheet_name = 'Definitions')
  
  # Should need to do this only once across studies 
  # (provided ontology list is stabilized)
  ontology_ids = api_register_ontology_from_definition_sheet(def = def, con = con)
  
  cat("#### Registering ONTOLOGY ####\n")
  register_ontology_term(df = data.frame(term = 'refer `sample_disease` column', 
                                         source_name= '...', 
                                         source_version = '...'), 
                                         con = con)
  if (abort_condition_met(register_upto_entity, check_with_entity = .ghEnv$meta$arrOntology)) {
    return(invisible(NULL))
  }
  
  cat("#### Registering PROJECT, STUDY ####\n")
  project_study_record = api_register_project_datasets(workbook = workbook, con = con)
  if (abort_condition_met(register_upto_entity, check_with_entity = .ghEnv$meta$arrDataset)) {
    return(invisible(NULL))
  }
  
  # loop through dataset records
  for (idx in 1:nrow(project_study_record)) {
    record = project_study_record[idx, ]
    # DEFINITION
    cat("#### Registering record ####", idx, "of", nrow(project_study_record), "\n")
    cat("#### Registering DEFINITION ####\n")
    definition_record = api_register_definitions(df_definitions = def, 
                                                 record = record,
                                                 con = con)
    if (abort_condition_met(register_upto_entity, check_with_entity = .ghEnv$meta$arrDefinition)) {
      next
    }
    
    # INDIVIDUAL
    cat("#### Registering INDIVIDUAL ####\n")
    indiv_rec = api_register_individuals(workbook = workbook, record = record, def = def)
    if (abort_condition_met(register_upto_entity, check_with_entity = .ghEnv$meta$arrIndividuals)) {
      next
    }
    
    # BIOSAMPLE
    cat("#### Registering BIOSAMPLE ####\n")
    bios_rec = api_register_biosamples(workbook = workbook, record = record, def = def)
    if (abort_condition_met(register_upto_entity, check_with_entity = .ghEnv$meta$arrBiosample)) {
      next
    }
    
    # EXPERIMENTSET and MEASUREMENTSET
    cat("#### Registering FEATURESET, EXPERIMENTSET and MEASUREMENTSET ####\n")
    expset_msmtset_rec = api_register_featuresets_experimentsets_measurementsets(workbook = workbook, 
                                                                     record = record, 
                                                                     def = def)
    if (abort_condition_met(register_upto_entity, check_with_entity = .ghEnv$meta$arrMeasurementSet)) {
      next
    }
    
    cat("#### Registering MEASUREMENT-DATA ####\n")
    # ==========================
    # TODO: The following can be wrapped up into a function called
    # api_register_measurementdata(workbook = workbook, ...)
    # PIPELINES correspond to rows pointing to Measurement data (e.g. RNASeq, Variant etc.)  
    pipelines_df = template_helper_extract_record_related_rows(workbook = workbook,
                                                               sheetName = 'Pipelines', 
                                                               record = record)
    pipelines_df$featureset_scidb = pipelines_df[, template_linker$featureset$pipelines_sel_col]
    pipelines_df = plyr::rename(pipelines_df, 
                                setNames(template_linker$featureset$choices_col,
                                         template_linker$featureset$pipelines_sel_col))
    
    bios_scidb = search_biosamples(dataset_id = record$dataset_id, dataset_version = record$dataset_version)
    reference_object = list(record = record,
                            biosample = bios_scidb,
                            featureset = get_featuresets(),
                            feature = NULL,
                            feature_synonym = get_feature_synonym())
    
    # loop through measurementsets in a dataset record
    for (msmtset_id_sel in expset_msmtset_rec$MeasurementSetRecord$measurementset_id) {
      cat("====================================================\n")
      cat("====================================================\n")
      reference_object$measurement_set = get_measurementsets(measurementset_id = msmtset_id_sel)
      if (register_measurement_entity != 'all' &
            register_measurement_entity != reference_object$measurement_set$entity) {
        cat("User chose to load entity of type:", register_measurement_entity, "only.\nSkipping measurementset_id",
            reference_object$measurement_set$measurementset_id, "of type:", reference_object$measurement_set$entity, "\n")
        next
      }
      if (!is.null(pipeline_name_filter)) {
        if (! (length(grep(pipeline_name_filter, 
                           reference_object$measurement_set$name,
                           ignore.case = TRUE)) > 0) ) {
          cat(paste0("User restricted loading to pipelines matching the string: '", pipeline_name_filter, 
                     "' only.\nSkipping current pipeline: ",
              reference_object$measurement_set$name, "\n"))
          next
        }
      }
      cat("Working on measurementset_id:", msmtset_id_sel,
          "(", reference_object$measurement_set$name,")\n")
      
      pip_sel = pipelines_df[((pipelines_df[, template_linker$pipeline$pipelines_sel_col] == 
                                 reference_object$measurement_set$pipeline_scidb) & 
                                (pipelines_df[, template_linker$filter$pipelines_sel_col] == 
                                   reference_object$measurement_set$filter_name)), ]
      na_to_blank = function(terms) { # macro to convert empty location in Excel file (read as NA) into "" (blank)
        ifelse(is.na(terms), "", terms)
      }
      pip_sel$file_path = file.path(na_to_blank(pip_sel$local_project_folder_prefix), 
                                    na_to_blank(pip_sel$project_folder),
                                    na_to_blank(pip_sel$project_subfolder),
                                    pip_sel$filename)
      # As different files are loaded,
      # local reference_object keeps track of current state of features in the database
      # This avoids downloading features for files that share a featureset
      
      # loop through unique files in a measurementsets
      for (file_path in unique(pip_sel$file_path)) {
        cat("----------------------------------------------\n")
        # file_path = unique(pip_sel$file_path)[1] # for testing
        reference_object$pipeline_df = pip_sel[pip_sel$file_path == file_path, ]
        readerObj = createDataReader(pipeline_df = reference_object$pipeline_df,
                                     measurement_set = reference_object$measurement_set)
        cat("Selected reader class:", class(readerObj), "\n")
        if (all(class(readerObj) =='NULL')) {
          cat("No reader for file:", file_path, "\n")
          next
        }
        
        errorStatus = tryCatch(expr = {
          readerObj$load_data_from_file()
        }, error = function(e) {
          print(e)
          return(e)
        })
        if ("error" %in% class(errorStatus)) {
          cat(paste0("Error loading file: ", file_path, ". Skipping\n"))
          next
        }
        loaderObj = createDataLoader(data_df = readerObj$get_data(), 
                                     reference_object = reference_object, 
                                     feature_annotation_df = readerObj$get_feature_annotation())
        cat("Selected loader class:", class(loaderObj), "\n")
        errorStatus = tryCatch(expr = {
          loaderObj$assign_biosample_ids()
        }, error = function(e) {
          print(e)
          return(e)
        })
        if ("error" %in% class(errorStatus)) {
          cat("Skipping unmatched sample\n")
          next
        }
        
        loaderObj$download_features_for_featureset()
        
        didRegisterFeatures = loaderObj$register_new_features()
        
        if (didRegisterFeatures) {
          loaderObj$update_reference_object()
        }
        
        # Keep track of the current state of features in database in local reference object
        reference_object$feature = loaderObj$retrieve_features()
        reference_object$feature_synonym = loaderObj$retrieve_feature_synonyms()
        
        errorStatus = tryCatch(expr = {
          loaderObj$assign_feature_ids()
        }, error = function(e) {
          print(e)
        })
        if ("error" %in% class(errorStatus)) {
          cat(paste0("Error assigning features for file:", file_path, "\n"))
          next
        }
        
        loaderObj$assign_other_ids()
        
        try({
          loaderObj$load_data()
        })
      } # end of loop through unique files in a measurementsets
    } # end of loop through measurementsets in a dataset record
  } # end of loop through dataset records
}

entity_to_excel_sheet_converter = function(entity = NULL) {
  picker = c('Studies', 'Subjects', 'Samples')
  names(picker) = c(.ghEnv$meta$arrDataset,
                    .ghEnv$meta$arrIndividuals,
                    .ghEnv$meta$arrBiosample)
  if (is.null(entity)) {
    picker 
  } else {
    picker[entity]
  }
}

apply_definition_constraints = function(df1 = df1,
                                        dataset_id = dataset_id,
                                        entity = entity,
                                        updateCache = FALSE, 
                                        con = con) {
  def = search_definitions(dataset_id = dataset_id,
                           updateCache = updateCache,
                           con = con)
  if (nrow(def) != 0) {
    sheetName = entity_to_excel_sheet_converter(entity)
    if (!is.na(sheetName)) {
      defi = template_helper_extract_definitions(sheetName = sheetName, 
                                                 def = def)
      if (nrow(defi) != 0) {
        defi_contr = defi[!is.na(defi$controlled_vocabulary), ]
        if (nrow(defi_contr) > 0) {
          cat(paste0("Applying constraint A: controlled-vocab fields (", 
                     nrow(defi_contr), " of ", nrow(defi), ") definitions\n"))
          
          df1 = join_ontology_terms(df = df1, 
                                    terms = defi_contr$attribute_name,
                                    updateCache = FALSE, 
                                    con = con)
        }
        
        cat("Applying constraint B: Column ordering same as excel sheet\n")
        allcols = colnames(df1)
        excel_defined_cols = defi$attribute_name[which((defi$attribute_name %in% allcols))]
        other_cols = allcols[!(allcols %in% excel_defined_cols)]
        stopifnot(all(c(other_cols,
                        excel_defined_cols) %in% allcols))
        df1 = df1[, c(other_cols,
                      excel_defined_cols)]
      }
    }
  }
  df1
}