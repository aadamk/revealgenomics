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

report_missing_mandatory_fields = function(df, arrayname){
  fields = get_mandatory_fields_for_register_entity(strip_namespace(arrayname))
  fields[which(!(fields %in% colnames(df)))]
}

test_mandatory_fields = function(df, arrayname, silent = TRUE){
  mandatory_fields = get_mandatory_fields_for_register_entity(arrayname)
  missing_fields =  report_missing_mandatory_fields(df, arrayname = arrayname)
  if (length(missing_fields) == 0){
    if (!silent){
      cat("Looks OK\n---Preview of mandatory fields:---\n")
      print(head(df[, mandatory_fields]))
      cat("---Flexible fields:\n")
      print(colnames(df)[!(colnames(df) %in% mandatory_fields)])
    }
  } else {
    stop("Following fields are missing: \n", paste(missing_fields, collapse = ", "))
  }
}

test_unique_fields = function(df, uniq){
  if(any(duplicated(df[, uniq]))) {
    stop("duplicate entries exist in data to be uploaded need to be removed. Check fields: ",
         paste(uniq, collapse = ", "))
  }
}

test_dataframe_formatting = function(df){
  test_return_carriage_in_df(df)
}

test_return_carriage_in_df = function(df){
  tt =sapply(df, FUN=function(col){grep("\n", col)})
  pos = which(lapply(tt, FUN=length)!=0)
  
  report = "Need to remove return carriage '\\n' from data. Found at:\n"
  if (length(pos) > 0){
    for (posi in pos){
      report = paste(report, "Column name:", names(tt)[[posi]], " -- Row(s):", paste(tt[[posi]], collapse = ", "), "\n")
    }
    stop(report)
  }
}

test_non_null_ontology_elements = function(df, suffix = c("_", "_term")) {
  ont_cols = colnames(df)[grep("_$|_term$", colnames(df))]
  if (length(ont_cols) >= 1) {
    dfx = df[, ont_cols]
    if (class(dfx) == 'data.frame') {
      if (nrow(dfx) > 1) {
        check_for_null = apply(apply(dfx, 2, is.na), 2, any)
      } else if (nrow(dfx) == 1) {
        check_for_null = apply(dfx, 2, is.na)
      } else {
        stop("Zero row DF should not exist here")
      }
    } else if (class(dfx) %in% c('integer', 'numeric')) {
      check_for_null = is.na(dfx)
    } else {
      stop("Expected data frame or integer values here")
    }
    if (any(check_for_null)) {
      stop("Found null element in ontology fields: ", 
           pretty_print(names(which(check_for_null))))
    }
  }
}

run_tests_dataframe = function(entity, df, uniq, silent) {
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = entity, silent = silent)
  test_unique_fields(df, uniq)
  test_non_null_ontology_elements(df)
}

test_register_project = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrProject, df, uniq, silent)
}

test_register_dataset = function(df, uniq, dataset_version, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrDataset, df, uniq, silent)
  if(length(unique(df$project_id))!=1) stop("Datasets to be registered must belong to a single project")
  if (dataset_version != 1) stop("to increment dataset versions, use the function `increment_dataset_version()`")
}

######################################################
# Versioned secure metadata entities

test_register_versioned_secure_metadata_entity = function(entity, df, uniq, silent = TRUE){
  if (is.null(uniq)) uniq = unique_fields()[[entity]]
  if (is.null(uniq)) stop("unique fields need to be defined for entity: ", 
                          entity, " in SCHEMA.yaml file")
  run_tests_dataframe(entity = entity, df, uniq, silent)
  if(length(unique(df$dataset_id))!=1) stop(tolower(entity), " to be registered must belong to a single dataset/study")
}

test_register_individual = function(df, uniq, silent = TRUE){
  # Add additional tests here -->
  
  # Test below was moved as a common test in register_versioned_secure_metadata_entity()
  # test_register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrIndividuals, 
  #                                                df, uniq, silent)
}

test_register_biosample = function(df, uniq, silent = TRUE){
  # Add additional tests here -->
  
  # Test below was moved as a common test in register_versioned_secure_metadata_entity()
  # test_register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrBiosample, 
  #                                                df, uniq, silent)
}

test_register_rnaquantificationset = function(df, uniq, silent = TRUE){
  # Add additional tests here -->
  
  # Test below was moved as a common test in register_versioned_secure_metadata_entity()
  # test_register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrRnaquantificationset, 
  #                                                df, uniq, silent)
}

test_register_experimentset = function(df, silent = TRUE){
  # Additional tests
  entity_df = get_entity_info()
  entity_df = entity_df[entity_df$class == 'measurementdata', ]
  entity_nm_upload = as.character(unique(df$measurement_entity))
  
  if (!(all(entity_nm_upload %in% as.character(entity_df$entity)))) {
    cat("Unexpected measurement entity: \n")
    print(entity_nm_upload[!(entity_nm_upload %in% as.character(entity_df$entity))])
    stop("Allowed measurement entities: ", pretty_print(entity_df$entity))
  }
}

test_register_variantset = function(df, uniq, silent = TRUE){
  # Add additional tests here -->
  
  # Test below was moved as a common test in register_versioned_secure_metadata_entity()
  # test_register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrVariantset, 
  #                                                df, uniq, silent)
}

test_register_fusionset = function(df, uniq, silent = TRUE){
  # Add additional tests here -->
  
  # Test below was moved as a common test in register_versioned_secure_metadata_entity()
  # test_register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrFusionset, 
  #                                                df, uniq, silent)
}

test_register_copynumberset = function(df, uniq, silent = TRUE){
  # Add additional tests here -->
  
  # Test below was moved as a common test in register_versioned_secure_metadata_entity()
  # test_register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrCopyNumberSet, 
  #                                                df, uniq, silent)
}

######################################################
# public metadata entities (not versioned by dataset_version)

test_register_ontology = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrOntology, df, uniq, silent)
}

test_register_featureset = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrFeatureset, df, uniq, silent)
}

test_register_referenceset = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrReferenceset, df, uniq, silent)
}

test_register_genelist = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrGenelist, df, uniq, silent)
}

test_register_genelist_gene = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrGenelist_gene, df, uniq, silent)
}

test_register_feature = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrFeature, df, uniq, silent)
}

test_register_feature_synonym = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrFeatureSynonym, df, uniq, silent)
}

######################################################
# measurementdata entities

test_register_expression_matrix = function(filepath,
                                           rnaquantificationset_id,
                                           featureset_id,
                                           feature_type,
                                           dataset_version){
  stopifnot(length(rnaquantificationset_id) == 1)
  stopifnot(length(featureset_id) == 1)
  stopifnot(feature_type == 'gene' | feature_type == 'transcript')
}

test_register_variant = function(df){
  test_dataframe_formatting(df)
  if(length(unique(df$dataset_id))!=1) stop("Variants to be registered must belong to a single dataset/study")
  test_mandatory_fields(df, arrayname = .ghEnv$meta$arrVariant)
}

test_register_copynumber_seg = function(experimentset){
  stopifnot(nrow(experimentset) == 1)
  stopifnot("file_path" %in% colnames(experimentset))
}

test_register_copnyumber_matrix_file = function(copynumberset, dataset_version){
  stopifnot(nrow(copynumberset) == 1)
  stopifnot("file_path" %in% colnames(copynumberset))
}

test_register_fusion_data = function(df, fusionset){
  test_dataframe_formatting(df)
  stopifnot(nrow(fusionset) == 1)
}

