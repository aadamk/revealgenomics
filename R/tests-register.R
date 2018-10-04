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
  
  if (length(pos) > 0){
    report = "Need to remove return carriage '\\n' from data. Found at:\n"
    for (posi in pos){
      report = paste(report, "Column name:", names(tt)[[posi]], " -- Row(s):", paste(tt[[posi]], collapse = ", "), "\n")
    }
    stop(report)
  }
}

#' Run tests on data frame 
run_tests_dataframe = function(entity, df, uniq, silent) {
  if (!identical(class(df), 'data.frame')) {
    stop("REVEAL/Genomics API allows uploads via data.frame only.
    You might be using data.table, or tibble. 
    If so, consider using as.data.frame() before calling any register_...() function")
  }
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = entity, silent = silent)
  test_unique_fields(df, uniq)
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
  if ("dataset_version" %in% colnames(df)) {
    stop("dataset_version should not be a column in dataframe to be uploaded. 
         dataset_version is to be supplied as a parameter to the register_", tolower(entity), "() function")
  }
}

test_register_individual = function(df, uniq, silent = TRUE){
  # Add additional tests here -->
  
  # Test below was moved as a common test in register_versioned_secure_metadata_entity()
  # test_register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrIndividuals, 
  #                                                df, uniq, silent)
}

test_register_biosample = function(df, silent = TRUE){
  if ('sample_name' %in% colnames(df)) {
    stop("Cannot use reserved column name: `sample_name`! 
         Use `name` or other column-name instead.")
  }
}

test_register_experimentset = function(df, silent = TRUE){
  # Additional tests
  expt_type_upload = as.character(unique(df$experiment_type_API))
  
  if (!(all(expt_type_upload %in% as.character(mappings_experimentset)))) {
    cat("Unexpected measurement entity: \n")
    print(expt_type_upload[!(expt_type_upload %in% as.character(mappings_experimentset))])
    stop("Allowed measurement entities: ", pretty_print(sort(mappings_experimentset)))
  }
}

test_register_measurementset  = function(df1, silent = TRUE){
  # Additional tests
  
  # BEGIN: Common test with test_.._experimentset
  entity_df = get_entity_info()
  entity_df = entity_df[entity_df$class == 'measurementdata' | 
                          entity_df$entity == 'MEASUREMENT', ] # File links are registered within Measurement entity
  entity_nm_upload = as.character(unique(df1$entity))
  
  if (!(all(entity_nm_upload %in% as.character(entity_df$entity)))) {
    cat("Unexpected measurement entity: \n")
    print(entity_nm_upload[!(entity_nm_upload %in% as.character(entity_df$entity))])
    stop("Allowed measurement entities: ", pretty_print(entity_df$entity))
  }
  # END: Common test with test_.._experimentset
  
  # any other tests
}

######################################################
# public metadata entities (not versioned by dataset_version)

test_register_ontology = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrOntology, df, uniq, silent)
}

test_register_definition = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrDefinition, df, uniq, silent)
}

test_register_metadata_attrkey = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrMetadataAttrKey, df, uniq, silent)
}

test_register_variant_key = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrVariantKey, df, uniq, silent)
}

test_register_chromosome_key = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrChromosomeKey, df, uniq, silent)
}

test_register_featureset = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrFeatureset, df, uniq, silent)
}

test_register_referenceset = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrReferenceset, df, uniq, silent)
}

test_register_gene_symbol = function(df, uniq, silent = TRUE){
  run_tests_dataframe(entity = .ghEnv$meta$arrGeneSymbol, df, uniq, silent)
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
                                           measurementset_id,
                                           featureset_id,
                                           feature_type,
                                           dataset_version){
  stopifnot(length(measurementset_id) == 1)
  stopifnot(length(featureset_id) == 1)
  stopifnot(feature_type == 'gene' | feature_type == 'transcript')
}

test_register_variant = function(df, variant_attr_cols){
  test_dataframe_formatting(df)
  if(length(unique(df$dataset_id))!=1) stop("Variants to be registered must belong to a single dataset/study")
  
  # `per_gene_variant_number`, `key_id` and `val` are mandatory fields that are typically added later --
  # -- hence run the test as follows
  df_temp = df
  df_temp$per_gene_variant_number = -1
  df_temp$key_id = -1
  df_temp$val = 'asdf'
  test_mandatory_fields(df_temp, arrayname = .ghEnv$meta$arrVariant)

  cols_to_check = c('measurementset_id', 'biosample_id', 'feature_id',
                    variant_attr_cols)
  if ( !all(cols_to_check  %in% colnames(df)) ) {
    stop("Following columns expected but not present:\n\t",
        pretty_print(cols_to_check[which(!(cols_to_check %in% colnames(df)))]), 
        "\n")
  }
  
  check_entity_exists_at_id(entity = .ghEnv$meta$arrMeasurementSet,
                            id = sort(unique(df$measurementset_id)))
  check_entity_exists_at_id(entity = .ghEnv$meta$arrBiosample,
                            id = sort(unique(df$biosample_id)))
  # check_entity_exists_at_id(entity = .ghEnv$meta$arrFeature,
  #                           id = sort(unique(df$feature_id)))
}

test_register_expression_dataframe = function(df1) {
  test_mandatory_fields(df = df1, arrayname = .ghEnv$meta$arrRnaquantification, 
                        silent = TRUE)

  check_entity_exists_at_id(entity = .ghEnv$meta$arrMeasurementSet,
                            id = sort(unique(df1$measurementset_id)))
  check_entity_exists_at_id(entity = .ghEnv$meta$arrBiosample,
                            id = sort(unique(df1$biosample_id)))
  # check_entity_exists_at_id(entity = 'FEATURE',
  #                           id = sort(unique(df1$feature_id)))
}

test_register_fusion = function(df, fusion_attr_cols){
  test_dataframe_formatting(df)
  if(length(unique(df$dataset_id))!=1) stop("Fusion data to be registered must belong to a single dataset/study")
  
  # `per_gene_pair_fusion_number`, `key_id` and `val` are mandatory fields that are typically added later --
  # -- hence run the test as follows
  df_temp = df
  df_temp$per_gene_pair_fusion_number = -1
  df_temp$key_id = -1
  df_temp$val = 'asdf'
  test_mandatory_fields(df_temp, arrayname = .ghEnv$meta$arrFusion)
  
  cols_to_check = c('measurementset_id', 'biosample_id', 'feature_id_left', 'feature_id_right',
                    fusion_attr_cols)
  if ( !all(cols_to_check  %in% colnames(df)) ) {
    stop("Following columns expected but not present:\n\t",
         pretty_print(cols_to_check[which(!(cols_to_check %in% colnames(df)))]), 
         "\n")
  }
  
  check_entity_exists_at_id(entity = .ghEnv$meta$arrMeasurementSet,
                            id = sort(unique(df$measurementset_id)))
  check_entity_exists_at_id(entity = .ghEnv$meta$arrBiosample,
                            id = sort(unique(df$biosample_id)))
  # check_entity_exists_at_id(entity = .ghEnv$meta$arrFeature,
  #                           id = sort(unique(df$feature_id)))
}

test_register_copynumbervariant_variable_columns = function(df, cnv_attr_cols){
  test_dataframe_formatting(df)
  if(length(unique(df$dataset_id))!=1) stop("CNV data to be registered must belong to a single dataset/study")
  
  # `per_gene_copynumbervariant_number`, `key_id` and `val` are mandatory fields that are typically added later --
  # -- hence run the test as follows
  df_temp = df
  df_temp$per_gene_copynumbervariant_number = -1
  df_temp$key_id = -1
  df_temp$val = 'asdf'
  test_mandatory_fields(df_temp, arrayname = .ghEnv$meta$arrCopynumber_variant)
  
  cols_to_check = c('measurementset_id', 'biosample_id', 'feature_id',
                    cnv_attr_cols)
  if ( !all(cols_to_check  %in% colnames(df)) ) {
    stop("Following columns expected but not present:\n\t",
         pretty_print(cols_to_check[which(!(cols_to_check %in% colnames(df)))]), 
         "\n")
  }
  
  check_entity_exists_at_id(entity = .ghEnv$meta$arrMeasurementSet,
                            id = sort(unique(df$measurementset_id)))
  check_entity_exists_at_id(entity = .ghEnv$meta$arrBiosample,
                            id = sort(unique(df$biosample_id)))
  # check_entity_exists_at_id(entity = .ghEnv$meta$arrFeature,
  #                           id = sort(unique(df$feature_id)))
}
