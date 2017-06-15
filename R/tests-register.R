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

test_register_project = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrProject, silent = silent)
  test_unique_fields(df, uniq)
}

test_register_dataset = function(df, uniq, dataset_version, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrDataset, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$project_id))!=1) stop("Datasets to be registered must belong to a single project")
  if (dataset_version != 1) stop("to increment dataset versions, use the function `increment_dataset_version()`")
}

test_register_individual = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrIndividuals, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$dataset_id))!=1) stop("Individuals to be registered must belong to a single dataset/study")
}

test_register_biosample = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrBiosample, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$dataset_id))!=1) stop("Biosamples to be registered must belong to a single dataset/study")
}

test_register_rnaquantificationset = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrRnaquantificationset, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$dataset_id))!=1) stop("Rnaquantificationset to be registered must belong to a single dataset/study")
}

test_register_ontology = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrOntology, silent = silent)
  test_unique_fields(df, uniq)
}

test_register_featureset = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrFeatureset, silent = silent)
  test_unique_fields(df, uniq)
}

test_register_referenceset = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrReferenceset, silent = silent)
  test_unique_fields(df, uniq)
}

test_register_genelist = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrGenelist, silent = silent)
  test_unique_fields(df, uniq)
}

test_register_genelist_gene = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrGenelist_gene, silent = silent)
  test_unique_fields(df, uniq)
}

test_register_feature = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrFeature, silent = silent)
  test_unique_fields(df, uniq)
}

test_register_feature_synonym = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrFeatureSynonym, silent = silent)
  test_unique_fields(df, uniq)
}

test_register_variantset = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrVariantset, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$dataset_id))!=1) stop("VariantSet to be registered must belong to a single dataset/study")
}

test_register_fusionset = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrFusionset, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$dataset_id))!=1) stop("FusionSet to be registered must belong to a single dataset/study")
}

test_register_copynumberset = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrCopyNumberSet, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$dataset_id))!=1) stop("CopyNumberSet to be registered must belong to a single dataset/study")
}

test_register_copynumbersubset = function(df, uniq, silent = TRUE){
  test_dataframe_formatting(df)
  test_mandatory_fields(df, arrayname = jdb$meta$arrCopyNumberSubSet, silent = silent)
  test_unique_fields(df, uniq)
  if(length(unique(df$dataset_id))!=1) stop("CopyNumberSubset to be registered must belong to a single dataset/study")
}

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
  test_mandatory_fields(df, arrayname = jdb$meta$arrVariant)
}

test_register_copynumber_seg = function(copynumberset){
  stopifnot(nrow(copynumberset) == 1)
  stopifnot("filepath" %in% colnames(copynumberset))
}

test_register_copnyumber_matrix_file = function(copynumberSubSet, dataset_version){
  stopifnot(nrow(copynumberSubSet) == 1)
  stopifnot("filepath" %in% colnames(copynumberSubSet))
}

test_register_fusion_data = function(df, fusionset){
  test_dataframe_formatting(df)
  stopifnot(nrow(fusionset) == 1)
}

