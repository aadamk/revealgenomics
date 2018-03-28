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

# TODO: these functions should be merged into DataFrameLoader under
# `DataFrameLoaderRnaseqCufflinksGenePerSample` and
# `DataFrameLoaderRnaseqCufflinksTranscriptPerSample`

# Lower level functions for interpreting specific file-types 
# (e.g. Cufflinks RNASeq files, Gemine DNASeq files) before ingesting into SciDB
# -- Not meant to be called directly by the API user.

#' register cufflinks gene file-type
#'
#' Should be replaced by `register_expression_file_cufflinks` once it is tested to handle
#' both Cufflinks isoform and gene file-types
#' 
#' @param biosample_name unique sample name at which to register biosample
#' @param biosample_df (optional) dataframe consisting biosamples of current study
#' @param featureset_id featureset ID in which to look for features
#' @param feature_df (optional) dataframe consisting of features on current featureset_id
register_expression_file_cufflinks_gene = function(filepath,
                                                   separator,
                                                   dataset_id,
                                                   dataset_version,
                                                   biosample_name,
                                                   featureset_id,
                                                   rnaquantificationset_id,
                                                   biosample_df = NULL,
                                                   feature_df = NULL) {
  if (is.null(biosample_df)) {
    cat("Reading in biosample dataframe for current study. 
        Consider providing this for faster uploads\n")
    biosample_df = search_biosamples(dataset_id = dataset_id,
                                     dataset_version = dataset_version)
  }
  if (is.null(feature_df)) {
    cat("Reading in feature dataframe for current study. 
        Consider providing this for faster uploads\n")
    feature_df = search_features(featureset_id = featureset_id)
  }
  cat("Reading the expression file")
  xx = read.delim(filepath, sep = separator, stringsAsFactors = FALSE)
  cat("... done\n")
  
  biosample = biosample_df[biosample_df$name == biosample_name, ]
  stopifnot(nrow(biosample) == 1)
  xx$biosample_id = biosample$biosample_id
  
  xx$dataset_id = dataset_id
  xx$rnaquantificationset_id = rnaquantificationset_id
  
  columns_to_drop = c('class_code', 
                      'nearest_ref_id', 
                      'tss_id', 'locus', 
                      'length', 'coverage', 
                      'FPKM_conf_lo', 'FPKM_conf_hi', 'FPKM_status')
  xx[, columns_to_drop] = NULL
  xx = plyr::rename(xx, 
                    c('FPKM' = 'value'))
  
  # features
  cat("Handle features\n")
  stopifnot(all.equal(xx$tracking_id, xx$gene_id))
  xx$tracking_id = NULL
  
  matchL = find_matches_and_return_indices(source = xx$gene_id, 
                                           target = feature_df$name)
  
  if (length(matchL$source_unmatched_idx) > 0) {
    # register new features
    cat("Found new features in file")
    new_features = xx[matchL$source_unmatched_idx, 
                      c('gene_id', 'gene_short_name')]
    head(new_features)
    dim(new_features)
    new_features = plyr::rename(new_features, 
                                c('gene_id' = 'name',
                                  'gene_short_name' = 'gene_symbol'))
    new_features$featureset_id = featureset_id
    new_features$source = 'ensembl_gene_id'
    new_features$chromosome = 'unknown'
    new_features$start = -1
    new_features$end = -1
    new_features$feature_type = 'gene'
    new_features$strand_term = search_ontology(terms = 'strand_term_unspecified')
    
    feature_id = register_feature(df = new_features, register_gene_synonyms = TRUE)
    
    # now rematch features
    feature_df = search_features(featureset_id = featureset_id)
    
    matchL = find_matches_and_return_indices(source = xx$gene_id, 
                                             target = feature_df$name)
    
    registeredNewFeatures = TRUE
  } else{
    registeredNewFeatures = FALSE
  }
  
  stopifnot(length(matchL$source_unmatched_idx) == 0)
  xx$feature_id = feature_df[matchL$target_matched_idx, ]$feature_id
  
  cat("Now registering", nrow(xx), "expression values\n")
  register_expression_dataframe(df1 = xx, dataset_version = version_num)
  
  return(list(registeredNewFeatures = registeredNewFeatures))
}

#' Register cufflinks isoform / gene file-type
#' 
#' Eventually this one function should make `register_expression_file_cufflinks_gene` redundant
#' 
#' @param biosample_name unique sample name at which to register biosample
#' @param biosample_df (optional) dataframe consisting biosamples of current study
#' @param featureset_id featureset ID in which to look for features
#' @param feature_df (optional) dataframe consisting of features on current featureset_id
register_expression_file_cufflinks = function(filepath,
                                              separator,
                                              dataset_id,
                                              dataset_version,
                                              biosample_name,
                                              featureset_id,
                                              rnaquantificationset_id,
                                              biosample_df = NULL,
                                              feature_df = NULL) {
  if (is.null(biosample_df)) {
    cat("Reading in biosample dataframe for current study. 
        Consider providing this for faster uploads\n")
    biosample_df = search_biosamples(dataset_id = dataset_id,
                                     dataset_version = dataset_version)
  }
  if (is.null(feature_df)) {
    cat("Reading in feature dataframe for current study. 
        Consider providing this for faster uploads\n")
    feature_df = search_features(featureset_id = featureset_id)
  }
  cat("Reading the expression file")
  xx = read.delim(filepath, sep = separator, stringsAsFactors = FALSE)
  cat("... done\n")
  
  biosample = biosample_df[biosample_df$name == biosample_name, ]
  stopifnot(nrow(biosample) == 1)
  xx$biosample_id = biosample$biosample_id
  
  xx$dataset_id = dataset_id
  xx$rnaquantificationset_id = rnaquantificationset_id
  
  columns_to_drop = c('class_code', 
                      'nearest_ref_id', 
                      'tss_id', 'locus', 
                      'length', 'coverage', 
                      'FPKM_conf_lo', 'FPKM_conf_hi', 'FPKM_status')
  xx[, columns_to_drop] = NULL
  xx = plyr::rename(xx, 
                    c('FPKM' = 'value'))
  
  # features
  cat("Handle features\n")
  if (identical(xx$tracking_id, xx$gene_id)) {
    cat("Identified gene FPKM file-type\n")
    feature_type = 'gene'
  } else {
    cat("Identified isoform FPKM file-type\n")
    feature_type = 'transcript'
  }
  xx$gene_id = NULL
  matchL = find_matches_and_return_indices(source = xx$tracking_id, 
                                           target = feature_df$name)
  
  if (length(matchL$source_unmatched_idx) > 0) {
    # register new features
    cat("Found new features in file")
    new_features = xx[matchL$source_unmatched_idx, 
                      c('tracking_id', 'gene_short_name')]
    head(new_features)
    dim(new_features)
    new_features = plyr::rename(new_features, 
                                c('tracking_id' = 'name',
                                  'gene_short_name' = 'gene_symbol'))
    new_features$featureset_id = featureset_id
    new_features$source = 'ensembl_gene_id'
    new_features$chromosome = 'unknown'
    new_features$start = -1
    new_features$end = -1
    new_features$feature_type = feature_type
    new_features$strand_term = search_ontology(terms = 'strand_term_unspecified')
    
    feature_id = register_feature(df = new_features, register_gene_synonyms = TRUE)
    
    # now rematch features
    feature_df = search_features(featureset_id = featureset_id)
    
    matchL = find_matches_and_return_indices(source = xx$tracking_id, 
                                             target = feature_df$name)
    
    registeredNewFeatures = TRUE
  } else{
    registeredNewFeatures = FALSE
  }
  
  stopifnot(length(matchL$source_unmatched_idx) == 0)
  xx$feature_id = feature_df[matchL$target_matched_idx, ]$feature_id
  
  cat("Now registering", nrow(xx), "expression values\n")
  register_expression_dataframe(df1 = xx, dataset_version = version_num)
  
  return(list(registeredNewFeatures = registeredNewFeatures))
}
