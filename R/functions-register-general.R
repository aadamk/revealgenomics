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
register_project = function(df,
                            only_test = FALSE, 
                            con = NULL){
  con = use_ghEnv_if_null(con)
  
  uniq = unique_fields()[[.ghEnv$meta$arrProject]]
  test_register_project(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrProject)
    register_tuple_return_id(df, arrayname, uniq, con = con)
  } # end of if (!only_test)
}

#' @export
register_dataset = function(df,
                            public = FALSE,
                            dataset_version = 1,
                            only_test = FALSE,
                            con = NULL
){
  con = use_ghEnv_if_null(con)
  
  stopifnot(class(public) == 'logical')
  
  uniq = unique_fields()[[.ghEnv$meta$arrDataset]]
  
  df$public = public
  test_register_dataset(df, uniq, dataset_version, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrDataset)
    register_tuple_return_id(df, arrayname, uniq, dataset_version = dataset_version, con = con)
  } # end of if (!only_test)
}

#' @export
register_individual = function(df, 
                               dataset_version = NULL,
                               only_test = FALSE,
                               con = NULL){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrIndividuals, 
                                            df, dataset_version, only_test, con = con)
}

#' @export
register_referenceset = function(df, only_test = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  uniq = unique_fields()[[.ghEnv$meta$arrReferenceset]]
  test_register_referenceset(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrReferenceset)
    register_tuple_return_id(df,
                             arrayname, uniq, con = con)
  } # end of if (!only_test)
}

#' Register genelist
#' 
#' Preferred method of registering genelist-s is to 
#' provide one genelist_name and genelist_description
#' at a time.
#' 
#' Alternatively, you can supply a data.frame with `name, description]`
#' of one or more genelist(s)
#' 
#' @param genelist_name the name of the geneliest
#' @param genelist_description the description for the genelist
#' @param isPublic bool to denote if the genelist is public
#' @param df (optional) a data-frame containing `name, description` of the genelist
#' 
#' @export
register_genelist = function(genelist_name = NULL, 
                             genelist_description = NULL, 
                             isPublic = TRUE, 
                             df = NULL, 
                             only_test = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (!is.null(df) & 
      (!is.null(genelist_name) | !is.null(genelist_description))) {
    stop("Cannot supply both df and [genelist_name, genelist_description]. 
         Use one method for using this function")
  }
  
  df1 = df
  if (is.null(df1)) {
    df1 = data.frame(name = genelist_name, 
                     description = genelist_description,
                     public = isPublic,
                     stringsAsFactors = FALSE)
  }
  
  df1$owner = get_logged_in_user(con = con)
  
  uniq = unique_fields()[[.ghEnv$meta$arrGenelist]]
  test_register_genelist(df1, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrGenelist)
    register_tuple_return_id(df1,
                             arrayname, uniq, con = con)
  } # end of if (!only_test)
  }

#' Register gene symbols in a gene-list
#' 
#' Preferred method of registering gene symbols in a genelist is to 
#' provide the target genelist_id and the symbols to be registered
#' (one genelist at a time)
#' 
#' Alternatively, one can supply a data.frame with `genelist_id, gene_symbol`
#'  
#' @param genelist_id the id of the genelist (returned by `register_genelist()`)
#' @param gene_symbols the gene-symbols to be stored in a gene-list (e.g. `c('TSPAN6', 'KCNIP2', 'CFAP58', 'GOT1', 'CPN1', 'PSIP1P1')`)
#' @param df (optional) a data-frame containing `genelist_id, gene_symbol`
#'
#' @examples 
#' register_genelist_gene(genelist_id = 11, # must already exist in `genelist` table
#'                        gene_symbols = c('TSPAN6', 'KCNIP2'))
#'                        
#' @export
register_genelist_gene = function(genelist_id = NULL, 
                                  gene_symbols = NULL, 
                                  df = NULL, 
                                  only_test = FALSE, con = NULL){
  uniq = unique_fields()[[.ghEnv$meta$arrGenelist_gene]]
  if (!is.null(df) & 
      (!is.null(genelist_id) | !is.null(gene_symbols))) {
    stop("Cannot supply both df and [genelist_id, gene_symbols]. 
         Use one method for using this function")
  }
  
  if (is.null(df)) {
    df = data.frame(genelist_id = genelist_id, 
                    gene_symbol = gene_symbols, 
                    stringsAsFactors = FALSE)
  }
  
  test_register_genelist_gene(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrGenelist_gene)
    register_tuple_return_id(df,
                             arrayname, uniq, con = con)
  } # end of if (!only_test)
  }

#' @export
register_featureset = function(df, only_test = FALSE, con = NULL){
  uniq = unique_fields()[[.ghEnv$meta$arrFeatureset]]
  test_register_featureset(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrFeatureset)
    register_tuple_return_id(df,
                             arrayname, uniq, con = con)
  } # end of if (!only_test)
}

#' @export
register_biosample = function(df, 
                              dataset_version = NULL,
                              only_test = FALSE,
                              con = NULL){
  # Extra tests for Biosample
  test_register_biosample(df, silent = ifelse(only_test, FALSE, TRUE))
  
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrBiosample, 
                                            df, dataset_version, only_test, con = con)
}

#' @export
register_feature = function(df1, register_gene_synonyms = TRUE, only_test = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  uniq = unique_fields()[[.ghEnv$meta$arrFeature]]
  test_register_feature(df1, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    # Register gene symbols for incoming set of features
    cat("Register gene symbols for incoming set of features\n")
    if ('gene_symbol_id' %in% colnames(df1)) {
      stop("did not expect `gene_symbol_id` column in feature data. 
           This is assigned by functions")
    }
    if ('full_name' %in% colnames(df1)) {
      gene_syms = unique(df1[, 'gene_symbol'])
      
      ff2 = df1[!is.na(df1$full_name), ]
      full_names = ff2[match(gene_syms, ff2$gene_symbol), ]$full_name
      
      df_gs = data.frame(gene_symbol = gene_syms,
                         full_name = full_names,
                         stringsAsFactors = FALSE)
    } else {
      df_gs = data.frame(
        gene_symbol = unique(df1[, 'gene_symbol']),
        full_name = as.character(NA),
        stringsAsFactors = FALSE
      )
    }
    gs_id = register_gene_symbol(df = df_gs, con = con)
    df_gs_all = get_gene_symbol(con = con)
    
    m1 = find_matches_and_return_indices(
      source = df1$gene_symbol,
      target = df_gs_all$gene_symbol
    )
    stopifnot(length(m1$source_unmatched_idx) == 0)
    df1$gene_symbol_id = df_gs_all[m1$target_matched_idx, ]$gene_symbol_id
    
    cat("Register features\n")
    arrayname = full_arrayname(.ghEnv$meta$arrFeature)
    fid = register_tuple_return_id(df = df1,
                                   arrayname = arrayname,
                                   uniq = uniq,
                                   con = con)
    fid = fid[, get_base_idname(.ghEnv$meta$arrFeature)]
    gene_ftrs = df1[df1$feature_type == 'gene', ]
    if (register_gene_synonyms & nrow(gene_ftrs) > 0){
      cat("Working on gene synonyms\n")
      if (length(fid) != nrow(gene_ftrs)) {
        stop("More than one type of feature_type being registered at one time. 
             Need to sub-select `fid` accordingly.")
      }
      df_syn = data.frame(synonym = gene_ftrs$name, 
                          feature_id = fid,
                          featureset_id = unique(gene_ftrs$featureset_id),
                          source = gene_ftrs$source,
                          stringsAsFactors = F)
      ftr_syn_id = register_feature_synonym(df = df_syn, con = con)
    } else {
      ftr_syn_id = NULL
    }
    list(gene_symbol_id = gs_id, 
         feature_id = fid,
         feature_synonym_id = ftr_syn_id$feature_synonym_id)
  } # end of if (!only_test)
}

#' @export
register_experimentset = function(df, dataset_version = NULL, only_test = FALSE, con = NULL){
  # Extra tests for ExperimentSet
  test_register_experimentset(df, silent = ifelse(only_test, FALSE, TRUE))
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrExperimentSet, 
                                            df, dataset_version, only_test, con = con)
}

register_measurement = function(df, dataset_version = NULL, only_test = FALSE, con = NULL){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrMeasurement, 
                                            df, dataset_version, only_test, con = con)
}

#' @export
register_measurementset = function(df, dataset_version = NULL, only_test = FALSE, con = NULL){
  # Extra tests for MeasurementSet
  test_register_measurementset(df, silent = ifelse(only_test, FALSE, TRUE))
  
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrMeasurementSet, 
                                            df, dataset_version, only_test, con = con)
}

#' @export
register_copynumberset = function(df, dataset_version = NULL, only_test = FALSE, con = NULL){
  register_versioned_secure_metadata_entity(entity = .ghEnv$meta$arrCopyNumberSet, 
                                            df, dataset_version, only_test, con = con)
}

