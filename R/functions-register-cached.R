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
register_metadata_attrkey = function(df1, only_test = FALSE, con = NULL){
  uniq = unique_fields()[[.ghEnv$meta$arrMetadataAttrKey]]
  test_register_metadata_attrkey(df1, uniq, silent = ifelse(only_test, FALSE, TRUE))
  
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrMetadataAttrKey)
    ids = register_tuple_return_id(df1, arrayname, uniq, con = con)
    
    # force update the cache
    update_metadata_attrkey_cache(con = con)
    
    return(ids)
  } # end of if (!only_test)
}


#' @export
register_variant_key = function(df1, only_test = FALSE, con = NULL){
  uniq = unique_fields()[[.ghEnv$meta$arrVariantKey]]
  test_register_variant_key(df1, uniq, silent = ifelse(only_test, FALSE, TRUE))
  
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrVariantKey)
    ids = register_tuple_return_id(df1, arrayname, uniq, con = con)
    
    # force update the cache
    update_variant_key_cache(con = con)
    
    return(ids)
  } # end of if (!only_test)
}

#' @export
register_chromosome_key = function(df1, only_test = FALSE, con = NULL){
  uniq = unique_fields()[[.ghEnv$meta$arrChromosomeKey]]
  test_register_chromosome_key(df1, uniq, silent = ifelse(only_test, FALSE, TRUE))
  
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrChromosomeKey)
    ids = register_tuple_return_id(df1, arrayname, uniq, con = con)
    
    # force update the cache
    update_chromosome_key_cache(con = con)
    
    return(ids)
  } # end of if (!only_test)
}

#' @export
register_ontology_term = function(df, only_test = FALSE, con = NULL){
  if (!('category' %in% colnames(df))) df$category = 'uncategorized'
  uniq = unique_fields()[[.ghEnv$meta$arrOntology]]
  test_register_ontology(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrOntology)
    ids = register_tuple_return_id(df, arrayname, uniq, con = con)
    
    # force update the ontology
    update_ontology_cache(con = con)
    
    return(ids)
  } # end of if (!only_test)
}

#' @export
register_definitions = function(df, only_test = FALSE, con = NULL){
  uniq = unique_fields()[[.ghEnv$meta$arrDefinition]]
  test_register_definition(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  
  # Manual correction as units are empty in template
  if (class(df[, 'units']) == 'logical') {
    df[, 'units'] = as.character(df[, 'units'])
  }
  
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrDefinition)
    ids = register_tuple_return_id(df, arrayname, uniq, con = con)
    
    # force update the cache
    update_definition_cache(con = con)
    
    return(ids)
  } # end of if (!only_test)
}

register_feature_synonym = function(df, only_test = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  uniq = unique_fields()[[.ghEnv$meta$arrFeatureSynonym]]
  test_register_feature_synonym(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrFeatureSynonym)
    ids = register_tuple_return_id(df, arrayname, uniq, con = con)
    
    # force update the cache
    update_feature_synonym_cache(con = con)
    
    return(ids)
  } # end of if (!only_test)
}

#' register gene_symbols
#' 
#' @param df data-frame containing gene_symbol and full name
#' 
#' @return gene_symbol_id assigned by the data-base
#' @export
register_gene_symbol = function(df, only_test = FALSE, con = NULL){
  uniq = unique_fields()[[.ghEnv$meta$arrGeneSymbol]]
  test_register_gene_symbol(df, uniq, silent = ifelse(only_test, FALSE, TRUE))
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrGeneSymbol)
    ids = register_tuple_return_id(df,
                                   arrayname, uniq, con = con)
    
    # force update the ontology
    update_gene_symbol_cache(con = con)
    
    return(ids)
  } # end of if (!only_test)
}
