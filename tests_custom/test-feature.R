
test_feature_synonym_registration = function() {
  library(revealgenomics); gh_connect('root', 'Paradigm4')
  
  # Any random referenceset
  refset = get_referenceset()
  if (nrow(refset) >= 1) { # If ReferenceSet has been  filled by this time
    refset_id = refset[1, ]$referenceset_id
  } else {
    refset_id = -1
  }
  
  fset_id = register_featureset(df = data.frame(referenceset_id = refset_id,
                                             name = "debug_test_featureset",
                                             description = "...",
                                             source_uri = "...", 
                                             stringsAsFactors = FALSE))
  
  test_gene_names = c('debug_test_ASDF', 'debug_test_JKL')
  
  df_ftr = data.frame(name = test_gene_names,
                      gene_symbol = c('null', 'null'),
                      featureset_id = fset_id,
                      chromosome = "debug_test_chromosome",
                      start = -1,
                      end = -1,
                      feature_type = "gene",
                      source = "...")
  
  feature_record = register_feature(df = df_ftr, register_gene_synonyms = TRUE)
  
  res1 = search_feature_by_synonym(synonym = test_gene_names[1], 
                            featureset_id = fset_id,
                            updateCache = TRUE)
  res2 = search_feature_by_synonym(synonym = test_gene_names[2], 
                                   featureset_id = fset_id)
  
  stopifnot(c(res1$feature_id, res2$feature_id) %in% 
              feature_record$feature_id)
  
  # Clean up
  delete_entity(entity = 'FEATURESET',      id = fset_id)
  delete_entity(entity = 'FEATURE',         id = feature_record$feature_id)
  delete_entity(entity = 'FEATURE_SYNONYM', id = feature_record$feature_synonym_id)
}

