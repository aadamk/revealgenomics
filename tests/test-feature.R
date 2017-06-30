
test_feature_synonym_registration = function() {
  library(scidb4gh); gh_connect('root', 'Paradigm4')
  
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
                      reference_name = "debug_test_reference_name",
                      start = -1,
                      end = -1,
                      strand_term = search_ontology(terms = "strand_term_unspecified"),
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

# Test the ability to auto register gene synonyms using MMRF data
test_automatic_feature_synonym_registration = function(){
  library(scidb4gh); gh_connect('root', 'Paradigm4')
  counts0 = get_entity_count(new_function = TRUE)

  CODEPATH <- "~/coding/jnj/sow2/load/"
  mmrf_base_path = "/app/scidb_scratch/sow2-data/20161220_Data_and_Layout/Data/MMRF/"
  source(file.path(CODEPATH, "mmrf/mmrf-helper-functions.R")) ## specific MMRF functions not included in library.
  source(file.path(CODEPATH, "mmrf/feature-helper-functions.R")) ## feature (gene/transcript) specific functions not included in library yet (probably will merge eventually)
  
  # Dummy featureset
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
  
  # Build gene list at featureset
  ftr_ref_record = build_reference_gene_set(featureset_id = fset_id,
                           gene_annotation_file_path = '/app/scidb_scratch/sow2-data/gene/hugo/hgnc_complete_set.txt', 
                           gene_location_file_path = '/app/scidb_scratch/sow2-data/gene/grch38_release85_homosap_gene/newGene.tsv')
  
  # when loading MMRF genes from Kallisto gene counts, new gene synonyms are uploaded
  mmrf_yaml_filepath = file.path(CODEPATH, "mmrf/README_file_descriptions_MMRF_IA9.yaml")
  mmrf_files_path = file.path(mmrf_base_path, "IA9", "summary_flat_files")
  rqsets_all = mmrf_get_measurement_list(mmrf_yaml_filepath = mmrf_yaml_filepath,
                                          mmrf_files_path = mmrf_files_path,
                                          measurement_type = 'rnaquantificationset')
  rqset = rqsets_all[grep("GTF.*Sailfish.*Gene", rqsets_all$name), ]
  if (nrow(rqset) != 1) stop("Need to work on only one GCT file here -- one with 52K+ genes")
  
  if (rqset$feature_id_type == 'ENSG_ID') {
    feature_type = "gene"
  } else if (rqset$feature_id_type == 'ENST_ID') {
    feature_type = "transcript"
  } else stop("ENSG_ID or ENST_ID expected")
  
  # FEATURES
  f1 = data.frame(name = mmrf_get_features_from_file(gctfilepath = rqset$file_path))
  df_feature = feature_merge_with_grch38_r85(f1 = f1, feature_type = feature_type)
  
  df_feature$featureset_id = fset_id
  
  feature_record = register_feature(df = df_feature, register_gene_synonyms = TRUE)
  
  # Make sure that all the MMRF genes are recorded as synonyms
  ftr_syn = scidb4gh:::get_feature_synonym()
  ftr_syn = ftr_syn[ftr_syn$featureset_id == fset_id, ]
  stopifnot(all(df_feature$name %in% ftr_syn$synonym))
  
  # Clean up
  uploaded_ftr  = sort(unique(c(ftr_ref_record$feature_id, feature_record$feature_id)))
  new_ftr_in_db = sort(unique(search_features(featureset_id = fset_id)$feature_id))
  stopifnot(uploaded_ftr == new_ftr_in_db)
  
  uploaded_syn = sort(unique(c(ftr_ref_record$feature_synonym_id,
                               feature_record$feature_synonym_id)))
  new_syn_in_db = sort(unique(ftr_syn$feature_synonym_id))
  stopifnot(uploaded_syn == new_syn_in_db)
  
  delete_entity(entity = 'FEATURESET',      id = fset_id)
  delete_entity(entity = 'FEATURE',         id = uploaded_ftr)
  delete_entity(entity = 'FEATURE_SYNONYM', id = uploaded_syn)
  
  counts1 = get_entity_count(new_function = TRUE)
  stopifnot(all.equal(counts0, counts1))
}