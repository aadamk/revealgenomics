context("test-download.R")

#### feature download works when feature_id is a long integer ####
test_that("Check that feature download works when feature_id is a long integer ", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({gh_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = c(.ghEnv$meta$arrFeature, 
                               .ghEnv$meta$arrGeneSymbol,
                               .ghEnv$meta$arrFeatureSynonym), force = TRUE)
    feature_id = register_feature(df = data.frame(name = "dummyFeature", 
                                     gene_symbol = "dummySymbol",
                                     featureset_id = 1000, 
                                     chromosome="...", 
                                     start="...", 
                                     end="...", 
                                     feature_type = "gene", 
                                     source = "...", 
                                     stringsAsFactors = FALSE))
    # https://github.com/Paradigm4/revealgenomics/issues/40
    ff = get_features(feature_id = seq(70000,800000,10))
    expect_true(nrow(ff) == 0) # in tests, currently do not expect to upload 700K+ features
    
    expect_true(nrow(get_features()) == 1)
    
    # Clean-up
    init_db(arrays_to_init = c(.ghEnv$meta$arrFeature, 
                               .ghEnv$meta$arrGeneSymbol,
                               .ghEnv$meta$arrFeatureSynonym), force = TRUE)
    
  }
})

#### Simple test: feature registration ####
test_that("Simple check that feature registration handles synonyms appropriately", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({gh_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = c(.ghEnv$meta$arrReferenceset, 
                               .ghEnv$meta$arrFeatureset, 
                               .ghEnv$meta$arrFeature, 
                               .ghEnv$meta$arrGeneSymbol,
                               .ghEnv$meta$arrFeatureSynonym), force = TRUE)
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
                        gene_symbol = c('ASDF', 'JKL'),
                        featureset_id = fset_id,
                        chromosome = "debug_test_chromosome",
                        start = '-1',
                        end = '-1',
                        feature_type = "gene",
                        source = "...")
    
    feature_record = register_feature(df = df_ftr, register_gene_synonyms = TRUE)
    
    res1 = search_feature_by_synonym(synonym = test_gene_names[1], 
                                     featureset_id = fset_id,
                                     updateCache = TRUE)
    res2 = search_feature_by_synonym(synonym = test_gene_names[2], 
                                     featureset_id = fset_id)
    
    expect_true(all(c(res1$feature_id, res2$feature_id) %in% 
                feature_record$feature_id))
    
    # Clean-up
    init_db(arrays_to_init = c(.ghEnv$meta$arrReferenceset, 
                               .ghEnv$meta$arrFeatureset, 
                               .ghEnv$meta$arrFeature, 
                               .ghEnv$meta$arrGeneSymbol,
                               .ghEnv$meta$arrFeatureSynonym), force = TRUE)
    
  }
})

#### Advanced test: feature registration ####
test_that("Advanced check that feature registration handles synonyms appropriately", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({gh_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = c(.ghEnv$meta$arrReferenceset, 
                               .ghEnv$meta$arrFeatureset, 
                               .ghEnv$meta$arrFeature, 
                               .ghEnv$meta$arrGeneSymbol,
                               .ghEnv$meta$arrFeatureSynonym), force = TRUE)
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
    
    feature_record = build_reference_gene_set(featureset_id = fset_id)
    
    ftrs = get_features()
    fsyn = revealgenomics:::get_feature_synonym()
    
    expect_true(length(unique(fsyn$feature_id)) == nrow(ftrs))
    expect_true(nrow(ftrs) == 55980)
    expect_true(nrow(fsyn) == 544383)
    
    cat("Check that HGNC:1 is a synonym of A12M1; https://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=1\n")
    spotCheckDf1 = search_feature_by_synonym(synonym = 'HGNC:1')
    expect_true(spotCheckDf1$name == 'A12M1')
    
    cat("Check that EGFR links to Ensembl ID ENSG00000146648; https://useast.ensembl.org/Homo_sapiens/Gene/Summary?g=ENSG00000146648;r=7:55019021-55211628\n")
    spotCheckDf2a = search_feature_by_synonym(synonym = 'EGFR')
    spotCheckDf2b = search_feature_by_synonym(synonym = spotCheckDf2a$name)
    expect_true(spotCheckDf2a$name == 'ENSG00000146648')
    expect_identical(spotCheckDf2a, spotCheckDf2b)
    
    cat("Check that CCDS48126 and CCDS48128 point to the same gene-symbol;
        http://useast.ensembl.org/Homo_sapiens/Transcript/Summary?g=ENSG00000204382;r=X:52512080-52520803;t=ENST00000518075;
        http://useast.ensembl.org/Homo_sapiens/Transcript/Summary?g=ENSG00000204382;r=X:52512077-52517057;t=ENST00000375613")
    spotCheckDf3a = search_feature_by_synonym(synonym = "CCDS48126")
    spotCheckDf3b = search_feature_by_synonym(synonym = "CCDS48128")
    expect_equal(spotCheckDf3a$gene_symbol, spotCheckDf3b$gene_symbol)
    spotCheckDf3c = search_features(gene_symbol = spotCheckDf3a$gene_symbol)
    expect_equal(as.character(spotCheckDf3a), 
                 as.character(spotCheckDf3c[spotCheckDf3c$feature_id == 
                                              spotCheckDf3a$feature_id, ]))
    expect_equal(as.character(spotCheckDf3a), 
                 as.character(spotCheckDf3c[spotCheckDf3c$feature_id == 
                                 spotCheckDf3a$feature_id, ]))
    
    cat("Check that gene symbols have been registered, and do some spot checks\n")
    df_gs = get_gene_symbol()
    expect_true(nrow(df_gs) == 55874)
    expect_true(length(unique(df_gs$gene_symbol)) == nrow(df_gs))
    expect_true(length(unique(df_gs$gene_symbol_id)) == nrow(df_gs))
    
    expect_true(
      search_gene_symbols(gene_symbol = 'EGFR')$full_name == 
        'epidermal growth factor receptor')
    expect_true(
      search_gene_symbols(gene_symbol = 'KRAS')$full_name == 
        'KRAS proto-oncogene, GTPase')
    expect_true(
      search_gene_symbols(gene_symbol = 'TP53')$full_name == 
        'tumor protein p53')
    
    cat("Add one protein probe linked to two gene symbols\n
        Make sure this registers two entries in Features DB but at same feature_id\n")
    prot_probe_res = register_feature(df = data.frame(
      name = "test_protein_probe_1", 
      gene_symbol = c("GCF1", "KRAS"), 
      featureset_id = 1, 
      chromosome="...", 
      start="...", end="...", 
      feature_type = "protein_probe", 
      source = "...",
      flex_field1 = 'any_value_here',
      flex_field2 = 'another_value_here',
      stringsAsFactors = FALSE))
    expect_equal(
      sort(get_gene_symbol(gene_symbol_id = prot_probe_res$gene_symbol_id)$gene_symbol),
      c("GCF1", "KRAS")
    )
    expect_equal(
      nrow(prot_probe_res$feature_id_df),
      2
    )
    expect_equal(
      length(unique(prot_probe_res$feature_id)), 
      1)
    expect_null(prot_probe_res$feature_synonym_id)
    
    prot_probe_res2 = register_feature(
      df = data.frame(
        name = c("test_protein_probe_1", "test_protein_probe_1", "test_protein_probe_2"), 
        gene_symbol = c("EGFR", "KRAS", "MYC"), 
        featureset_id = 1, 
        chromosome="...", 
        start="...", 
        end="...", 
        feature_type = "protein_probe", 
        source = "...",
        flex_field1 = 'any_value_here',
        flex_field2 = 'another_value_here',
        flex_field3 = 'yet_another_value_here',
        stringsAsFactors = FALSE))
    expect_equal(
      sort(get_gene_symbol(gene_symbol_id = prot_probe_res2$gene_symbol_id)$gene_symbol),
      c("EGFR", "KRAS", "MYC")
    )
    expect_equal(
      nrow(prot_probe_res2$feature_id_df),
      3
    )
    expect_equal(
      length(unique(prot_probe_res2$feature_id)), 
      2)
    expect_equal(
      sum(prot_probe_res2$feature_id %in% prot_probe_res$feature_id),
      2
    )
    expect_null(prot_probe_res2$feature_synonym_id)
    
    cat("Now check for downloads\n")
    expect_equal(
      nrow(get_features(feature_id = prot_probe_res2$feature_id)),
      4
    )
    ftr1 = search_features(gene_symbol = 'EGFR', feature_type = 'protein_probe')
    ftr2 = search_features(gene_symbol = 'KRAS', feature_type = 'protein_probe')
    expect_equal(
      ftr1$feature_id,
      ftr2$feature_id
    )
    
    expect_equal(
      sort(search_features(gene_symbol = 'EGFR')$feature_type),
      c("gene", "protein_probe")
    )
    
    # Clean-up
    init_db(arrays_to_init = c(.ghEnv$meta$arrReferenceset, 
                               .ghEnv$meta$arrFeatureset, 
                               .ghEnv$meta$arrFeature, 
                               .ghEnv$meta$arrGeneSymbol,
                               .ghEnv$meta$arrFeatureSynonym), force = TRUE)
    
  }
})
