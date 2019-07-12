context("test04-features: ")

#### feature download works when feature_id is a long integer ####
test_that("Check that feature download works when feature_id is a long integer ", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({rg_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = c(.ghEnv$meta$arrFeature, 
                               .ghEnv$meta$arrGeneSymbol,
                               .ghEnv$meta$arrFeatureSynonym, 
                               .ghEnv$meta$arrMetadataAttrKey), 
            force = TRUE)
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
                               .ghEnv$meta$arrFeatureSynonym, 
                               .ghEnv$meta$arrMetadataAttrKey), 
            force = TRUE)
    
  }
})

#### Simple test: feature registration ####
test_that("Simple check that feature registration handles synonyms appropriately", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({rg_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = c(.ghEnv$meta$arrReferenceset, 
                               .ghEnv$meta$arrFeatureset, 
                               .ghEnv$meta$arrFeature, 
                               .ghEnv$meta$arrGeneSymbol,
                               .ghEnv$meta$arrFeatureSynonym, 
                               .ghEnv$meta$arrMetadataAttrKey), force = TRUE)
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
                        source = "...", 
                        stringsAsFactors = FALSE)
    
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
                               .ghEnv$meta$arrFeatureSynonym, 
                               .ghEnv$meta$arrMetadataAttrKey), force = TRUE)
    
  }
})

#### Advanced test: feature registration ####
test_that("Advanced check that feature registration handles synonyms appropriately", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({rg_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = c(.ghEnv$meta$arrReferenceset, 
                               .ghEnv$meta$arrFeatureset, 
                               .ghEnv$meta$arrFeature, 
                               .ghEnv$meta$arrGeneSymbol,
                               .ghEnv$meta$arrFeatureSynonym, 
                               .ghEnv$meta$arrMetadataAttrKey), force = TRUE)
    # Any random referenceset
    refset = get_referenceset()
    if (nrow(refset) >= 1) { # If ReferenceSet has been  filled by this time
      refset_id = refset[1, ]$referenceset_id
    } else {
      refset_id = -1
    }
    
    fset_id = register_featureset(df = data.frame(referenceset_id = refset_id,
                                                  name = "debug_test_featureset_grch38",
                                                  description = "...",
                                                  source_uri = "...", 
                                                  stringsAsFactors = FALSE))
    
    feature_record = build_reference_gene_set(featureset_id = fset_id)
    
    ftrs = get_features()
    ftrs_mandatory_fields = get_features(mandatory_fields_only = TRUE)
    fsyn = revealgenomics:::get_feature_synonym()
    
    expect_true(length(unique(fsyn$feature_id)) == nrow(ftrs))
    expect_true(ncol(ftrs) > ncol(ftrs_mandatory_fields))
    expect_true(nrow(ftrs) == nrow(ftrs_mandatory_fields))
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
    # First set of probes: No previously existing probes, so matching code is not invoked
    # probe1 <==> (GCF1, KRAS)
    # probe2 <==> (EGFR)
    df_probe1 = data.frame(
      name = c("probe1", "probe1", "probe2") ,
      gene_symbol = c("GCF1", "KRAS", "EGFR"), 
      featureset_id = 1, 
      chromosome="...", 
      start="...", end="...", 
      feature_type = "protein_probe", 
      source = "...",
      flex_field1 = 'any_value_here',
      flex_field2 = 'another_value_here',
      stringsAsFactors = FALSE)
    prot_probe_res = register_feature(df = df_probe1)
    expect_equal(
      sort(get_gene_symbol(gene_symbol_id = prot_probe_res$gene_symbol_id)$gene_symbol),
      c("EGFR", "GCF1", "KRAS")
    )
    expect_equal(
      nrow(prot_probe_res$feature_id_df),
      3
    )
    expect_equal(
      length(unique(prot_probe_res$feature_id)), 
      2)
    expect_null(prot_probe_res$feature_synonym_id)
    
    # Next set of probes: Matching code will be invoked
    # probe1 <==> (GCF1) # previously registered
    # probe2 <==> (EGFR, MYC) # first one previously registered, but second not registered
    # probe3 <==> (TP53)
    df_probe2 = data.frame(
      name = c("probe1", "probe2", "probe2", "probe3"), 
      gene_symbol = c("GCF1", "EGFR", "MYC", "TP53"), 
      featureset_id = 1, 
      chromosome="...", 
      start="...", 
      end="...", 
      feature_type = "protein_probe", 
      source = "...",
      flex_field1 = 'any_value_here',
      flex_field2 = 'another_value_here',
      flex_field3 = 'yet_another_value_here',
      stringsAsFactors = FALSE)
    prot_probe_res2 = register_feature(
      df = df_probe2)
    expect_equal(
      sort(get_gene_symbol(gene_symbol_id = prot_probe_res2$gene_symbol_id)$gene_symbol),
      sort(df_probe2$gene_symbol)
    )
    # unique number of feature ids assigned by upload-2 should be equal to 
    # number of rows in input data frame
    expect_equal(
      nrow(prot_probe_res2$feature_id_df),
      nrow(df_probe2)
    )
    # unique number of feature ids assigned by upload-2 should be equal to
    # number of unique probe names
    expect_equal(
      length(unique(prot_probe_res2$feature_id)), 
      length(unique(df_probe2$name)))
    # Two of the feature ids in upload-2 should overlap with upload-1
    expect_equal(
      sum(unique(prot_probe_res2$feature_id) %in% prot_probe_res$feature_id),
      2
    )
    expect_null(prot_probe_res2$feature_synonym_id)
    
    cat("Now check for downloads\n")
    # Total number of probes should be equal to unique probe-gene combinations
    str1 = paste0(df_probe1$name, "__", df_probe1$gene_symbol)
    str2 = paste0(df_probe2$name, "__", df_probe2$gene_symbol)
    # Retrieve by search
    expect_equal(
      nrow(search_features(feature_type = 'protein_probe')),
      length(unique(c(str1, str2)))
    )
    # Retrieve by search -- mandatory_fields_only
    expect_equal(
      nrow(search_features(feature_type = 'protein_probe', mandatory_fields_only = TRUE)),
      length(unique(c(str1, str2)))
    )
    # Retrieve by get
    expect_equal(
      nrow(get_features(
        feature_id = unique(
          c(prot_probe_res$feature_id, 
            prot_probe_res2$feature_id)
        ))),
      length(unique(c(str1, str2)))
    )
    
    # Retrieve by get -- mandatory_fields_only
    expect_equal(
      nrow(get_features(
        feature_id = unique(
          c(prot_probe_res$feature_id, 
            prot_probe_res2$feature_id)
        ), mandatory_fields_only = TRUE)),
      length(unique(c(str1, str2)))
    )
    
    ftr1 = search_features(gene_symbol = 'GCF1', feature_type = 'protein_probe')
    ftr2 = search_features(gene_symbol = 'KRAS', feature_type = 'protein_probe')
    expect_equal(
      ftr1$feature_id,
      ftr2$feature_id
    )
    
    expect_equal(
      sort(search_features(gene_symbol = 'EGFR')$feature_type),
      c("gene", "protein_probe")
    )
    
    # Tests that `get_features()` works with and without mandatory fields flag
    # All features
    ff_all1 = get_features(feature_id = NULL)
    ff_all2 = get_features(feature_id = NULL, mandatory_fields_only = TRUE)
    expect_gt(
      nrow(ff_all1), 0
    )
    expect_equal(
      nrow(ff_all2),
      nrow(ff_all2)
    )
    expect_equal(
      sort(ff_all1$feature_id),
      sort(ff_all2$feature_id)
    )
    
    # 10 features
    vec10 = sample(c(1:max(ff_all1$feature_id)), size = 10)
    ff_10_1 = get_features(feature_id = vec10)
    ff_10_2 = get_features(feature_id = vec10, 
                           mandatory_fields_only = TRUE)
    expect_equal(
      nrow(ff_10_1), 10
    )
    expect_equal(
      nrow(ff_10_1),
      nrow(ff_10_2)
    )
    expect_equal(
      sort(ff_10_1$feature_id),
      sort(ff_10_2$feature_id)
    )
    
    # 1000 features
    vec1000 = sample(c(1:max(ff_all1$feature_id)), size = 1000)
    ff_1000_1 = get_features(feature_id = vec1000)
    ff_1000_2 = get_features(feature_id = vec1000, 
                           mandatory_fields_only = TRUE)
    expect_equal(
      nrow(ff_1000_1), 1000
    )
    expect_equal(
      nrow(ff_1000_1),
      nrow(ff_1000_2)
    )
    expect_equal(
      sort(ff_1000_1$feature_id),
      sort(ff_1000_2$feature_id)
    )
    
    # 30000 features -- must use as.scidb
    vec30k = sample(c(1:max(ff_all1$feature_id)), size = 30000)
    ff_30k_1 = get_features(feature_id = vec30k)
    ff_30k_2 = get_features(feature_id = vec30k, 
                             mandatory_fields_only = TRUE)
    expect_true(
      nrow(ff_30k_1) >= 30000 # protein probe features can have one feature_id mapping to multiple feature_id, gene_symbol_id tuples
    )
    expect_equal(
      nrow(ff_30k_1),
      nrow(ff_30k_2)
    )
    expect_equal(
      sort(ff_30k_1$feature_id),
      sort(ff_30k_2$feature_id)
    )
    
    # Clean-up
    init_db(arrays_to_init = c(.ghEnv$meta$arrReferenceset, 
                               .ghEnv$meta$arrFeatureset, 
                               .ghEnv$meta$arrFeature, 
                               .ghEnv$meta$arrGeneSymbol,
                               .ghEnv$meta$arrFeatureSynonym, 
                               .ghEnv$meta$arrMetadataAttrKey), force = TRUE)
  }
})
