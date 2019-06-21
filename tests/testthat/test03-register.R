context("test-register.R")


test_that("register_biosample with reserved column `sample_name` fails", {
  e = try({
    register_biosample(df = data.frame(sample_name = c('a', 'b'), 
                                       dataset_id = 1000, 
                                       name = c('a', 'b'), 
                                       description = '...', 
                                       individual_id = 3))
    }, silent = TRUE)
  expect_true(class(e) == "try-error")
  expect_true(length(grep("reserved.*sample_name", e[1])) ==1)
})

test_that("Check that variant_key registration works properly", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({rg_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = .ghEnv$meta$arrVariantKey, force = TRUE)
    # Get the existing variant_key fields
    vk1 = get_variant_key()
    expect_true(nrow(vk1) == 0)
    
    # Register a dummy variant_key field
    dummy_val = "dummy"
    new_variant_key_id = register_variant_key(
      df = data.frame(key = dummy_val, stringsAsFactors = FALSE))
    # Check that cache is increased by 1 element
    vk2 = get_variant_key()
    expect_true(nrow(vk2) == nrow(vk1) + 1)
    
    # Verify that the dummy key was uploaded properly
    expect_true(get_variant_key(variant_key_id = new_variant_key_id)$key == dummy_val)
    
    # Delete the dummy variant_key field
    delete_entity(entity = .ghEnv$meta$arrVariantKey, id = new_variant_key_id)
    # Check that the cache is updated, and count has decreased by 1
    vk3 = get_variant_key()
    expect_true(nrow(vk3) == nrow(vk1))
    expect_true(nrow(get_variant_key(variant_key_id = new_variant_key_id)) == 0)
    
    ###### PHASE 2A #####
    # Now upload two keys at a time
    dummy_val_2a = c("dummy1", "dummy2")
    new_variant_key_id_2a = register_variant_key(
      df = data.frame(key = dummy_val_2a, stringsAsFactors = FALSE))
    expect_true(length(new_variant_key_id_2a) == 2)
    
    # Now upload two keys at a time
    dummy_val_2b = c("dummy1", "dummy3")
    new_variant_key_id_2b = register_variant_key(
      df = data.frame(key = dummy_val_2b, stringsAsFactors = FALSE))
    expect_true(length(new_variant_key_id_2b) == 2)
    expect_true(all(
      get_variant_key(variant_key_id = new_variant_key_id_2b)$key %in% 
               c("dummy1", "dummy3")))
    expect_true(identical(sort(unique(get_variant_key()$key)), 
                    sort(unique(c(dummy_val_2a, dummy_val_2b)))))
    
    # Clean-up
    init_db(arrays_to_init = c(.ghEnv$meta$arrVariantKey), force = TRUE)
  }
})

test_that("Check that chromosome_key registration works properly", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({rg_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = .ghEnv$meta$arrChromosomeKey, force = TRUE)
    # Get the existing chromosome_key fields
    ck1 = revealgenomics:::get_chromosome_key()
    expect_true(nrow(ck1) == 0)
    
    # Register a dummy chromosome_key field
    dummy_val = "chrX"
    new_chromosome_key_id = register_chromosome_key(
      df = data.frame(chromosome = dummy_val, stringsAsFactors = FALSE))
    # Check that cache is increased by 1 element
    ck2 = revealgenomics:::get_chromosome_key()
    expect_true(nrow(ck2) == nrow(ck1) + 1)
    
    # Verify that the dummy chromosome was uploaded properly
    expect_true(revealgenomics:::get_chromosome_key(chromosome_key_id = new_chromosome_key_id)$chromosome == dummy_val)
    
    # Delete the dummy chromosome_key field
    delete_entity(entity = .ghEnv$meta$arrChromosomeKey, id = new_chromosome_key_id)
    # Check that the cache is updated, and count has decreased by 1
    ck3 = get_chromosome_key()
    expect_true(nrow(ck3) == nrow(ck1))
    expect_true(nrow(get_chromosome_key(chromosome_key_id = new_chromosome_key_id)) == 0)
    
    ###### PHASE 2A #####
    # Now upload two keys at a time
    dummy_val_2a = c("chr1", "chr2")
    new_chromosome_key_id_2a = register_chromosome_key(
      df = data.frame(chromosome = dummy_val_2a, stringsAsFactors = FALSE))
    expect_true(length(new_chromosome_key_id_2a) == 2)
    
    # Now upload two keys at a time
    dummy_val_2b = c("chr1", "chr3")
    new_chromosome_key_id_2b = register_chromosome_key(
      df = data.frame(chromosome = dummy_val_2b, stringsAsFactors = FALSE))
    expect_true(length(new_chromosome_key_id_2b) == 2)
    expect_true(all(
      revealgenomics:::get_chromosome_key(chromosome_key_id = new_chromosome_key_id_2b)$chromosome %in% 
        c("chr1", "chr3")))
    expect_true(identical(sort(unique(revealgenomics:::get_chromosome_key()$chromosome)), 
                          sort(unique(c(dummy_val_2a, dummy_val_2b)))))
    
    # Clean-up
    init_db(arrays_to_init = c(.ghEnv$meta$arrChromosomeKey), force = TRUE)
  }
})

test_that("Check that variant registration works properly", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({rg_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = c(
      .ghEnv$meta$arrVariantKey,
      .ghEnv$meta$arrVariant,
      .ghEnv$meta$arrFeature,
      .ghEnv$meta$arrFeatureSynonym,
      .ghEnv$meta$arrBiosample,
      .ghEnv$meta$arrMeasurementSet,
      .ghEnv$meta$arrDataset,
      .ghEnv$meta$arrMetadataAttrKey,
      .ghEnv$meta$arrGeneSymbol
      ), 
      force = TRUE)
    
    # Register dummy dataset
    dataset_id = register_dataset(df = data.frame(
      name = 'dummyDataset', 
      description = '...', 
      project_id = 1, 
      stringsAsFactors = FALSE))
    
    # Register features at 1:3
    ftr_id_res = register_feature(df = data.frame(
      name = c('asdf1', 'asdf2', 'asdf3'), 
      featureset_id = 1, 
      gene_symbol = 'asdf', 
      chromosome = 'X', 
      start = '1', 
      end = '10000', 
      feature_type = 'dummy', 
      source = 'api-test', 
      stringsAsFactors = FALSE), 
      register_gene_synonyms = FALSE)
    expect_true(length(ftr_id_res$feature_id) == 3)
    expect_true(is.null(ftr_id_res$feature_synonym_id))
    
    # Register_biosamples at id 1
    bios_id = register_biosample(df = data.frame(
      name = 'dummy_biosample', 
      dataset_id = 1, 
      description = '...', 
      individual_id = 10000000, 
      stringsAsFactors = FALSE), 
      dataset_version = 1)
    
    # Register measurementset at id 1, 2
    ms_id = register_measurementset(df = data.frame(
      name = c('dummy_measurementset1', 'dummy_measurementset2'), 
      dataset_id = 1, 
      experimentset_id = 10000, 
      entity = .ghEnv$meta$arrVariant, 
      description = '...', 
      featureset_id = 1, 
      stringsAsFactors = FALSE), 
      dataset_version = 1)
    
    # Register variant
    df_var1 = data.frame(
      dataset_id = 1, 
      measurementset_id = 1, 
      feature_id = c(1, 1, 2), 
      biosample_id = 1, 
      chromosome = 'X', 
      start = c(11,33, 44),  
      end = c(11, 33, 44), 
      id = 'asdf', 
      reference = c('T', 'G', 'A'), 
      alternate = c('G', 'T', 'C'), 
      stringsAsFactors = FALSE)
    
    register_variant(df = df_var1)
    df_var1_res = search_variant(
      measurementset = get_measurementsets(measurementset_id = 1), 
      feature = get_features(feature_id = ftr_id_res$feature_id))
    expect_true(all.equal(
      (dim(df_var1) + c(0,2)), #' two extra columns added are: `per_gene_variant_number`, `dataset_version` 
      dim(df_var1_res)))
    df_var1_res = df_var1_res[, colnames(df_var1)]
    expect_true(all.equal(
      as.matrix(df_var1_res), 
      as.matrix(df_var1)))
    
    # Some extra columns added
    df_var2 = cbind(df_var1, 
                    data.frame(newColumn1 = 
                                rep('333', nrow(df_var1)),
                              newColumn2 = 
                                c('asdf', 'jkl', 'ppp'), 
                              stringsAsFactors = FALSE))
    df_var2$measurementset_id = 2
    register_variant(df = df_var2)
    df_var2_res = search_variant(
      measurementset = get_measurementsets(measurementset_id = 2), 
      feature = get_features(feature_id = ftr_id_res$feature_id))
    expect_true(all.equal(
      (dim(df_var2) + c(0,2)),
      dim(df_var2_res)))
    df_var2_res = df_var2_res[, colnames(df_var2)]
    expect_true(all.equal(
      as.matrix(df_var2_res), 
      as.matrix(df_var2)))

    # Clean-up
    init_db(arrays_to_init = c(
      .ghEnv$meta$arrVariantKey,
      .ghEnv$meta$arrVariant,
      .ghEnv$meta$arrFeature,
      .ghEnv$meta$arrFeatureSynonym,
      .ghEnv$meta$arrBiosample,
      .ghEnv$meta$arrMeasurementSet,
      .ghEnv$meta$arrDataset,
      .ghEnv$meta$arrMetadataAttrKey,
      .ghEnv$meta$arrGeneSymbol
    ), 
    force = TRUE)
  }
})

test_that("Check that ontology_category registration works properly", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({rg_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = .ghEnv$meta$arrOntologyCategory, force = TRUE)
    # Get the existing variant_key fields
    oc1 = revealgenomics:::get_ontology_category()
    expect_true(nrow(oc1) == 0)
    
    # Register a dummy ontology_category field
    dummy_category = "dummy"
    new_ontology_category_id = revealgenomics:::register_ontology_category(
      df = data.frame(ontology_category = dummy_category, stringsAsFactors = FALSE))
    # Check that cache is increased by 1 element
    oc2 = revealgenomics:::get_ontology_category()
    expect_true(nrow(oc2) == nrow(oc1) + 1)
    
    # Verify that the dummy key was uploaded properly
    expect_true(revealgenomics:::get_ontology_category(ontology_category_id = new_ontology_category_id)$ontology_category == dummy_category)
    
    # Delete the dummy ontology_category field
    delete_entity(entity = .ghEnv$meta$arrOntologyCategory, id = new_ontology_category_id)
    # Check that the cache is updated, and count has decreased by 1
    oc3 = revealgenomics:::get_ontology_category()
    expect_true(nrow(oc3) == nrow(oc1))
    expect_true(nrow(revealgenomics:::get_ontology_category(ontology_category_id = new_ontology_category_id)) == 0)
    
    ###### PHASE 2A #####
    # Now upload two keys at a time
    dummy_category_2a = c("dummy1", "dummy2")
    new_ontology_category_id_2a = revealgenomics:::register_ontology_category(
      df = data.frame(ontology_category = dummy_category_2a, stringsAsFactors = FALSE))
    expect_true(length(new_ontology_category_id_2a) == 2)
    
    # Now upload two keys at a time
    dummy_category_2b = c("dummy1", "dummy3")
    new_ontology_category_id_2b = revealgenomics:::register_ontology_category(
      df = data.frame(ontology_category = dummy_category_2b, stringsAsFactors = FALSE))
    expect_true(length(new_ontology_category_id_2b) == 2)
    expect_true(all(
      revealgenomics:::get_ontology_category(ontology_category_id = new_ontology_category_id_2b)$ontology_category %in% 
        c("dummy1", "dummy3")))
    expect_true(identical(sort(unique(revealgenomics:::get_ontology_category()$ontology_category)), 
                          sort(unique(c(dummy_category_2a, dummy_category_2b)))))
    
    # Clean-up
    init_db(arrays_to_init = c(.ghEnv$meta$arrOntologyCategory), force = TRUE)
  }
})

