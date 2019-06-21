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
    # Get the existing ontology_category fields -- one category should already exist == 'uncategorized'
    oc1 = revealgenomics:::get_ontology_category()
    expect_true(nrow(oc1) == 1)
    
    expect_equal(
      ontology_category_id = revealgenomics:::search_ontology_category(ontology_category = 'uncategorized')$ontology_category_id,
      1)
    
    # Register a dummy ontology_category field
    ontology_category_category = "ontology_category"
    new_ontology_category_id = revealgenomics:::register_ontology_category(
      df = data.frame(ontology_category = ontology_category_category, stringsAsFactors = FALSE))
    # Check that cache is increased by 1 element
    oc2 = revealgenomics:::get_ontology_category()
    expect_true(nrow(oc2) == nrow(oc1) + 1)
    
    # Verify that the ontology_category key was uploaded properly
    expect_true(revealgenomics:::get_ontology_category(ontology_category_id = new_ontology_category_id)$ontology_category == ontology_category_category)
    
    # Delete the ontology_category field
    delete_entity(entity = .ghEnv$meta$arrOntologyCategory, id = new_ontology_category_id)
    # Check that the cache is updated, and count has decreased by 1
    oc3 = revealgenomics:::get_ontology_category()
    expect_true(nrow(oc3) == nrow(oc1))
    expect_true(nrow(revealgenomics:::get_ontology_category(ontology_category_id = new_ontology_category_id)) == 0)
    
    ###### PHASE 2A #####
    # Now upload two keys at a time
    ontology_category_2a = c("ontology_category1", "ontology_category2")
    new_ontology_category_id_2a = revealgenomics:::register_ontology_category(
      df = data.frame(ontology_category = ontology_category_2a, stringsAsFactors = FALSE))
    expect_true(length(new_ontology_category_id_2a) == 2)
    
    # Now upload two keys at a time
    ontology_category_2b = c("ontology_category1", "ontology_category3")
    new_ontology_category_id_2b = revealgenomics:::register_ontology_category(
      df = data.frame(ontology_category = ontology_category_2b, stringsAsFactors = FALSE))
    expect_true(length(new_ontology_category_id_2b) == 2)
    expect_true(all(
      revealgenomics:::get_ontology_category(ontology_category_id = new_ontology_category_id_2b)$ontology_category %in% 
        c("ontology_category1", "ontology_category3")))
    expect_true(identical(sort(unique(revealgenomics:::get_ontology_category()$ontology_category)), 
                          sort(unique(c(ontology_category_2a, ontology_category_2b)))))
    
    # Search function
    testthat::expect_equal(
      length(unique(revealgenomics:::search_ontology_category(ontology_category = c("ontology_category3", "ontology_category2", "ontology_category1"))$ontology_category_id)),
      3
    )
    testthat::expect_equal(
      sum(is.na(revealgenomics:::search_ontology_category(ontology_category = c("ontology_category3", "ontology_category2", "ontology_category1x"))$ontology_category_id)),
      1
    )
    testthat::expect_equal(
      sort(unique(revealgenomics:::search_ontology_category(ontology_category = ontology_category_2b)$ontology_category_id)),
      sort(new_ontology_category_id_2b)
    )
    
    # Clean-up
    init_db(arrays_to_init = c(.ghEnv$meta$arrOntologyCategory), force = TRUE)
  }
})

#### metadata_value ####
test_that("Check that metadata_value registration works properly", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({rg_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = .ghEnv$meta$arrMetadataValue, force = TRUE)
    # Get the existing metadata_value fields
    mv1 = revealgenomics:::get_metadata_value()
    expect_true(nrow(mv1) == 0)
    
    # Register a dummy metadata_value field
    metadata_value = "metadata_value"
    ontology_category_id_uncategorized = revealgenomics:::search_ontology_category(ontology_category = 'uncategorized')$ontology_category_id
    new_metadata_value_id = revealgenomics:::register_metadata_value(
      df = data.frame(
        metadata_value = metadata_value, 
        ontology_category_id = ontology_category_id_uncategorized, 
        stringsAsFactors = FALSE))
    # Check that cache is increased by 1 element
    mv2 = revealgenomics:::get_metadata_value()
    expect_true(nrow(mv2) == nrow(mv1) + 1)
    
    # Verify that the metadata_value key was uploaded properly
    expect_true(revealgenomics:::get_metadata_value(metadata_value_id = new_metadata_value_id)$metadata_value == metadata_value)
    
    # Delete the metadata_value field
    delete_entity(entity = .ghEnv$meta$arrMetadataValue, id = new_metadata_value_id)
    # Check that the cache is updated, and count has decreased by 1
    mv3 = revealgenomics:::get_metadata_value()
    expect_true(nrow(mv3) == nrow(mv1))
    expect_true(nrow(revealgenomics:::get_metadata_value(metadata_value_id = new_metadata_value_id)) == 0)
    
    ###### PHASE 2A #####
    # Now upload two keys at a time
    metadata_value_2a = c("metadata_value1", "metadata_value2")
    new_metadata_value_id_2a = revealgenomics:::register_metadata_value(
      df = data.frame(metadata_value = metadata_value_2a, 
                      ontology_category_id = ontology_category_id_uncategorized, 
                      stringsAsFactors = FALSE))
    expect_true(length(new_metadata_value_id_2a) == 2)
    
    # Now upload two keys at a time
    metadata_value_2b = c("metadata_value1", "metadata_value3")
    new_metadata_value_id_2b = revealgenomics:::register_metadata_value(
      df = data.frame(metadata_value = metadata_value_2b, 
                      ontology_category_id = ontology_category_id_uncategorized, 
                      stringsAsFactors = FALSE))
    expect_true(length(new_metadata_value_id_2b) == 2)
    expect_true(all(
      revealgenomics:::get_metadata_value(metadata_value_id = new_metadata_value_id_2b)$metadata_value %in% 
        c("metadata_value1", "metadata_value3")))
    expect_true(identical(sort(unique(revealgenomics:::get_metadata_value()$metadata_value)), 
                          sort(unique(c(metadata_value_2a, metadata_value_2b)))))
    
    # Search function
    testthat::expect_equal(
      length(unique(revealgenomics:::search_metadata_value(metadata_value = c("metadata_value3", "metadata_value2", "metadata_value1"))$metadata_value_id)),
      3
    )
    testthat::expect_equal(
      sum(is.na(revealgenomics:::search_metadata_value(metadata_value = c("metadata_value3", "metadata_value2", "metadata_value1x"))$metadata_value_id)),
      1
    )
    testthat::expect_equal(
      sort(unique(revealgenomics:::search_metadata_value(metadata_value = metadata_value_2b)$metadata_value_id)),
      sort(new_metadata_value_id_2b)
    )
    
    # Register value at separate ontology
    init_db(arrays_to_init = c(.ghEnv$meta$arrOntologyCategory), force = TRUE)
    ontology_category_id = revealgenomics:::register_ontology_category(df1 = data.frame(ontology_category = 'primary_disease'))
    disease_vec = c('leukemia', 'myeloma', 'rheumatoid arthritis')
    metadata_value_id = revealgenomics:::register_metadata_value(
      df1 = data.frame(
        metadata_value = disease_vec,
        ontology_category_id = ontology_category_id, 
        stringsAsFactors = FALSE
      ))
    expect_equal(
      nrow(revealgenomics:::search_metadata_value(metadata_value = 'leukemia')),
      0
    )
    expect_equal(
      nrow(revealgenomics:::search_metadata_value(metadata_value = 'leukemia', ontology_category = 'primary_disease')),
      1
    )
    expect_equal(
      sort(revealgenomics:::search_metadata_value(metadata_value = NULL, ontology_category = 'primary_disease')$metadata_value),
      sort(disease_vec)
    )
    expect_equal(
      class(try(revealgenomics:::search_metadata_value(metadata_value = NULL, ontology_category = NULL), silent = T)),
      "try-error")
    expect_equal(
      nrow(revealgenomics:::search_metadata_value(metadata_value = 'leukemia', ontology_category = NULL)),
      1)
    
    # Register leukemia as a general term and then run the search
    revealgenomics:::register_metadata_value(
      df1 = data.frame(
        metadata_value = 'leukemia', 
        ontology_category_id = revealgenomics:::search_ontology_category(ontology_category = 'uncategorized')$ontology_category_id,
        stringsAsFactors = FALSE
      ))
    # Now run the search across ontology categories
    expect_equal(
      nrow(revealgenomics:::search_metadata_value(metadata_value = 'leukemia', ontology_category = NULL)),
      2)
    init_db(arrays_to_init = c(.ghEnv$meta$arrOntologyCategory), force = TRUE)

    # Clean-up
    init_db(arrays_to_init = c(.ghEnv$meta$arrMetadataValue), force = TRUE)
  }
})

