context("test-api-loader.R")

test_that("Register entities via workbook works OK", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({rg_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = get_entity_names(),
            force = TRUE)
    df_referenceset = data.frame(
      name = c("GRCh37", "GRCh38"), 
      description = "...",
      species = 'homo sapiens',
      assembly_id = c("GrCh37", "GrCh38_r85"),
      source_uri = "...",
      source_accessions = "...",
      is_derived = TRUE, 
      stringsAsFactors = FALSE)
    
    cat("Registering reference set:", pretty_print(df_referenceset$name), "\n")
    referenceset_id = register_referenceset(df = df_referenceset)
    
    wb = myExcelLoader(
      filename = system.file(
        "extdata", "scidb_metadata_sample.xlsx", package = "revealgenomics"))
    
    # Load metadata first
    register_entities_workbook(workbook = wb, 
                               register_upto_entity = 'MEASUREMENTSET')
    
    # Build up a featureset to be used for loading data
    fsets = get_featuresets()
    stopifnot(nrow(fsets) == 2)
    target_featureset_id = fsets[grep("37", fsets$name), ]$featureset_id
    ftr_record = build_reference_gene_set(featureset_id = target_featureset_id)  # why is ftr_record unused?
    
    ########### FUSION DATA ############
    # Now load the data
    register_entities_workbook(workbook = wb, 
                               register_measurement_entity = 'FUSION')
    
    # Now do some checks on the data load
    ms = get_measurementsets()
    
    # TODO: walk all pipelines and measurementset IDs to verify data.
    ftrs = search_features(gene_symbol = c('TXNIP'))
    mn = ms[ms$pipeline_scidb == '[external]-[Fusion] Tophat Fusion',]
    mn = mn[mn$measurementset_id == 3,]
    v1 = search_fusion(measurementset = mn, feature = ftrs)
    cat('TXNIP feature search\n')
    expect_true(all.equal(dim(v1), c(3, 16)))
    
    ftrs = search_features(gene_symbol = c('IGH'))
    mn = ms[ms$pipeline_scidb == '[external]-[Fusion] custom pipeline - Foundation Medicine',]
    mn = mn[mn$measurementset_id == 2,]
    v1 = search_fusion(measurementset = mn, feature = ftrs)
    cat('IGH feature search\n')
    expect_true(all.equal(dim(v1), c(1, 16)))
    
    # TODO: This test doesn't work and I think it's because of an incorrect choice of column-to-dataframe
    # mapping in the DataReaderDeFuseTophat.  Also, I'm not sure how to build the feature synonyms up
    # in a way that makes sense.  Kriti, what am I missing?
    ftrs = search_features(gene_symbol = c('KANSL1'))
    mn = ms[ms$pipeline_scidb == '[external]-[Fusion] Defuse',]
    mn = mn[mn$measurementset_id == 6,]
    v1 = search_fusion(measurementset = mn, feature = ftrs)
    cat('... feature search\n')
    # expect_true(all.equal(dim(v1), c(3, 16)))
    
    ########### VARIANT DATA ############
    # Now load the variant data
    register_entities_workbook(workbook = wb, 
                               register_measurement_entity = 'VARIANT')
    
    # Now do some checks on the variant data load
    ms = get_measurementsets()
    ms_variant = ms[grep("Variant", ms$pipeline_scidb), ]
    stopifnot(nrow(ms_variant) == 1)
    
    ftrs = search_features(gene_symbol = c('PARP2', 'RHOA', 'JAK2'))
    v1 = search_variants(measurementset = ms_variant, feature = ftrs)
    expect_true(all.equal(dim(v1), c(3, 21)))
    
    ftrs = search_features(gene_symbol = c('PARP2', 'RHOA', 'JAK2', 'TP53'))
    v2 = search_variants(measurementset = ms_variant, feature = ftrs)
    expect_true(all.equal(dim(v2), c(5, 21)))
  }
})
