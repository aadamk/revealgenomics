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
    
    # Spot checks that ontology fields of study have been captured properly
    datasets = get_datasets()
    expect_true(all(!is.na(datasets$study_type)))
    expect_true(all(!is.na(datasets$DAS)))
    expect_true(all(datasets$DAS %in% wb$Studies$DAS))
    expect_true(all(datasets$study_type %in% wb$Studies$study_type))
    
    # Similar spot checks using `search_datasets()` function
    p = get_projects()
    stopifnot(nrow(p) == 1) # Expect one project to be loaded at this time
    project_id = p[1, ]$project_id
    datasets = search_datasets(project_id = project_id)
    expect_true(all(!is.na(datasets$study_type)))
    expect_true(all(!is.na(datasets$DAS)))
    expect_true(all(datasets$DAS %in% wb$Studies$DAS))
    expect_true(all(datasets$study_type %in% wb$Studies$study_type))
    
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
    if (nrow(ftrs) != 1) {
      stop("If more than one feature for TXNIP at this point; need to adjust test")
    }
    mn = ms[ms$pipeline_scidb == '[external]-[Fusion] Tophat Fusion',]
    mn = mn[mn$featureset_id == ftrs$featureset_id,]
    v1 = search_fusion(measurementset = mn, feature = ftrs)
    cat('TXNIP feature search\n')
    expect_true(all.equal(dim(v1), c(3, 15)))
    
    ftrs = search_features(gene_symbol = c('IGH'))
    mn = ms[ms$pipeline_scidb == '[external]-[Fusion] custom pipeline - Foundation Medicine',]
    v1 = search_fusion(measurementset = mn, feature = ftrs)
    cat('IGH feature search\n')
    expect_true(all.equal(dim(v1), c(2, 25)))
    
    # deFuse fusion (Also this is at GRCh38 in the test Excel sheet)
    ftrs = search_features(gene_symbol = c('KANSL1'))
    mn = ms[ms$pipeline_scidb == '[external]-[Fusion] Defuse',]
    v1 = search_fusion(measurementset = mn, feature = ftrs)
    cat('KANSL1 feature search\n')
    print(dim(v1))
    expect_true(all.equal(dim(v1), c(1, 80)))
    
    ftrs = search_features(gene_symbol = c('ARL17A', 'KANSL1', 'AKNA'))
    mn = ms[ms$pipeline_scidb == '[external]-[Fusion] Defuse',]
    v1 = search_fusion(measurementset = mn, feature = ftrs)
    cat('KANSL1 feature search\n')
    print(dim(v1))
    expect_true(all.equal(dim(v1), c(2, 80)))
    
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
