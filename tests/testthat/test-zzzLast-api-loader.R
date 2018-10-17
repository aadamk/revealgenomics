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
    # register_measurement_entity = 'VARIANT')
    
    # Build up a featureset to be used for loading data
    fsets = get_featuresets()
    stopifnot(nrow(fsets) == 2)
    target_featureset_id = fsets$featureset_id[1]  # TODO: walk all matching featuresets.
    ftr_record = build_reference_gene_set(featureset_id = target_featureset_id)  # why is ftr_record unused?
    
    # Now load the data
    register_entities_workbook(workbook = wb, 
                               register_measurement_entity = 'FUSION')
    
    # Now do some checks on the data load
    ftrs = search_features(gene_symbol = c('TXNIP'))
    ms = get_measurementsets()
    # TODO: walk all pipelines and verify data rather than cherry-picking just one.
    ms = ms[ms$pipeline_scidb == '[external]-[Fusion] Tophat Fusion',]
    ms = ms[ms$measurementset_id == 3,]
    v1 = search_fusion(measurementset = ms, feature = ftrs)
    expect_true(all.equal(dim(v1), c(3, 16)))
  }
})
