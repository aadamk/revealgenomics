context("test-download.R")

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
