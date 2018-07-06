context("test-download.R")

test_that("Check that feature download works when feature_id is a long integer ", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({gh_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    # https://github.com/Paradigm4/reveal-genomics/issues/40
    ff = get_features(feature_id = seq(70000,800000,10))
    expect_true(nrow(ff) == 0) # in tests, currently do not expect to upload 700K+ features
  }
})
