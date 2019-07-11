context("test02-excel_loader: ")

test_that("Check that upload fails if user does not use data.frame", {
  scidb_tmpl_path = system.file("extdata",
                                "scidb_metadata_template.xlsx", 
                                package="revealgenomics")
  dfDefn = readxl::read_excel(scidb_tmpl_path, sheet = 'Definitions', trim_ws = TRUE)
  dfDefn$dataset_id = 9999
  dfDefn$Notes = '...'
  
  # Verify that tibble cannot be registered
  e1 = tryCatch({
    register_definitions(df = dfDefn, only_test = T)
  }, error = function(e) {
    return(e)
  })
  
  # cat("Verify that tibble cannot be registered\n")
  expect_true("error" %in% class(e1))
  expect_true(length(grep("data.table", e1$message)) > 0)
})

test_that("Check that upload works for `Definitions` sheet of Excel template ", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({rg_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    scidb_tmpl_path = system.file("extdata",
                                  "scidb_metadata_template.xlsx", 
                                  package="revealgenomics")
    dfDefn = readxl::read_excel(scidb_tmpl_path, sheet = 'Definitions', trim_ws = TRUE)
    dfDefn$dataset_id = 9999
    dfDefn$Notes = '...'

    init_db(
      arrays_to_init = c(.ghEnv$meta$arrDefinition, 
                         .ghEnv$meta$arrMetadataAttrKey),
      force = TRUE)
    
    # cat("# Register definitions\n")
    def_id = register_definitions(df = as.data.frame(dfDefn))
    expect_true(nrow(def_id) == nrow(dfDefn))
    
    # cat("# Download definitionsn")
    dfDefn2 = get_definitions(updateCache = T)
    expect_true(nrow(dfDefn) == nrow(dfDefn2))
    
    
    # cat("# Verify that downloaded definitions are consistent\n")
    expect_true(ncol(dfDefn2) == (ncol(dfDefn) + 1))
    expect_true(all.equal(dfDefn, dfDefn2[, colnames(dfDefn)]))
    
    # cat("# Verify that output of search_definitions is consistent\n")
    expect_true(nrow(search_definitions(dataset_id = 10000)) == 0)
    res = search_definitions(dataset_id = 9999)
    expect_true(nrow(dfDefn) == nrow(res))
    expect_true(all.equal(dfDefn2[, colnames(res)], res))
    
    # cat("# Clean-up\n")
    delete_entity(entity = .ghEnv$meta$arrDefinition, id = def_id$definition_id)
    expect_true(nrow(get_definitions(updateCache = T)) == 0)
  }
})
