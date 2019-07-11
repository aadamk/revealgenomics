context("test00-schema")


test_that("get_base_idname(ENTITY) matches entity_id for metadata classes", {
  expect_base_idname = function(entity) {
    expect_equal(revealgenomics:::get_base_idname(entity), 
                 paste0(tolower(entity), "_id"))
  }
  expect_base_idname(entity = .ghEnv$meta$arrFeature)
  expect_base_idname(entity = .ghEnv$meta$arrFeatureSynonym)
  expect_base_idname(entity = .ghEnv$meta$arrFeatureset)
  expect_base_idname(entity = .ghEnv$meta$arrProject)
  expect_base_idname(entity = .ghEnv$meta$arrDataset)
  expect_base_idname(entity = .ghEnv$meta$arrIndividuals)
  expect_base_idname(entity = .ghEnv$meta$arrBiosample)
  expect_base_idname(entity = .ghEnv$meta$arrExperimentSet)
  expect_base_idname(entity = .ghEnv$meta$arrMeasurementSet)
  expect_base_idname(entity = .ghEnv$meta$arrGeneSymbol)
  expect_base_idname(entity = .ghEnv$meta$arrGenelist)
  expect_base_idname(entity = .ghEnv$meta$arrGenelist_gene)
  expect_base_idname(entity = .ghEnv$meta$arrDefinition)
})

test_that("dataset_id is a mandatory column for certain metadata entities", {
  expect_true('dataset_id' %in% mandatory_fields()[[.ghEnv$meta$arrIndividuals]])
  expect_true('dataset_id' %in% mandatory_fields()[[.ghEnv$meta$arrBiosample]])
  expect_true('dataset_id' %in% mandatory_fields()[[.ghEnv$meta$arrMeasurementSet]])
  expect_true('dataset_id' %in% mandatory_fields()[[.ghEnv$meta$arrMeasurement]])
  expect_true('dataset_id' %in% mandatory_fields()[[.ghEnv$meta$arrDefinition]])
  # add more
})