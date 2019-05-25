context("test-expressionset-formation")

test_that("Check that expressionset is formed properly for sparse case", {
  rm(list = ls())
  df1 = data.frame(
    feature_id =   c(1, 2, 2, 2, 2),
    biosample_id = c(2, 3, 4, 6, 7), 
    value =        c(3, 3, 4, 3, 3),
    stringsAsFactors = FALSE
  )
  
  b_id_vec = unique(df1$biosample_id)
  b_ref_df = data.frame(
    dataset_id = 1,
    dataset_version = 1,
    biosample_id = b_id_vec,
    name = c('S00-A', 'S00-B', 'S00-C', 'S00-D', 'S00-E'),
    description = '...',
    created = '...',
    updated = '...',
    stringsAsFactors = FALSE
  )
  f_id_vec = unique(df1$feature_id)
  f_ref_df = data.frame(
    featureset_id = 1,
    gene_symbol_id = 1, 
    feature_id  = f_id_vec,
    name = c('ENSG00000000003', 'ENSG00000000005'),
    gene_symbol = c('TSPAN6', 'TNMD'),
    chromosome = 'chrX',
    start = '...',
    end = '...',
    feature_type = 'gene',
    source = '...',
    created = '...',
    updated = '...',
    stringsAsFactors = FALSE
  )
  
  es1 = revealgenomics:::convertToExpressionSet(
    expr_df = df1, 
    biosample_df = b_ref_df,
    feature_df = f_ref_df, 
    measurementset_df = data.frame(name = 'Pipeline1', stringsAsFactors = FALSE))
  testthat::expect_equal(as.vector(exprs(es1)),
                         c(3, NA, NA, 3, NA, 4, NA, 3, NA, 3))
  testthat::expect_equal(
    rownames(exprs(es1)),
    c('TSPAN6', 'TNMD'))
  testthat::expect_equal(
    colnames(exprs(es1)),
    c('S00-A', 'S00-B', 'S00-C', 'S00-D', 'S00-E'))
})
