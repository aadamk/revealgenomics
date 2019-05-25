context("test-expressionset-formation")

testthat::test_that("Check that expressionset is formed properly for sparse case", {
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
    name = as.character(b_id_vec),
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
    name = as.character(f_id_vec),
    gene_symbol = as.character(f_id_vec),
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
    as.character(f_id_vec))
  testthat::expect_equal(
    colnames(exprs(es1)),
    as.character(b_id_vec))
})

testthat::test_that("Check that expressionset is formed properly for discontinuous dense matrix case", {
  rm(list = ls())
  set.seed(1); 
  xx = matrix(data = runif(10), nrow = 2); 
  
  rownames(xx) = c(1, 3) # <=== discontinuous
  colnames(xx) = c(2, 3, 4, 6, 7)
  
  expr_df = as.data.frame(as.table(xx), stringsAsFactors=F)
  colnames(expr_df) = c('feature_id', 'biosample_id', 'value')
  expr_df$feature_id = as.integer(expr_df$feature_id)
  expr_df$biosample_id = as.integer(expr_df$biosample_id)
  
  b_id_vec = unique(expr_df$biosample_id)
  b_ref_df = data.frame(
    dataset_id = 1,
    dataset_version = 1,
    biosample_id = b_id_vec,
    name = as.character(b_id_vec),
    description = '...',
    created = '...',
    updated = '...',
    stringsAsFactors = FALSE
  )
  f_id_vec = unique(expr_df$feature_id)
  f_ref_df = data.frame(
    featureset_id = 1,
    gene_symbol_id = 1, 
    feature_id  = f_id_vec,
    name = as.character(f_id_vec),
    gene_symbol = as.character(f_id_vec),
    chromosome = 'chrX',
    start = '...',
    end = '...',
    feature_type = 'gene',
    source = '...',
    created = '...',
    updated = '...',
    stringsAsFactors = FALSE
  )

  es2 = revealgenomics:::convertToExpressionSet(
    expr_df = expr_df, 
    biosample_df = b_ref_df,
    feature_df = f_ref_df, 
    measurementset_df = data.frame(name = 'Pipeline1', stringsAsFactors = FALSE))
  
  testthat::expect_equal(
    xx,
    exprs(es2)
  )
})

testthat::test_that("Check that expressionset is formed properly for continuous dense matrix case", {
  rm(list = ls())
  set.seed(1); 
  xx = matrix(data = runif(10), nrow = 2); 
  
  rownames(xx) = c(1:2) # <=== continuous
  colnames(xx) = c(2:6)
  
  expr_df = as.data.frame(as.table(xx), stringsAsFactors=F)
  colnames(expr_df) = c('feature_id', 'biosample_id', 'value')
  expr_df$feature_id = as.integer(expr_df$feature_id)
  expr_df$biosample_id = as.integer(expr_df$biosample_id)
  
  b_id_vec = unique(expr_df$biosample_id)
  b_ref_df = data.frame(
    dataset_id = 1,
    dataset_version = 1,
    biosample_id = b_id_vec,
    name = as.character(b_id_vec),
    description = '...',
    created = '...',
    updated = '...',
    stringsAsFactors = FALSE
  )
  f_id_vec = unique(expr_df$feature_id)
  f_ref_df = data.frame(
    featureset_id = 1,
    gene_symbol_id = 1, 
    feature_id  = f_id_vec,
    name = as.character(f_id_vec),
    gene_symbol = as.character(f_id_vec),
    chromosome = 'chrX',
    start = '...',
    end = '...',
    feature_type = 'gene',
    source = '...',
    created = '...',
    updated = '...',
    stringsAsFactors = FALSE
  )
  
  es3 = revealgenomics:::convertToExpressionSet(
    expr_df = expr_df, 
    biosample_df = b_ref_df,
    feature_df = f_ref_df, 
    measurementset_df = data.frame(name = 'Pipeline1', stringsAsFactors = FALSE))
  
  testthat::expect_equal(
    xx,
    exprs(es3)
  )
})
