context("test-helper-functions.R")

test_that("verify output of pretty_print", {
  expect_true(pretty_print(c(1:3)) == "1, 2, 3")
  expect_true(pretty_print(c(1:7)) == "1, 2, 3, 4, 5, 6, 7")
  expect_true(pretty_print(c(1:8)) == "1, 2...(Total: 8)... 7, 8")
  expect_true(pretty_print(c(1:8), prettify_after = 8) == "1, 2, 3, 4, 5, 6, 7, 8")
})
  
test_that("verify output of names_to_numbered_vec_by_uniqueness", {
  vec_names1 = c('a', 'b', 'a', 'c')
  vec_names2 = c(11, 35, 44, 11, 35, 66)
  
  res1 = c(1, 2, 1, 3)
  names(res1) = vec_names1
  expect_equal(
    revealgenomics:::names_to_numbered_vec_by_uniqueness(vec_names1),
    res1
  )
  
  res2 = c(1, 2, 3, 1, 2, 4)
  names(res2) = as.character(vec_names2)
  expect_equal(
    revealgenomics:::names_to_numbered_vec_by_uniqueness(vec_names2),
    res2
  )
})
