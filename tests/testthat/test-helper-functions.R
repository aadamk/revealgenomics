context("test-helper-functions.R")

test_that("verify output of pretty_print", {
  expect_true(pretty_print(c(1:3)) == "1, 2, 3")
  expect_true(pretty_print(c(1:7)) == "1, 2, 3, 4, 5, 6, 7")
  expect_true(pretty_print(c(1:8)) == "1, 2...(Total: 8)... 7, 8")
  expect_true(pretty_print(c(1:8), prettify_after = 8) == "1, 2, 3, 4, 5, 6, 7, 8")
})
  