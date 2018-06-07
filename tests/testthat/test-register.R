context("test-register.R")


test_that("register_biosample with reserved column `sample_name` fails", {
  e = try({
    register_biosample(df = data.frame(sample_name = c('a', 'b'), 
                                       dataset_id = 1000, 
                                       name = c('a', 'b'), 
                                       description = '...', 
                                       disease_ = 1, 
                                       individual_id = 3))
    }, silent = TRUE)
  expect_true(class(e) == "try-error")
  expect_true(length(grep("reserved.*sample_name", e[1])) ==1)
})