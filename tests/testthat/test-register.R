context("test-register.R")


test_that("register_biosample with reserved column `sample_name` fails", {
  e = try({
    register_biosample(df = data.frame(sample_name = c('a', 'b'), 
                                       dataset_id = 1000, 
                                       name = c('a', 'b'), 
                                       description = '...', 
                                       individual_id = 3))
    }, silent = TRUE)
  expect_true(class(e) == "try-error")
  expect_true(length(grep("reserved.*sample_name", e[1])) ==1)
})

test_that("Check that variant_key registration works properly", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({gh_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = .ghEnv$meta$arrVariantKey, force = TRUE)
    # Get the existing variant_key fields
    vk1 = get_variant_key()
    expect_true(nrow(vk1) == 0)
    
    # Register a dummy variant_key field
    dummy_val = "dummy"
    new_variant_key_id = register_variant_key(
      df = data.frame(key = dummy_val, stringsAsFactors = FALSE))
    # Check that cache is increased by 1 element
    vk2 = get_variant_key()
    expect_true(nrow(vk2) == nrow(vk1) + 1)
    
    # Verify that the dummy key was uploaded properly
    expect_true(get_variant_key(variant_key_id = new_variant_key_id)$key == dummy_val)
    
    # Delete the dummy variant_key field
    delete_entity(entity = .ghEnv$meta$arrVariantKey, id = new_variant_key_id)
    # Check that the cache is updated, and count has decreased by 1
    vk3 = get_variant_key()
    expect_true(nrow(vk3) == nrow(vk1))
    expect_true(nrow(get_variant_key(variant_key_id = new_variant_key_id)) == 0)
    
    ###### PHASE 2A #####
    # Now upload two keys at a time
    dummy_val_2a = c("dummy1", "dummy2")
    new_variant_key_id_2a = register_variant_key(
      df = data.frame(key = dummy_val_2a, stringsAsFactors = FALSE))
    expect_true(length(new_variant_key_id_2a) == 2)
    
    # Now upload two keys at a time
    dummy_val_2b = c("dummy1", "dummy3")
    new_variant_key_id_2b = register_variant_key(
      df = data.frame(key = dummy_val_2b, stringsAsFactors = FALSE))
    expect_true(length(new_variant_key_id_2b) == 2)
    expect_true(all(
      get_variant_key(variant_key_id = new_variant_key_id_2b)$key %in% 
               c("dummy1", "dummy3")))
    expect_true(identical(sort(unique(get_variant_key()$key)), 
                    sort(unique(c(dummy_val_2a, dummy_val_2b)))))
    
    # Clean-up
    init_db(arrays_to_init = c(.ghEnv$meta$arrVariantKey), force = TRUE)
  }
})
