context("test-caching")

test_that("Check that cache is automatically updated while registering or deleting cached entities", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({rg_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = .ghEnv$meta$arrOntology, force = TRUE)
    # Get the existing ontology fields
    ont1 = get_ontology()
    
    # Register a dummy ontology field
    dummy_val = "dummy"
    new_ont_id = register_ontology_term(df = data.frame(term = dummy_val, source_name = "...", source_version = "..."))
    # Check that cache is increased by 1 element
    ont2 = get_ontology()
    stopifnot(nrow(ont2) == nrow(ont1) + 1)
    
    # Verify that the dummy term was uploaded properly
    stopifnot(get_ontology(ontology_id = new_ont_id)$term == dummy_val)
    
    # Delete the dummy ontology field
    delete_entity(entity = .ghEnv$meta$arrOntology, id = new_ont_id)
    # Check that the cache is updated, and count has decreased by 1
    ont3 = get_ontology()
    stopifnot(nrow(ont3) == nrow(ont1))
    stopifnot(nrow(get_ontology(ontology_id = new_ont_id)) == 0)
    
    # Clean-up
    init_db(arrays_to_init = c(.ghEnv$meta$arrOntology), force = TRUE)
  }
})
