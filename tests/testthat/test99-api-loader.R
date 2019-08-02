context("test-api-loader.R")

test_that("Register entities via workbook works OK", {
  # cat("# Now connect to scidb\n")
  e0 = tryCatch({rg_connect()}, error = function(e) {e})
  if (!("error" %in% class(e0))) { # do not run this on EE installs, mainly targeted for Travis
    init_db(arrays_to_init = get_entity_names(), force = TRUE)
    df_referenceset = data.frame(
      name = c("GRCh37", "GRCh38"), 
      description = "...",
      species = 'homo sapiens',
      assembly_id = c("GrCh37", "GrCh38_r85"),
      source_uri = "...",
      source_accessions = "...",
      is_derived = TRUE, 
      stringsAsFactors = FALSE)
    
    cat("Registering reference set:", pretty_print(df_referenceset$name), "\n")
    referenceset_id = register_referenceset(df = df_referenceset)
    
    wb = myExcelLoader(
      filename = system.file(
        "extdata", "scidb_metadata_sample.xlsx", package = "revealgenomics"))
    
    # Load metadata first
    register_entities_workbook(workbook = wb, 
                               register_upto_entity = 'MEASUREMENT')
    
    ##### Spot checks on Dataset #####
    # Spot checks that ontology fields of study have been captured properly
    datasets = get_datasets()
    expect_true(all(!is.na(datasets$study_type)))
    expect_true(all(!is.na(datasets$DAS)))
    expect_true(all(datasets$DAS %in% wb$Studies$DAS))
    expect_true(all(datasets$study_type %in% wb$Studies$study_type))
    
    # Similar spot checks using `search_datasets()` function
    p = get_projects()
    stopifnot(nrow(p) == 1) # Expect one project to be loaded at this time
    project_id = p[1, ]$project_id
    datasets = search_datasets(project_id = project_id)
    expect_true(all(!is.na(datasets$study_type)))
    expect_true(all(!is.na(datasets$DAS)))
    expect_true(all(datasets$DAS %in% wb$Studies$DAS))
    expect_true(all(datasets$study_type %in% wb$Studies$study_type))
    
    ######## Update Study ##########
    wb$Studies$URL[2] = "https://clinicaltrials.gov/ct2/show/zzzz" # edit an existing field
    wb$Studies$new_field1 = c('entry1', 'entry2') # add a new column
    register_entities_workbook(workbook = wb, 
                               register_upto_entity = 'DEFINITION', 
                               entity_to_update = 'DATASET')
    datasets = get_datasets()
    expect_true(length(grep("zzzz", datasets$URL)) ==1)
    expect_true(all(datasets$new_field1 %in% c('entry1', 'entry2')))
    
    ######## Update Individual ##########
    ## Edit an entry
    rand_idx = 1
    test_disease_str = 'test disease 1'
    wb$Subjects[rand_idx, ]$primary_disease = test_disease_str
    
    ## Add a new column
    rand_string = stringi::stri_rand_strings(nrow(wb$Subjects), length = 6)
    wb$Subjects$new_field1 = rand_string
    # Introduce new definition for the new field
    new_definition = wb$Definitions[wb$Definitions$attribute_name == 'subject_id', ]
    new_definition$attribute_name = 'new_field1'
    new_definition$attribute_in_Samples = FALSE
    wb$Definitions = rbind(
      wb$Definitions, 
      new_definition
      )
    register_entities_workbook(workbook = wb, 
                               register_upto_entity = 'INDIVIDUAL', 
                               entity_to_update = 'INDIVIDUAL')
    
    # Spot checks for the updates
    ii = get_individuals()
    expect_true(all(ii$new_field1 %in% rand_string))
    expect_equal(
      length(which(ii$primary_disease == test_disease_str)),
      1)
    
    ######## Update MeasurementSet (trickier) ##########
    ## Edit an entry related to `pipeline_choices` sheet;
    # `pipeline_applications` column: "Defuse" ==> "deFuse"
    wb$pipeline_choices[wb$pipeline_choices$pipeline_scidb 
                        == '[external]-[Fusion] Defuse', 
                        ]$pipeline_applications = "deFuse"
    # Edit an entry related to `filter_choices` sheet;
    # `filter_description2` column 
    rand_string2 = stringi::stri_rand_strings(n = 1, length = 10)
    filter_name = 'gene - counts'
    wb$filter_choices[wb$filter_choices$filter_name 
                        == filter_name, 
                        ]$filter_description2 = rand_string2
    register_entities_workbook(workbook = wb, 
                               register_upto_entity = 'MEASUREMENTSET', 
                               entity_to_update = 'MEASUREMENTSET')
    
    # Spot checks
    ms = get_measurementsets()
    expect_equal(
      length(which(ms$pipeline_applications == "deFuse")),
      1)
    expect_true(
      all(ms[ms$filter_name == filter_name, ]$filter_description2 == rand_string2))
    
    ##### FeatureSet #####
    # Build up a featureset to be used for loading data
    fsets = get_featuresets()
    stopifnot(nrow(fsets) == 2)
    target_featureset_id = fsets[grep("37", fsets$name), ]$featureset_id
    ftr_record = build_reference_gene_set(
      featureset_id = target_featureset_id, 
      gene_annotation_file_path = system.file("extdata", 
                                              "gene__hugo__hgnc_complete_set.txt.gz", package="revealgenomics"), # these are grch38 files but we use this to populate the featureset anyway
      gene_location_file_path = system.file("extdata", 
                                            "gene__grch38_release85_homosap_gene__newGene.tsv.gz", package="revealgenomics") # again a grch38 file
                                          ) 
    
    target_featureset_id = fsets[grep("38", fsets$name), ]$featureset_id
    ftr_record = build_reference_gene_set(featureset_id = target_featureset_id)
    
    ########### FUSION DATA ############
    # Now load the data
    register_entities_workbook(workbook = wb, 
                               register_measurement_entity = 'FUSION')
    
    # Now do some checks on the data load
    ms = get_measurementsets()
    
    # TODO: walk all pipelines and measurementset IDs to verify data.
    ftrs = search_features(gene_symbol = c('TXNIP'))
    if (nrow(ftrs) != 1) {
      stop("If more than one feature for TXNIP at this point; need to adjust test")
    }
    mn = ms[ms$pipeline_scidb == '[external]-[Fusion] Tophat Fusion',]
    mn = mn[grep("37", mn$featureset_name), ]
    v1 = search_fusion(measurementset = mn[1, ], feature = ftrs)
    cat('TXNIP feature search\n')
    expect_true(all.equal(dim(v1), c(3, 15)))
    
    ftrs = search_features(gene_symbol = c('IGH'))
    mn = ms[ms$pipeline_scidb == '[external]-[Fusion] custom pipeline - Foundation Medicine',]
    v1 = search_fusion(measurementset = mn, feature = ftrs)
    cat('IGH feature search\n')
    expect_true(all.equal(dim(v1), c(2, 25)))
    
    # deFuse fusion (Also this is at GRCh38 in the test Excel sheet)
    ftrs = search_features(gene_symbol = c('KANSL1'))
    mn = ms[ms$pipeline_scidb == '[external]-[Fusion] Defuse',]
    v1 = search_fusion(measurementset = mn, feature = ftrs)
    cat('KANSL1 feature search\n')
    expect_true(all.equal(dim(v1), c(1, 75)))
    
    ftrs = search_features(gene_symbol = c('ARL17A', 'KANSL1', 'AKNA'))
    mn = ms[ms$pipeline_scidb == '[external]-[Fusion] Defuse',]
    v1 = search_fusion(measurementset = mn, feature = ftrs)
    cat('KANSL1 feature search\n')
    expect_true(all.equal(dim(v1), c(2, 75)))
    
    ########### VARIANT DATA ############
    # Now load the variant data
    register_entities_workbook(workbook = wb, 
                               register_measurement_entity = 'VARIANT')
    
    # Now do some checks on the variant data load
    ms = get_measurementsets()
    ms_variant = ms[grep("Variant", ms$pipeline_scidb), ]
    stopifnot(nrow(ms_variant) == 1)
    
    ftrs = search_features(gene_symbol = c('PARP2', 'RHOA', 'JAK2'))
    v1 = search_variant(measurementset = ms_variant, feature = ftrs)
    expect_true(all.equal(dim(v1), c(3, 21)))
    
    ftrs = search_features(gene_symbol = c('PARP2', 'RHOA', 'JAK2', 'TP53'))
    v2 = search_variant(measurementset = ms_variant, feature = ftrs)
    expect_true(all.equal(dim(v2), c(5, 21)))
    
    # Clean up
    init_db(arrays_to_init = get_entity_names(), force = TRUE)
  }
})
