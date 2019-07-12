# Only for studies loaded via Excel sheet
# Check that samples loaded for each pipeline have correct suffix
# - Fusion, Variant, CopyNumber: `DNA`
# - Gene expression: `RNA` (could be `__mRNA` or `__RNA`)
context("test03-excel_loader-samples: ")

test_that("For data loaded via Excel template, check that biosamples associated with specific pipelines
          have the appropiate suffix: DNA or RNA", {
  # can generalize this to any study that has been loaded by Excel sheet
  def = get_definitions()
  if (nrow(def) > 0) {
    d_all_excel = get_datasets(dataset_id = unique(def$dataset_id))
    for (study_idx in 1:nrow(d_all_excel)) { 
      cat("=====================\n")
      d = d_all_excel[study_idx, ]
      print(d[, c('dataset_id', 'name')])
      
      ms_all = search_measurementsets(dataset_id = d$dataset_id)
      exp_all = search_experimentsets(dataset_id = d$dataset_id)
      ms_all = merge(ms_all[, c('measurementset_id', 'experimentset_id', 'name')],
                     exp_all[, c('experimentset_id', 'measurement_entity')],
                     by = 'experimentset_id')
      cat("Found", nrow(ms_all), "pipelines:\n\t", 
          pretty_print(ms_all$name), "\n")
      for (idx in 1:nrow(ms_all)) {
        ms = ms_all[idx, ]
        cat("sub-idx:", idx, "Pipeline:", ms$name, "\n")
        ms_stat = iquery(.ghEnv$db, 
                         paste0("aggregate(
                                 filter(", 
                                revealgenomics:::full_arrayname(ms$measurement_entity),
                                ", measurementset_id = ", ms$measurementset_id, 
                                "), count(*), biosample_id)"),
                         return = TRUE)
        if (nrow(ms_stat) > 0) {
          bios_df = get_biosamples(biosample_id = ms_stat$biosample_id)
          cat("biosample names at pipeline", ms$measurementset_id, 
              "of study", d$dataset_id, ":\n\t", 
              pretty_print(bios_df$name), "\n")
          suffix = ifelse(
            ms$measurement_entity %in% c(.ghEnv$meta$arrVariant, 
                                         .ghEnv$meta$arrFusion,
                                         .ghEnv$meta$arrCopynumber_mat, 
                                         .ghEnv$meta$arrCopynumber_seg),
            "DNA", # couuld be '__mRNA' or '__RNA'
            "RNA")
          if (length(grep(suffix, bios_df$name, value=TRUE)) == 
              length(unique(ms_stat$biosample_id))) {
            cat("Check passed for pipeline", ms$measurementset_id, "\n")
          } else {
            stop("Check failed")
          }
        }
      }
    }
  }
})
