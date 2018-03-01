library(R6)

#' convenience function for matching features between database and file
match_features = function(features_in_file, df_features_db, feature_type, column_in_db) {
  df_features_db = df_features_db[df_features_db$feature_type == feature_type, ]
  
  print(head(as.character(features_in_file)))
  matchL = find_matches_and_return_indices(features_in_file, df_features_db[, column_in_db])
  
  # Assume that all features are already registered
  if (length(matchL$source_unmatched_idx) != 0) {
    cat("Out of", length(unique(features_in_file)), "features, ")
    cat("unmatched features:", 
        pretty_print(vec = unique(features_in_file[mL$source_unmatched_idx])), "\n")
    stop("...")
  }
  df_features_db$feature_id[matchL$target_matched_idx]
}

DataFrameLoader = R6Class(classname = "DataFrameLoader", 
        public = list(
          assign_biosample_ids = function(){
            cat("Function: Assign biosample_ids (Level: DataFrameLoader)\n")
            bios_ref = private$.reference_object$biosample
            if (nrow(private$.reference_object$pipeline_df) > 1) { 
              # multiple Measurements combined into one file
              # ==> data must contain a column called `biosample_name`
              cat("Multiple Measurements combined into one file\n")
              cat("Replacing `biosample_name` with `biosample_id`\n")
              
              mL0 = match(private$.reference_object$pipeline_df$original_sample_name,
                          private$.data_df$biosample_name)
              cat("Summary file has", length(unique(private$.data_df$biosample_name)), "biosamples\n")
              stopifnot(length(private$.reference_object$pipeline_df$original_sample_name) ==
                          length(unique(private$.reference_object$pipeline_df$original_sample_name)))
              cat("Current pipeline record expects", length(private$.reference_object$pipeline_df$original_sample_name), 
                  "entries\n")
              if (!all(private$.data_df$biosample_name %in% private$.reference_object$pipeline_df$original_sample_name)) {
                cat("Dropping extra entries\n")
                private$.data_df = private$.data_df[private$.data_df$biosample_name %in% private$.reference_object$pipeline_df$original_sample_name, ]
              }
              
              mL = find_matches_and_return_indices(private$.data_df$biosample_name, 
                                                   bios_ref$original_sample_name)
              
              private$.data_df$biosample_id = -1
              private$.data_df$biosample_id[mL$source_matched_idx] = bios_ref$biosample_id[mL$target_matched_idx]
              
              unmatched = unique(private$.data_df[private$.data_df$biosample_id == -1, ]$biosample_name)
              if (length(unmatched) > 0) {
                cat("dropping", length(unmatched), "samples:", pretty_print(unmatched, prettify_after = length(unmatched)), "\n")
                private$.data_df = private$.data_df[private$.data_df$biosample_id != -1, ]
              }
              
              private$.data_df$biosample_name = NULL
            } else if (nrow(private$.reference_object$pipeline_df) == 1) {
              # One Measurement per file 
              # ==> information about biosample will exist in `pipeline_df` reference object
              cat("One Measurement per file\n")
              
              sample_name_in_file = private$.reference_object$pipeline_df$sample_name
              
              mL = find_matches_and_return_indices(sample_name_in_file,
                                                   bios_ref$name)
              
              if (length(mL$source_unmatched_idx) != 0) {
                stop("Excel sheet must provide link between sample in Pipelines sheet and Sample sheet under column `sample_name`")
              }
              
              sample_id_db = bios_ref$biosample_id[mL$target_matched_idx]
              cat("sample-name:", sample_name_in_file, "==> biosample_id:", 
                  sample_id_db, "\n")
              private$.data_df$biosample_id = sample_id_db
            }
            invisible(self)
          },
          
          #' download features for current featureset
          #' 
          #' compare with `featureset_id` of the current measurementSet
          #' and update the feature stored in `reference_object` if there has been a change in `featureset_id`
          download_features_for_featureset = function() {
            fsets_scidb = private$.reference_object$featureset
            # print(dim(fsets_scidb))
            
            featureset_name = unique(private$.reference_object$pipeline_df$featureset_name)
            stopifnot(length(featureset_name) == 1)
            # print(featureset_name)
            
            fset = fsets_scidb[match(featureset_name, fsets_scidb$name), ]
            stopifnot(nrow(fset) == 1)
            # print(fset)
            
            if (is.null(private$.reference_object$feature)) {
              cat("feature_ref = NULL; downloading features for featureset", fset$featureset_id, "into loaderObject\n")
              private$.reference_object$feature = search_features(featureset_id = fset$featureset_id)
            } else if (unique(private$.reference_object$feature$featureset_id) != fset$featureset_id) {
              cat("featureset has changed; downloading features for featureset", fset$featureset_id, "into loaderObject\n")
              private$.reference_object$feature = search_features(featureset_id = fset$featureset_id)
            }
          },
          
          #' register new features (if any)
          #' 
          register_new_features = function() {
            cat("Function: Register new features (Level: DataFrameLoader)\n")
            invisible(self)
          },
          
          #' return reference_object 
          #' 
          #'  For DEBUG only
          # get_reference_object = function(){
          #   private$.reference_object
          # },
          
          # return features in reference object
          retrieve_features = function() {
            private$.reference_object$feature
          },
          
          retrieve_feature_synonyms = function() {
            private$.reference_object$feature_synonym
          },
          
          #' assign feature ids
          #' 
          #' assume that new features have been registered by this time
          assign_feature_ids = function(){
          },
          
          #' assign dataset_id and measurementset_id
          assign_other_ids = function(){
            private$.data_df$dataset_id = private$.reference_object$record$dataset_id
            private$.data_df$measurementset_id = private$.reference_object$measurement_set$measurementset_id
          },
          load_data = function(){
            
          },
          returnReferenceObject = function(){
            
          },
          initialize = function(data_df, reference_object){
            private$.data_df = data_df
            private$.reference_object = reference_object
          }
        ), private = list(
          .data_df = NULL,
          .reference_object = NULL
        ))
DataFrameLoaderRNASeq = R6Class(classname = "DataFrameLoaderRNASeq",
                                inherit = DataFrameLoader,
                                public = list(
                                  load_data = function(){
                                    cat("Hello RNASeq\n")
                                    register_expression_dataframe(df1 = private$.data_df, 
                                                                  dataset_version = private$.reference_object$record$dataset_version)
                                    
                                  }))
DataFrameLoaderRNASeqCufflinksGene = R6Class(classname = "DataFrameLoaderRNASeqCufflinksGene",
                                             inherit = DataFrameLoaderRNASeq,
                                             public = list(
                                               assign_feature_ids = function(){
                                                 super$assign_feature_ids()
                                                 cat("Assign feature_id (Level: RNASeq Cufflinks Gene)\n")
                                                 
                                                 browser()
                                                 private$.data_df$feature_id = match_features(
                                                                features_in_file = private$.data_df$tracking_id,
                                                                df_features_db = private$.reference_object$feature,
                                                                feature_type = 'gene',
                                                                column_in_db = 'name')
                                                 private$.data_df$tracking_id = NULL
                                               }))

DataFrameLoaderRNASeqCufflinksIsoform = R6Class(classname = "DataFrameLoaderRNASeqCufflinksIsoform",
                                                inherit = DataFrameLoaderRNASeq,
                                                public = list(
                                                  assign_feature_ids = function(){
                                                    super$assign_feature_ids()
                                                    cat("Hello RNASeq Cufflinks Isoform\n")
                                                    
                                                    private$.data_df$feature_id = match_features(
                                                      features_in_file = private$.data_df$tracking_id,
                                                      df_features_db = private$.reference_object$feature,
                                                      feature_type = 'transcript',
                                                      column_in_db = 'name')
                                                    private$.data_df$tracking_id = NULL
                                                    
                                                  }))

DataFrameLoaderVariant = R6::R6Class(classname = 'DataFrameLoaderVariant',
                                     inherit = DataFrameLoader,
                                     public = list(
                                       load_data = function() {
                                         register_variant(df = private$.data_df, 
                                                          dataset_version = private$.reference_object$record$dataset_version)
                                       }
                                     ))

DataFrameLoaderVariantGemini = R6::R6Class(classname = 'DataFrameLoaderVariantGemini',
                                     inherit = DataFrameLoaderVariant,
                                     public = list(
                                       assign_feature_ids = function(){
                                         super$assign_feature_ids()
                                         cat("Assign feature_id (Level: Variant Gemini)\n")
                                         
                                         fset_id = private$.reference_object$measurement_set$featureset_id
                                         cat("Match features in file to feature synonyms at featureset_id =", fset_id, "\n")
                                         fsyn = private$.reference_object$feature_synonym
                                         # print(dim(fsyn))
                                         fsyn_sel = fsyn[fsyn$featureset_id == fset_id, ]
                                         # print(dim(fsyn_sel))
                                         
                                         m2 = find_matches_and_return_indices(private$.data_df$gene, 
                                                                              fsyn_sel$synonym)
                                         stopifnot(length(m2$source_unmatched_idx) == 0)
                                         
                                         private$.data_df$feature_id = fsyn_sel$feature_id[m2$target_matched_idx]
                                       },
                                       
                                       register_new_features = function() {
                                         cat("Function: Register new features (Level: DataFrameLoaderVariantGemini)\n")
                                         super$register_new_features()
                                         
                                         stopifnot(nrow(private$.reference_object$pipeline_df) == 1)
                                         fsets_scidb = private$.reference_object$featureset
                                         fset = drop_na_columns(fsets_scidb[match(private$.reference_object$pipeline_df$featureset_name, 
                                                                                  fsets_scidb$name), ])
                                         stopifnot(nrow(fset) == 1)
                                         cat("Matching features in file by feature-synonyms in DB at featureset_id", 
                                             fset$featureset_id, "\n")
                                         fsyn_sel = private$.reference_object$feature_synonym[
                                           private$.reference_object$feature_synonym$featureset_id == 
                                             fset$featureset_id, ]
                                         m1 = find_matches_and_return_indices(private$.data_df$gene, fsyn_sel$synonym)
                                         # length(m1$source_matched_idx)
                                         # length(m1$source_unmatched_idx)
                                         # private$.data_df$gene[(m1$source_unmatched_idx)]
                                         
                                         private$.data_df$feature_id = -1
                                         if (length(m1$source_unmatched_idx) > 0) {
                                           private$.data_df$feature_id[m1$source_matched_idx] = fsyn_sel$feature_id[m1$target_matched_idx]
                                           
                                           unmatched = private$.data_df[private$.data_df$feature_id == -1, ]
                                           unmatched_genes = as.character(unique(unmatched$gene))
                                           cat("Manually registering", length(unmatched_genes), "new features\n")
                                           ftr_new = data.frame(name = unmatched_genes,
                                                                featureset_id = fset$featureset_id,
                                                                chromosome = 'unknown',
                                                                gene_symbol = unmatched_genes,
                                                                start = '...',
                                                                end = '...',
                                                                strand_term = search_ontology(terms = 'strand_term_unspecified'),
                                                                feature_type = 'gene',
                                                                source = 'GEMINI mutation file',
                                                                stringsAsFactors = FALSE)
                                           new_ftr_ids = register_feature(df = ftr_new, register_gene_synonyms = TRUE)
                                           
                                           # update the selected features
                                           cat("updating feature in reference object\n")
                                           private$.reference_object$feature = search_features(featureset_id = fset$featureset_id)
                                           cat("updating feature-synonym in reference object\n")
                                           fsyn = scidb4gh:::get_feature_synonym()
                                           private$.reference_object$feature_synonym = fsyn[fsyn$featureset_id == fset$featureset_id, ]
                                         } else {
                                           cat("No new features to register\n")
                                         }
                                         invisible(self)
                                       }
                                     ))

DataFrameLoaderVariantGeminiFiltered = R6::R6Class(classname = 'DataFrameLoaderVariantGeminiFiltered',
                                           inherit = DataFrameLoaderVariantGemini,
                                           public = list())

                            
createDataLoader = function(data_df, reference_object){
  switch(reference_object$measurement_set$concat,
         'RNAQuantification / RNASeq / Cufflinks_Isoform_FPKM' = DataFrameLoaderRNASeqCufflinksIsoform$new(data_df = data_df,
                                                                                              reference_object = reference_object),
         'RNAQuantification / RNASeq / Cufflinks_Gene_FPKM' = DataFrameLoaderRNASeqCufflinksGene$new(data_df = data_df,
                                                                                                     reference_object = reference_object),
         'RNAQuantification / RNASeq / HTSeq' = DataFrameLoaderRNASeqCufflinksGene$new(data_df = data_df,
                                                                                                     reference_object = reference_object),
         'Variant / SNV / Mutect / SNPeff / GEMINI / NS / (other filters)' = DataFrameLoaderVariantGeminiFiltered$new(data_df = data_df,
                                                                   reference_object = reference_object))
}

