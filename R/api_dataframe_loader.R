#' convenience function for matching features between database and file
match_features = function(features_in_file, df_features_db, feature_type, column_in_db) {
  df_features_db = df_features_db[df_features_db$feature_type == feature_type | 
                                    df_features_db$feature_type == 'controls', ]
  
  print(head(as.character(features_in_file)))
  matchL = find_matches_and_return_indices(features_in_file, df_features_db[, column_in_db])
  
  # Assume that all features are already registered
  if (length(matchL$source_unmatched_idx) != 0) {
    cat("Out of", length(unique(features_in_file)), "features, ")
    cat("unmatched features:", 
        pretty_print(vec = unique(features_in_file[matchL$source_unmatched_idx])), "\n")
    stop("...")
  }
  df_features_db$feature_id[matchL$target_matched_idx]
}

##### DataLoader #####
DataLoader = R6::R6Class(classname = "DataLoader", 
        public = list(
          print_level = function() {cat("----(Level: DataLoader)\n")},
          assign_biosample_ids = function(){
            cat("assign_biosample_ids()"); self$print_level()
            bios_ref = private$.reference_object$biosample
            if (private$.reference_object$measurement_set$entity %in% c(.ghEnv$meta$arrRnaquantification)) {
              suffix = "__RNA"
            } else if (private$.reference_object$measurement_set$entity %in% 
                       c(.ghEnv$meta$arrVariant,
                         .ghEnv$meta$arrFusion)) {
              suffix = "__DNA"
            } else {
              stop("Assigning suffix '__DNA' or '__RNA' based on entity type. 
                   Suffix needs to be assigned for current entity:", private$.reference_object$measurement_set$entity)
            }
            bios_ref = bios_ref[grep(suffix, bios_ref$name), ]
            cat("Chose suffix:", suffix, "for entity:", private$.reference_object$measurement_set$entity, 
                "\nRetained:", nrow(bios_ref), "of total:", nrow(private$.reference_object$biosample), "in manifest\n")
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
                print(sample_name_in_file)
                print(head(bios_ref$name))
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
            cat("download_features_for_featureset()"); self$print_level()
            fsets_scidb = private$.reference_object$featureset
            # print(dim(fsets_scidb))
            
            featureset_name = unique(private$.reference_object$pipeline_df[, 
                                                      template_linker$featureset$choices_col])
            stopifnot(length(featureset_name) == 1)
            # print(featureset_name)
            
            fset = fsets_scidb[match(featureset_name, 
                                     fsets_scidb[, template_linker$featureset$choices_col]), ]
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
            cat("register_new_features()"); self$print_level()
            return(FALSE)
          },
          
          #' return reference_object 
          #' 
          #'  For DEBUG only
          # get_reference_object = function(){
          #   private$.reference_object
          # },
          
          # return features in reference object
          retrieve_features = function() {
            cat("retrieve_features()"); self$print_level()
            private$.reference_object$feature
          },
          
          retrieve_feature_synonyms = function() {
            cat("retrieve_feature_synonyms()"); self$print_level()
            private$.reference_object$feature_synonym
          },
          
          #' update feature and feature synonym in reference object
          update_reference_object = function() {
            # update the selected features
            fsets_scidb = private$.reference_object$featureset
            # print(dim(fsets_scidb))
            
            featureset_name = unique(private$.reference_object$pipeline_df[, 
                                                                           template_linker$featureset$choices_col])
            stopifnot(length(featureset_name) == 1)
            # print(featureset_name)
            
            fset = fsets_scidb[match(featureset_name, 
                                     fsets_scidb[, template_linker$featureset$choices_col]), ]

            cat("updating feature in reference object\n")
            
            private$.reference_object$feature = search_features(featureset_id = 
                                fset$featureset_id)
            cat("updating feature-synonym in reference object\n")
            fsyn = get_feature_synonym()
            private$.reference_object$feature_synonym = fsyn[fsyn$featureset_id == fset$featureset_id, ]
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
          initialize = function(data_df, reference_object, feature_annotation_df = NULL){
            private$.data_df = data_df
            private$.reference_object = reference_object
            private$.feature_annotation_df = feature_annotation_df
          }
        ), private = list(
          .data_df = NULL,
          .feature_annotation_df = NULL, 
          .reference_object = NULL
        ))
##### DataLoaderRNAQuant #####
DataLoaderRNAQuant = R6::R6Class(classname = "DataLoaderRNAQuant",
                                inherit = DataLoader,
                                public = list(
                                  print_level = function() {cat("----(Level: DataLoaderRNAQuant)\n")},
                                  load_data = function(){
                                    cat("load_data()"); self$print_level()
                                    df_size_mb = as.integer(object.size(private$.data_df))/1024/1024
                                    upload_chunk_max = 200 # 200 MB
                                    if (df_size_mb < upload_chunk_max) {
                                      register_expression_dataframe(df1 = private$.data_df, 
                                                                    dataset_version = private$.reference_object$record$dataset_version)
                                    } else{
                                      factorLarger = round(df_size_mb/upload_chunk_max)
                                      cat("Dataframe is of size:", df_size_mb, "Mb. Uploading in", factorLarger, "pieces\n")
                                      stepSize = round(nrow(private$.data_df)/factorLarger)
                                      starts = seq(1, nrow(private$.data_df), stepSize)
                                      ends = starts + stepSize - 1
                                      ends[length(ends)] = nrow(private$.data_df)
                                      for (idx in 1:factorLarger) {
                                        cat("Uploading data.frame sub-chunk #", idx, "\n")
                                        register_expression_dataframe(df1 = private$.data_df[c(starts[idx]:ends[idx]), ], 
                                                                      dataset_version = private$.reference_object$record$dataset_version)
                                      }
                                    }
                                  },
                                  
                                  assign_feature_ids = function(feature_type, column_in_file) {
                                    super$assign_feature_ids()
                                    cat("assign_feature_ids()"); self$print_level()
                                    
                                    private$.data_df$feature_id = match_features(
                                      features_in_file = private$.data_df[, column_in_file],
                                      df_features_db = private$.reference_object$feature,
                                      feature_type = feature_type,
                                      column_in_db = 'name')
                                    private$.data_df[, column_in_file] = NULL
                                  }
                                ))
                                  
##### DataLoaderRNAQuantRNASeq #####
DataLoaderRNAQuantRNASeq = R6::R6Class(classname = "DataLoaderRNAQuantRNASeq",
                                inherit = DataLoaderRNAQuant, 
                                public = list(
                                  print_level = function() {cat("----(Level: DataLoaderRNAQuantRNASeq)\n")},
                                  register_new_features = function() {
                                    cat("register_new_features()"); self$print_level()
                                    if ('gene_short_name' %in% colnames(private$.data_df)) {
                                      stopifnot(nrow(private$.reference_object$pipeline_df) == 1)
                                      fsets_scidb = private$.reference_object$featureset
                                      fset = drop_na_columns(fsets_scidb[match(private$.reference_object$pipeline_df[, 
                                                                                                                     template_linker$featureset$choices_col], 
                                                                               fsets_scidb[,
                                                                                           template_linker$featureset$choices_col]), ])
                                      stopifnot(nrow(fset) == 1)
                                      # cat("Reading annotation info from RNA-seq data file\n")
                                      feature_df = data.frame(name = private$.data_df$tracking_id,
                                                           gene_symbol = private$.data_df$gene_short_name,
                                                           featureset_id = fset$featureset_id,
                                                           chromosome = 'unknown',
                                                           start = '...',
                                                           end = '...',
                                                           strand_term = search_ontology(terms = 'strand_term_unspecified'),
                                                           feature_type = 'gene',
                                                           source = 'RNA-seq file',
                                                           stringsAsFactors = FALSE)
                                      
                                      cat("Matching features in file by feature-names in DB at featureset_id", 
                                          fset$featureset_id, "\n")
                                      features_sel = private$.reference_object$feature
                                      m1 = find_matches_and_return_indices(private$.data_df$tracking_id, features_sel$name)
                                      
                                      if (length(m1$source_unmatched_idx) > 0) {
                                        new_ftr_ids = register_feature(df = feature_df)
                                      
                                        return(TRUE)
                                      } else {
                                        cat("No new features to register\n")
                                        return(FALSE)
                                      }
                                    }
                                  }))
##### DataLoaderRNASeqGeneFormat #####
# loader corresponding to output of all DataReaderRNASeqGene* types 
# e.g. DataReaderRNASeqGeneFormatA, DataReaderRNAQuantRNASeqCufflinks
DataLoaderRNASeqGeneFormat = R6::R6Class(classname = "DataLoaderRNASeqGeneFormat",
                                             inherit = DataLoaderRNAQuantRNASeq,
                                             public = list(
                                               print_level = function() {cat("----(Level: DataLoaderRNASeqGeneFormat)\n")},
                                               register_new_features = function() {
                                                 cat("register_new_features()"); self$print_level()
                                                 col_match_ftr_name = 'tracking_id'
                                                 if (col_match_ftr_name %in% colnames(private$.data_df)) {
                                                   matchTarget = unique(private$.reference_object$pipeline_df[, 
                                                                                                              template_linker$featureset$choices_col])
                                                   stopifnot(length(matchTarget) == 1)
                                                   fsets_scidb = private$.reference_object$featureset
                                                   fset = drop_na_columns(fsets_scidb[match(matchTarget, 
                                                                                            fsets_scidb[,
                                                                                                        template_linker$featureset$choices_col]), ])
                                                   stopifnot(nrow(fset) == 1)
                                                   
                                                   cat("Matching features in file by feature-names in DB at featureset_id", 
                                                       fset$featureset_id, "\n")
                                                   features_sel = private$.reference_object$feature
                                                   m1 = find_matches_and_return_indices(private$.data_df[, col_match_ftr_name], 
                                                                                        features_sel$name)
                                                   
                                                   if (length(m1$source_unmatched_idx) > 0) {
                                                     stop("Need to implement this code-path. Follow template for `DataLoaderRNAQuantMicroarray::register_new_features()`")
                                                     ftr_ann_df = data.frame(name = "...", stringsAsFactors = FALSE)

                                                     register_feature(df = ftr_ann_df, 
                                                                      register_gene_synonyms = FALSE)
                                                     return(TRUE)
                                                   } else {
                                                     cat("No new features to register\n")
                                                     return(FALSE)
                                                   }
                                                 }
                                               },
                                               assign_feature_ids = function(){
                                                 cat("assign_feature_ids()"); self$print_level()
                                                 super$assign_feature_ids(feature_type = 'gene',
                                                                          column_in_file = 'tracking_id')
                                               }))

##### DataLoaderRNAQuantRNASeqCufflinksIsoform #####
DataLoaderRNAQuantRNASeqCufflinksIsoform = R6::R6Class(classname = "DataLoaderRNAQuantRNASeqCufflinksIsoform",
                                                inherit = DataLoaderRNAQuantRNASeq,
                                                public = list(
                                                  print_level = function() {cat("----(Level: DataLoaderRNAQuantRNASeqCufflinksIsoform)\n")},
                                                  assign_feature_ids = function(){
                                                    cat("assign_feature_ids()"); self$print_level()
                                                    super$assign_feature_ids(feature_type = 'transcript',
                                                                             column_in_file = 'tracking_id')
                                                  }))


##### DataLoaderRNAQuantMicroarray #####
DataLoaderRNAQuantMicroarray = R6::R6Class(classname = "DataLoaderRNAQuantMicroarray",
                                       inherit = DataLoaderRNAQuant, 
                                       public = list(
                                         print_level = function() {cat("----(Level: DataLoaderRNAQuantMicroarray)\n")},
                                         register_new_features = function() {
                                           cat("register_new_features()"); self$print_level()
                                           col_match_ftr_name = 'probeset_id'
                                           if (col_match_ftr_name %in% colnames(private$.data_df)) {
                                             matchTarget = unique(private$.reference_object$pipeline_df[, 
                                                                                                        template_linker$featureset$choices_col])
                                             stopifnot(length(matchTarget) == 1)
                                             fsets_scidb = private$.reference_object$featureset
                                             fset = drop_na_columns(fsets_scidb[match(matchTarget, 
                                                                                      fsets_scidb[,
                                                                                                  template_linker$featureset$choices_col]), ])
                                             stopifnot(nrow(fset) == 1)
                                             
                                             cat("Matching features in file by feature-names in DB at featureset_id", 
                                                 fset$featureset_id, "\n")
                                             features_sel = private$.reference_object$feature
                                             m1 = find_matches_and_return_indices(private$.data_df[, col_match_ftr_name], 
                                                                                  features_sel$name)
                                             
                                             if (length(m1$source_unmatched_idx) > 0) {
                                               ftr_ann_df = private$.feature_annotation_df
                                               
                                               ftr_ann_df$name = rownames(ftr_ann_df)
                                               ftr_ann_df = plyr::rename(ftr_ann_df, 
                                                                         c('ENTREZID' = 'entrez_gene_id',
                                                                           'ENSEMBL' = 'ensembl_gene_id',
                                                                           'SYMBOL' = 'gene_symbol',
                                                                           'GENENAME' = 'gene_full_name', 
                                                                           'GENEFAMILY' = 'gene_family',
                                                                           'INICalls' = 'INI_calls',
                                                                           'PROBENUM' = 'probe_num'))
                                               
                                               unmatched_features = unique(private$.data_df[, 
                                                                        col_match_ftr_name][m1$source_unmatched_idx])
                                               
                                               m2 = find_matches_and_return_indices(unmatched_features, 
                                                                                    ftr_ann_df$name)
                                               if (length(m2$source_unmatched_idx) > 0) {
                                                 stop("Feature annotation data does not contain annotation for",
                                                      pretty_print(unmatched_features[m2$source_unmatched_idx]))
                                               }
                                               
                                               ftr_ann_df = ftr_ann_df[m2$target_matched_idx, ]
                                               rownames(ftr_ann_df) = 1:nrow(ftr_ann_df)
                                               
                                               ftr_ann_df$featureset_id = fset$featureset_id
                                               ftr_ann_df$chromosome = 'NA'
                                               ftr_ann_df$start = 'NA'
                                               ftr_ann_df$end = 'NA'
                                               ftr_ann_df$strand_term = search_ontology(terms = 'strand_term_unspecified')
                                               ftr_ann_df$feature_type = 'probeset'
                                               ftr_ann_df$source = 'expression_set_object'
                                               register_feature(df = ftr_ann_df, 
                                                                register_gene_synonyms = FALSE)
                                               return(TRUE)
                                             } else {
                                               cat("No new features to register\n")
                                               return(FALSE)
                                             }
                                           }
                                         },
                                         assign_feature_ids = function(){
                                           cat("assign_feature_ids()"); self$print_level()
                                           super$assign_feature_ids(feature_type = 'probeset',
                                                                    column_in_file = 'probeset_id')
                                         }))

##### DataLoaderVariant #####
DataLoaderVariant = R6::R6Class(classname = 'DataLoaderVariant',
                                     inherit = DataLoader,
                                     public = list(
                                       print_level = function() {cat("----(Level: DataLoaderVariant)\n")},
                                       load_data = function() {
                                         cat("load_data()"); self$print_level()
                                         register_variant(df = private$.data_df, 
                                                          dataset_version = private$.reference_object$record$dataset_version)
                                       }
                                     ))

##### DataLoaderVariantGemini #####
DataLoaderVariantGemini = R6::R6Class(classname = 'DataLoaderVariantGemini',
                                     inherit = DataLoaderVariant,
                                     public = list(
                                       print_level = function() {cat("----(Level: DataLoaderVariant)\n")},
                                       assign_feature_ids = function(){
                                         cat("assign_feature_ids()"); self$print_level()
                                         super$assign_feature_ids()
                                         
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
                                         cat("Function: Register new features (Level: DataLoaderVariantGemini)\n")
                                         super$register_new_features()
                                         
                                         fset_choice = unique(private$.reference_object$pipeline_df[,
                                                                template_linker$featureset$choices_col])
                                         stopifnot(length(fset_choice) == 1)
                                         fsets_scidb = private$.reference_object$featureset
                                         fset = drop_na_columns(fsets_scidb[match(fset_choice, 
                                                                                  fsets_scidb[,
                                                                                              template_linker$featureset$choices_col]), ])
                                         stopifnot(nrow(fset) == 1)
                                         cat("Matching features in file by feature-synonyms in DB at featureset_id", 
                                             fset$featureset_id, "\n")
                                         fsyn_sel = private$.reference_object$feature_synonym[
                                           private$.reference_object$feature_synonym$featureset_id == 
                                             fset$featureset_id, ]
                                         m1 = find_matches_and_return_indices(private$.data_df$gene, 
                                                                              fsyn_sel[fsyn_sel$source == 'gene_symbol', ]$synonym)
                                         
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
                                           
                                           return(TRUE)
                                         } else {
                                           cat("No new features to register\n")
                                           return(FALSE)
                                         }
                                         invisible(self)
                                       }
                                     ))

##### DataLoaderFusionTophat #####
DataLoaderFusionTophat = R6::R6Class(classname = 'DataLoaderFusionTophat',
                                     inherit = DataLoader,
                                     public = list(
                                       print_level = function() {cat("----(Level: DataLoaderFusionTophat)\n")},
                                       register_new_features = function() {
                                         fset_choice = unique(private$.reference_object$pipeline_df[,
                                                                                                    template_linker$featureset$choices_col])
                                         stopifnot(length(fset_choice) == 1)
                                         fsets_scidb = private$.reference_object$featureset
                                         fset = drop_na_columns(fsets_scidb[match(fset_choice, 
                                                                                  fsets_scidb[,
                                                                                              template_linker$featureset$choices_col]), ])
                                         stopifnot(nrow(fset) == 1)
                                         cat("Matching features in file by feature-synonyms in DB at featureset_id", 
                                             fset$featureset_id, "\n")
                                         fsyn_sel = private$.reference_object$feature_synonym[
                                           private$.reference_object$feature_synonym$featureset_id == 
                                             fset$featureset_id, ]
                                         
                                         list_of_features = unique(c(as.character(private$.data_df$gene_left), 
                                                                     as.character(private$.data_df$gene_right)))
                                         head(list_of_features)
                                         
                                         matches_synonym = find_matches_and_return_indices(list_of_features, 
                                                                                           fsyn_sel$synonym)
                                         
                                         unmatched = matches_synonym$source_unmatched_idx
                                         cat("Number of unmatched gene symbols:", length(unmatched), "\n e.g.", 
                                             pretty_print(list_of_features[unmatched]), "\n")
                                         
                                         if (length(list_of_features[unmatched]) > 0) {
                                           newfeatures = data.frame(
                                             name = list_of_features[unmatched],
                                             gene_symbol = 'NA',
                                             featureset_id = fset$featureset_id,
                                             chromosome = "unknown",
                                             start = '...', 
                                             end = '...',
                                             strand_term = search_ontology('strand_term_unspecified'),
                                             feature_type = "gene",
                                             source = "Tophat fusion file")
                                           
                                           feature_record = register_feature(df = newfeatures)
                                           
                                           return(TRUE)
                                         } else {
                                           cat("No new features to register\n")
                                           return(FALSE)
                                         }                                         
                                       },
                                       assign_feature_ids = function(){
                                         cat("assign_feature_ids()"); self$print_level()
                                         super$assign_feature_ids()
                                         
                                         syn = private$.reference_object$feature_synonym
                                         syn = syn[syn$featureset_id == 
                                                     private$.reference_object$measurement_set$featureset_id, ]
                                         
                                         # Now register the left and right genes with system feature_id-s
                                         private$.data_df$feature_id_left = syn[match(private$.data_df$gene_left, syn$synonym), ]$feature_id
                                         private$.data_df$feature_id_right = syn[match(private$.data_df$gene_right, syn$synonym), ]$feature_id
                                         stopifnot(!any(is.na(private$.data_df$feature_id_left)))
                                         stopifnot(!any(is.na(private$.data_df$feature_id_right)))
                                       },
                                       load_data = function() {
                                         cat("load_data()"); self$print_level()
                                         private$.data_df = plyr::rename(private$.data_df,
                                                                         c('biosample_name' = 
                                                                             'sample_name_unabbreviated'))
                                         register_fusion_data(df = private$.data_df,
                                                              measurementset = private$.reference_object$measurement_set)
                                       }
                                     ))

##### createDataLoader #####
#' @export      
createDataLoader = function(data_df, reference_object, feature_annotation_df = NULL){
  temp_string = paste0("{",
                       reference_object$measurement_set$pipeline_scidb, 
                       "}{", 
                       reference_object$measurement_set$quantification_level, 
                       "}")
  switch(temp_string,
         "{[external]-[RNA-seq] Cufflinks}{gene}" = ,
         "{[external]-[RNA-seq] HTSeq}{gene}" = ,
         "{[external]-[RNA-seq] Salmon}{gene}" = ,
         "{[external]-[RNA-seq] Sailfish}{gene}" = ,
         "{[DNAnexus]-[RNAseq_Expression_AlignmentBased v1.3.3] Cufflinks}{gene}" = ,
           DataLoaderRNASeqGeneFormat$new(data_df = data_df,
                                                     reference_object = reference_object,
                                                     feature_annotation_df = feature_annotation_df),
         "{[external]-[RNA-seq] Cufflinks}{transcript}" = 
           DataLoaderRNAQuantRNASeqCufflinksIsoform$new(data_df = data_df,
                                                  reference_object = reference_object),
         "{[Affymetrix]-[Microarray] Affymetrix Bioconductor CDF v3.2.0}{gene}" = ,
         "{[Affymetrix]-[Microarray] UMich Alt CDF v20.0.0}{gene}" =
           DataLoaderRNAQuantMicroarray$new(data_df = data_df, 
                                            reference_object = reference_object,
                                            feature_annotation_df = feature_annotation_df),
         "{[DNAnexus]-[Variant_Custom: MuTect HC + PoN + Annotate] Mutect / SnpEff / GEMINI}{DNA}" = ,
         "{[DNAnexus]-[Variant_Custom: GATK + Annotate] GATK / SnpEff / GEMINI}{DNA}" = ,
         "{[DNAnexus]-[Variant_Custom: GATK + PoN + Annotate] GATK / SnpEff / GEMINI}{DNA}" = ,
         "{[DNAnexus]-[Variant_Custom: VarScan + Annotate] VarScan / SnpEff / GEMINI}{DNA}" = ,
         "{[DNAnexus]-[Variant_Custom: VarScan + PoN + Annotate] VarScan / SnpEff / GEMINI}{DNA}" = ,
         "{[DNAnexus]-[DNA-seq Tumor Only v1.3] Mutect / SnpEff / GEMINI (non-TCGA gnomAD & ExAC)}{DNA}" =
           DataLoaderVariantGemini$new(data_df = data_df,
                                                    reference_object = reference_object),
         "{[external]-[Fusion] Tophat Fusion}{gene}" = 
           DataLoaderFusionTophat$new(data_df = data_df,
                                           reference_object = reference_object)
  )
}

