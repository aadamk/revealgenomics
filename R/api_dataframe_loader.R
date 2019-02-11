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

#' find featureset from Pipeline sheet info
#' 
#' Row or row(s) of Pipeline sheet are used to load data. Unique choice
#' of featureset name is used to subselect a specific featureset registered
#' in the database. 
find_matching_featureset = function(
  pipeline_df, # row(s) of Pipeline sheets that are being loaded
  featureset_link_col, # column linking selection in Pipelines sheet to featureset definition
  fsets_scidb # dataframe containing all featuresets registered with system
) {
  matchTarget = unique(pipeline_df[, featureset_link_col])
  stopifnot(length(matchTarget) == 1)
  fset = drop_na_columns(fsets_scidb[match(matchTarget, 
                                           fsets_scidb[,
                                                       featureset_link_col]), ])
  stopifnot(nrow(fset) == 1)
  fset
}

#' Level 1 feature matching
#' 
#' Match column in file with feature names
#' Return result is a structure with matched and unmatched indices
feature_matching_level1 = function(data_df, # data-frame containing feature and measurement values
                                   col_match_ftr_name, # column in data to match with scidb features
                                   fset, # specific featureset to be used for matching
                                   feature_df # features data frame at specific featureset_id
) {
  cat("Matching features in file by feature-names in DB at featureset_id", 
      fset$featureset_id, "\n")
  find_matches_and_return_indices(data_df[, col_match_ftr_name], 
                                  feature_df$name)
}       

##### DataLoader #####
DataLoader = R6::R6Class(classname = "DataLoader", 
        public = list(
          print_level = function() {cat("----(Level: DataLoader)\n")},
          assign_biosample_ids = function(){
            cat("assign_biosample_ids()"); self$print_level()
            bios_ref = private$.reference_object$biosample
            entity = unique(private$.reference_object$measurement_set$entity)
            if (entity %in% c(.ghEnv$meta$arrRnaquantification,
                              .ghEnv$meta$arrVariant,
                              .ghEnv$meta$arrCytometry_cytof)) {
              suffix = template_helper_suffix_by_entity(entity = private$.reference_object$measurement_set$entity)
              bios_ref = bios_ref[grep(suffix, bios_ref$name), ]
              cat("Chose suffix:", suffix, "for entity:", private$.reference_object$measurement_set$entity, 
                  "\nRetained:", nrow(bios_ref), "of total:", nrow(private$.reference_object$biosample), "in manifest\n")
            }
            if (nrow(private$.reference_object$pipeline_df) > 1) { 
              # multiple Measurements combined into one file
              # ==> data must contain a column called `biosample_name`
              cat("Multiple Measurements combined into one file\n")
              cat("Replacing `biosample_name` with `biosample_id`\n")
              
              cat("Summary file has", length(unique(private$.data_df$biosample_name)), "biosamples\n")
              cat("Current pipeline record expects", length(private$.reference_object$pipeline_df$original_sample_name), 
                  "entries\n")
              
              if (private$.match_biosample_name_exactly) {
                cat("Doing exact matching\n")
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
              } else {
                cat("Doing inexact name matching\n") # Tested on CyTOF data
                candidate_sample_names = bios_ref$original_sample_name
                file_sample_names = unique(private$.data_df$biosample_name)
                match_file_to_candidate = sapply(
                  file_sample_names, function(nm) {
                    # cat("Sample name: ", nm, "\n")
                    res = unlist(sapply(candidate_sample_names, 
                           function(bios_ref_nm) grepl(bios_ref_nm, nm)))
                    res = which(res)
                    stopifnot(length(res) == 1)
                    as.integer(res)
                  })
                private$.data_df$biosample_id = 
                  bios_ref[match_file_to_candidate[private$.data_df$biosample_name], ]$biosample_id
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
                stop("did not expect to have to match original_sample_names while handling
                     one row of Pipeline sheet (i.e. per sample file)")
              }
              
              if (length(grep(sample_name_in_file, 
                              bios_ref$name)) > 1) {
                if (entity %in% c(.ghEnv$meta$arrRnaquantification,
                                  .ghEnv$meta$arrVariant)) {
                  stop("Did not expect this error for RNA-seq/GXP or variant entities")
                }
                stop("sample name in pipeline sheet was matched to more than one sample in biosample dataframe.
                    Must add logic to handle this e.g.
                    - FUSION data is typically derived from RNA sample, and rarely from DNA sample
                    - COPYNUMBER data is typically derived from DNA sample, and rarely from RNA sample")
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
            
            if (is.null(private$.reference_object$feature) ||
                nrow(private$.reference_object$feature) == 0) {
              cat("feature_ref = NULL; downloading features for featureset", fset$featureset_id, "into loaderObject\n")
              private$.reference_object$feature = search_features(featureset_id = fset$featureset_id)
            } else {
              if (unique(private$.reference_object$feature$featureset_id) != fset$featureset_id) {
                cat("featureset has changed; downloading features for featureset", fset$featureset_id, "into loaderObject\n")
                private$.reference_object$feature = search_features(featureset_id = fset$featureset_id)
              }
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
          get_selected_featureset = function() {
            # retrieve the featureset for selected set of entries from Pipeline sheet 
            # (must have downselected to a unique featureset at the time of calling)
            fset_choice = unique(
              private$.reference_object$pipeline_df[,
                                                    template_linker$featureset$choices_col])
            stopifnot(length(fset_choice) == 1)
            fsets_scidb = private$.reference_object$featureset
            fset = drop_na_columns(
              fsets_scidb[match(fset_choice,
                                fsets_scidb[,
                                            template_linker$featureset$choices_col]), ])
            stopifnot(nrow(fset) == 1)
            fset
          },
          get_feature_synonym_df_for_selected_featureset = function() {
            # get feature synonym dataframe for use in matching
            # (assumes user must have downselected to a unique featureset at the time of calling)
            fset = private$get_selected_featureset()
            cat("Matching features in file by feature-synonyms in DB at featureset_id", 
                fset$featureset_id, "\n")
            fsyn_sel = private$.reference_object$feature_synonym[
              private$.reference_object$feature_synonym$featureset_id == 
                fset$featureset_id, ]
            fsyn_sel
          },
          .data_df = NULL,
          .feature_annotation_df = NULL, 
          .reference_object = NULL,
          .match_biosample_name_exactly = TRUE
        ))


##### DataLoaderBlankSlots #####
# Class where all slots are blank on purpose i.e. do no action
# To be used as a dummy
DataLoaderBlankSlots = R6::R6Class(
  classname = "DataLoaderBlankSlots",
  inherit = DataLoader,
  public = list(
    assign_biosample_ids = function() {}, 
    download_features_for_featureset = function() {}, 
    register_new_features = function() {return(FALSE)}, 
    retrieve_features = function() {}, 
    retrieve_feature_synonyms = function() {}, 
    update_reference_object = function() {}, 
    assign_feature_ids = function() {}, 
    assign_other_ids = function() {}, 
    load_data = function() {}
  )
)
##### DataLoaderExpression #####
# class to be shared between loaders for
# - GeneExpression (RNAQuantRNASeq), 
# - ProteinExpression
# - etc.
DataLoaderExpression = R6::R6Class(classname = "DataLoaderExpression",
                                inherit = DataLoader,
                                public = list(
                                  print_level = function() {cat("----(Level: DataLoaderExpression)\n")},
                                  load_data = function(){
                                    cat("load_data()"); self$print_level()
                                    df_size_mb = as.integer(
                                      as.double(object.size(
                                        private$.data_df)
                                        )/1024/1024)
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
                                inherit = DataLoaderExpression, 
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
                                    } else if ('tracking_id' %in% colnames(private$.data_df)) {
                                      col_match_ftr_name = 'tracking_id'
                                      fset = find_matching_featureset(pipeline_df = private$.reference_object$pipeline_df,
                                                                      featureset_link_col = template_linker$featureset$choices_col,
                                                                      fsets_scidb = private$.reference_object$featureset
                                      )
                                      m1 = feature_matching_level1(data_df = private$.data_df,
                                                                   col_match_ftr_name = col_match_ftr_name,
                                                                   fset = fset,
                                                                   feature_df = private$.reference_object$feature
                                                              )
                                      
                                      if (length(m1$source_unmatched_idx) > 0) {
                                        unmatched_ftrs = unique(private$.data_df[m1$source_unmatched_idx,
                                                                                 col_match_ftr_name])
                                        ftr_ann_df = private$.feature_annotation_df
                                        
                                        if (ncol(ftr_ann_df) == 1) {
                                          ftr_ann_df_unmatched = data.frame(name = ftr_ann_df[
                                            match(unmatched_ftrs, ftr_ann_df[, 1]), ],  # matches for RSEM type
                                            stringsAsFactors = FALSE)
                                        } else {
                                          stop("Need to implement this code-path. Follow template for `DataLoaderRNAQuantMicroarray::register_new_features()`")
                                        }
                                        
                                        ftr_ann_df_unmatched$featureset_id = fset$featureset_id
                                        ftr_ann_df_unmatched$gene_symbol = 'NA'
                                        ftr_ann_df_unmatched$chromosome = 'NA'
                                        ftr_ann_df_unmatched$start = 'NA'
                                        ftr_ann_df_unmatched$end = 'NA'
                                        if (length(grep("gene", 
                                                        private$.reference_object$measurement_set$filter_name)) == 1) {
                                          ftr_ann_df_unmatched$feature_type = 'gene'
                                        } else if (length(grep("transcript", 
                                                               private$.reference_object$measurement_set$filter_name)) == 1) {
                                          ftr_ann_df_unmatched$feature_type = 'transcript'
                                        }
                                        ftr_ann_df_unmatched$source = 'rnaseq_file'
                                        
                                        stopifnot(nrow(ftr_ann_df_unmatched) == length(unmatched_ftrs))
                                        register_feature(df = ftr_ann_df_unmatched, 
                                                         register_gene_synonyms = TRUE)
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
                                               assign_feature_ids = function(){
                                                 cat("assign_feature_ids()"); self$print_level()
                                                 super$assign_feature_ids(feature_type = 'gene',
                                                                          column_in_file = 'tracking_id')
                                               }))

##### DataLoaderRNASeqTranscriptFormat #####
DataLoaderRNASeqTranscriptFormat = R6::R6Class(classname = "DataLoaderRNASeqTranscriptFormat",
                                                inherit = DataLoaderRNAQuantRNASeq,
                                                public = list(
                                                  print_level = function() {cat("----(Level: DataLoaderRNASeqTranscriptFormat)\n")},
                                                  assign_feature_ids = function(){
                                                    cat("assign_feature_ids()"); self$print_level()
                                                    super$assign_feature_ids(feature_type = 'transcript',
                                                                             column_in_file = 'tracking_id')
                                                  }))


##### DataLoaderRNAQuantMicroarray #####
DataLoaderRNAQuantMicroarray = R6::R6Class(classname = "DataLoaderRNAQuantMicroarray",
                                       inherit = DataLoaderExpression, 
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

##### DataLoaderProteomicsMaxQuant #####
DataLoaderProteomicsMaxQuant = R6::R6Class(
  classname = "DataLoaderProteomicsMaxQuant",
  inherit = DataLoaderExpression, 
  public = list(
    print_level = function() {cat("----(Level: DataLoaderProteomicsMaxQuant)\n")},
    register_new_features = function() {
      col_match_ftr_name = 'tracking_id'
      fset = find_matching_featureset(pipeline_df = private$.reference_object$pipeline_df,
                                      featureset_link_col = template_linker$featureset$choices_col,
                                      fsets_scidb = private$.reference_object$featureset
      )
      m1 = feature_matching_level1(data_df = private$.data_df, 
                                   col_match_ftr_name = col_match_ftr_name,
                                   fset = fset,
                                   feature_df = private$.reference_object$feature
      )
      
      if (length(m1$source_unmatched_idx) > 0) {
        stop("Need to implement level 2 matching -- see DataLoaderRNAQuantRNASeq")
        return(FALSE)
      } else {
        return(FALSE)
      }
    },
    assign_feature_ids = function(){
      cat("assign_feature_ids()"); self$print_level()
      super$assign_feature_ids(feature_type = 'protein_probe',
                               column_in_file = 'tracking_id')
    }
  )
)

##### DataLoaderVariant #####
DataLoaderVariant = R6::R6Class(classname = 'DataLoaderVariant',
                                     inherit = DataLoader,
                                     public = list(
                                       print_level = function() {cat("----(Level: DataLoaderVariant)\n")},
                                       load_data = function() {
                                         cat("load_data()"); self$print_level()
                                         register_variant(df1 = private$.data_df, 
                                                          dataset_version = private$.reference_object$record$dataset_version)
                                       }
                                     ))

##### DataLoaderVariantGemini #####
#' loader corresponding to GEMINI files
#' - May need to use feature-synonym for feature matching, but current implementation is inefficient
#' - also has hard-coded links to column names used for feature matching (e.g. `gene`)
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
                                         if ('scidb_feature_col' %in% colnames(private$.data_df)) {
                                           cat("Dropping extra column `scidb_feature_col` \n")
                                           private$.data_df$scidb_feature_col = NULL
                                         }
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

##### DataLoaderVariantFormatA #####
#' loader to try and handle various variant formats
#' - currently assumes that features in the data-file are previously registered from a GTF file
#' - may use the `feature_annotation_df` data.frame to figure out action related to feature matching
DataLoaderVariantFormatA = R6::R6Class(classname = 'DataLoaderVariantFormatA',
                                      inherit = DataLoaderVariant,
                                      public = list(
                                        print_level = function() {cat("----(Level: DataLoaderVariant)\n")},
                                        assign_feature_ids = function(){
                                          cat("assign_feature_ids()"); self$print_level()
                                          super$assign_feature_ids()
                                          
                                          column_in_file = 'scidb_feature_col'
                                          feature_type = 'gene' # so far, all variant data files link to gene features only
                                          column_names = colnames(private$.feature_annotation_df)
                                          cat("Feature annotation dataframe has columns:",
                                              paste0(column_names,
                                                     collapse = ", "), 
                                              "\n\tWill match with first column\n")
                                          private$.data_df$feature_id = match_features(
                                            features_in_file = private$.data_df[, column_in_file],
                                            df_features_db = private$.reference_object$feature,
                                            feature_type = feature_type,
                                            column_in_db = column_names[1])
                                          private$.data_df[, column_in_file] = NULL
                                        },
                                        
                                        register_new_features = function() {
                                          cat("Function: Register new features (Level: DataLoaderVariantFormatA)\n")
                                          super$register_new_features()
                                          
                                          fset_id = private$.reference_object$measurement_set$featureset_id
                                          cat("Match features in file to features at featureset_id =", fset_id, "\n")
                                          
                                          ftrs = private$.reference_object$feature
                                          stopifnot(unique(ftrs$featureset_id) == fset_id)
                                          
                                          column_names = colnames(private$.feature_annotation_df)
                                          cat("Feature annotation dataframe has columns:",
                                              paste0(column_names,
                                                     collapse = ", "), 
                                              "\n\tWill match with first column\n")
                                          # =========== Level 1 matching: match with gene symbols ===========
                                          m1 = find_matches_and_return_indices(
                                            private$.data_df$scidb_feature_col,
                                            ftrs[, column_names[1]]
                                          )
                                          if (length(m1$source_unmatched_idx) > 0) {
                                            cat("Level 1 matching with gene symbols is insufficient;\n\t
                                                Proceeding to match with synonyms\n")
                                            ftrs_unmatched_v1 = private$.data_df$scidb_feature_col[
                                              m1$source_unmatched_idx]
                                            fsyn = private$.reference_object$feature_synonym
                                            fsyn = fsyn[fsyn$featureset_id == fset_id, ]
                                            
                                            # =========== Level 2 matching: match with gene synonyms ===========
                                            m2 = find_matches_and_return_indices(
                                              ftrs_unmatched_v1, 
                                              fsyn$synonym
                                            )
                                            if (length(m2$source_unmatched_idx) > 0) {
                                              stop("currently assumes that features in the data-file are 
                                                 previously registered from a GTF file,
                                                   match with standard hugo gene symbol list, 
                                                   or match with gene synonyms")
                                              return(TRUE)
                                            }
                                            matched_syn_feature_id = fsyn[m2$target_matched_idx, ]$feature_id
                                            syn_ftrs = get_features(feature_id = fsyn[m2$target_matched_idx, ]$feature_id)
                                            syn_ftrs = syn_ftrs[match(matched_syn_feature_id, 
                                                           syn_ftrs$feature_id), ]
                                            stopifnot(nrow(syn_ftrs) == length(m1$source_unmatched_idx))
                                            
                                            cat("Now overwriting synonym in data with standard hugo symbol:\n\t",
                                                pretty_print(unique(ftrs_unmatched_v1)),
                                                "==>", 
                                                pretty_print(unique(syn_ftrs$gene_symbol)), 
                                                "\n")
                                            private$.data_df[m1$source_unmatched_idx, ]$scidb_feature_col = 
                                              syn_ftrs$gene_symbol
                                            return(FALSE)
                                          } else {
                                            cat("No new features to register\n")
                                            return(FALSE)
                                          }
                                          invisible(self)
                                        }
                                      ))

##### DataLoaderFusionFormatA #####
DataLoaderFusionFormatA = R6::R6Class(
  classname = 'DataLoaderFusionFormatA',
  inherit = DataLoader,
  public = list(
    print_level = function() {cat("----(Level: DataLoaderFusionFormatA)\n")},
    register_new_features = function() {
      fset = private$get_selected_featureset()
      fsyn_sel = private$get_feature_synonym_df_for_selected_featureset()
      
      list_of_features = unique(c(as.character(private$.data_df$gene_left), 
                                  as.character(private$.data_df$gene_right)))
      
      matches_synonym = find_matches_and_return_indices(list_of_features, 
                                                        fsyn_sel$synonym)
      
      unmatched = matches_synonym$source_unmatched_idx
      cat("Number of unmatched gene symbols:", length(unmatched), "\n e.g.", 
          pretty_print(list_of_features[unmatched]), "\n")
      
      if (length(list_of_features[unmatched]) > 0) {
        ftr_source = paste0(
          "measurementset_id: ", 
          private$.reference_object$measurement_set$measurementset_id, 
          "; pipeline_name: ", 
          private$.reference_object$measurement_set$name, 
          "; data_type: ", 
          private$.reference_object$measurement_set$entity)
        
        newfeatures = data.frame(
          name = list_of_features[unmatched],
          gene_symbol = "NA",
          featureset_id = fset$featureset_id,
          chromosome = "unknown",
          start = '...', 
          end = '...',
          feature_type = "gene",
          source = ftr_source)
        
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
      
      syn = private$get_feature_synonym_df_for_selected_featureset()
      
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
      register_fusion(df1 = private$.data_df,
                      measurementset = private$.reference_object$measurement_set)
    }
  ))

##### DataLoaderCopyNumberMatrix #####
DataLoaderCopyNumberMatrix = R6::R6Class(
  classname = "DataLoaderCopyNumberMatrix",
  inherit = DataLoaderExpression, 
  public = list(
    print_level = function() {cat("----(Level: DataLoaderCopyNumberMatrix)\n")},
    register_new_features = function() {
      col_match_ftr_name = 'tracking_id'
      fset = find_matching_featureset(pipeline_df = private$.reference_object$pipeline_df,
                                      featureset_link_col = template_linker$featureset$choices_col,
                                      fsets_scidb = private$.reference_object$featureset
      )
      m1 = feature_matching_level1(data_df = private$.data_df, 
                                   col_match_ftr_name = col_match_ftr_name,
                                   fset = fset,
                                   feature_df = private$.reference_object$feature
      )
      if (length(m1$source_unmatched_idx) > 0) {
        stop("Need to implement level 2 matching -- see DataLoaderRNAQuantRNASeq")
        return(FALSE)
      } else {
        return(FALSE)
      }
    },
    assign_feature_ids = function(){
      cat("assign_feature_ids()"); self$print_level()
      super$assign_feature_ids(feature_type = 'gene',
                               column_in_file = 'tracking_id')
    }
  )
)

##### DataLoaderCopyNumberVariantFormatA #####
#' loader to try and handle various copy number variant subformats that have variable number of columns
#' - currently assumes that features in the data-file are previously registered from a GTF file
#' - may use the `feature_annotation_df` data.frame to figure out action related to feature matching
DataLoaderCopyNumberVariantFormatA = R6::R6Class(
  classname = 'DataLoaderCopyNumberVariantFormatA',
  inherit = DataLoader,
  public = list(
   print_level = function() {cat("----(Level: DataLoaderVariant)\n")},
   assign_feature_ids = function(){
     cat("assign_feature_ids()"); self$print_level()
     super$assign_feature_ids()
     
     syn = private$get_feature_synonym_df_for_selected_featureset()
     
     # Now register the left and right genes with system feature_id-s
     column_in_file = 'scidb_feature_col'
     private$.data_df$feature_id = syn[match(private$.data_df[, column_in_file], syn$synonym), ]$feature_id
     stopifnot(!any(is.na(private$.data_df$feature_id)))
     private$.data_df[, column_in_file] = NULL
   },
   
   register_new_features = function() {
     fset = private$get_selected_featureset()
     fsyn_sel = private$get_feature_synonym_df_for_selected_featureset()
     
     list_of_features = unique(as.character(private$.data_df[, 'scidb_feature_col']))
     
     matches_synonym = find_matches_and_return_indices(list_of_features, 
                                                       fsyn_sel$synonym)
     
     unmatched = matches_synonym$source_unmatched_idx
     cat("Number of unmatched gene symbols:", length(unmatched), "\n e.g.", 
         pretty_print(list_of_features[unmatched]), "\n")
     
     if (length(list_of_features[unmatched]) > 0) {
       ftr_source = paste0(
         "measurementset_id: ", 
         private$.reference_object$measurement_set$measurementset_id, 
         "; pipeline_name: ", 
         private$.reference_object$measurement_set$name, 
         "; data_type: ", 
         private$.reference_object$measurement_set$entity)
       
       newfeatures = data.frame(
         name = list_of_features[unmatched],
         gene_symbol = "NA",
         featureset_id = fset$featureset_id,
         chromosome = "unknown",
         start = '...', 
         end = '...',
         feature_type = "gene",
         source = ftr_source, 
         stringsAsFactors = FALSE)
       
       feature_record = register_feature(df = newfeatures)
       
       return(TRUE)
     } else {
       cat("No new features to register\n")
       return(FALSE)
     }                                         
   },
   load_data = function() {
     cat("load_data()"); self$print_level()
     register_copynumbervariant_variable_columns(df1 = private$.data_df,
                     measurementset = private$.reference_object$measurement_set)
   }
  ))


DataLoaderCyTOF = R6::R6Class(
  classname = 'DataLoaderCyTOF',
  inherit = DataLoaderExpression,
  public = list(
    print_level = function() {cat("----(Level: DataLoaderCyTOF)\n")},
    assign_feature_ids = function(){
      cat("assign_feature_ids()"); self$print_level()
      # super$assign_feature_ids() # override the definition in parent class
      
      ftr_df = private$.reference_object$feature
      
      # Now register the left and right genes with system feature_id-s
      column_in_file = 'scidb_feature_col'
      private$.data_df$feature_id = ftr_df[match(private$.data_df[, column_in_file], ftr_df$name), ]$feature_id
      stopifnot(!any(is.na(private$.data_df$feature_id)))
      private$.data_df[, column_in_file] = NULL
    },
    
    register_new_features = function() {
      fset = private$get_selected_featureset()
      ftr_sel = private$.reference_object$feature
      
      list_of_features = unique(as.character(private$.data_df[, 'scidb_feature_col']))
      
      matches_synonym = find_matches_and_return_indices(list_of_features, 
                                                        ftr_sel$name)
      
      unmatched = matches_synonym$source_unmatched_idx
      cat("Number of unmatched gene symbols:", length(unmatched), "\n e.g.", 
          pretty_print(list_of_features[unmatched]), "\n")
      
      if (length(list_of_features[unmatched]) > 0) {
        stop("Expected all features to be preregistered")
        
        return(TRUE)
      } else {
        cat("No new features to register\n")
        return(FALSE)
      }                                         
    }
  ),
  private = list(
    .match_biosample_name_exactly = FALSE
  )
)

##### createDataLoader #####
#' @export      
createDataLoader = function(data_df, reference_object, feature_annotation_df = NULL){
  # Special formulation for entries that need to be disambuiguated by filter_choices
  if (grepl("COPY|CNV|CyTOF", reference_object$measurement_set$pipeline_scidb) |
      grepl("file link", reference_object$measurement_set$filter_name)) { 
    temp_string = paste0("{", reference_object$measurement_set$pipeline_scidb, "}{", 
                         reference_object$measurement_set$filter_name, "}")
  } else {
    temp_string = paste0("{", reference_object$measurement_set$pipeline_scidb, "}{", 
                         reference_object$measurement_set$quantification_level, "}")
  }
  switch(temp_string,
         "{[external]-[RNA-seq] Cufflinks}{gene}" = ,
         "{[external]-[RNA-seq] HTSeq}{gene}" = ,
         "{[external]-[RNA-seq] Salmon}{gene}" = ,
         "{[external]-[RNA-seq] Sailfish}{gene}" = ,
         "{[internal]-[RNA-Seq] RSEM}{gene}" = ,
         "{[DNAnexus]-[RNAseq_Expression_AlignmentBased v1.3.3] Cufflinks}{gene}" = ,
         "{[external]-[RNA-seq] featureCounts}{gene}" =
           DataLoaderRNASeqGeneFormat$new(data_df = data_df,
                                          reference_object = reference_object,
                                          feature_annotation_df = feature_annotation_df),
         "{[external]-[RNA-seq] Cufflinks}{transcript}" = ,
         "{[external]-[RNA-seq] Salmon}{transcript}" = ,
         "{[external]-[RNA-seq] Sailfish}{transcript}" = 
           DataLoaderRNASeqTranscriptFormat$new(data_df = data_df,
                                          reference_object = reference_object, 
                                          feature_annotation_df = feature_annotation_df),
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
         "{[external]-[Single Nucleotide Variant] MuTect / seurat / strelka}{DNA}" =,
         "{[external]-[Single Nucleotide Variant] Targeted Region - DNA Analysis Pipeline for Cancer (Personalis)}{DNA}" = ,
         "{[external]-[Single Nucleotide Variant] Targeted Region - FoundationOne Heme (FMI)}{DNA}" =
           DataLoaderVariantFormatA$new(data_df = data_df,
                                        reference_object = reference_object, 
                                        feature_annotation_df = feature_annotation_df),
         "{[internal]-[Proteomics] MaxQuant}{Protein}" = 
           DataLoaderProteomicsMaxQuant$new(data_df = data_df,
                                            reference_object = reference_object),
         "{[external]-[Fusion] Tophat Fusion}{gene}" = ,
         "{[external]-[Fusion] FoundationOne Heme (FMI)}{gene}" = ,
         "{[external]-[Fusion] Defuse}{gene}" =
           DataLoaderFusionFormatA$new(data_df = data_df,
                                   reference_object = reference_object,
                                   feature_annotation_df = feature_annotation_df),
         "{[external]-[Exome CNV] BWA-MEM / GATK / Picard / CNVkit}{DNA - copy number value - log2 ratio}" = ,
         "{[external]-[Exome CNV] BWA-MEM / GATK / Picard / CNVkit}{DNA - copy number value - 3 state call}" = ,
         "{[external]-[Exome CNV] BWA-MEM / GATK / Picard / CNVkit}{DNA - copy number value - 5 state call}" = ,
         "{[external]-[Exome CNV] BWA-MEM / GATK / Picard / CNV Radar}{DNA - copy number value - log2 ratio}" = ,
         "{[external]-[Exome CNV] BWA-MEM / GATK / Picard / CNV Radar}{DNA - copy number value - 3 state call}" = ,
         "{[external]-[Exome CNV] BWA-MEM / GATK / Picard / CNV Radar}{DNA - copy number value - 5 state call}" = ,
         "{[external]-[Exome CNV] CBS - Circular Binary Segmentation}{DNA - copy number value (log2) - largest segment}" = ,
         "{[external]-[Exome CNV] CBS - Circular Binary Segmentation}{DNA - copy number value (log2) - lowest segment}" = ,
         "{[external]-[Whole Genome CNV] CBS - Circular Binary Segmentation}{DNA - copy number value (log2) - largest segment}" = ,
         "{[external]-[Whole Genome CNV] CBS - Circular Binary Segmentation}{DNA - copy number value (log2) - lowest segment}" = 
           DataLoaderCopyNumberMatrix$new(data_df = data_df,
                                          reference_object = reference_object, 
                                          feature_annotation_df = feature_annotation_df),
         "{[external]-[Targeted Region CNV] FoundationOne Heme (FMI)}{DNA - copy number value - custom filter - external partner}" = ,
         "{[external]-[Targeted Region CNV] DNA Analysis Pipeline for Cancer (Personalis)}{DNA - copy number value - custom filter - external partner}" = 
           DataLoaderCopyNumberVariantFormatA$new(data_df = data_df,
                                          reference_object = reference_object, 
                                          feature_annotation_df = feature_annotation_df),
         "{[external]-[Exome CNV] BWA-MEM / GATK / Picard / CNVkit}{DNA - copy number value - images (file link)}" = ,
         "{[external]-[Exome CNV] BWA-MEM / GATK / Picard / CNV Radar}{DNA - copy number value - images (file link)}" = ,
         "{[external]-[Exome CNV] BWA-MEM / GATK / Picard / CNVkit}{DNA - copy number value - segmentation (file link)}" = ,
         "{[external]-[Exome CNV] BWA-MEM / GATK / Picard / CNV Radar}{DNA - copy number value - segmentation (file link)}" = ,
         "{[external]-[Exome CNV] BWA-MEM / GATK / Picard / CNV Radar}{DNA - copy number value - b-allele frequency (file link)}" = ,
         "{[DNAnexus]-[Variant_Custom: MuTect HC + PoN + Annotate] Mutect / SnpEff / GEMINI}{DNA - mutations - unfiltered (file link)}" =
           DataLoaderBlankSlots$new(data_df = data_df,
                                    reference_object = reference_object, 
                                    feature_annotation_df = feature_annotation_df)
  )
}

