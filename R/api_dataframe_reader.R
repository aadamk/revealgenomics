##### DataReader #####
DataReader = R6::R6Class(classname = 'DataReader', 
                         public = list(
                           initialize = function(pipeline_df,
                                                 measurement_set){
                             private$.measurement_set = measurement_set
                             private$.pipeline_df = pipeline_df
                           },
                           print_level = function() {cat("----(Level: DataReader)\n")},
                           load_data_from_file = function() {
                             cat("load_data_from_file()"); self$print_level()
                             file_path = unique(private$.pipeline_df$file_path)
                             stopifnot(length(file_path) == 1)
                             cat(paste0("Reading *", switch(private$.separator,
                                                   '\t' = 'tab',
                                                   ',' = 'comma'), 
                                 "* delimited file:\n\t", file_path, "\n"))
                             if (!private$.header) {
                               cat("--- No header in file\n")
                             }
                             private$.data_df = read.delim(file = file_path,
                                                           sep = private$.separator,
                                                           check.names = FALSE,
                                                           stringsAsFactors = FALSE,
                                                           header = private$.header)
                             cat("Dimensions:", dim(private$.data_df), "\n")
                             invisible(self)
                           },
                           get_feature_annotation = function() {
                             private$.feature_annotation_df
                           },
                           get_data = function(){
                             private$.data_df
                           }
                           # get_pipeline_df = function() { private$.pipeline_df }
                         ),
                         private = list(
                           .measurement_set = NULL,
                           .pipeline_df = NULL,
                           .feature_annotation_df = NULL,
                           .header = TRUE, # whether to read first row of data-file as header
                           .separator = '\t',
                           .data_df = NULL
                         ))

##### DataReaderVariant #####
DataReaderVariant = R6::R6Class(classname = 'DataReaderVariant',
                                inherit = DataReader,
                                public = list(
                                  print_level = function() {cat("----(Level: DataReaderVariant)\n")}
                                ))

##### DataReaderVariantGemini #####
DataReaderVariantGemini = R6::R6Class(classname = 'DataReaderVariantGemini',
                                      inherit = DataReaderVariant,
                                      public = list(
                                        print_level = function() {cat("----(Level: DataReaderVariantGemini)\n")},
                                        load_data_from_file = function() {
                                          cat("load_data_from_file()"); self$print_level()
                                          
                                          tsvReadSuccess = tryCatch({
                                            cat("Trying to read as TSV file\n")
                                            temp = read.delim(file = unique(private$.pipeline_df$file_path),
                                                            sep = private$.separator,
                                                            nrows = 2, # file should at least have two lines
                                                            check.names = FALSE)
                                            if (ncol(temp) == 1) {
                                              cat("Unlikely that variant file has 1 column. Try CSV next\n")
                                              FALSE
                                            } else {
                                              cat("Read attempt as TSV succeeded\n")
                                              TRUE
                                            }
                                          }, error = function(e) {
                                            cat("Read attempt as TSV failed. Try CSV next\n")
                                            FALSE
                                          })
                                          
                                          if (!tsvReadSuccess) {
                                            private$.separator = ','
                                          }
                                            
                                          super$load_data_from_file()
                                          
                                          cat("Rule 1:\n")
                                          mandatory_columns = c('ref', 'alt', 'chrom', 'gene', 'vcf_id')
                                          cat("======\n\t test presence of columns:\n\t\t", 
                                              pretty_print(mandatory_columns), "\n")
                                          stopifnot(mandatory_columns %in% colnames(private$.data_df))
                                          
                                          cat("Rule 2:\n")
                                          column_rename = c('ref' = 'reference',
                                                            'alt' = 'alternate',
                                                            'chrom' = 'chromosome',
                                                            'vcf_id' = 'id')
                                          cat("======\n\t rename columns:\n\t\t", 
                                              pretty_print(names(column_rename)), "==>", 
                                              pretty_print(column_rename), "\n")
                                          private$.data_df = plyr::rename(private$.data_df, 
                                                                          column_rename)
                                          
                                          if (nrow(private$.pipeline_df) > 1) {
                                            cat("This is a summarized GEMINI file\n")
                                            cat("Expecting 'Analysis_Barcode' column to hold the sample name\n")
                                            stopifnot('Analysis_Barcode' %in% colnames(private$.data_df) )
                                            if ('biosample_name' %in% colnames(private$.data_df)) {
                                              stop("Did not expect `biosample_name` column in variant file\n")
                                            }
                                            private$.data_df$biosample_name = private$.data_df$Analysis_Barcode
                                          }
                                        }
                                      ))

##### DataReaderRNAQuant #####
DataReaderRNAQuant = R6::R6Class(classname = 'DataReaderRNAQuant',
                               inherit = DataReader,
                               public = list(
                                 print_level = function() {cat("----(Level: DataReaderRNAQuant)\n")}
                               ), 
                               private = list(
                                 convert_wide_to_tall_skinny = function() {
                                   cat("convert_wide_to_tall_skinny()"); self$print_level()
                                   
                                   cat("-----\n\tConverting wide matrix form to tall-skinny table\n")
                                   private$.data_df = tidyr::gather(data = private$.data_df, 
                                                                    key = 'biosample_name', 
                                                                    value='value', 
                                                                    colnames(private$.data_df)[2:length(colnames(private$.data_df))])
                                 }
                               ))

##### DataReaderRNAQuantRNASeqCufflinks #####
DataReaderRNAQuantRNASeqCufflinks = R6::R6Class(classname = 'DataReaderRNAQuantRNASeqCufflinks',
                                inherit = DataReaderRNAQuant,
                                public = list(
                                  print_level = function() {cat("----(Level: DataReaderRNAQuantRNASeqCufflinks)\n")},
                                  load_data_from_file = function() {
                                    super$load_data_from_file()
                                    cat("load_data_from_file()"); self$print_level()
                                    
                                    # - if 1 pipeline info row for a file, try per-sample reader. 
                                    #     + if that fails, try aggregate reader
                                    # - if multiple pipeline info rows for a file, try aggregate reader
                                    if (nrow(private$.pipeline_df) == 1) {
                                      cat("One pipeline information row for file. Attempting to use per-sample file loader\n")
                                      tryAggregateLoader = tryCatch({
                                        # code for per-sample loader
                                        stopifnot(length(private$.pipeline_df$original_sample_name) == 1)
                                        private$.data_df$biosample_name = private$.pipeline_df$original_sample_name
                                        
                                        columns_to_drop = c('class_code', 
                                                            'nearest_ref_id', 
                                                            'tss_id', 'locus', 
                                                            'length', 'coverage', 
                                                            'FPKM_conf_lo', 'FPKM_conf_hi', 'FPKM_status')
                                        private$.data_df[, columns_to_drop] = NULL
                                        private$.data_df = plyr::rename(private$.data_df, 
                                                                        c('FPKM' = 'value'))
                                        
                                        if (identical(private$.data_df$tracking_id, private$.data_df$gene_id)) {
                                          cat("Identified gene file-type\n")
                                          private$.data_df$gene_id = NULL
                                        } else {
                                          cat("Identified isoform file-type\n")
                                          stop("Need to code for this. Refer function: register_expression_file_cufflinks()")
                                        }
                                        
                                        return(FALSE) # do not need to try aggregate loader 
                                      }, error = function(e) {
                                        cat("Tried per-sample file loader and failed. Trying aggregateLoader\n")
                                        return(TRUE)
                                      })
                                    } else { # 
                                      cat("multiple pipeline information row for file. Attempting to use aggregate file loader\n")
                                      tryAggregateLoader = TRUE
                                    }
                                    
                                    if (tryAggregateLoader) {
                                      # code for aggregate file loader

                                      # Cufflinks file has feature annotation as well as expression data
                                      # Step 1 of 2 -- Extract annotation data
                                      ftr_col = 'gene'
                                      stopifnot(length(unique(private$.data_df[, ftr_col])) == 
                                                  nrow(private$.data_df[, ftr_col]))
                                      ftr_ann_columns = c(ftr_col, 'mrna', 'refseq', 'ucscid', 
                                                          'description', 'entrez', 
                                                          'chr', 'beg', 'end')
                                      private$.feature_annotation_df = private$.data_df[, ftr_ann_columns]
                                      colnames(private$.feature_annotation_df) = c('gene_symbol', 
                                                                                   'mrna', 
                                                                                   'refseq', 
                                                                                   'ucscid', 
                                                                                   'description', 
                                                                                   'entrez', 
                                                                                   'chromosome', 
                                                                                   'start', 
                                                                                   'end')
                                      
                                      # Step 2 of 2 -- Extract expression data
                                      private$.data_df = private$.data_df[, 
                                                                          c(ftr_col, 
                                                                            colnames(private$.data_df)[!(colnames(private$.data_df) %in% 
                                                                                                           ftr_ann_columns)])]
                                      column_names = colnames(private$.data_df)
                                      
                                      # There is a "_0" suffix in all column names -- remove that
                                      cat("Local rule 1\n")
                                      cat("============\n")
                                      cat("\tThere is a \"_0\" suffix in all column names -- remove that\n")
                                      stopifnot(length(grep("_0$", column_names)) == (length(column_names) -1))
                                      colnames(private$.data_df) = gsub("_[0-9]$", "", column_names)
                                      
                                      cat("Local rule 2\n")
                                      cat("============\n")
                                      cat("\tColumn name for feature; renaming as tracking id\n")
                                      colnames(private$.data_df)[1] = 'tracking_id'

                                      super$convert_wide_to_tall_skinny()
                                    }
                                  }
                                ))

##### DataReaderRNAQuantRNASeqRSEM #####
DataReaderRNAQuantRNASeqRSEM = R6::R6Class(classname = 'DataReaderRNAQuantRNASeqRSEM',
                                                inherit = DataReaderRNAQuant,
                                                public = list(
                                                  print_level = function() {cat("----(Level: DataReaderRNAQuantRNASeqRSEM)\n")},
                                                  load_data_from_file = function() {
                                                    super$load_data_from_file()
                                                    cat("load_data_from_file()"); self$print_level()
                                                    
                                                    # - if 1 pipeline info row for a file, try per-sample reader. 
                                                    #     + if that fails, try aggregate reader
                                                    # - if multiple pipeline info rows for a file, try aggregate reader
                                                    if (nrow(private$.pipeline_df) == 1) {
                                                      stop("Need to fill in code for RSEM per-sample loader")
                                                    } else { # 
                                                      cat("multiple pipeline information row for file. Attempting to use aggregate file loader")
                                                      tryAggregateLoader = TRUE
                                                    }
                                                    
                                                    if (tryAggregateLoader) {
                                                      # code for aggregate file loader
                                                      column_names = colnames(private$.data_df)
                                                      
                                                      colnames(private$.data_df)[1] = 'tracking_id'
                                                      
                                                      super$convert_wide_to_tall_skinny()
                                                    }
                                                  }
                                                ))
##### DataReaderRNAQuantRNASeqHTSeq #####
DataReaderRNAQuantRNASeqHTSeq = R6::R6Class(classname = 'DataReaderRNAQuantRNASeqHTSeq',
                                    inherit = DataReaderRNAQuant,
                                    public = list(
                                      print_level = function() {cat("----(Level: DataReaderRNAQuantRNASeqHTSeq)\n")},
                                      load_data_from_file = function() {
                                        super$load_data_from_file()
                                        cat("load_data_from_file()"); self$print_level()
                                        
                                        cat("Automatically interpreting specific format of data by matching with biosample names\n")
                                        bios = search_biosamples(dataset_id = private$.measurement_set$dataset_id, 
                                                                 dataset_version = private$.measurement_set$dataset_version)
                                        
                                        sample_from_manifest = bios[grep("__RNA", bios$name), ]$original_sample_name
                                        m1 = lapply(colnames(private$.data_df), 
                                                    function(colnm) {grep(colnm, sample_from_manifest)})
                                        m1_len = sapply(m1, function(elem) {length(elem)})
                                        if (!all(unique(m1_len) %in% c(0,1))) {
                                          stop("Expected columns to be either feature annotation or unique biosample names.
                                               Received data that has some non-unique biosample names:\n\t",
                                               pretty_print(colnames(private$.data_df), prettify_after = 15))
                                        }
                                        sample_manifest_matched_pos = unlist(m1)
                                        potential_sample_col_pos = which(m1_len == 1)
                                        first_sample_col_pos = min(potential_sample_col_pos)
                                        stopifnot(first_sample_col_pos >= 2)
                                        
                                        potential_sample_cols = colnames(private$.data_df)[potential_sample_col_pos]
                                        ftr_ann_columns       = colnames(private$.data_df)[1:(first_sample_col_pos - 1)]
                                        potential_sample_names_manifest = sample_from_manifest[sample_manifest_matched_pos]
                                        if (identical(ftr_ann_columns, 
                                                      c('gene', 'mrna', 'refseq', 'ucscid', 
                                                                 'description', 'entrez', 
                                                                 'chr', 'beg', 'end'))) {
                                          ftr_col = 'gene'
                                          ftr_ann_columns_replace = c('gene_symbol', 
                                                                      'mrna', 
                                                                      'refseq', 
                                                                      'ucscid', 
                                                                      'description', 
                                                                      'entrez', 
                                                                      'chromosome', 
                                                                      'start', 
                                                                      'end')
                                        } else if (ftr_ann_columns == 'GENE_ID') {
                                          ftr_col = 'GENE_ID'
                                          ftr_ann_columns_replace = c('gene_symbol')
                                        } else {
                                          stop("Case not covered. Feature annotation columns:", ftr_ann_columns)
                                        }
                                        # HTSeq file has feature annotation as well as expression data
                                        # Step 1 of 2 -- Extract annotation data
                                        stopifnot(length(unique(private$.data_df[, ftr_col])) == 
                                                    nrow(private$.data_df[, ftr_col]))
                                        if (length(ftr_ann_columns) == 1) {
                                          private$.feature_annotation_df = data.frame(ftr_ann_columns = 
                                                                                        private$.data_df[, ftr_ann_columns],
                                                                                      stringsAsFactors = FALSE)
                                        } else {
                                          private$.feature_annotation_df = private$.data_df[, ftr_ann_columns]
                                        }
                                        colnames(private$.feature_annotation_df) = ftr_ann_columns_replace
                                        
                                        # Step 2 of 2 -- Extract expression data
                                        private$.data_df = private$.data_df[, 
                                                            c(ftr_col, 
                                                              potential_sample_cols)]

                                        # column renames 
                                        cat("Local rule(s)\n")
                                        cat("============\n")
                                        cat("\tColumn name for feature; renaming as tracking id\n")
                                        cat("============\n")
                                        cat("\tRename the sample columns in the format that matched with the sample manifest\n")

                                        colnames(private$.data_df) = c('tracking_id', potential_sample_names_manifest)
                                        
                                        super$convert_wide_to_tall_skinny()
                                      }
                                      
                                    ))


##### DataReaderRNAQuantMicroarray #####
DataReaderRNAQuantMicroarray = R6::R6Class(classname = 'DataReaderRNAQuantMicroarray',
                                 inherit = DataReaderRNAQuant,
                                 public = list(
                                   print_level = function() {cat("----(Level: DataReaderRNAQuantMicroarray)\n")},
                                   load_data_from_file = function() {
                                     cat("load_data_from_file()"); self$print_level()
                                     file_path = unique(private$.pipeline_df$file_path)
                                     stopifnot(length(file_path) == 1)
                                     # restore bioconductor expressionSet object from file path
                                     cat(paste0("restore bioconductor expressionSet object from file path:\n\t", file_path, "\n"))
                                     exprSetObj = readRDS(file_path)
                                     
                                     cat("Extracting expression data\n")
                                     private$.data_df = as.data.frame(
                                       Biobase::exprs(exprSetObj) # to distinguish from plyr::exprs
                                       )
                                     private$.data_df = cbind(
                                       data.frame('probeset_id' = rownames(private$.data_df),
                                                  stringsAsFactors = FALSE),
                                       private$.data_df)
                                     rownames(private$.data_df) = 1:nrow(private$.data_df)
                                     
                                     super$convert_wide_to_tall_skinny()
                                     
                                     cat("Dimensions:", dim(private$.data_df), "\n")
                                     
                                     cat("Extracting feature data\n")
                                     private$.feature_annotation_df = exprSetObj@featureData@data
                                     
                                     invisible(self)
                                     
                                   }
                                 ), 
                                 private = list(
                                 ))
                                   
##### DataReaderFusionTophat #####
DataReaderFusionTophat = R6::R6Class(classname = 'DataReaderFusionTophat',
                                     inherit = DataReader,
                                     public = list(
                                       print_level = function() {cat("----(Level: DataReaderFusionTophat)\n")},
                                       load_data_from_file = function() {
                                         private$.header = FALSE
                                         super$load_data_from_file()
                                         colnames(private$.data_df) = c('biosample_name', 
                                                                        'gene_left', 'chromosome_left', 'pos_left',
                                                                        'gene_right', 'chromosome_right', 'pos_right',
                                                                        'num_spanning_reads', 'num_mate_pairs', 
                                                                        'num_mate_pairs_fusion', 
                                                                        'quality_score')
                                       }
                                     ))
##### createDataReader #####
#' @export
createDataReader = function(pipeline_df, measurement_set){
  temp_string = paste0("{",measurement_set$pipeline_scidb, "}{", 
                       measurement_set$quantification_level, "}")
  switch(temp_string,
         "{[external]-[RNA-seq] Cufflinks}{gene}" = ,
         "{[external]-[RNA-seq] Cufflinks}{transcript}" = ,
         "{[DNAnexus]-[RNAseq_Expression_AlignmentBased v1.3.3] Cufflinks}{gene}" = 
             DataReaderRNAQuantRNASeqCufflinks$new(pipeline_df = pipeline_df,
                                           measurement_set = measurement_set),
         "{(internal)-(RNA-Seq) RSEM}{gene}" =
           DataReaderRNAQuantRNASeqRSEM$new(pipeline_df = pipeline_df,
                                                 measurement_set = measurement_set),
         "{[external]-[RNA-seq] HTSeq}{gene}" = 
             DataReaderRNAQuantRNASeqHTSeq$new(pipeline_df = pipeline_df,
                                       measurement_set = measurement_set),
         "{[DNAnexus]-[Variant_Custom: MuTect HC + PoN + Annotate] Mutect / SnpEff / GEMINI}{DNA}" = ,
         "{[DNAnexus]-[Variant_Custom: GATK + Annotate] GATK / SnpEff / GEMINI}{DNA}" = ,
         "{[DNAnexus]-[Variant_Custom: GATK + PoN + Annotate] GATK / SnpEff / GEMINI}{DNA}" = ,
         "{[DNAnexus]-[Variant_Custom: VarScan + Annotate] VarScan / SnpEff / GEMINI}{DNA}" = ,
         "{[DNAnexus]-[Variant_Custom: VarScan + PoN + Annotate] VarScan / SnpEff / GEMINI}{DNA}" = ,
         "{[DNAnexus]-[DNA-seq Tumor Only v1.3] Mutect / SnpEff / GEMINI (non-TCGA gnomAD & ExAC)}{DNA}" =
             DataReaderVariantGemini$new(pipeline_df = pipeline_df,
                                                 measurement_set = measurement_set),
         "{[external]-[Fusion] Tophat Fusion}{gene}" = 
             DataReaderFusionTophat$new(pipeline_df = pipeline_df,
                                        measurement_set = measurement_set),
         "{[external]-[Fusion] Defuse}{gene}" =
             DataReader$new(pipeline_df = pipeline_df,
                            measurement_set = measurement_set),
         "{[Affymetrix]-[Microarray] Affymetrix Bioconductor CDF v3.2.0}{gene}" = ,
         "{[Affymetrix]-[Microarray] UMich Alt CDF v20.0.0}{gene}" =
           DataReaderRNAQuantMicroarray$new(pipeline_df = pipeline_df,
                          measurement_set = measurement_set),
         stop("Need to add reader for choice:\n", temp_string)
         )
}
