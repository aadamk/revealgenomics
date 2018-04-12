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
                           get_data = function(){
                             private$.data_df
                           }
                           # get_pipeline_df = function() { private$.pipeline_df }
                         ),
                         private = list(
                           .measurement_set = NULL,
                           .pipeline_df = NULL,
                           .header = TRUE, # whether to read first row of data-file as header
                           .separator = '\t',
                           .data_df = NULL
                         ))

DataReaderVariant = R6::R6Class(classname = 'DataReaderVariant',
                                inherit = DataReader,
                                public = list(
                                  print_level = function() {cat("----(Level: DataReaderVariant)\n")}
                                ))

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

DataReaderRNASeq = R6::R6Class(classname = 'DataReaderRNASeq',
                               inherit = DataReader,
                               public = list(
                                 print_level = function() {cat("----(Level: DataReaderRNASeq)\n")}
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

DataReaderRNASeqCufflinks = R6::R6Class(classname = 'DataReaderRNASeqCufflinks',
                                inherit = DataReaderRNASeq,
                                public = list(
                                  print_level = function() {cat("----(Level: DataReaderRNASeqCufflinks)\n")},
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
                                      cat("multiple pipeline information row for file. Attempting to use aggregate file loader")
                                      tryAggregateLoader = TRUE
                                    }
                                    
                                    if (tryAggregateLoader) {
                                      # code for aggregate file loader
                                      column_names = colnames(private$.data_df)

                                      # There is a "_0" suffix in all column names -- remove that
                                      cat("Local rule 1\n")
                                      cat("============\n")
                                      cat("\tThere is a \"_0\" suffix in all column names -- remove that\n")
                                      stopifnot(length(grep("_0$", column_names)) == (length(column_names) -1))
                                      colnames(private$.data_df) = gsub("_[0-9]$", "", column_names)
                                      
                                      super$convert_wide_to_tall_skinny()
                                    }
                                  }
                                ))

DataReaderRNASeqHTSeq = R6::R6Class(classname = 'DataReaderRNASeqHTSeq',
                                    inherit = DataReaderRNASeq,
                                    public = list(
                                      print_level = function() {cat("----(Level: DataReaderRNASeqHTSeq)\n")},
                                      load_data_from_file = function() {
                                        super$load_data_from_file()
                                        cat("load_data_from_file()"); self$print_level()
                                        
                                        column_names = colnames(private$.data_df)

                                        # There is a 'xxx/' prefix in all column names -- remove that
                                        cat("Local rule 1\n")
                                        cat("============\n")
                                        cat("\tThere is a 'xxx/' prefix  in all column names -- remove that\n")
                                        stopifnot(length(grep("/", column_names)) == (length(column_names) -1))
                                        colnames(private$.data_df) = c(
                                          column_names[1],
                                          sapply(strsplit(tail(column_names,-1), "/"), function(vec) vec[2])
                                        )
                                        
                                        cat("Local rule 2\n")
                                        cat("============\n")
                                        cat("\tEmpty column name for feature; renaming as tracking id\n")
                                        colnames(private$.data_df)[1] = 'tracking_id'
                                        
                                        super$convert_wide_to_tall_skinny()
                                      }
                                      
                                    ))

DataReaderFusionTophat = R6::R6Class(classname = 'DataReaderFusionTophat',
                                     inherit = DataReader,
                                     public = list(
                                       print_level = function() {cat("----(Level: DataReaderRNASeq)\n")},
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
#' @export
createDataReader = function(pipeline_df, measurement_set){
  temp_string = paste0("{",measurement_set$pipeline_scidb, "}{", 
                       measurement_set$quantification_level, "}")
  switch(temp_string,
         "{[external]-[RNA-seq] Cufflinks}{gene}" = ,
         "{[external]-[RNA-seq] Cufflinks}{transcript}" = ,
         "{[DNAnexus]-[RNAseq_Expression_AlignmentBased v1.3.3] Cufflinks}{gene}" =
             DataReaderRNASeqCufflinks$new(pipeline_df = pipeline_df,
                                           measurement_set = measurement_set),
         "{[external]-[RNA-seq] HTSeq}{gene}" = 
             DataReaderRNASeqHTSeq$new(pipeline_df = pipeline_df,
                                       measurement_set = measurement_set),
         "{[DNAnexus]-[Variant_Custom: MuTect HC + PoN + Annotate] Mutect / SnpEff / GEMINI}{DNA}" = ,
         "{[DNAnexus]-[Variant_Custom: GATK + PoN + Annotate] GATK / SnpEff / GEMINI}{DNA}" = ,
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
         stop("Need to add reader for choice:\n", temp_string)
         )
}
