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
                             private$.data_df = read.delim(file = file_path,
                                                           sep = private$.separator,
                                                           check.names = FALSE)
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
                                        }
                                      ))

DataReaderVariantGeminiFiltered = R6::R6Class(classname = 'DataReaderVariantGeminiFiltered',
                                              inherit = DataReaderVariantGemini,
                                              public = list(
                                                initialize = function(pipeline_df,
                                                                      measurement_set) {
                                                  private$.separator = ','
                                                  super$initialize(pipeline_df,
                                                                   measurement_set)
                                                }
                                              ))

DataReaderVariantGeminiUnfiltered = R6::R6Class(classname = 'DataReaderVariantGeminiUnfiltered',
                                              inherit = DataReaderVariantGemini,
                                              public = list(
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
                                    
                                    column_names = colnames(private$.data_df)
                                    # # There is a "X" prefix assigned by read.delim into all column names (as sample names begin with numbers)
                                    # # -- remove the prefix
                                    # not required after using check.names = FALSE in read.delim
                                    # stopifnot(length(grep("^X", column_names)) == length(column_names-1))
                                    # colnames(private$.data_df) = gsub("^X", "", colnames(private$.data_df))
                                    
                                    # There is a "_0" suffix in all column names -- remove that
                                    cat("Local rule 1\n")
                                    cat("============\n")
                                    cat("\tThere is a \"_0\" suffix in all column names -- remove that\n")
                                    stopifnot(length(grep("_0$", column_names)) == (length(column_names) -1))
                                    colnames(private$.data_df) = gsub("_[0-9]$", "", column_names)
                                    
                                    super$convert_wide_to_tall_skinny()
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

#' @export
createDataReader = function(pipeline_df, measurement_set){
  temp_string = paste0("{",measurement_set$pipeline_scidb, "}{", 
                       measurement_set$quantification_level, "}")
  switch(temp_string,
         "{[external]-[RNA-seq] Cufflinks}{gene}" = 
             DataReaderRNASeqCufflinks$new(pipeline_df = pipeline_df,
                                           measurement_set = measurement_set),
         "{[external]-[RNA-seq] Cufflinks}{transcript}" = 
             DataReaderRNASeqCufflinks$new(pipeline_df = pipeline_df,
                                           measurement_set = measurement_set),
         "{[external]-[RNA-seq] HTSeq}{gene}" = 
             DataReaderRNASeqHTSeq$new(pipeline_df = pipeline_df,
                                       measurement_set = measurement_set),
         "{[DNAnexus]-[Variant_Custom: MuTect HC + PoN + Annotate] Mutect / SnpEff / GEMINI}{DNA}" = 
             DataReaderVariantGeminiFiltered$new(pipeline_df = pipeline_df,
                                                 measurement_set = measurement_set))
}
