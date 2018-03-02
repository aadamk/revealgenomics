# This is the metadata about pipeline and filter choices
filter_choices_df = myExcelReader(workbook = workbook, sheet_name = 'filter-choices')
pipeline_choices_df = myExcelReader(workbook = workbook, sheet_name = 'pipeline-choices')

# This is the actual Pipeline information related to a study
pipelines_df = myExcelReader(workbook = workbook, sheet_name = 'Pipelines')

unique(pipeline_choices_df$pipeline_scidb)
# "[external]-[Fusion] Defuse"                                              
# "[external]-[Fusion] Tophat Fusion"                                       
# "[external]-[RNA-seq] Cufflinks"                                          
# "[external]-[RNA-seq] HTSeq"                                              
# "[DNAnexus]-[Variant_Custom: MuTect HC + PoN + Annotate] Mutect / SnpEff / GEMINI"
# ....

unique(filter_choices_df$filter_name)
# "gene - counts"                                 
# "gene - FPKM"                                   
# "transcript - FPKM"                             
# "DNA - mutations - custom filter - GEMINI_David"
# "DNA - mutations - unfiltered"
# ....

# Create choices objects from metadata sheet
pipelineChoicesObj = PipelineChoices$new(
  pipeline_choices_df = pipeline_choices_df)

filterChoicesObj = FilterChoices$new(
  filter_choices_df = filter_choices_df)

#####################################
# Retrieve information for multiple keys in a study in one shot
m1 = sapply(unique(pipelines_df$pipeline_choice), 
       function(pipeline_choice) c(
         'measurement_entity'= pipelineChoicesObj$get_measurement_entity(pipeline_choice),
         'data_subtype' =      pipelineChoicesObj$get_data_subtype(pipeline_choice)))
info = as.data.frame(t(m1))
info$measurement_entity
info$data_subtype

m1 = sapply(unique(pipelines_df$pipeline_output_filter), 
            function(filter_name) c(
              'measurement_entity'= filterChoicesObj$get_quantification_level(filter_name = filter_name),
              'data_subtype' =      filterChoicesObj$get_quantification_unit(filter_name = filter_name)))
info = as.data.frame(t(m1))
info$measurement_entity
info$data_subtype

#####################################
# Retrieve information for one row of Pipelines sheet

# given any row index of the Pipelines sheet
n = 2
key_pipeline = pipelines_df$pipeline_choice[n]
key_filter   = pipelines_df$pipeline_output_filter[n]
cat("Here is how to retrieve metadata for a row of Pipelines sheet\n")
cat("Pipeline metadata:\n")
print(pipelineChoicesObj$get_pipeline_metadata(pipeline_scidb =  key_pipeline))
cat("Filter metadata:\n")
print(filterChoicesObj$get_filter_metadata(filter_name = key_filter))

if (pipelineChoicesObj$get_measurement_entity(key_pipeline) ==
    filterChoicesObj$get_measurement_entity(key_filter)) {
  cat("Measurement entities matched as expected\n")
} else {
  cat("Pipeline info points to measurement entity:", pipelineChoicesObj$get_measurement_entity(key_pipeline),
      "\nFilter info points to measurement entity:", filterChoicesObj$get_measurement_entity(key_filter), "\n")
}