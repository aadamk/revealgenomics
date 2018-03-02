#' classes to handle different choices sheets in Excel template
#' 
#' e.g. pipeline-choices, filter-choices, featureset-choices

Choices = R6::R6Class(
  classname = 'Choices',
  public = list(
    initialize = function(choices_df, keyname) {
      if (length(unique(choices_df[, keyname])) != nrow(choices_df)) {
        cat("Choices in choices sheet under column `", keyname, "` must be unique\n")
        print(table(choices_df[, keyname]))
        stop("...")
      }
      private$.choices_df = choices_df
      private$.keyname = keyname
    }
  ), 
  private = list(
    get_selected_row = function(key) {
      private$.choices_df[private$.choices_df[, private$.keyname] == key, ]
    },
    .choices_df = NULL,
    .keyname = NULL
  )
)

#' extract relevant info pertaining to pipeline-choices sheet
#' 
#' @export
PipelineChoices = R6::R6Class(
  classname = 'PipelineChoices',
  inherit = Choices,
  public = list(
    initialize = function(pipeline_choices_df) {
      super$initialize(choices_df = pipeline_choices_df, 
                       keyname = 'pipeline_scidb')
    },
    get_measurement_entity = function(pipeline_scidb) {
      private$get_selected_row(key = pipeline_scidb)$measurement_entity
    },
    get_data_subtype = function(pipeline_scidb) {
      private$get_selected_row(key = pipeline_scidb)$data_subtype
    },
    get_pipeline_metadata = function(pipeline_scidb) {
      private$get_selected_row(key = pipeline_scidb)
    }
  )
)

#' extract relevant info pertaining to filter-choices sheet
#' 
#' @export
FilterChoices = R6::R6Class(
  classname = 'FilterChoices',
  inherit = Choices,
  public = list(
    initialize = function(filter_choices_df) {
      super$initialize(choices_df = filter_choices_df, 
                       keyname = 'filter_name')
    },
    get_quantification_level = function(filter_name) {
      private$get_selected_row(key = filter_name)$quantification_level
    },
    get_quantification_unit = function(filter_name) {
      private$get_selected_row(key = filter_name)$quantification_unit
    },
    get_measurement_entity = function(filter_name) {
      private$get_selected_row(key = filter_name)$measurement_entity
    },
    get_filter_metadata = function(filter_name) {
      private$get_selected_row(key = filter_name)
    }
  )
)
