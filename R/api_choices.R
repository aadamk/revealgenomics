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
    #' multiple matches at a time
    get_selected_rows = function(keys) {
      m1 = find_matches_and_return_indices(keys, private$.choices_df[, private$.keyname])
      if (length(m1$source_unmatched_idx) != 0) {
        stop("Unmatched keys: ", pretty_print(keys[m1$source_unmatched_idx]))
      }
      private$.choices_df[m1$target_matched_idx, ]
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
      private$get_selected_rows(keys = pipeline_scidb)$measurement_entity
    },
    get_data_subtype = function(pipeline_scidb) {
      private$get_selected_rows(keys = pipeline_scidb)$data_subtype
    },
    get_pipeline_metadata = function(pipeline_scidb) {
      private$get_selected_rows(keys = pipeline_scidb)
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
      private$get_selected_rows(keys = filter_name)$quantification_level
    },
    get_quantification_unit = function(filter_name) {
      private$get_selected_rows(keys = filter_name)$quantification_unit
    },
    get_measurement_entity = function(filter_name) {
      private$get_selected_rows(keys = filter_name)$measurement_entity
    },
    get_filter_metadata = function(filter_name) {
      private$get_selected_rows(keys = filter_name)
    }
  )
)

#' extract relevant info pertaining to featureset-choices sheet
#' 
#' @export
FeaturesetChoices = R6::R6Class(
  classname = 'FeaturesetChoices',
  inherit = Choices,
  public = list(
    initialize = function(featureset_choices_df) {
      super$initialize(choices_df = featureset_choices_df, 
                       keyname = 'featureset_altName')
    },
    get_featureset_name = function(featureset_altName) {
      private$get_selected_rows(keys = featureset_altName)$featureset_name
    },
    get_featureset_metadata = function(featureset_altName) {
      private$get_selected_rows(keys = featureset_altName)
    }
  )
)
