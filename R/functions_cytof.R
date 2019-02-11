# Functions for CyTOF

#' @export
search_cytof = function(measurementset, feature = NULL, biosample = NULL, 
                        population = 'singlets', children = TRUE, 
                        con = NULL) {
  feature = filter_feature_df_by_cell_population(feature = feature, 
                                                 population = population, 
                                                 return_children = children)
  search_expression(measurementset = measurementset, 
                  biosample = biosample, 
                  feature = feature, 
                  formExpressionSet = FALSE, 
                  con = con)

}

#' Formulate data tree from CyTOF feature data.frame
#' 
#' @param feature_df features data.frame corresponding to a CyTOF featureset. Must contain columns \code{c('cellPopulation', 'parent1', 'parent2', ...)}
#' 
#' @return data.tree object with hierarchy of cell populations. See \code{\link{Node}} from package \code{data.tree}
#' 
#' @export
formulate_cell_population_data_tree_from_features_dataframe = function(feature_df) {
  if (!("cellPopulation" %in% colnames(feature_df))) {
    stop("Expected column: `cellPopulation` in features data.frame")
  }
  parent_colnames = colnames(feature_df)[grep('^parent[1-9]*$', 
                                                colnames(feature_df))]
  if (length(parent_colnames) == 0) {
    stop("Expected columns named: parent1, parent2, etc. ... in feature data.frame")
  } else if (length(parent_colnames) < 10) {
    parent_colnames = sort(parent_colnames)
  } else {
    stop("Need natural sorting")
  }
  feature_df = feature_df[, c('cellPopulation', 
                                  parent_colnames)]
  df1 = unique(feature_df)
  nodes = unique(df1$cellPopulation)
  df1$pathString = ""
  for (parentCol in sort(parent_colnames, decreasing = T)) {
    df1$pathString = ifelse(is.na(df1[, parentCol]), 
                            df1$pathString, 
                            paste(df1$pathString, df1[, parentCol], sep = "/"))  
  }
  df1$pathString = paste(df1$pathString, df1$cellPopulation, sep = '/')
  as.Node(df1)
}

filter_feature_df_by_cell_population = function(feature, population = 'singlets', return_children = TRUE) {
  if (population == 'singlets' & return_children) { # singlets is the highest level, and we are returning all children
    return(feature)
  } else {
    if (!("cellPopulation" %in% colnames(feature))) {
      stop("Expected column: `cellPopulation` in features data.frame")
    }
    if (!(population %in% feature$cellPopulation)) {
      all_poplns = unique(feature$cellPopulation)
      stop("Did not find population: '", population, "' in features data.frame\n Following populations available:\n\t", 
           pretty_print(all_poplns, prettify_after = (length(all_poplns) + 2) ))
    }
    if (!return_children) {
      return(feature[feature$cellPopulation == population, ])
    } else {
      feature_df = feature
      parent_colnames = colnames(feature_df)[grep('^parent[1-9]*$', 
                                                          colnames(feature_df))]
      if (length(parent_colnames) == 0) {
        stop("Expected columns named: parent1, parent2, etc. ... in feature data.frame")
      } else if (length(parent_colnames) < 10) {
        parent_colnames = sort(parent_colnames)
      } else {
        stop("Need natural sorting")
      }
      df1 = feature_df
      df1$pathString = ""
      for (parentCol in sort(parent_colnames, decreasing = T)) {
        df1$pathString = ifelse(is.na(df1[, parentCol]), 
                                df1$pathString, 
                                paste(df1$pathString, df1[, parentCol], sep = "/"))  
      }
      df1$pathString = paste(df1$pathString, df1$cellPopulation, sep = '/')
      df1 = df1[grep(population, df1$pathString, fixed = TRUE), ]
      df1$pathString = NULL
      df1
    }
  }
}







