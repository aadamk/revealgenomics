#
# BEGIN_COPYRIGHT
#
# PARADIGM4 INC.
# This file is part of the Paradigm4 Enterprise SciDB distribution kit
# and may only be used with a valid Paradigm4 contract and in accord
# with the terms and conditions specified by that contract.
#
# Copyright (C) 2011 - 2017 Paradigm4 Inc.
# All Rights Reserved.
#
# END_COPYRIGHT
#

formulate_base_selection_query = function(fullarrayname, id){
  # this limit is based on the number of operands that SciDB can handle in an expression 
  # https://paradigm4.atlassian.net/browse/SDB-5801
  THRESH_K = 397  
  
  sorted=sort(id)
  breaks=c(0, which(diff(sorted)!=1), length(sorted))
  idname = get_base_idname(fullarrayname)
  if (length(breaks) <= (THRESH_K + 2)) # few sets of contiguous tickers; use `cross_between`
  {
    expr_query1 = paste( sapply(seq(length(breaks)-1), function(i) {
      left = sorted[breaks[i]+1]
      right = sorted[breaks[i+1]]
      if (left == right) {
        paste0("(", idname, "=", right, ")")
      } else {
        sprintf("(%s>=%d AND %s<=%d)", idname, left,
                idname, right)
      }
    }), collapse=" OR ")
  } else {
    stop("Try fewer ids at a time")
  }
  return(expr_query1)
}

#' Add version filter on top a filter string
#' 
#' Checks that entity is versioned before adding the version string
#' 
#' @param entity entity on which filtering is to be run
#' @param base_selection_query typically the output of running `formulate_base_selection_query`
#'                             e.g. (individual_id>=1 AND individual_id<=3)
#' @param dataset_version the version at which to filter
formulate_versioned_selection_query = function(entity, base_selection_query, dataset_version) {
  # Clear out the array
  if (is_entity_versioned(entity)) {
    expr_query = paste("(dataset_version = ", dataset_version, ") AND (", base_selection_query, ")", sep = "")
  } else {
    expr_query = base_selection_query
  }
  return(expr_query)
}

#' @export
delete_entity = function(entity, id, dataset_version = NULL, delete_by_entity = NULL, con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (is.null(delete_by_entity)) {
    delete_by_entity = get_delete_by_entity(entity)
  } else {
    if (delete_by_entity != get_delete_by_entity(entity)) {
      stop("Deletion of ", entity, " can be done only via ", get_delete_by_entity(entity), " entity")
    }
  }
  
  if (!(entity %in% get_entity_names())) stop("Entity '", entity, "' does not exist")
  if (is.null(id)) return()
  if (is_entity_versioned(entity)) {
    if (is.null(dataset_version)) {
      stop("must supply dataset_version for versioned entities.
           Use dataset_version = 1 if only one version exists for relevant clinical study")
    } else {
      if (length(dataset_version) != 1) {stop("Can supply only one dataset_version at a time")}
    }
  }

  if (get_entity_class(entity = entity) == 'measurementdata') {
    if (length(id) != 1) stop("Delete of entity ", entity, " can be done only 1 ", 
                              get_base_idname(delete_by_entity), " at a time")
  } 
  # First check that entity exists at this id
  if (is_entity_versioned(entitynm = entity)){
    status = try(check_entity_exists_at_id(entity = delete_by_entity, id = id, 
                                           dataset_version = dataset_version, all_versions = F,
                                           con = con))
  } else {
    status = try(check_entity_exists_at_id(entity = delete_by_entity, id = id, con = con))
  }

  if (class(status) == 'try-error') {
    stop()
  }
  # Now that search_by_entities are proven to exist at specified id-s, delete them
  
  # Find the correct namespace
  arr = full_arrayname(entity)
  
  # Delete the mandatory fields array
  if (get_entity_class(entity = entity)  == 'measurementdata') { # Special handling for measurement data class
                                                                          # -- they do not have get_<ENTITY>() calls
                                                                          # -- only allow deleting by one preferred id at a time
    cat("Deleting entries for ", get_base_idname(delete_by_entity), " = ",  
        id, " from ", arr, " entity\n", sep = "")
    qq = paste("delete(", arr, ", ", get_base_idname(delete_by_entity), " = ",  
               id, " AND dataset_version = ", dataset_version, ")", sep = "")
    print(qq)
    iquery(con$db, qq)
  } else {
    base_selection_query  = formulate_base_selection_query(fullarrayname = arr, id = id)
    versioned_selection_query = formulate_versioned_selection_query(entity = entity, 
                                                                    base_selection_query = base_selection_query, 
                                                                    dataset_version = dataset_version)
    cat("Deleting entries for ids ", pretty_print(sort(id)), " from ", arr, " entity\n", sep = "")
    qq = paste("delete(", arr, ", ", versioned_selection_query, ")", sep = "")
    print(qq)
    iquery(con$db, qq)
  }
  
  # Clear out the info array
  infoArray = .ghEnv$meta$L$array[[entity]]$infoArray
  if (infoArray){
    delete_info_fields(fullarrayname = arr, id = id, dataset_version = dataset_version, con = con)
  }
  
  if (is_entity_cached(entity)) {
    update_entity_cache(entitynm = entity, con = con)
  }
}

delete_info_fields = function(fullarrayname, id, dataset_version, delete_by_entity = NULL, con = NULL){
  entity = strip_namespace(fullarrayname)
  if (is.null(delete_by_entity)) {
    delete_by_entity = get_delete_by_entity(entity)
  } else {
    if (delete_by_entity != get_delete_by_entity(entity)) {
      stop("Deletion of ", entity, " can be done only via ", get_delete_by_entity(entity), " entity")
    }
  }
  arr = fullarrayname
  arrInfo = paste(arr, "_INFO", sep = "")
  entity = strip_namespace(fullarrayname)

  # Delete the mandatory fields array
  if (get_entity_class(entity = entity) == 'measurementdata') { # Special handling for measurement data class
    # -- they do not have get_<ENTITY>() calls
    # -- only allow deleting by one preferred id at a time
    cat("Deleting entries for ", get_base_idname(delete_by_entity), " = ",  
        id, " from ", arr, "_INFO\n", sep = "")
    qq = paste("delete(", arrInfo, ", ", get_base_idname(delete_by_entity), " = ",  
               id, " AND dataset_version = ", dataset_version, ")", sep = "")
    print(qq)
    iquery(con$db, qq)
  } else {
    base_selection_query = formulate_base_selection_query(fullarrayname = fullarrayname, 
                                                          id = id)  
    versioned_selection_query = formulate_versioned_selection_query(entity = entity, 
                                                                    base_selection_query = base_selection_query, 
                                                                    dataset_version = dataset_version)
    
    qq = paste("delete(", arrInfo, ", ", versioned_selection_query, ")", sep = "")
    cat("Deleting entries for ids ", pretty_print(sort(id)), " from info array: ", arrInfo, "\n", sep = "")
    print(qq)
    iquery(con$db, qq)
  }
}


#' @export
delete_project <- function(project_id, con = NULL) {
  ##---------------=
  ## If there is more than one project_id, or it is not a valid ID, then give an error.
  ##---------------=
  if (!length(project_id) == 1) {
    stop("You can only specify one project ID at a time to 'delete_project()'.")
  }
  ## Note, incorrect projectIDs will be caught by the call to "search_datasets()".

  project_df = get_projects(project_id = project_id, con = con)
  if (nrow(project_df) != 1) {
    cat("Project ", project_id, " does not exist. Nothing to delete.\n")
    return(invisible(NULL))
  }
  ##---------------=
  ## Get the datasets under this project.
  ##---------------=
  datasets <- search_datasets(project_id = project_id, con = con)
  
  datasetStructures.lst <- list()
  if (class(datasets) == "NULL") {
    ## If there are no datasests, then indicate that there are no datasets underneath.
    cat("Project", project_id, "contains no datasets. \n\n")
  } else if (nrow(datasets) > 0) {
    ## First re-order the datasets by ascending dataset number and descending version 
    ##  number (so the most recent version of each dataset is first).
    datasets <-  datasets[order(datasets[,"dataset_id"], -datasets[,"dataset_version"]), ]
    
    datasetNames <- apply(
      datasets[, c("dataset_id", "dataset_version", "name")],  
      MARGIN = 1,  
      FUN = function(x) { paste0(x[1], "_ver", x[2], " (", x[3], ")")})
    cat("Project ", project_id, " (", project_df$name, ")", 
        " consists of DATASET(S):  \t", paste(datasetNames, collapse=", "), "\n\n", sep="")
    
    
    
    ##---------------=
    ## For each dataset, call "get_dataset_subelements", to print the list of subelements
    ##  of each dataset.
    ##---------------=
    for (i in 1:nrow(datasets)) {
      nextDataset <- datasets[i, "dataset_id"]
      nextDatasetVersion <- datasets[i, "dataset_version"]
      # nextDatasetName <- paste0(nextDataset, "_v", nextDatasetVersion)
      datasetStructures.lst[[ datasetNames[i] ]] <- print_dataset_subelements(nextDataset, nextDatasetVersion, con = con)
    }
  } else {
    ## If there are no datasests, then indicate that there are no datasets underneath.
    cat("Project", project_id, "contains no datasets. \n\n")
  }
  
  
  
  ##---------------=
  ## Ask the user to confirm that they want to delete this project.
  ## If the user said to procede, then delete the datasets.
  ##---------------=
  userResponse <- NA
  while(is.na(userResponse) | nchar(userResponse) == 0) { 
    userResponse <- readline(prompt = "Do you want to continue deleting this project? (yes/no): \n  ")
    switch(tolower(userResponse), 
           "yes" = {  
             cat("Deleting project ", project_id, "... \n", sep = "")
             if (length(datasetStructures.lst) > 0) {
               ## Delete all of the datasets and their sub-elements.
               for (i in 1:length(datasetStructures.lst)) {
                 nextDatasetStructure <- datasetStructures.lst[[i]]
                 delete_dataset_internal(dataset_id = attr(nextDatasetStructure, "dataset_id"),
                                datasetVersion = attr(nextDatasetStructure, "datasetVersion"),
                                datasetStructure = nextDatasetStructure, 
                                con = con)
               }
             }
             ## Now delete the actual project ID.
             delete_entity(entity = "PROJECT", 
                           id = project_id,
                           con = con)
             },
           "no" = {  message("Project ", project_id, " was NOT deleted. \n", sep = "") },
           { message("Please respond with yes or no"); userResponse <- NA })
  }
}

#' @export
delete_dataset <- function(dataset_id, con = NULL) {
  ##---------------=
  ## If there is more than one dataset_id, or it is not a valid ID, then give an error.
  ##---------------=
  if (!length(dataset_id) == 1) {
    stop("You can only specify one dataset ID at a time to 'delete_dataset()'.")
  }
  
  datasetExists = try({check_entity_exists_at_id(entity = .ghEnv$meta$arrDataset, id = dataset_id)},
                      silent = TRUE)
  if (class(datasetExists) == 'try-error') {
    cat("dataset ", dataset_id, " does not exist. Nothing to delete.\n")
    return(invisible(NULL))
  }
  ##---------------=
  ## Get the datasets under this project.
  ##---------------=
  datasets <- get_datasets(dataset_id = dataset_id, con = con)
  
  datasetStructures.lst <- list()
  
  if (nrow(datasets) > 0) {
    ## First re-order the datasets by ascending dataset number and descending version 
    ##  number (so the most recent version of each dataset is first).
    datasets <-  datasets[order(datasets[,"dataset_id"], -datasets[,"dataset_version"]), ]
    
    datasetNames <- apply(datasets[, c("dataset_id", "dataset_version")],  
                          MARGIN = 1,  
                          FUN = function(x) { paste(x, collapse = "_ver")})
    cat("Requested to delete:  \t", paste(datasetNames, collapse=", "), "\n\n", sep="")
    
    
    
    ##---------------=
    ## For each dataset, call "get_dataset_subelements", to print the list of subelements
    ##  of each dataset.
    ##---------------=
    for (i in 1:nrow(datasets)) {
      nextDataset <- datasets[i, "dataset_id"]
      nextDatasetVersion <- datasets[i, "dataset_version"]
      # nextDatasetName <- paste0(nextDataset, "_v", nextDatasetVersion)
      datasetStructures.lst[[ datasetNames[i] ]] <- print_dataset_subelements(nextDataset, nextDatasetVersion, con = con)
    }
  } else {
    ## If there are no datasests, then indicate that there are no datasets underneath.
    cat("dataset", dataset_id, "does not exist \n\n")
  }
  
  
  
  ##---------------=
  ## Ask the user to confirm that they want to delete this project.
  ## If the user said to procede, then delete the datasets.
  ##---------------=
  userResponse <- NA
  while(is.na(userResponse) | nchar(userResponse) == 0) { 
    userResponse <- readline(prompt = "Do you want to continue deleting this dataset? (yes/no): \n  ")
    switch(tolower(userResponse), 
           "yes" = {  
             # cat("Deleting dataset ", project_id, "... \n", sep = "")
             if (length(datasetStructures.lst) > 0) {
               ## Delete all of the datasets and their sub-elements.
               for (i in 1:length(datasetStructures.lst)) {
                 nextDatasetStructure <- datasetStructures.lst[[i]]
                 delete_dataset_internal(dataset_id = attr(nextDatasetStructure, "dataset_id"),
                                         datasetVersion = attr(nextDatasetStructure, "datasetVersion"),
                                         datasetStructure = nextDatasetStructure, 
                                         con = con)
               }
             }
           },
           "no" = {  message("dataset ", dataset_id, " was NOT deleted. \n", sep = "") },
           { message("Please respond with yes or no"); userResponse <- NA })
  }
}


get_dataset_subelements <- function(dataset_id, datasetVersion, con = NULL, ...) {
  ## DEBUG: A flag for whether to supress the errors for searching for entities
  ## that might not be there.  This should be coded concretely one way or the
  ## other.
  SEARCH_SILENTLY <- TRUE  
  
  ##---------------------=
  ## Programmatic retrieval of the elements in the dataset.
  ##---------------------=
  dataset_subelements <- list()
  ## Get the matrix specifying the database schema to determine what
  ##  child tables exist for the each dataset.
  db_schema <- get_entity_info()


  entities_to_search <- db_schema[(db_schema$search_by_entity == 'DATASET')
                                  & !is.na(db_schema$search_by_entity), ]$entity

  for (next.entity in as.character(entities_to_search)) {
    if (next.entity == .ghEnv$meta$arrDefinition) {
      # all child-entries of a DATASET are versioned, except the DEFINITIONS entity
      dataset_subelements[[next.entity]] <- try(search_entity(entity = next.entity, id = dataset_id, 
                                                              con = con, ...), 
                                                silent = SEARCH_SILENTLY)
    } else {
      dataset_subelements[[next.entity]] <- try(search_entity(entity = next.entity, id = dataset_id, 
                                                              dataset_version = datasetVersion, con = con, ...), 
                                                silent = SEARCH_SILENTLY)
    }
  }
  
  
  ## Now set each of the fields to NULL that did not return any values (i.e. that produced a try-error).
  for (next.name in names(dataset_subelements)) {
    if (class(dataset_subelements[[next.name]]) == "try-error") {
      dataset_subelements[[next.name]] <- NA 
      if (!SEARCH_SILENTLY) {
        cat(" ** WARNING -- Searching for entity='", next.name, "' produced an error! \n", sep="")
      }
    }
  } 
  
  ## And add the dataset_id and datasetVersion as attributes for this datasetStructure object,
  ##  so that they do not conflict with the other information types, but are still embedded 
  ##  in the dataset object.
  attr(dataset_subelements, which = "dataset_id") <- dataset_id
  attr(dataset_subelements, which = "datasetVersion") <- datasetVersion
  
  
  return (dataset_subelements)
}


print_dataset_subelements <- function(dataset_id, datasetVersion, print.nonexistant.metadata = FALSE, con = NULL) {
  
  ## Note that "print.nonexistent.metadata" is just an option to explicitly state which types of 
  ##  information have been searched but do not exist for this dataset.  It is FALSE by default.
  
  ##---------------=
  ## Get the dataset's subelements
  ##---------------=
  dataset_subelements <- get_dataset_subelements(dataset_id, datasetVersion, con = con)
  
  ##---------------=
  ## Print to the screen the lists of elements that will be deleted.
  ##---------------=
  cat("Summary of dataset = ", dataset_id, " (version = ", datasetVersion, "):  \n", sep="")
  max_char_length <- max(nchar(names(dataset_subelements)))
  for(next.subelement in names(dataset_subelements)) {
    
    if (is.data.frame(dataset_subelements[[next.subelement]])) {
    
      if ( nrow(dataset_subelements[[next.subelement]]) == 0) {
        ## Then there were no data of this type in the given dataset.
        
        if (print.nonexistant.metadata) {
          ## Then we will print out that there was no metadata of this type.
          cat("\t", formatC(paste0(next.subelement, " ids: "), width = -(max_char_length + 5)), "\t<NA> \n", sep="")
        }
      } else {
        ## There are elements of this metadata type located in this dataset, so print them out.
        cat("\t", formatC(paste0(next.subelement, " ids: "), width = -(max_char_length + 5)), "\t", sep="")
        
        ## Get the name for the column which will contain these IDs.
        base_idname <- get_base_idname(next.subelement)
        
        ## The object ids appear to be in the first column in each entity matrix.  
        ids_vec <- dataset_subelements[[next.subelement]][, base_idname]
        cat(convert_to_id_ranges(ids_vec), "\n")
      }
      
    } else {
     
      ## Then the search_entity function for this variable had created an error, or was
      ##  otherwise unable to complete the search.
      if (print.nonexistant.metadata) {
        ## Then we will print out that there was no metadata of this type.
        cat("\t", formatC(paste0(next.subelement, " ids: "), width = -(max_char_length + 5)), "\t<Could not search for this metadata type.> \n", sep="")
      }
    }
  }
  return(dataset_subelements)
}


##-----------------------------------------------------------------------------=
## This function will loop through and delete each of the entities within a 
##  dataset, but for the moment it will only delete the metadata.  It will
##  eventually delete the actual measurement data as well.
##-----------------------------------------------------------------------------=
delete_dataset_internal <- function(dataset_id, datasetVersion, datasetStructure = NULL, con = NULL) {
  con = use_ghEnv_if_null(con = con)
  
  ##-----------------=
  ## Delete permissions for this dataset from permissions table
  ##-----------------=
  if (as.logical(options('revealgenomics.use_scidb_ee'))) {
    query = paste0("delete(permissions.dataset_id, dataset_id = ", dataset_id, ")")
    cat("Delete permissions for this dataset from permissions table\n", query, "\n")
    iquery(con$db, query = query)
  }
  
  ## Get the list of sub-entities.
  ## If the dataset's structure is provided, then use that structure, otherwise call
  ##  the function to get the dataset's sub-structure.
  if (is.null(datasetStructure)) {
    datasetStructure <- get_dataset_subelements(dataset_id, datasetVersion, con = con)
  } else {
    ## If the dataset structure was provided, verify that it is the same as is being 
    ##  specified by the dataset_id and datasetVersion.
    stopifnot( attr(datasetStructure, "dataset_id")==dataset_id 
               & attr(datasetStructure, "datasetVersion")==datasetVersion )
  }
  
  
  
  ##---------------------------------------=
  ## For each type of dataset entities, delete the actual measurements, 
  ##  then delete the metadata.  Note that the metadata field name is the 
  ##  actual entity name as well.
  ##---------------------------------------=
  
  ##-----------------=
  ## Delete the measurement data of each type that exists for entities in this dataset.
  ##-----------------=
  
  ## Use the get_entity_info() function to get the list of measurement data types in this dataset.
  entityInfo.mat <- get_entity_info()
  measurementsInfo.mat <- entityInfo.mat[entityInfo.mat$class=="measurementdata", ]
  
  ## For each of measurement types, delete any associated measurement data if it exists.
  for (i in 1:nrow(measurementsInfo.mat)) {
    next.entity <- as.character( measurementsInfo.mat$entity[i] )
    next.parent.entity <- as.character( measurementsInfo.mat$delete_by_entity[i] )
    
    ## See if this next measurement data type is associated with this dataset.
    next.entity.ids <- datasetStructure[[ next.parent.entity ]]
    if ( !all(is.null( next.entity.ids )) & !all(is.na( next.entity.ids )) )  {
      if (nrow(next.entity.ids) > 0) {
        ## Then there is measurement data associated with this measurement type in this dataset.
        ## So, delete this entire type of measurement data for this dataset.  Loop through and 
        ##  delete each pipeline ID, one at a time.
        
        column.name <- get_base_idname(next.parent.entity)  ## Get the name for the column that contains the IDs for this measurement type.
        
        for (next.row in 1:nrow(next.entity.ids) ) {
          if (next.entity.ids[next.row, ]$entity == next.entity) {
            delete_entity(entity = next.entity, 
                          id = next.entity.ids[next.row, column.name], 
                          dataset_version = next.entity.ids[next.row, "dataset_version"],  
                          delete_by_entity = next.parent.entity, 
                          con = con)
          }
        }
      }
    }
  }
  
  ##-----------------=
  ## Delete the metadata entities from this dataset.
  ##-----------------=
  for (next.metadata.name in names(datasetStructure)) {
    next.metadata.ids.mat <- datasetStructure[[next.metadata.name]]

    ## Get the name for the column that contains the IDs for this metadata type.
    column.name <- get_base_idname(next.metadata.name)

    ## Delete all of the entries for this metadata type (in a 
    ##  single delete_entity() call).
    if ( !all(is.null( next.metadata.ids.mat )) & !all(is.na( next.metadata.ids.mat )) )  {
      if (nrow(next.metadata.ids.mat) > 0) {
        delete_entity(entity = next.metadata.name,
                      id = next.metadata.ids.mat[, column.name],
                      dataset_version = datasetVersion,
                      delete_by_entity = next.metadata.name,
                      con = con)
      }
    }
  }
  
  ##-----------------=
  ## Delete the dataset_id itself from the DATASET metadata table.
  ##-----------------=
  delete_entity(entity = "DATASET", 
                id = dataset_id, 
                dataset_version = datasetVersion, 
                delete_by_entity = "DATASET", 
                con = con)
  
}

#' @export
delete_featureset = function(featureset_id, con = NULL) {
  con = use_ghEnv_if_null(con = con)
  fset = get_featuresets(featureset_id = featureset_id, con = con)
  if (nrow(fset) != 1) stop("Cannot retrieve featureset with specified featureset_id")
  ff = search_features(featureset_id = featureset_id, mandatory_fields_only = T, con = con)
  if (nrow(ff) == 0) {
    cat("No features to delete. Proceeding to delete featureset\n")
    userResponse = 'continueDeleting'
  } else {
    subq = revealgenomics:::formulate_base_selection_query('FEATURE', sort(ff$feature_id))
    
    ei = get_entity_info()
    entities = ei[ei$class == 'measurementdata', ]$entity
    
    counts_at_entity = sapply(
      entities, 
      function(entity) {
        cat("Checking entity:", entity, "\n")
        fullnm = revealgenomics:::full_arrayname(entity)
        exists_array = revealgenomics:::scidb_exists_array(arrayName = fullnm, con = con)
        
        if (exists_array) {
          if (entity == .ghEnv$meta$arrFusion) {
            query = paste0("op_count(filter(",fullnm, ", ", 
                           gsub("feature_id", "feature_id_left", subq),
                           " OR ", 
                           gsub("feature_id", "feature_id_right", subq),
                           "))")
          } else {
            query = paste0("op_count(filter(",fullnm, ", ", subq, "))")
          }
          iquery(
            con$db, 
            query,
            return = TRUE
          )$count
        } else{
          NA
        }
      }
    )
    names(counts_at_entity) = entities
    if (any(counts_at_entity != 0)) {
      non_empty_entities = names(counts_at_entity)[which( !is.na(counts_at_entity) & counts_at_entity > 0)]
      stop("Cannot delete featureset: ", featureset_id, ", featureset_name: ", fset$name, 
           "\nData exists for featureset_id at following entities: ",
           pretty_print(
             non_empty_entities, prettify_after = 100
           ))
    } else {
      cat("No data is registered at any of the MeasurementData entities for features", 
          "associated with this featureset", 
          "\nProceeding to delete features")
    }
    
    if (user_confirms_action(action = 
                             paste0("Delete features contained within featureset_id: ",
                                    featureset_id, ", featureset_name: ", fset$name))) {
      cat("Deleting associated feature synonyms\n")
      iquery(
        con$db, 
        paste0("delete(", 
               revealgenomics:::full_arrayname(.ghEnv$meta$arrFeatureSynonym), 
               ", ", subq, ")")
      )
      
      cat("Deleting features\n")
      delete_entity(entity = .ghEnv$meta$arrFeature, id = sort(ff$feature_id), con = con)
      userResponse = 'continueDeleting'
    } else {
      userResponse = 'stopDeleting'
    }
  } # if features exist within a featureset
  
  if (userResponse == 'continueDeleting') {
    delete_entity(entity = .ghEnv$meta$arrFeatureset, id = featureset_id, con = con)
  }
}


##-----------------------------------------------------------------------------=
## Utility function (based on Kriti's "formulate_base_selection_query()" function)
##  for collapsing a vector of numerical ids into a representation that indicates 
##  ranges of IDs by starting and end points.
##-----------------------------------------------------------------------------=
convert_to_id_ranges = function(id_vec, exclude_duplicates=TRUE){
  # THRESH_K = 100
  if (exclude_duplicates) {
    id_vec <- unique(id_vec)
  }
  sorted = sort(id_vec)
  breaks = c(0, which(diff(sorted)!=1), length(sorted))
  # idname = ""
  # if (length(breaks) <= (THRESH_K + 2)) # few sets of contiguous tickers; use `cross_between`
  id_ranges = paste( sapply(seq(length(breaks)-1), 
                              function(i) {
                                left = sorted[breaks[i]+1]
                                right = sorted[breaks[i+1]]
                                if (left == right) {
                                  as.character(right)
                                  # paste("(", right, ")")
                                } else {
                                  sprintf("%d-%d", left,
                                          right)
                                }
                              }), 
                       collapse = ", ")

  return(id_ranges)
}

