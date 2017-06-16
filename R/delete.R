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
  THRESH_K = 100
  sorted=sort(id)
  breaks=c(0, which(diff(sorted)!=1), length(sorted))
  idname = get_base_idname(fullarrayname)
  if (length(breaks) <= (THRESH_K + 2)) # few sets of contiguous tickers; use `cross_between`
  {
    expr_query1 = paste( sapply(seq(length(breaks)-1), function(i) {
      left = sorted[breaks[i]+1]
      right = sorted[breaks[i+1]]
      if (left == right) {
        paste("(", idname, "=", right, ")")
      } else {
        sprintf("(%s >= %d AND %s <= %d)", idname, left,
                idname, right)
      }
    }), collapse=" OR ")
  } else {
    stop("Try fewer ids at a time")
  }
  return(expr_query1)
}

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
delete_entity = function(entity, id, dataset_version = NULL, delete_by_entity = NULL){
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
                                           dataset_version = dataset_version, all_versions = F))
  } else {
    status = try(check_entity_exists_at_id(entity = delete_by_entity, id = id))
  }

  if (class(status) == 'try-error') {
    stop()
  }
  # Now that search_by_entities are proven to exist at specified id-s, delete them
  
  # Find the correct namespace
  if (is_entity_secured(entity)) {
    namespaces = find_namespace(id = id, entitynm = delete_by_entity)
    nmsp = unique(namespaces)
  } else { nmsp = 'public' }
  if (length(nmsp) != 1) stop("Can run delete only at one namespace at a time")
  arr = paste(nmsp, entity, sep = ".")
  
  # Delete the mandatory fields array
  if (get_entity_class(entity = entity)  == 'measurementdata') { # Special handling for measurement data class
                                                                          # -- they do not have get_<ENTITY>() calls
                                                                          # -- only allow deleting by one preferred id at a time
    cat("Deleting entries for ", get_base_idname(delete_by_entity), " = ",  
        id, " from ", arr, " entity\n", sep = "")
    qq = paste("delete(", arr, ", ", get_base_idname(delete_by_entity), " = ",  
               id, " AND dataset_version = ", dataset_version, ")", sep = "")
    print(qq)
    iquery(.ghEnv$db, qq)
  } else {
    base_selection_query  = formulate_base_selection_query(fullarrayname = arr, id = id)
    versioned_selection_query = formulate_versioned_selection_query(entity = entity, 
                                                                    base_selection_query = base_selection_query, 
                                                                    dataset_version = dataset_version)
    cat("Deleting entries for ids ", pretty_print(sort(id)), " from ", arr, " entity\n", sep = "")
    qq = paste("delete(", arr, ", ", versioned_selection_query, ")", sep = "")
    print(qq)
    iquery(.ghEnv$db, qq)
  }
  
  # Clear out the info array
  infoArray = .ghEnv$meta$L$array[[entity]]$infoArray
  if (infoArray){
    delete_info_fields(fullarrayname = arr, id = id, dataset_version = dataset_version)
  }
  
  # Clear out the lookup array if required
  if ( get_entity_class(entity = entity) != 'measurementdata' ) { # No lookup handling for measurement data class
    if (is_entity_secured(entity)){
      # Check if there are no remaining entities at this ID at any version
      qcount = paste("op_count(filter(", arr, ", ",
                     base_selection_query, "))" )
      newcount = iquery(.ghEnv$db, qcount, return = TRUE)$count
      if (newcount == 0){ # there are no entities at this level
        arrLookup = paste(entity, "_LOOKUP", sep = "")
        qq = paste("delete(", arrLookup, ", ", base_selection_query, ")", sep = "")
        cat("Deleting entries for ids ", paste(sort(id), collapse = ", "), " from lookup array: ", arrLookup, "\n", sep = "")
        print(qq)
        iquery(.ghEnv$db, qq)
        updatedcache = entity_lookup(entityName = entity, updateCache = TRUE)
      }
    } # end of: if (is_entity_secured(entity))
  } # end of: if ( get_entity_class(entity = entity) != 'measurementdata' )
}

delete_info_fields = function(fullarrayname, id, dataset_version, delete_by_entity = NULL){
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
    iquery(.ghEnv$db, qq)
  } else {
    base_selection_query = formulate_base_selection_query(fullarrayname = fullarrayname, 
                                                          id = id)  
    versioned_selection_query = formulate_versioned_selection_query(entity = entity, 
                                                                    base_selection_query = base_selection_query, 
                                                                    dataset_version = dataset_version)
    
    qq = paste("delete(", arrInfo, ", ", versioned_selection_query, ")", sep = "")
    cat("Deleting entries for ids ", pretty_print(sort(id)), " from info array: ", arrInfo, "\n", sep = "")
    print(qq)
    iquery(.ghEnv$db, qq)
  }

###############################################################################
## NEXT STEPS / ELEMENTS STILL REMAINING TO IMPLEMENT:
## 
##  - Have print_dataset_subelements() return the list of all elements that 
##    it identifies for the given dataset.
##  - Call the actual "delete_entity()" function for each of the elements within
##    a dataset (as specified within the list of returned objects)
##      - Note: Will have to figure out how to programmatically identify the
##        type of entity that each object is, since this is necessary for the 
##        "delete_entity()" function.
##  - Call the function that deletes actual data entries associated with a
##    given dataset once Kriti implements that function.
##  - Process the user input project_ID to verify that it is only a single ID,
##    and that it exists.
###############################################################################

delete_project <- function(projectID) {
  ##---------------=
  ## If there is more than one project_ID, or it is not a valid ID, then give an error.
  ##---------------=
  
  #####################
  ## << FINISH!!! >> ## 
  #####################
  
  
  ##---------------=
  ## Get the datasets under this project.
  ##---------------=
  datasets <- search_datasets(project_id = projectID)
  #   datasetIDs <- datasets$dataset_id
  #   datasetVersions <- datasets$dataset_version
  
  ##---------------=
  ## For each dataset, call "get_dataset_subelements", to print the list of subelements
  ##  of each dataset.
  ##---------------=
  datasetStructures.lst <- list()
  for (i in 1:nrow(datasets)) {
    nextDataset <- datasets[i, "dataset_id"]
    nextDatasetVersion <- datasets[i, "dataset_version"]
    nextDatasetName <- paste0(nextDataset, "_v", nextDatasetVersion)
    datasetStructures.lst[[nextDatasetName]] <- print_dataset_subelements(nextDataset, nextDatasetVersion)
  }
  
  ##---------------=
  ## Ask the user to confirm that they want to delete this project.
  ##---------------=
  userResponse <- readline(prompt = "Do you want to continue deleting this project? (yes/no)  ")
  
  ##---------------=
  ## If the user said to procede, then delete the datasets.
  ##---------------=
  if(charmatch(tolower(userResponse), "yes") == 1) {
    cat("Deleting project ", projectID, "... \n",  sep="")
    
    ## Now delete all of the datasets and their sub-elements.
    for (nextDataset in names(datasetStructures.lst)) {
      #delete_dataset(nextDataset)  ## Actually don't need a recursive delete since we can currently use dataset_ID.
      cat("DEBUG-- Will eventually be deleting dataset - ", nextDataset, " here...\n")
    }
    
    #####################
    ## << FINISH!!! >> ## 
    #####################
    ## Replace the "delete_dataset()" with calls to the  
    ##  "delete_entity()" function and goes through the list of objects r
    ##  returned above and deletes each one.
    ## Also, need to add in the calls to delete actual data from the 
    ##  data matrixes once that functionality is available.
    
  } else {
    ## Don't delete the project.
    cat("Project ", projectID, " was NOT deleted. ",  sep="")
  }
  
}


get_dataset_subelements <- function(datasetID, datasetVersion) {
  ## DEBUG: A flag for whether to surpess the errors for searching for entities
  ## that might not be there.  This should be coded concretely one way or the
  ## other.
  SEARCH_SILENTLY <- TRUE  
  
  ##---------------=
  ## Get all of the sub-elements of this dataset.
  ##
  ## For now, this will have to be done explicitly, but hopefully will
  ##  be able to do it programatically in the future.
  ##---------------=
  dataset_subelements <- list()
  dataset_subelements[[jdb$meta$arrIndividuals]] <- try(search_individuals(dataset_id = datasetID, dataset_version = datasetVersion), silent = SEARCH_SILENTLY)
  dataset_subelements[[jdb$meta$arrBiosample]] <- try(search_biosamples(dataset_id = datasetID, dataset_version = datasetVersion), silent = SEARCH_SILENTLY)
  dataset_subelements[[jdb$meta$arrRnaquantificationset]] <- try(search_rnaquantificationset(dataset_id = datasetID, dataset_version = datasetVersion), silent = SEARCH_SILENTLY)
  dataset_subelements[[jdb$meta$arrVariantset]] <- try(search_variantsets(dataset_id = datasetID, dataset_version = datasetVersion), silent = SEARCH_SILENTLY)
  dataset_subelements[[jdb$meta$arrCopyNumberSet]] <- try(search_copynumbersets(dataset_id = datasetID, dataset_version = datasetVersion), silent = SEARCH_SILENTLY)
  dataset_subelements[[jdb$meta$arrCopyNumberSubSet]] <- try(search_copynumbersubsets(dataset_id = datasetID, dataset_version = datasetVersion), silent = SEARCH_SILENTLY)
  
  ## Now set each of the fields to NULL that did not return any values (i.e. that produced a try-error).
  for (next.name in names(dataset_subelements)) {
    if (class(dataset_subelements[[next.name]]) == "try-error")
      dataset_subelements[[next.name]] <- NA 
  } 
  
  return (dataset_subelements)
}


## IDEA: Might be able to eventually create a table that lists all of the types of metadata
##  that will need to be accessed.  It will give the name (ex: "arrIndividuals"), 
##  entity-name (ex: "INDIVIDUAL"), the lookup function (ex: "search_individuals"),
##  the ID column name (ex: "individual_id"), and the parent object? (ex: the dataset ID or project ID?)


print_dataset_subelements <- function(datasetID, datasetVersion, print.nonexistant.metadata = FALSE) {
  
  ## A flag on whether to explicitly state which types of information have been searched
  ##  but do not exist for this dataset. 
  # print.nonexistant.metadata
  
  
  ##---------------=
  ## Get the dataset's subelements
  ##---------------=
  dataset_subelements <- get_dataset_subelements(datasetID, datasetVersion)
  
  ##---------------=
  ## Print to the screen the lists of elements that will be deleted.
  ##---------------=
  cat("Summary of dataset = ", datasetID, " (version = ", datasetVersion, "):  \n", sep="")
  for(next.subelement in names(dataset_subelements)) {
    if ( all(is.na(dataset_subelements[[next.subelement]])) ) {
      ## Then there were no data of this type in the given dataset.
      
      if (print.nonexistant.metadata) {
        ## Then we will print out that there was no metadata of this type.
        cat("\t", next.subelement, ":\t <NA> \n", sep="")
      }
    } else {
      ## There are elements of this metadata type located in this dataset, so print them out.
      cat("\t", next.subelement, ":\t ", sep="")
      cat(dataset_subelements[[next.subelement]][, 1], sep=", ")  ## The object ids appear to be in the first column in each entity matrix.
      cat("\n")
      
      
      #####################
      ## << FINISH!!! >> ## 
      #####################
      ## (1) Add the ability to output a range if it is present, rather than just the explicit 
      ##    list of all IDs for the given object.  For example, if within this dataset, there are
      ##    individual_IDs 1,2,3,4,5, it would be nice to output "individuals: 1-5" than listing
      ##    all of them individually.
      ##
      ## (2) *** This is important -- Currently I'm hard-coding that the IDs for each 
      ##    type of element are in column 1.  This happens to be true in the data that 
      ##    I've been looking at, but it doesn't necessarily have to be!  I need to 
      ##    figure out a way to make this more programmatic.
      ##
      ##  So, for each object type in "dataset_subelements", the ID is in the row:
      ##    $individual --> "individual_id"
      ##    $biosample --> "biosample_id"
      ##    $rnaquantificationsets --> "rnaquantificationset_id"
      ##    $variantsets --> ??
      ##    $copynumbersets --> ??
      ##    $copynumbersubsets --> ??
      
    }
  }
  return(dataset_subelements)
}


##-----------------------------------------------------------------------------=
## This function will loop through and delete each of the entities within a 
##  dataset, but for the moment it will only delete the metadata.  It will
##  eventually delete the actual measurement data as well.
##-----------------------------------------------------------------------------=
delete_dataset <- function(datasetID, datasetVersion, datasetStructure = NULL) {
  
  ## Get the list of sub-entities.
  ## If the dataset's structure is provided, then use that structure, otherwise call
  ##  the function to get the dataset's sub-structure.
  if (is.null(datasetStructure)) {
    datasetStructure <- get_dataset_subelements(datasetID, datasetVersion)
  }
  
  
  ## For each dataset entities, delete the the actual measurements, then delete
  ##  the metadata.  Note that the metadata field name is the actual entity
  ##  name as well.
  for (next.metadata.name in names(datasetStructure)) {
    next.metadata.object <- datasetStructure[[next.metadata.name]]
    
   
    
    if (!is.na(next.metadata.object)) {
      ## Get the list of object IDs.
      
      ## Delete each of the measurements based on their ID, dataset ID, and dataset version.
      
      ## Now delete the metadata entity.
      delete_entity(entity = next.metadata.name, ids = )
    }
    
    
    ######################################
    ##### FINISH!!!! ###############
    ######################################
    nextIDs <- next.metadata.name[, 1]#### THE NAME OF THE COLUMN THAT HOLDS THE IDS IN THIS DATAFRAME!!!!! ######] 
    ######################################
    ##### FINISH!!!! ###############
    ######################################
    
    
    
  }
  
  
  ## NOTE: Currently there is no list of child-entities attached to a higher
  ##  level entity. So for now, this will have to be done manually by calling
  ##  each of the search functions that could possibly exist at this level.
  delete_entity(entity = 'INDIVIDUAL', ids = i$individual_id[1:2], dataset_version = 1)
  
  ## Delete each of the sub-entities.
  
  ## Delete this dataset.
  delete_entity(entity = 'INDIVIDUAL', ids = i$individual_id[1:2], dataset_version = 1)
}
