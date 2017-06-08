############################################################
# Functions exclusively for handling versioning
############################################################

# maintain a cache of `dataset_id, dataset_version`, and to retrieve the lookup
get_dataset_version_lookup = function(updateCache = FALSE){
  str = 'DATASET_VERSION'
  if (updateCache | is.null(jdb$cache$lookup[[str]])){
    namespaces = jdb$cache$nmsp_list
    qq = "public.DATASET"
    if (length(namespaces) > 1)  {
      for (nmsp in namespaces[namespaces != 'public']) qq = paste("merge(", qq, ", ", nmsp, ".DATASET)", sep = "")
    }
    # qq2 = paste("project(apply(", qq, ", dataset_id, dataset_id, dataset_version, dataset_version), dataset_id, dataset_version)", sep = "")
    df = iquery(jdb$db, qq, return = T)
    jdb$cache$lookup[[str]] = df[, c('dataset_id', 'dataset_version')]
  }
  return(jdb$cache$lookup[[str]])
}

# Find the current maximum `dataset_version` for a user specified `dataset_id`
get_dataset_max_version = function(dataset_id, updateCache = FALSE){
  df = get_dataset_version_lookup(updateCache)
  if (!(dataset_id %in% df$dataset_id)) {stop("Either this dataset_id does not exist or you do not have access to it")}
  df = df[df$dataset_id == dataset_id, ]
  max(df$dataset_version)
}

# increment the version for a given dataset
# parameter df is typically the output of a get_datasets(dataset_id = ...) call, and required modifications on that result
#' @export
increment_dataset_version = function(df){
  if(length(df$dataset_id)!=1) stop("Can increment version for one specific dataset_id only")
  
  arrayname = jdb$meta$arrDataset
  nmsp = find_namespace(id = df$dataset_id, entitynm = arrayname)
  arrayname = paste(nmsp, arrayname, sep = ".")
  df$dataset_version = get_dataset_max_version(dataset_id = df$dataset_id, updateCache = TRUE) + 1
  df$created = NULL
  df$updated = NULL
  mandatory_fields = get_mandatory_fields_for_register_entity(jdb$meta$arrDataset)
  
  register_tuple_update_lookup(df = prep_df_fields(df,
                                                   c(mandatory_fields,
                                                     get_idname(jdb$meta$arrDataset))),
                               arrayname = arrayname, updateLookup = FALSE)
  return(df[, c(get_idname(arrayname))])
}



