#BEGIN_COPYRIGHT
#
# PARADIGM4 INC.
# This file is distributed along with the Paradigm4 Enterprise SciDB
# distribution kit and may only be used with a valid Paradigm4 contract
# and in accord with the terms and conditions specified by that contract.
#
# Copyright (C) 2010 - 2018 Paradigm4 Inc.
# All Rights Reserved.
#
#END_COPYRIGHT

#USAGE:
# con <- rg_connect2(...)
# list_users(con)
# list_datasets(con)
# show_user_permissions(con, 'todd')
# show_dataset_permissions(con, 'gemini_dataset')
#
# GRANT:
# set_permissions(con, 'gary', 'gemini_dataset', TRUE)
#
# REVOKE:
# set_permissions(con, 'gary', 'gemini_dataset', FALSE)
#
# GRANT to multiple datsets:
# set_permissions(con, 'gary', c('gemini_dataset', 'Kriti_test_dataset'), TRUE)

PERMISSIONS_ARRAY = function() { 'permissions.dataset_id' }
DATASET_ARRAY = function() { full_arrayname(.ghEnv$meta$arrDataset) }

#' list users registered with scidb
#'
list_users = function(con = NULL)
{
  con = use_ghEnv_if_null(con)
  iquery(con$db, "project(list('users'), name)",
         return=T, only_attributes=T,
         schema="<user_name:string>[i]")
}

#' list studies
#'
#' faster optionthan get_datasets()
list_datasets = function(con = NULL)
{
  con = use_ghEnv_if_null(con)
  iquery(con$db, paste0("project(filter(", DATASET_ARRAY(), ", dataset_version=1), name)"),
         return=T, only_attributes=T,
         schema = "<dataset_name:string>[i]")
}

# Helper function used below
show_permissions_inner = function(con = NULL, dataset_filter= 'true', user_filter='true')
{
  con = use_ghEnv_if_null(con)
  iquery(con$db,
         paste0(
           "project(
                   equi_join(
                             project(
                                     apply(
                                           filter(",
                                                  DATASET_ARRAY(), ",
                                                  (", dataset_filter, ") and dataset_version = 1
                                           ),
                                           dataset_name, name
                                    ),
                                    dataset_name
                             ) as l,
                             equi_join(
                                      filter(",
                                             PERMISSIONS_ARRAY(),",
                                             access
                                      ) as PERMS,
                                      project(
                                              apply(
                                                    filter(
                                                           list('users'),",
                                                           user_filter, "
                                                    ),
                                                    idx, int64(id),
                                              user_name, name
                                              ),
                                              user_name, idx
                                     ) as USERS,
                                     left_names: user_id,
                                     right_names:idx,
                                     algorithm:'hash_replicate_right',
                                     keep_dimensions:true
                            ) as r,
                            left_names:l.dataset_id,
                            right_names:r.dataset_id,
                            algorithm:'hash_replicate_right'
                     ),
           dataset_name, user_name
     )"),
     return=T, only_attributes=T,
     schema='<dataset_name:string, user_name:string>[i]'
  )
}

#' show user permissions
#'
#' to be run only by scidbadmin, or user
#' with Read capability to permissions and secured namespaces
#' @export
show_user_permissions = function(con = NULL, user_name)
{
  con = use_ghEnv_if_null(con)
  show_permissions_inner(con, user_filter = paste0("name = '", user_name,"'"))
}

#' show study permissions
#'
#' to be run only by scidbadmin, or user
#' with Read capability to permissions and secured namespaces
#' @export
show_dataset_permissions = function(con = NULL, dataset_name)
{
  con = use_ghEnv_if_null(con)
  show_permissions_inner(con, dataset_filter = paste0("name = '", dataset_name,"'"))
}

#' set per study access permissions for a user
#'
#' to be run only by scidbadmin, or user
#' with Read / Write capability to permissions and secured namespaces
#'
#' can supply multiple study names at a time
#'
#' @export
set_permissions = function(con = NULL, user_name, dataset_names, allowed)
{
  con = use_ghEnv_if_null(con)
  if(length(user_name) != 1)
  {
    stop("Must specify exactly one user")
  }
  if(length(dataset_names) < 1)
  {
    stop("Must specify 1 or more datasets")
  }
  user_idx = iquery(con$db, paste0(
    "project(
    filter(
    list('users'),
    name = '", user_name, "'
    ),
    id
  )"), return=T, only_attributes=T, schema="<id:uint64>[i]"
  )
  if(nrow(user_idx) != 1)
  {
    stop(paste("Can't find user", user_name))
  }
  user_idx = as.numeric(user_idx$id)
  ds_len = length(dataset_names)
  dataset_build_str = paste0("[", paste0("(\\'", dataset_names, "\\')", collapse=",") ,"]")
  dataset_build_str = paste0("build(<dataset_name:string>[i], '", dataset_build_str, "',true)")
  dataset_idx = iquery(con$db, paste0(
    "project(
    equi_join(
    filter(",
    DATASET_ARRAY(), ",
    dataset_version=1
    ),",
         dataset_build_str, ",
    left_names: name,
    right_names:dataset_name,
    keep_dimensions:true,
    algorithm:'hash_replicate_right'
    ),
    dataset_id
    )"), schema = "<dataset_idx:int64>[i]",
     return=T, only_attributes=T
  )
  if(nrow(dataset_idx) != ds_len)
  {
    stop(paste("Can't find all specified datasets. Found", nrow(dataset_idx), "of", ds_len, "datasets"))
  }
  dataset_idx = as.numeric(dataset_idx$dataset_idx)
  if( allowed == FALSE )
  {
    permission='false'
  } else
  {
    permission='true'
  }
  dataset_idx_str = paste0("[", paste0("(", sprintf("%.0f", dataset_idx), ")", collapse=",") ,"]")
  iquery(con$db, paste0(
    "insert(
      redimension(
       apply(
        build(<dataset_id:int64>[i=0:*], '",dataset_idx_str,"', true),
         user_id,", sprintf("%.0f", user_idx), ",
         access,", permission, "
        ),
        ", PERMISSIONS_ARRAY(),"
       ),
       ", PERMISSIONS_ARRAY(),"
      )"))
  max_version = max(iquery(con$db, sprintf("versions(%s)", PERMISSIONS_ARRAY()), return=TRUE)$version_id)
  iquery(con$db, sprintf("remove_versions(%s, %i)", PERMISSIONS_ARRAY(), max_version))
}

#' grant initial access to a user
#'
#' function to be called while onboarding a user
#' can only be called by scidbadmin
#'
#' @export
grant_initial_access = function(con = NULL, user_name) {
  con = use_ghEnv_if_null(con)
  if(length(user_name) != 1)
  {
    stop("Must specify exactly one user")
  }
  user_idx = iquery(con$db, paste0(
    "project(
    filter(
    list('users'),
    name = '", user_name, "'
    ),
    id
  )"), return=T, only_attributes=T, schema="<id:uint64>[i]"
  )
  if(nrow(user_idx) != 1)
  {
    stop(paste("Can't find user", user_name))
  }
  user_idx = as.numeric(user_idx$id)

  available_roles = iquery(con$db, "list('roles')", return = T)$name
  reader_role = 'reveal_data_readers'
  if (reader_role %in% available_roles) { # Preferred method
    cat("Adding user to role:", reader_role, "\n")
    query = paste0("add_user_to_role('", user_name, "', '", reader_role, "')")
    iquery(con$db, query)
  } else { # Not the preferred method
    stop("Asking to individually add namespace permissions for user -- create a central role: ",  reader_role, "instead\n")
    cat("Grant read access to public namespace\n")
    iquery(con$db,
           paste0("set_role_permissions('", user_name, "', 'namespace', '",
                  find_namespace('FEATURE'), "', 'rl')"))
    cat("Grant read, write access to GENELIST namespace\n")
    iquery(con$db,
           paste0("set_role_permissions('", user_name, "', 'namespace', '",
                  find_namespace(.ghEnv$meta$arrGenelist), "', 'crudl')"))
    cat("Grant list access to secure namespace (required for study level security)\n")
    iquery(con$db,
           paste0("set_role_permissions('", user_name, "', 'namespace', '",
                  find_namespace('DATASET'), "', 'l')"))
  }

  cat("Granting access to publc studies\n")
  studylist = iquery(con$db, paste0("project(", full_arrayname(.ghEnv$meta$arrDataset), ", public)"), return = T)
  studylist$dataset_version = NULL
  studylist = unique(studylist)
  studylist = studylist[studylist$public, ]

  if (nrow(studylist) > 0) {
    query = paste0("build(<dataset_id:int64>
                   [dataset_id_idx=1:",nrow(studylist),"],",
                   "'[", paste0(studylist$dataset_id, collapse = ","),"]',true )")
    query = paste0("apply(", query, ",user_id,", user_idx, ",access, true)")
    query = paste0("redimension(", query, ", ", PERMISSIONS_ARRAY(), ")")
    query = paste0("insert(", query, ", ", PERMISSIONS_ARRAY(), ")")
    iquery(con$db, query)
    return(TRUE)
  } else {
    return(FALSE)
  }
}

#' whether to use secure_scan or not
#'
#' use this as a switch to choose between
#' - secure_scan (apply study-level security on dataset_id; must have permissions.dataset_id array)
#' - scan (just use regular scan of arrays)
custom_scan = function() {
  # Use secure_scan for SciDB enterprise edition only
  ifelse(options("revealgenomics.use_scidb_ee"),
         "secure_scan",
         "scan")
}

#' placeholder to be filled in
add_user_to_data_loaders = function(con) {
 # iquery -aq "create_role('revealgenomics_data_loaders')"
 # iquery -aq "set_role_permissions('revealgenomics_data_loaders', 'namespace', 'gh_secure', 'ruld')"
 # iquery -aq "set_role_permissions('revealgenomics_data_loaders', 'namespace', 'gh_public', 'ruld')"
 # iquery -aq "set_role_permissions('revealgenomics_data_loaders', 'namespace', 'gh_public_rw', 'ruld')"
 # iquery -aq "add_user_to_role('secure_user', 'revealgenomics_data_loaders')"
}
