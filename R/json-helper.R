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
#####################################

#' List count of sub-columns per row in a column that is a JSON formatted string
#' 
#' @param df1 A dataframe 
#' @param aux_info_colname Column name of JSON formatted string
#' @examples 
#' df1 = data.frame(name = c("Alice", "Bob"),
#'                  details__JSON = c('[{  "boolean": true,   "null": null,   "number": 123,   "string":  "Hello World" }]',
#'                                    '[{  "boolean": false,                  "number": 456,   "differentString":  "Hi World" }]'), 
#'                  stringsAsFactors = FALSE)
#' list_ncol_in_aux_column(df1, 'details__JSON')
#' @export
list_ncol_in_aux_column = function(df1, aux_info_colname){
  if (!(aux_info_colname %in% colnames(df1))) {
    stop("No column", aux_info_colname, "in columns of given dataframe")
  }
  sapply(1:nrow(df1), function(i) {
    df_i = fromJSON(df1[i, aux_info_colname])
    ncol1 = ncol(df_i)
    ifelse(is.null(ncol1), 0, ncol1)
  })
}

#' List count of sub-rows per row in a column that is a JSON formatted string
#' 
#' @param df1 A dataframe 
#' @param aux_info_colname Column name of JSON formatted string
#' @examples 
#' df1 = data.frame(name = c("Alice", "Bob"),
#'                  visits__JSON = c('[{"visit_num":1,"condition":"Baseline"}] ',
#'                                    '[{"visit_num":1,"condition":"Baseline"},{"visit_num":2,"condition":"Relapse"}] '), 
#'                  stringsAsFactors = FALSE)
#' list_nrow_in_aux_column(df1, 'visits__JSON')
#' @export
list_nrow_in_aux_column = function(df1, aux_info_colname){
  if (!(aux_info_colname %in% colnames(df1))) {
    stop("No column '", aux_info_colname, "' in columns of given dataframe")
  }
  sapply(1:nrow(df1), function(i) {
    df_i = fromJSON(df1[i, aux_info_colname])
    nrow1 = nrow(df_i)
    ifelse(is.null(nrow1), 0, nrow1)
  })
}

#' Get auxiliary information from a file, and merge as JSON strings into master dataframe 
#' 
#' This is a wrapper for merge_aux_info_from_df() in that it fails gracefully if there is an
#' error at any stage. Also it prints some output as it goes along. 
#' @param  master_df Dataframe containing master merge information (e.g. names of individuals)
#' @param aux_file_path File from which to read auxiliary information
#' @param aux_info_colname Column name in which to store auxiliary information as JSON objects
#' @param by.x Merge key on master dataframe
#' @param by.y Merge key on auxiliary dataframe
#' @param verbose_read boolean flag to print output on reading file
#' @param verbose_merge boolean flag to print output while merging 
#' @export
merge_aux_info_from_file = function(master_df, 
                             aux_file_path,
                             aux_info_colname,
                             by.master,
                             by.aux, 
                             verbose_read = TRUE,
                             verbose_merge = FALSE) {
  if (verbose_read) cat("Merging auxiliary table from\n", aux_file_path, "\n")
  if (verbose_read) cat("Column name:", aux_info_colname, "\n")
  tryCatch({
    aux_df = read.delim(file = aux_file_path,sep = delim)
    if (verbose_read) cat("Dimensions of auxiliary table:", dim(aux_df)[1], "X", dim(aux_df)[2], "\n")
    if (verbose_read) cat("Checking if all entries in auxiliary table have a parent in master\n")
    if (verbose_read) cat(all(aux_df[, by.aux] %in% master_df[, by.master]), "\n")
    if (verbose_read) cat("Now run the merge\n")
    master_df = merge_aux_info_from_df(master_df = master_df, 
                                   aux_df = aux_df, 
                                   aux_info_colname = aux_info_colname, 
                                   by.x = by.master, 
                                   by.y = by.aux, 
                                   verbose_merge)
  }, error = function(e){
    cat("#! Error loading this file\n")
    print(e)
    master_df[, aux_info_colname] = NA
  })
  master_df
}

#' Get auxiliary information from a dataframe, and merge as JSON strings into master dataframe 
#' 
#' @param master_df Dataframe containing master merge information (e.g. names of individuals)
#' @param aux_df Dataframe containing auxiliary information
#' @param aux_info_colname Column name in which to store auxiliary information as JSON objects
#' @param by.x Merge key on master dataframe
#' @param by.y Merge key on auxiliary dataframe
#' @param verbose_merge boolean flag to print output while merging 
#' @examples 
#' individual_df = data.frame(name = c('Alex', 'Bob'), age = c(44, 33), stringsAsFactors = FALSE)
#' visit_df = data.frame(individual = c('Alex', 'Alex', 'Bob'),
#'                     visit_num = c(1, 2, 1), 
#'                     reason = c('Baseline', 'Relapse', 'Baseline'),
#'                     temperature = c(99.5, 97.9, 99.2))
#' merge_aux_info_from_df(master_df = individual_df, 
#'                        aux_df = visit_df, 
#'                        aux_info_colname = 'visits__JSON', 
#'                        by.x = 'name', by.y = 'individual')
#' @export
merge_aux_info_from_df = function(master_df, aux_df, aux_info_colname, by.x, by.y, verbose_merge = FALSE) {
  join_on_key = as.character(master_df[, by.x])
  stopifnot(length(unique(join_on_key)) == length(join_on_key))
  aux_json = lapply(join_on_key, function(key) {
    mydf = aux_df[aux_df[, by.y] == key, ]
    mydf = drop_na_columns(mydf)
    
    if (verbose_merge) cat("Debug output: ")
    if (verbose_merge) if (nrow(mydf) > 1) {cat("..(", nrow(mydf), ")", sep = "")}
    if (nrow(mydf) > 0) {
      # mydf[, by.y] = NULL
      toJSON(mydf)
    } else {
      toJSON(NULL)
    }
  })
  if (verbose_merge) cat("\n")
  
  aux_json_vec = unlist(aux_json)
  master_df[, aux_info_colname] = aux_json_vec
  master_df
}

