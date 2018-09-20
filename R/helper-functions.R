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

############################################################
# Helper functions for dataframe / text manipulation
############################################################

#' Auto-promote characters to types
#' 
#' Custom function
#' - should not autopromote non-character columns for VARIANT data
#' - for columns that are interpreted as logical, should use strict checking for TRUE/True, or FALSE/False
#' ('T' is a valid character in genomic data)
#' 
#' @param convert_logicals if TRUE, accept automatically converted logicals (see note above)
autoconvert_char = function(df1, convert_logicals = TRUE) {
  if (nrow(df1) == 0) return(df1)
  
  col_types = sapply(df1, class)
  col_types_chars    = names(col_types[which(col_types == 'character')])
  col_types_nonchars = names(col_types[which(col_types != 'character')])
  
  df_ac = df1[, col_types_chars] %>% 
    mutate_all(
      funs(
        type.convert(as.character(.), as.is = TRUE, numerals = "warn.loss")
      )
    )
  
  col_types_ac = sapply(df_ac, class)
  col_types_ac_logicals    = names(col_types_ac[which(col_types_ac == 'logical')])
  col_types_ac_nonlogicals = names(col_types_ac[which(col_types_ac != 'logical')])
  if (convert_logicals) {
    df_ac = cbind(as_tibble(df1)[, col_types_nonchars], 
                  df_ac)
    
  } else {
    df_ac = cbind(as_tibble(df1)[, col_types_nonchars], 
                  as_tibble(df1)[, col_types_ac_logicals],
                  as_tibble(df1)[, col_types_ac_nonlogicals])
  }
  df_ac[, colnames(df1)]
}

#' Compare with mandatory fields passed by user
#' 
#' Rename remaining columns of dataframe as info_<column-name>
#' Take the info columns that are non-string and convert to string
prep_df_fields = function(df, mandatory_fields){
  available_fields = colnames(df)
  
  pos = which(!(available_fields %in% mandatory_fields))
  
  colnames(df)[pos] = paste("info_", available_fields[pos], sep = "")
  
  posNotChar = which((sapply(df, class) != "character") & 
                       !(colnames(df) %in% mandatory_fields))
  for (posi in posNotChar){
    df[, posi] = paste(df[, posi])
  }
  df
}

#' @export
remove_duplicates = function(df_data){
  df_data[which(!duplicated(df_data)), ]
}

df_rename_column = function(df, oldname, newname){
  colnames(df)[grep(paste("^",oldname,"$",sep = ""), colnames(df))] = newname
  df
}
## YAML related
strip_namespace = function(arrayname) sub("^.*[.]", "", arrayname)

## YAML related
get_namespace = function(arrayname) sub("[.].*$", "", arrayname)

#' @export
drop_na_columns = function(df){
  # http://stackoverflow.com/questions/2643939/remove-columns-from-dataframe-where-all-values-are-na
  df[,colSums(is.na(df))<nrow(df)]
}

rename_column = function(x1, old_name, new_name){
  colnames(x1)[colnames(x1) == old_name] = new_name
  x1
}

# Helper function to convert dataframe columns from factors to characters
# Use with caution: Floats and integer values that are stored as factors 
# will be forced into characters
convert_factors_to_char = function(dfx){
  i <- sapply(dfx, is.factor)
  dfx[i] <- lapply(dfx[i], as.character)
  dfx
}

#' Pretty print a large vector of strings, integers etc. 
#' 
#' @param vec vector that is to be pretty printed
#' @param prettify_after prettify output if length of vector is longer than this limit
#' @export
pretty_print = function(vec, prettify_after = 7) {
  prettify_after = ifelse(prettify_after >= 7, prettify_after, 7) # force parameter to have a minimum value of 7
  ifelse(length(vec) <= prettify_after,
         paste(vec, collapse = ", "),
         paste(pretty_print(head(vec, ceiling((prettify_after-3)/2))),
               "...(Total: ", length(vec), ")... ",
               pretty_print(tail(vec, ceiling((prettify_after-3)/2))),
               sep = ""))}


#' helper function to report matches between vectors
#' 
#' @param source source vector for finding matches from
#' @param target target vector in which to find matches
#' 
#' @return 
#' list(match_res, source_matched_idx, source_unmatched_idx, target_matched_idx)
#' @export
find_matches_and_return_indices = function(source, target){
  match_res = match(source, target)
  match_idx = which(!is.na(match_res))
  non_match_idx = which(is.na(match_res))
  
  list(match_res = match_res,
       source_matched_idx = match_idx,
       source_unmatched_idx = non_match_idx,
       target_matched_idx = match_res[match_idx])
}

#' names to list of numbers by uniqueness
#' 
#' function to convert a list of names to
#' a numbered vector \code{1:N} where \code{N} is the number of unique names
#' (each unique name has a different number)
#' 
#' @examples
#' names_to_numbered_vec_by_uniqueness(c('a', 'b', 'a', 'c')) # returns: 1, 2, 1, 3
#' names_to_numbered_vec_by_uniqueness(c(11, 35, 44, 11, 35, 66)) # returns: 1, 2, 3, 1, 2, 4
names_to_numbered_vec_by_uniqueness = function(names_vec) {
  names_vec = as.character(names_vec)
  lookup_idx = 1:length(unique(names_vec))
  names(lookup_idx) = unique(names_vec)
  
  lookup_idx[names_vec]
}

