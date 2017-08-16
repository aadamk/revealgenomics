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

# Compare with mandatory fields passed by user
# Rename remaining columns of dataframe as info_<column-name>
# Take the info columns that are non-string and convert to string
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
#' @export
pretty_print = function(vec) {
  ifelse(length(vec) <= 7,
         paste(vec, collapse = ", "),
         paste(pretty_print(head(vec, 2)),
               "...(", length(vec)-4, " other)... ",
               pretty_print(tail(vec, 2)),
               sep = ""))}


############################################################
# Helper functions for SciDB array operations
############################################################
#' @export
scidb_exists_array = function(arrayName) {
  !is.null(tryCatch({iquery(.ghEnv$db, paste("show(", arrayName, ")", sep=""), return=TRUE, binary = FALSE)}, error = function(e) {NULL}))
}

convert_attr_double_to_int64 = function(arr, attrname){
  attrnames = schema(arr, "attributes")$name
  randString = "for_int64_conversion"
  arr = scidb_attribute_rename(arr, old = attrname, new = randString)
  arr = .ghEnv$db$apply(srcArray = arr, newAttr = R(attrname), expression = int64(R(randString)))
  arr = .ghEnv$db$project(arr, R(paste(attrnames, collapse = ", ")))
  arr
}

#' @export
scidb_attribute_rename = function(arr, old, new){
  attrs = schema(arr, what = "attributes")
  attrnames = attrs$name
  stopifnot(old %in% attrnames)
  
  attrs[match(old, attrnames), "name"] = new
  # dims = schema(arr, "dimensions")
  
  attr_schema = paste(
    paste(
      paste(attrs$name, attrs$type, sep = ": "),
      ifelse(attrs$nullable, "", "NOT NULL"), sep = " "),
    collapse = ", ")
  dim_schema = gsub("<.*> *", "", schema(arr)) # TODO : build up from scratch
  newSchema = paste("<", attr_schema, ">", dim_schema)
  
  arr = .ghEnv$db$cast(srcArray = arr, schemaArray = R(newSchema))
  arr
}

#' @export
scidb_array_count = function(array){
  qq = paste("op_count(", array@name, ")", sep = "")
  # scidb(.ghEnv$db, qq)
  iquery(.ghEnv$db, qq, schema="<count:uint64> [i=0:0]", return = T)$count
}

#' @export
scidb_array_head= function(array, n = 5){
  as.R(.ghEnv$db$limit(array, R(n)))
}

