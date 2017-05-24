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
