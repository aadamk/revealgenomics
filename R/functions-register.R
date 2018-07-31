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

#' @export
register_variant_key = function(df1, only_test = FALSE, con = NULL){
  uniq = unique_fields()[[.ghEnv$meta$arrVariantKey]]
  test_register_variant_key(df1, uniq, silent = ifelse(only_test, FALSE, TRUE))
  
  if (!only_test) {
    arrayname = full_arrayname(.ghEnv$meta$arrVariantKey)
    ids = register_tuple_return_id(df1, arrayname, uniq, con = con)
    
    # force update the cache
    update_variant_key_cache(con = con)
    
    return(ids)
  } # end of if (!only_test)
}
