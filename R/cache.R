#
# BEGIN_COPYRIGHT
#
# PARADIGM4 INC.
# This file is part of the Paradigm4 Enterprise SciDB distribution kit
# and may only be used with a valid Paradigm4 contract and in accord
# with the terms and conditions specified by that contract.
#
# Copyright (C) 2011 - 2018 Paradigm4 Inc.
# All Rights Reserved.
#
# END_COPYRIGHT
#

update_variant_key_cache = function(con = NULL){
  con = use_ghEnv_if_null(con)
  
  entitynm = .ghEnv$meta$arrVariantKey
  arraynm =  full_arrayname(entitynm)
  .ghEnv$cache$dfVariantKey = iquery(con$db, arraynm, return = TRUE)
}

get_variant_key_from_cache = function(updateCache = FALSE, con = NULL){
  con = use_ghEnv_if_null(con)
  
  if (updateCache | is.null(.ghEnv$cache$dfVariantKey)){
    update_variant_key_cache(con = con)
  }
  if (nrow(.ghEnv$cache$dfVariantKey) == 0) update_variant_key_cache(con = con)
  return(.ghEnv$cache$dfVariantKey)
}
