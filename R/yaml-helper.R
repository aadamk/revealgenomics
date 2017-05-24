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
# BEGIN: Helper functions for using YAML schema object
yaml_to_dim_str = function(dims){
  dim_str = paste(
    names(dims), "=",
    sapply(dims, function(x) {paste(x$start, ":",
                                    ifelse(x$end == Inf, "*", x$end), ",", x$chunk_interval, ",",
                                    x$overlap, sep = "")}),
    sep = "", collapse = ", ")
  dim_str
}

yaml_to_attr_string = function(attributes, compression_on = FALSE){
  if (!compression_on) { 
    paste(names(attributes), ":", attributes, collapse=" , ") 
  } else {
    paste(names(attributes), ":", attributes, "COMPRESSION 'zlib'", collapse=" , ") 
  }
}
# END: Helper functions for using YAML schema object
############################################################
