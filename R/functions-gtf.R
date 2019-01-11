#' Register features from GTF file
#' 
#' @param featureset_name Exact featureset name at which to register features
#' @param gtf_filepath Path to GTF file
#' @param nSkip Number of rows to skip from top of GTF file
#' @param register_transcripts whether to register_transcripts (TRUE) or not (FALSE)
#' 
#' @return list containing feature id-s of registered genes and transcripts (if selected)
#' 
#' @export
register_features_from_gtf_file = function(featureset_name, gtf_filepath, nSkip, 
                                           register_transcripts, 
                                           con = NULL) {
  
  con = revealgenomics:::use_ghEnv_if_null(con = con)
  
  stopifnot(is.logical(register_transcripts))
  
  # Find the matching featureset
  fs = get_featuresets(con = con)
  fset = fs[fs$name == featureset_name, ]
  stopifnot(nrow(fset) == 1)
  cat("Found matching featureset_id:", fset$featureset_id, "for featureset:", featureset_name, "\n")
  featureset_id = fset$featureset_id
  
  cat("Reading GTF file:", gtf_filepath, "\n")
  feature_df = read.delim(file = gtf_filepath, 
                          header = F, 
                          stringsAsFactors = F, 
                          skip = nSkip)
  
  colnames(feature_df) = c('seqname', 'source', 'feature', 'start', 'end', 
                           'score', 'strand', 'frame', 'attribute')
  
  # attribute column is of the form
  #    "gene_id ENSG00000223972; gene_name DDX11L1; gene_source ensembl_havana; gene_biotype pseudogene;"
  # we create a function to extract out different substrings
  extract_info = function(vec, str_to_search) {
    sep_match_loc_all = gregexpr(pattern = ";", text = vec)
    match_loc = regexpr(pattern = str_to_search, text = vec)
    sep_match_loc = sapply(
      1:length(match_loc), 
      function(idx) {
        ifelse (match_loc[idx] == -1, 
                0, 
                sep_match_loc_all[[idx]][min(which( (sep_match_loc_all[[idx]] - match_loc[idx])  > 0))]
        )
      }
    )
    extract = substr(vec, 
                     match_loc + attr(match_loc, "match.length") + 1, 
                     sep_match_loc - 1)
    extract[extract == ""] = NA
    extract
  }
  
  # We then use the above function on segments of the dataframe at a time
  attribute_col = as.character(feature_df$attribute)
  cat("Splitting the attribute column to extract gene level info.\nShowing one entry of the column as reference\n\t", 
      attribute_col[1], "\n")
  # Add new columns by parsing the `attribute` column
  for (elem in c('gene_id', 'transcript_id', 'gene_name',
                 'gene_biotype', 'transcript_name')) {
    feature_df[, elem] = NA
    cat("\n\n======\nElem:", elem, "\n======\n")
    
    N = nrow(feature_df)
    step = 500000 # step size used to process `attribute` column
    starts = seq(1, N, step)
    ends   = seq(step, step+N, step)
    ends[length(ends)] = N
    ends
    
    if (length(starts) != length(ends)) stop("Need to cover this case")
    
    for (idx in 1:length(starts)) {
      cat("\nRange:", idx, "of total", length(starts), "\n\tTiming: ")
      t1 = proc.time()
      subrange = starts[idx]:ends[idx]
      feature_df[subrange, elem] = extract_info(vec = attribute_col[subrange], 
                                                str_to_search = elem)
      cat((proc.time()-t1)[[3]], "seconds")
    }
  }
  
  cat("Formulating feature annotation dataframe from dataframe parsed from GTF\n")
  feature_ann_df = feature_df[, c('gene_id', 'transcript_id', 'gene_name',
                                  'gene_biotype', 'transcript_name')]
  feature_ann_df = unique(feature_ann_df)
  
  #### GENES ####
  cat("Registering genes\n")
  ftr_df1 = unique(feature_ann_df[, c('gene_id', 'gene_name', 'gene_biotype')])
  dim(ftr_df1)
  colnames(ftr_df1) = c('name', 'gene_symbol', 'gene_biotype')
  if (any(is.na(ftr_df1$name))) stop("Unexpected")
  ftr_df1$feature_type = 'gene'
  ftr_df1$chromosome = '...'
  ftr_df1$start = '...'
  ftr_df1$end = '...'
  ftr_df1$featureset_id = featureset_id
  ftr_df1$source = 'ensembl_gene_id'
  ftr_df1_id = register_feature(df = ftr_df1, register_gene_synonyms = TRUE, con = con)
  
  #### TRANSCRIPT ####
  if (register_transcripts) {
    cat("Registering transcripts\n")
    table(feature_ann_df$gene_biotype)
    ftr_df2 = unique(feature_ann_df[, c('transcript_id', 'gene_name', 'transcript_name')])
    stopifnot(nrow(ftr_df2) == nrow(feature_ann_df))
    colnames(ftr_df2) = c('name', 'gene_symbol', 'transcript_name')
    ftr_df2$feature_type = 'transcript'
    ftr_df2$chromosome = '...'
    ftr_df2$start = '...'
    ftr_df2$end = '...'
    ftr_df2$featureset_id = fset$featureset_id
    ftr_df2$source = 'ensembl_gene_id'
    ftr_df2_id = register_feature(df = ftr_df2, register_gene_synonyms = FALSE)
    
    return(list(
      gene_features_id = ftr_df1_id, 
      transcripts_features_id = ftr_df2_id))
  } else {
    return(list(
      gene_features_id = ftr_df1_id, 
      transcripts_features_id = NULL))
  }
}
