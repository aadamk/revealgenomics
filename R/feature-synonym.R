
# This function combines duplicate gene names 
# col1 is the column that is to be retained
# when col2 is the same as col1, do nothing
# when col1 is NA, and col2 is not NA, paste col2 into col1
# When col2 is different from col1, merge into 'column_to_combine_into' 
# (a '|' separated list of gene names)
combine_duplicate_column = function(dfx, col1, col2, column_to_combine_into){
  for (i in 1:nrow(dfx)) {
    entry1 = dfx[i, col1]
    entry2 = dfx[i, col2]
    if (is.na(entry1) & !is.na(entry2)) {
      dfx[i, col1] = entry2
    } else if (!is.na(entry1) & !is.na(entry2) & entry1 != entry2){
      # If `col2` is blank, do nothing
      if (exists('trace_debug')) cat(i, " ", sep = "")
      if (entry2 != "") {
        # Check if the symbol already exists
        splitsym = strsplit(x = dfx[i, column_to_combine_into], 
                            split = "[|]")
        stopifnot(length(splitsym) == 1)
        splitsym = splitsym[[1]]
        if (!(entry2 %in% splitsym)){
          dfx[i, column_to_combine_into] = paste(dfx[i, column_to_combine_into],
                                                 entry2, sep = "|")
        } else {
          if (exists('trace_debug')) cat("* ") # Alias already existed
        }
      } else {
        if (exists('trace_debug')) cat("$ ") # entry 2 was blank
      }
    }
  }
  
  dfx
}

# This is a manual cleanup function that has two assumptions
# 1. a duplicate name has only one other duplicate
# 2. an entry that has a duplicate name has another name within the hierarchy 
# (note that there are multiple other candidates for placing within the hierarchy)
handle_duplicate_name_source = function(dfx, hierarchy){
  duplicated_rows = which(duplicated(paste(dfx$name, dfx$source, sep = "_")))
  for (i in duplicated_rows){
    # print(i)
    rowx = dfx[i, ]
    duplicates = dfx[dfx$name == rowx$name & dfx$source == rowx$source, ]
    stopifnot(nrow(duplicates) == 2) # currently coded for 2 duplicates
    pos_in_hierarchy = which(hierarchy == rowx$source)
    potential_names = rowx[, hierarchy]
    
    potential_pos = which(potential_names != "" & !is.na(potential_names))
    stopifnot(any(potential_pos > pos_in_hierarchy))
    next_source_pos = min(potential_pos[which(potential_pos > pos_in_hierarchy)])
    # cat("Potential names: ", paste(potential_names, collapse = ", "), "\nEarlier selection was: ", hierarchy[pos_in_hierarchy], 
    #     "Current selection:", hierarchy[next_source_pos], "\n")
    dfx[i, "name"] = rowx[, hierarchy[next_source_pos]]
    dfx[i, "source"] = hierarchy[next_source_pos]
  }
  dfx
}

# Split synonyms that are a pipe separated list and add to synonym list
split_synonyms_by_pipe = function(dfx, field){
  prev = dfx
  prev1 = prev[grep("[|]", prev[, field], invert = T), ]
  prev2 = prev[grep("[|]", prev[, field]), ]
  
  split_symbols = strsplit(as.character(prev2[, field]), split = "[|]")
  tt = lapply(1:nrow(prev2), function(i){data.frame(featureset_id = prev2[i, ]$featureset_id,
                                                    feature_id = prev2[i, ]$feature_id,
                                                    field = split_symbols[[i]],
                                                    source = prev2[i, ]$source, 
                                                    stringsAsFactors = FALSE)}
  )
  tt2 = do.call("rbind", tt)
  colnames(tt2)[colnames(tt2) == 'field'] = field
  
  prev = rbind(prev1, tt2)
  colnames(prev)[colnames(prev) == field] = 'synonym'
  
  stopifnot(any(is.na(prev$feature_id)) == FALSE)
  prev
}

# List name and source by hierarchy
list_name_source_by_hierarchy = function(dfx, hierarchy){
  find_source_by_hierarchy = function(entries, hierarchy){
    non_empty_pos = entries[hierarchy] != "" 
    non_null_pos = !is.na(entries[hierarchy])
    valid_pos = which(non_empty_pos & non_null_pos)
    if (length(valid_pos) == 0) {
      source = NA 
    } else {
      source = hierarchy[min(valid_pos)]
    }
    source
  }
  dfx$source = apply(dfx[, hierarchy], 1, FUN = function(entries) {find_source_by_hierarchy(entries, hierarchy)})
  dfx$name = apply(dfx, 1, FUN = function(row){row[row['source']]})
  dfx
}

# Preparatory function for getting comprehensive gene lists (to start building annotation database)
# Read file <hugo/hgnc_complete_set.txt> for annotation info
# Read file <grch38_release85_homosap_gene/newGene.tsv> as gene location info
# Do a full outer join on the two dataframes based on matching ensembl ID-s
# Then merge the gene symbols in the two files 
merge_gene_annotation_and_location = function(gene_annotation_file_path,
                                               gene_location_file_path){
  x1 = read.delim(gene_annotation_file_path)
  x2 = read.delim(gene_location_file_path)
  
  stopifnot(c('name', 'symbol', 'ensembl_gene_id', 'alias_symbol') %in% colnames(x1))
  stopifnot(!(c('gene_symbol') %in% colnames(x1)))
  stopifnot(c('Chr', 'Start', 'End', 'Gene_id', 'Gene_Name') %in% colnames(x2))
  
  x1 = convert_factors_to_char(x1)
  x2 = convert_factors_to_char(x2)
  x1 = rename_column(x1, old_name = 'name', new_name = 'full_name')
  x1 = rename_column(x1, old_name = 'symbol', new_name = 'gene_symbol')
  x2 = rename_column(x2, old_name = 'Start', new_name = 'start')
  x2 = rename_column(x2, old_name = 'End', new_name = 'end')
  x2 = rename_column(x2, old_name = 'Chr', new_name = 'reference_name')
  
  x12 = merge(x1, x2, 
              by.x = "ensembl_gene_id", 
              by.y = "Gene_id",
              all = TRUE)
  
  x12b = combine_duplicate_column(dfx = x12,
                                  col1 = 'gene_symbol',
                                  col2 = 'Gene_Name',
                                  column_to_combine_into = 'alias_symbol')
  
  x12b$Gene_Name = NULL
  
  x12b
}

#' Build a reference gene set at user specified FeatureSet id
#' 
#' Build a reference set of genes and synonym at a given featureset_id. Need paths to 
#' <hugo/hgnc_complete_set.txt> for annotation info (param: gene_annotation_file_path)
#' and <grch38_release85_homosap_gene/newGene.tsv> as gene location info 
#' (param: gene_location_file_path)
#' 
#' @param featureset_id FeatureSet id at which to build a reference gene set
#' @param gene_annotation_file_path file providing annotation info (e.g. <hugo/hgnc_complete_set.txt>)
#' @param gene_location_file_path gene location info (e.g. <grch38_release85_homosap_gene/newGene.tsv>)
#' 
#' @export 
build_reference_gene_set = function( featureset_id,
                                     gene_annotation_file_path = NULL, 
                                     gene_location_file_path = NULL, 
                                     alias_fields = c('hgnc_id', 'symbol', 'alias_symbol', 'prev_symbol',
                                                      'entrez_id', 'ensembl_gene_id', 'vega_id', 'ucsc_id', 
                                                      'ena', 'refseq_accession', 'ccds_id', 'uniprot_ids', 
                                                      'pubmed_id', 'mgd_id', 'rgd_id',
                                                      'cosmic', 'omim_id', 'mirbase',
                                                      'homeodb', 'snornabase', 'bioparadigms_slc',
                                                      'orphanet', 'pseudogene.org', 'horde_id',
                                                      'merops', 'imgt', 'iuphar',
                                                      'kznf_gene_catalog', 'mamit.trnadb', 'cd',
                                                      'lncrnadb', 'enzyme_id', 'intermediate_filament_db'),
                                     hierarchy = c('ensembl_gene_id', 'vega_id', 
                                                   'ucsc_id', 'ccds_id', 'gene_symbol', 'hgnc_id')
){
  if (is.null(gene_annotation_file_path)) {
    gene_annotation_file_path = system.file("extdata", 
                                            "gene__hugo__hgnc_complete_set.txt.gz", package="scidb4gh")
  }
  if (is.null(gene_location_file_path)) {
    gene_location_file_path = system.file("extdata", 
                                          "gene__grch38_release85_homosap_gene__newGene.tsv.gz", package="scidb4gh")
  }
  cat("Reading following files and merging into one dataframe. \nAnnotation: ", gene_annotation_file_path, 
      "\nGene location: ", gene_location_file_path, "\n")
  x12b = merge_gene_annotation_and_location(gene_annotation_file_path = gene_annotation_file_path, 
                                            gene_location_file_path = gene_location_file_path)
  
  # List name and source by hierarchy
  cat("Generating a gene name and source for each row\n")
  x12d = list_name_source_by_hierarchy(dfx = x12b, hierarchy = hierarchy)
  
  cat("Handling duplicate name and sources\n")
  if (nrow(x12d[duplicated(x12d$name), ]) > 0){
    x12e = handle_duplicate_name_source(dfx = x12d, hierarchy = hierarchy)
  } else {
    x12e = x12d
  }
  
  if (nrow(x12e[duplicated(x12e$name), ]) != 0) {
    stop("will need to edit handle_duplicate_name_source() and rerun till duplicates handled.
         Might need to drop rows that do not have any candidate for removing duplicates")
  }
  
  cat("Taking the non-alias fields and registering to get unique feature id-s\n")
  df1 = x12e[, which(!(colnames(x12e) %in% alias_fields))]
  strand_term = search_ontology(terms = "strand_term_unspecified")
  if (is.na(strand_term)) {
    cat("Registering ontology term\n")
    strand_term = register_ontology_term(df = data.frame(term = "strand_term_unspecified",
                                                                                source_name = "...",
                                                                                source_version = "...", 
                                                                                stringsAsFactors = FALSE))
  }
  df1$strand_term = strand_term
  df1$feature_type = "gene"
  df1$featureset_id = featureset_id

  # Register the features (without the alias fields)
  feature_id = register_feature(df = df1, 
                                register_gene_synonyms = FALSE) # Manually do gene synonym registration below
  
  # Now work on registering the alias values as synonyms
  cat("-------\nNow working on registering the alias values as synonyms\n")
  features_db = search_features(featureset_id = featureset_id)
  x12f = merge(x12e, features_db[, c('featureset_id', 'feature_id', 'name','source')],
               by = c('name', 'source'))
  
  alias_fields[2] = "gene_symbol"
  alias_field_col_pos = match(alias_fields, colnames(x12f))
  
  # Layout the alias fields as key-value pairs
  df2 = gather(data = x12f, key = "source_new", value = "synonym", 
               alias_field_col_pos)
  
  # Retain only the relevant columns
  df3 = df2[, c('name', 'source', 'feature_id', 'featureset_id', 
                'source_new', 'synonym')]
  
  # Weed out the rows with empty or null synonym values
  df4 = df3[!is.na(df3$synonym) & df3$synonym != "", ]
  stopifnot(nrow(df3) == nrow(df4) + 
              length(which(df3$synonym == "")) + length(which(is.na(df3$synonym))))
  
  # Check that all original names and sources are already added as synonym-s 
  stopifnot(all(
    paste(df4$name, df4$source, sep = "_") %in% 
      paste(df4$synonym, df4$source_new, sep = "_")))
  
  df5 = df4[, c('featureset_id', 'feature_id', 'source_new', 'synonym')]
  df5 = rename_column(x1 = df5, old_name = 'source_new', new_name = 'source')
  head(which(duplicated(paste(df5$synonym, df5$source, sep = "_"))))
  
  # Break up the pipes into synonym-s
  df6 = split_synonyms_by_pipe(dfx = df5, field = "synonym")
  stopifnot(nrow(df6) >= nrow(df5))
  
  cat("Registering", nrow(df6), "synonyms belonging to", length(alias_fields), 
      "alias_field-s mapping to", nrow(features_db), "features\n")
  ## Register with DB
  feature_synonym_id = register_feature_synonym(df = df6)
  l1 = list(feature_id, feature_synonym_id)
  names(l1) = c('feature_id', 'feature_synonym_id')
  l1
}