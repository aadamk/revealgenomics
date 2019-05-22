#' @export
plot_heatmap_for_expression_set = function(es, type = c('gene_expression',
                                                        'protein_expression')) {
  type = match.arg(type)
  mat = exprs(es)
  
  ftr = fData(es)
  if (length(unique(ftr$gene_symbol)) == 
      nrow(mat)) {
    rownames(mat) = ftr$gene_symbol
  }
  d = as.data.frame(mat)
  colnames(d) = gsub(pattern = "__RNA", replacement = "", x = colnames(d))
  colnames(d) = gsub(pattern = "__Protein", replacement = "", x = colnames(d))
  colnames(d) = gsub("hg19MTERCC-ensembl75-genes-", "", colnames(d))
  if (length(colnames(d)) > 10) {
    # Try reducing names for viz. only
    reduced_names = paste0(
      stringi::stri_sub(colnames(d), 1, 5),
      "...",
      stringi::stri_sub(colnames(d), -5, -1)
    )
    if (length(which(duplicated(reduced_names))) == 0) {
      colnames(d) = reduced_names
    }
  }
  d = cbind(
    data.frame(
      gene = rownames(mat),
      stringsAsFactors = FALSE
    ),
    d
  )
  # # Find common substr
  # for (i in 1:(length(colnames(d))-1)) {
  #   if (i == 1) {
  #     substr_i = LCS(strsplit(colnames(d)[1], '')[[1]], strsplit(colnames(d)[2], '')[[1]])$LCS
  #   } else {
  #     substr_i = LCS(substr_i, strsplit(colnames(d)[i+1], '')[[1]])$LCS
  #   }
  #   cat(i, ":", substr_i, "\n")
  # }
  # substr_final = paste0(substr_i, collapse = '')
  # colnames(d) = gsub(pattern = substr_final, 
  #                    replacement = "...", fixed = TRUE, 
  #                    x = colnames(d))
  rownames(d) = 1:nrow(d)
  dd <- d %>% tidyr::gather(sample, value, -gene)
  dd <- reshape2::melt(d, variable.name = "sample")
  pp = ggplot(dd, aes(x = sample, y = gene, fill = value)) + 
    geom_tile() 
  if (type == 'protein_expression') {
    pp 
  } else {
    pp +
      theme(axis.text.x = element_text(angle = 90, hjust = 1))
  }
}

