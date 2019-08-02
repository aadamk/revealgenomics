rm(list = ls())

#### Load data
ip_date = '20190730'
load(paste('data/processed/test_processed_', ip_date, '.RData', sep = ''))

#### Box-plot

##-------------------------------------
## STATIC PLOT
dType = 'exp'                                                                                      ## 'exp', 'bio'
rtdat_sorted <- rtdat[order(unname(unlist(lapply(X = rtdat, FUN = function(k) {
  as.numeric(gsub(pattern = ' Mb', replacement = '', x = k[[paste(dType, '_size', sep = '')]]))
}))))]
bdat1 <- as.data.frame(do.call(cbind, lapply(rtdat_sorted, FUN = function(k) k[[paste(dType, '_dt', sep = '')]])),
                       stringsAsFactors = FALSE)
colnames(bdat1) <- gsub(pattern = '[.]master|[.]dt_enc_v2', replacement = '', x = colnames(bdat1))
colnames(bdat1) <- unname(unlist(lapply(X = strsplit(colnames(bdat1), split = '_'),
                                        FUN = function(k) paste(k[[1]], k[[length(k)]], sep = '_'))))
bplot_labels <- colnames(bdat1)
bplot_labels[1:(ncol(bdat1)/2)*2] <- ''
colnames(bdat1) <- NULL

boxplot(bdat1, col = rep(c('blue', 'green'), (ncol(bdat1)/2)),
        main = 'search_biosample() - run time', ylab = 'seconds', xlab = '', xaxt = 'n', xaxs = FALSE)
legend(x = 'bottomright', fill = c('blue', 'green'), bty = 'n', horiz = TRUE,
       legend = c('master', 'data.table_enhancements_v2'))
grid(col = 'black')
axis(side = 1, labels = bplot_labels, at = 1:ncol(bdat1), las = 2, cex.axis = 0.6)

##-------------------------------------
## INTERACTIVE PLOT
library('plotly')

dType = 'exp'                                                                                      ## 'exp', 'bio'
bdat1 <- lapply(rtdat, FUN = function(k) {
  ret <- k[[paste(dType, '_dt', sep = '')]]
  ret$size <- as.numeric(gsub(pattern = ' Mb', replacement = '', 
                              x = k[[paste(dType, '_size', sep = '')]]))
  return(ret)
})
bdat1 <- lapply(bdat1, FUN = function(k) {
  ret <- data.frame(branch = c(rep(colnames(k)[[1]], nrow(k)), rep(colnames(k)[[2]], nrow(k))),
                    time = c(k[[1]], k[[2]]),
                    size = k$size[[1]],
                    stringsAsFactors = FALSE)
  return(ret)
})
bdat1 <- as.data.frame(do.call(rbind, bdat1), stringsAsFactors = FALSE)
bdat1$study <- rownames(bdat1); rownames(bdat1) <- NULL
bdat1$study <- gsub(pattern = '[.][0-9]+', replacement = '', bdat1$study)
bdat1$study <- unname(unlist(lapply(X = strsplit(bdat1$study, split = '_'),
                                    FUN = function(k) paste(k[[1]], k[[length(k)]], sep = '_'))))
bdat1 <- bdat1[which(!is.na(bdat1$time)),]
bdat1 <- bdat1[order(bdat1$size, bdat1$study),]
bdat1$branch[which(bdat1$branch %in% 'dt_enc_v2')] <- 'dt_enhancements_v2'
bdat1$branch <- as.factor(bdat1$branch)
bdat1$study <- factor(bdat1$study, 
                      levels = unique(bdat1$study)[order(bdat1$size, decreasing = FALSE)])
bdat1$time <- as.numeric(bdat1$time)

p <- plot_ly(bdat1, x = ~study, y = ~time, color = ~branch, type = 'box',
             text = ~paste('Size (Mb): ', size)) %>%
  layout(xaxis = list(title = 'Study (dataset name_measurement set ID)', showgrid = TRUE, side = 'bottom'),
         yaxis = list(title = 'Time (seconds)', showgrid = TRUE))#,
#         legend = list(x = 0.01, y = 120, orientation = 'h'))
p