#' JNJ SciDB Toolbox for R
#'
#' Available package options (you can override the defaults with the \code{options} function):
#'
#' \itemize{
#' \item \code{jnjscidb.dummy_option=7999}
#' }
#' Placeholder for documentation
#'
#' @name jnjscidb
#' @seealso \code{\link{get_individuals}}, \code{\link{search_individuals}},  
#' @docType package
NULL

.onAttach = function(libname, pkgname)
{
  packageStartupMessage("To get started see ?jnjscidb for a list of available functions. Each\nfunction has a detailed help page with examples.\nThe original PDF specification is available from vignette('jnjscidb')." , 
                        domain = NULL, appendLF = TRUE)
  options("jnjscidb.dummy_option"=7999)
}

# A global environment used to store the metadata information, and cache state by some functions
jdb = new.env()

# Set up a lazily-assigned cache for miscellaneous tables used by getSeries
#' delayedAssign("tickers_table", iquery("tickers", return=TRUE), assign.env=.pdqEnv)
#' delayedAssign("exchanges_table", iquery("exchanges", return=TRUE), assign.env=.pdqEnv)
#' delayedAssign("sources_table", iquery("sources", return=TRUE), assign.env=.pdqEnv)
#' #' Clear cached exchanges, sources, and tickers tables forcing
#' #' \code{getSeries()} to look them up again.
#' #' @export
#' clearLocalCache = function()
#' {
#'   rm(list=c("tickers_table", "exchanges_table", "sources_table"), envir=.pdqEnv)
#'   delayedAssign("tickers_table", iquery("tickers", return=TRUE), assign.env=.pdqEnv)
#'   delayedAssign("exchanges_table", iquery("exchanges", return=TRUE), assign.env=.pdqEnv)
#'   delayedAssign("sources_table", iquery("sources", return=TRUE), assign.env=.pdqEnv)
#' }