#' GA4GH inspired and SciDB backed R API for genomic data 
#'
#' Available package options (you can override the defaults with the \code{options} function):
#'
#' \itemize{
#' \item \code{revealgenomics.dummy_option=7999}
#' }
#' Placeholder for documentation
#'
#' @name revealgenomics
#' @seealso \code{\link{get_individuals}}, \code{\link{search_individuals}},  
#' @docType package
NULL

.onAttach = function(libname, pkgname)
{
  packageStartupMessage("To get started see ?revealgenomics for a list of available functions. Each\nfunction has a detailed help page with examples.\nThe original PDF specification is available from vignette('revealgenomics')." , 
                        domain = NULL, appendLF = TRUE)
  options("revealgenomics.use_scidb_ee"=TRUE)
}

# A global environment used to store the metadata information, and cache state by some functions
#' @export
.ghEnv = new.env(parent = emptyenv())

.ghEnv$meta$L = yaml.load_file(system.file("data", "SCHEMA.yaml", package="revealgenomics"))

.ghEnv$meta$arrProject = 'PROJECT'
.ghEnv$meta$arrDataset = 'DATASET'
.ghEnv$meta$arrIndividuals = 'INDIVIDUAL'
.ghEnv$meta$arrOntology = 'ONTOLOGY'
.ghEnv$meta$arrDefinition = 'DEFINITION'
.ghEnv$meta$arrBiosample = 'BIOSAMPLE'
.ghEnv$meta$arrRnaquantification = 'RNAQUANTIFICATION'
.ghEnv$meta$arrFeature = 'FEATURE'
.ghEnv$meta$arrFeatureSynonym = 'FEATURE_SYNONYM'
.ghEnv$meta$arrFeatureset = 'FEATURESET'
.ghEnv$meta$arrReferenceset = 'REFERENCESET'
.ghEnv$meta$arrGeneSymbol = 'GENE_SYMBOL'
.ghEnv$meta$arrGenelist = 'GENELIST'
.ghEnv$meta$arrGenelist_gene = 'GENELIST_GENE'
.ghEnv$meta$arrVariant = 'VARIANT'
.ghEnv$meta$arrVariantKey = 'VARIANT_KEY'
.ghEnv$meta$arrFusion = 'FUSION'
.ghEnv$meta$arrProteomics = 'PROTEOMICS'
.ghEnv$meta$arrCopynumber_seg = 'COPYNUMBER_SEG'
.ghEnv$meta$arrCopynumber_mat = 'COPYNUMBER_MAT'
.ghEnv$meta$arrExperimentSet = 'EXPERIMENTSET'
.ghEnv$meta$arrMeasurement = 'MEASUREMENT'
.ghEnv$meta$arrMeasurementSet = 'MEASUREMENTSET'

# Prepare variables for the cache
.ghEnv$cache$lookup = list()
.ghEnv$cache$biosample_ref = NULL
.ghEnv$cache[[.ghEnv$meta$arrOntology]] = NULL
.ghEnv$cache[[.ghEnv$meta$arrVariantKey]] = NULL
.ghEnv$cache[[.ghEnv$meta$arrDefinition]] = NULL
.ghEnv$cache[[.ghEnv$meta$arrFeatureSynonym]] = NULL
.ghEnv$cache[[.ghEnv$meta$arrGeneSymbol]] = NULL
