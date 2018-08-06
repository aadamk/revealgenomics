rm(list=ls())
library(revealgenomics)
try({
  # Try to connect without security
  cat("Trying to connect without authentication")
  gh_connect()
  stopifnot(options('revealgenomics.use_scidb_ee') == FALSE)
})
if (options('revealgenomics.use_scidb_ee')) {
  source('~/.ga4gh_config.R')
  con1 = gh_connect2('root', password = rootpassword)
  con2 = gh_connect2('secure_user', password = secure_password)
  con3 = gh_connect2('public_user', password = public_password)
  
  grant_initial_access(con = con1, user_name = 'secure_user')
  grant_initial_access(con = con1, user_name = 'public_user')
  
  t1 = proc.time(); zz1 = get_datasets(con = con1); zz1[, c('dataset_id', 'name')]; proc.time()-t1
  t1 = proc.time(); zz2 = get_datasets(con = con2); zz2[, c('dataset_id', 'name')]; proc.time()-t1
  t1 = proc.time(); zz3 = get_datasets(con = con3); zz3[, c('dataset_id', 'name')]; proc.time()-t1
  
  t1 = proc.time(); zz1 = get_individuals(con = con1); dim(zz1); proc.time()-t1
  t1 = proc.time(); zz2 = get_individuals(con = con2); dim(zz2); proc.time()-t1
  t1 = proc.time(); zz3 = get_individuals(con = con3); dim(zz3); proc.time()-t1
  
  t1 = proc.time(); zz1 = get_biosamples(con = con1); dim(zz1); proc.time()-t1
  t1 = proc.time(); zz2 = get_biosamples(con = con2); dim(zz2); proc.time()-t1
  t1 = proc.time(); zz3 = get_biosamples(con = con3); dim(zz3); proc.time()-t1
  
  identical(get_referenceset(con = con1), 
            get_referenceset(con = con2))
  identical(get_referenceset(con = con1), 
            get_referenceset(con = con3))
  
  ff1 = get_features(con = con1)
  ff2 = get_features(con = con2)
  identical(ff1, ff2)
  
  identical(get_featuresets(con = con1), 
            get_featuresets(con = con2))
  
  
  fsyn1 = revealgenomics:::get_feature_synonym(con = con1)
  fsyn2 = revealgenomics:::get_feature_synonym(con = con2)
  identical(fsyn1, fsyn2)
  
  r2 = get_measurementsets(con = con2)
  r3 = get_measurementsets(con = con3)
  search_measurementsets(measurementset = r2[8, ], 
                         measurement_type = .ghEnv$meta$arrRnaquantification,
                         con = con2)
  
}
