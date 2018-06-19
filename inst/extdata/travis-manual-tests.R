# to be run in Travis after `04-load-api.Rmd` has been run
library(scidb4gh)
gh_connect()
populate_measurements()
stopifnot(nrow(get_datasets()) == 1)
stopifnot(nrow(get_experimentset()) == 1)
stopifnot(nrow(get_experiments()) == 3)
stopifnot(nrow(get_measurements()) == 3)

# check mandatory fields flag
stopifnot(all(dim(get_datasets()) == c(1, 9)))
stopifnot(all(dim(get_datasets(mandatory_fields_only = T)) == c(1, 8)))
stopifnot(all(dim(get_individuals()) == c(3, 10)))
stopifnot(all(dim(get_individuals(mandatory_fields_only = T)) == c(3, 9)))
stopifnot(all(dim(get_biosamples()) == c(3, 10)))
stopifnot(all(dim(get_biosamples(mandatory_fields_only = T)) == c(3, 9)))

stopifnot(all(dim(get_measurements()) == c(3, 11)))
stopifnot(class(try({get_measurements(mandatory_fields_only=T)}, silent = T)) == 'try-error')
