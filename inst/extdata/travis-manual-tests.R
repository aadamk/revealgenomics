# to be run in Travis after `04-load-api.Rmd` has been run
library(scidb4gh)
gh_connect()
stopifnot(nrow(get_datasets()) == 1)
stopifnot(nrow(get_experimentset()) == 1)
stopifnot(nrow(get_experiment()) == 0)
