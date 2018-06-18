# to be run in Travis after `04-load-api.Rmd` has been run

stopifnot(nrow(get_datasets()) == 1)
stopifnot(nrow(get_experiment()) == 0)
stopifnot(nrow(get_experiment()) == 0)
