rm(list=ls())
library(scidb4gh)
source('~/coding/downloads/scidb4gh/R/delete.R')
gh_connect('root', 'Paradigm4')

# Get the current list of projects
p.list = get_projects()
print(p.list[, c(1:2)])

# Say we want to delete one of the projects that have the word "Emmett" in them
p.sublist = p.list[grep("Emmett", p.list$name, ignore.case = TRUE), ]

# Pick one (in case there are multiple)
p = p.sublist[1, ]
print(p)

# Actually, let's look at the last project, which should have versions of the databases associated.
p <- p.sublist[nrow(p.sublist),]
print(p)

# Search for datasets within that project
d.list = search_datasets(project_id = p$project_id)
print(d.list)
#   dataset_id dataset_version                name description project_id             created             updated flexfield1  flexfield_new
# 1        235               1 Emmett_test_dataset         ...        210 2017-06-27 18:55:47 2017-06-27 18:55:47  any-value           <NA>
# 2        235               2 Emmett_test_dataset   version 2        210 2017-06-27 18:57:31 2017-06-27 18:57:31       <NA> new value here

# Search for individuals within a dataset
# Pick a dataset (in case there are multiple)
d = d.list[1, ]
i = search_individuals(dataset_id = d$dataset_id, dataset_version = d$dataset_version)

print(i)

# Now let us delete two individuals from this list
try(delete_entity(entity = 'INDIVIDUAL', id = i$individual_id[1:2]))
# The above gives an error because dataset_version must be specified for versioned entities


delete_entity(entity = 'INDIVIDUAL', id = i$individual_id[1:2], dataset_version = 1)

# Now search for individuals in the same dataset
i2 = search_individuals(dataset_id = d$dataset_id)
print(head(i2))


## [EJS (7/27/17)]
## Now delete the entire project
delete_project(project_id = p$project_id)







