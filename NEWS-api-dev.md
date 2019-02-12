(External facing changes are listed here)

## Version 0.5

- start using `options(scidb.aio=TRUE)` across the board

## Version 0.5

- rename scidb connection function from `gh_connect` to `rg_connect`
- start using `options(scidb.aio=TRUE)` for downloads where faster `aio_save` path of not having to `apply` dimensions can be used
- revamped fusion data implementation leads to modifications in `search_fusion` function
- for more details, see [https://github.com/Paradigm4/revealgenomics/issues/49](https://github.com/Paradigm4/revealgenomics/issues/49)


## Version 0.4

- package name changed from `scidb4gh` to `revealgenomics`
- Github repo changed from `reveal-genomics` to `revealgenomics`
- for more details, see [https://github.com/Paradigm4/revealgenomics/issues/44](https://github.com/Paradigm4/revealgenomics/issues/44)

## Version 0.3

- improved ontology management
  + Following terms dropped: REFERENCESET.species_, FEATURE.strand_term, BIOSAMPLE.disease_, INDIVIDUAL.sex_, INDIVIDUAL.species_

## At commit 352afe9

Recent changes:

1. The following external-facing API formulates a table containing information about
entities in the hierarchy

```R
get_entity_info()
#                  entity               class     search_by_entity     delete_by_entity
# 1               PROJECT            metadata                 <NA>              PROJECT
# 2               DATASET            metadata              PROJECT              DATASET
# ...
# 7     RNAQUANTIFICATION                data       MEASUREMENTSET       MEASUREMENTSET
```

2. Delete of measurement data is now working as 

```R
delete_entity(entity = 'RNAQUANTIFICATION', 
            ids = 2, dataset_version = 1, 
            delete_by_entity = 'MEASUREMENTSET')
# RnaQuantification data can be deleted by the parent MeasurementSet

delete_entity(entity = 'VARIANT', 
            ids = 1, dataset_version = 2, 
            delete_by_entity = 'MEASUREMENTSET')
# Variant data can be deleted by the parent MeasurementSet

# The earlier API for metadata and feature-data continues to work
delete_entity(entity = 'DATASET', id = 1, dataset_version = 2)
delete_entity(entity = 'ONTOLOGY', id = 1)
```

3. Change in `delete_entity()` and `get_entity()` API: 
parameter `ids` is now called `id`
(to be consistent with other calls in the API)

4. Two internal API to infer hierarchy and delete preferences
```R
## parent entity by which one would search an entity 
revealgenomics:::get_search_by_entity(entity = 'RNAQUANTIFICATION')
# [1] "MEASUREMENTSET"
revealgenomics:::get_search_by_entity(entity = 'MEASUREMENTSET')
# [1] "DATASET"
```
    
```R
## entity by which one would delete an entity 
revealgenomics:::get_delete_by_entity(entity = 'RNAQUANTIFICATION')
# [1] "MEASUREMENTSET"
revealgenomics:::get_delete_by_entity(entity = 'MEASUREMENTSET')
# [1] "MEASUREMENTSET"
```
5. `search_copynumberset(dataset_id = ..)` was throwing an error earlier when there 
were no CopyNumberSets in a dataset. This should be fixed now.
