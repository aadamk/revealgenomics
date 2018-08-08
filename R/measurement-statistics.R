#
# BEGIN_COPYRIGHT
#
# PARADIGM4 INC.
# This file is part of the Paradigm4 Enterprise SciDB distribution kit
# and may only be used with a valid Paradigm4 contract and in accord
# with the terms and conditions specified by that contract.
#
# Copyright (C) 2011 - 2017 Paradigm4 Inc.
# All Rights Reserved.
#
# END_COPYRIGHT
#

#' Calculate statistics on Variant data
#' 
#' @export
calculate_statistics_variant = function(measurementset_id = NULL, con = NULL) {
  con = use_ghEnv_if_null(con = con)
  db = con$db
  if (!is.null(measurementset_id)) {
    res = iquery(db, 
                 paste0("aggregate(
                        filter(", 
                        full_arrayname(.ghEnv$meta$arrVariant), 
                        ", measurementset_id=", measurementset_id, 
                        "), count(*) AS variant_count, measurementset_id, biosample_id)"),
                 return = TRUE)
    m = get_measurementsets(measurementset_id = measurementset_id)
  } else { # for all measurementsets
    res = iquery(db, 
                 paste0("aggregate(",
                        full_arrayname(.ghEnv$meta$arrVariant), 
                        ", count(*) AS variant_count, measurementset_id, biosample_id)"),
                 return = TRUE)
    res = res[order(res$measurementset_id, res$biosample_id), ]
    res
    
    m = get_measurementsets(measurementset_id = sort(unique(res$measurementset_id)))
  }
  
  if (unique(m$entity) != 'VARIANT') {
    if (!is.null(measurementset_id)) stop("entity entries should have been of VARIANT type here")
  }
  m = plyr::rename(m, c('name' = 'pipeline'))
  
  d = get_datasets(dataset_id = unique(sort(m$dataset_id)))
  d = plyr::rename(d, c('name' = 'study'))
  
  md = merge(d[, c('dataset_id', 'dataset_version', 'study')],
             m[, c('dataset_id', 'dataset_version', 'measurementset_id', 'pipeline')],
             by = c('dataset_id', 'dataset_version'))
  
  # res = plyr::rename(res, c('count' = 'variant_count'))
  res = res %>% 
    group_by(measurementset_id) %>% 
    dplyr::summarise(mean_variants_per_sample = mean(variant_count),
                     num_samples = n())
  mdc = merge(md, res,
              by = 'measurementset_id')
  
  mdc
}

#' Calculate statistics on Gene-Expression data
#' 
#' @export
calculate_statistics_rnaquantification = function(measurementset_id = NULL, decorateResults = TRUE, con = NULL){
  con = use_ghEnv_if_null(con)
  idname = get_base_idname(.ghEnv$meta$arrMeasurementSet)
  if (is.null(measurementset_id)){
    c = iquery(con$db, 
               paste0("aggregate(", 
                      custom_scan(), "(", 
                      full_arrayname(.ghEnv$meta$arrRnaquantification), "),", 
                      "count(*), ", 
                      idname, 
                      ", dataset_version)"), 
               return = TRUE)
  } else { #specific measurementset_id is specified
    qq = paste0("filter(", 
                custom_scan(), "(", 
                full_arrayname(.ghEnv$meta$arrRnaquantification), "), ", 
                idname , "=", measurementset_id, ")")
    qq = paste0("aggregate(", qq, ", count(*), ", idname, ", dataset_version)")
    c = iquery(con$db, qq, return = T)
  }
  c = c[order(c[, idname], c$dataset_version), ]
  if (!decorateResults) {
    return(c)
  } else {
    ms = get_measurementsets(measurementset_id = unique(sort(c$measurementset_id)), 
                             all_versions =  TRUE,
                             con = con)
    ms = plyr::rename(ms, c('name' = 'pipeline'))
    
    d = get_datasets(dataset_id = unique(sort(c$dataset_id)),
                     all_versions =  TRUE,
                     con = con)
    d = plyr::rename(d, c('name' = 'study_name'))
    
    res = merge(ms[, c('measurementset_id', 'dataset_id', 'dataset_version', 'pipeline')], 
                c,
                by = c('measurementset_id', 'dataset_version'))
    res = merge(d[, c('dataset_id', 'dataset_version', 'study_name')],
                res, 
                by = c('dataset_id', 'dataset_version'))
    return(res[order(res$dataset_id, 
                     res$dataset_version,
                     res$measurementset_id), ])
  }
}

#' Calculate summary statistics across measurementsets
#' 
#' @examples calculate_statistics_across_measurementsets(
#'              df_measurementset = search_measurementsets(dataset_id = 31))
#' @examples calculate_statistics_across_measurementsets(
#'              df_measurementset = get_measurementsets(measurementset_id = c(2:5)))
#' 
#' @export
calculate_statistics_across_measurementsets = function(df_measurementset, con = NULL) {
  con = use_ghEnv_if_null(con = con)
  db = con$db
  idx = df_measurementset$measurementset_id
  zz = df_measurementset[, c('measurementset_id', 'name', 'entity')]
  
  
  L1 = lapply(unique(zz$entity),
              function(entitynm) {
                counts = iquery(db, paste0("aggregate(filter(", custom_scan(), "(", full_arrayname(entitynm), "), ", 
                                           formulate_base_selection_query(fullarrayname = .ghEnv$meta$arrMeasurementSet,
                                                                                     id = idx), "),
                                           count(*), measurementset_id)"), return = TRUE)
                if (nrow(counts) == 0) {
                  counts = data.frame(measurementset_id = idx,
                                      count = rep(NA, length(idx)))
                }
                merge(zz[zz$entity == entitynm, ], 
                      counts, by = 'measurementset_id', all.x = TRUE)
              }
  )
  # names(L1) = unique(zz$entity)
  
  dplyr::bind_rows(L1)
}

#' Aggregate project, study info by phenotype column(s)
#' 
#' Aggregate project, study info by phenotype column(s)
#' 
#' Aggregate project and study information by selected phenotype columns
#' For example, `disease` is phenotype related information that is retained under the
#' `BIOSAMPLE` entity. If we specify `filter_column = 'primary_disease'`, then 
#' the function aggregates counts by disease by study. Function also joins in 
#' project id and name
#' 
#' @param filter_column one or more filte rcolumns to aggregate by
#' 
#' @examples aggr_proj_study_by_pheno(filter_column = c('sample_cell_type', 'sample_molecule_type'))
#'           returns
#'           study_id study_version project_id  project_name          study_name     filter_column     filter_value      total
#'           528      237             1          1 MMRF CoMMpass MMRF CoMMpass IA13a primary_disease   multiple myeloma  2035
#'           529      238             1          1 MMRF CoMMpass MMRF CoMMpass IA14a primary_disease   multiple myeloma  2072
#' @export           
aggr_proj_study_by_pheno = function(
  filter_column, method = 1, con = NULL
) {
  con = revealgenomics:::use_ghEnv_if_null(con = con)
  query0 = paste0("key='", filter_column, "'", collapse = " OR ")
  if (method == 1) {
    query1 = paste0(
      "grouped_aggregate(
      filter(",
      revealgenomics:::full_arrayname(.ghEnv$meta$arrBiosample), "_INFO, ",
      query0, "), 
      min(key) AS filter_column, 
      count(*) AS total, 
      dataset_id, dataset_version, val)"
    )
    query2 = paste0(
      "equi_join(",
      query1, ", ", 
      "project(apply(", 
      revealgenomics:::full_arrayname(.ghEnv$meta$arrDataset), ", ",
      "study_name, name), study_name, project_id), ",
      "'left_names=dataset_id,dataset_version', 
      'right_names=dataset_id,dataset_version', 'keep_dimensions=0'",
      ")"
    )
    query3 = paste0(
      "equi_join(",
      query2, ", ", 
      "project(apply(", 
      revealgenomics:::full_arrayname(.ghEnv$meta$arrProject), ", ",
      "project_name, name), project_name), ",
      "'left_names=project_id', 
    'right_names=project_id', 
    'keep_dimensions=0'",
      ")"
    )
    
    res = iquery(con$db, 
                 query3, 
                 return = T, 
                 only_attributes=T)
    res = plyr::rename(res, 
                       c("dataset_id"      = "study_id",
                         "dataset_version" = "study_version", 
                         "val"             = "filter_value"))
    def = get_definitions(con = con)
    res2 = merge(
      res, 
      def[, c('dataset_id', 'attribute_name', 'controlled_vocabulary')], 
      by.x = c('study_id', 'filter_column'), 
      by.y = c('dataset_id', 'attribute_name'), 
      all.x = TRUE)
    
    controlled_idx = which(!is.na(res2$controlled_vocabulary))
    if (length(controlled_idx) > 0) {
      controlled_idx_ont_ids = res2[controlled_idx, 'filter_value']
      ont_df = get_ontology(ontology_id = unique(controlled_idx_ont_ids), con = con)
      res2[controlled_idx, 'filter_value'] = 
        ont_df[match(controlled_idx_ont_ids, ont_df$ontology_id), ]$term
    }
    # res2$controlled_vocabulary = NULL
    res2 = res2[, c('study_id', 'study_version', 'project_id', 'project_name', 'study_name',
                    'filter_column', 'filter_value', 'total')]
    
    return(res2)
  } else if (method == 2) {
    if ('sample_molecule_type' %in% filter_column) {
      stop("`sample_molecule_type` as a reserved column. 
         Do not use as filter_column")
    }
    query1 = paste0(
      "grouped_aggregate(
        filter(
          equi_join(
            apply(", revealgenomics:::full_arrayname(.ghEnv$meta$arrBiosample), 
              "_INFO, dataset_id, dataset_id, dataset_version, dataset_version), 
            project(
              apply(
                filter(", revealgenomics:::full_arrayname(.ghEnv$meta$arrBiosample),
                  "_INFO, key='sample_molecule_type'), 
                sample_molecule_type, val), 
              sample_molecule_type), 
          'left_names=biosample_id', 
          'right_names=biosample_id', 
          'left_outer=true'), ",
        query0, 
        "), 
      count(*) AS total, 
      dataset_id, dataset_version, key, val, sample_molecule_type)"
    )
    query2 = paste0(
      "equi_join(",
      query1, ", ", 
      "project(apply(", 
      revealgenomics:::full_arrayname(.ghEnv$meta$arrDataset), ", ",
      "study_name, name), study_name, project_id), ",
      "'left_names=dataset_id,dataset_version', 
      'right_names=dataset_id,dataset_version', 'keep_dimensions=0'",
      ")"
    )
    query3 = paste0(
      "equi_join(",
      query2, ", ", 
      "project(apply(", 
      revealgenomics:::full_arrayname(.ghEnv$meta$arrProject), ", ",
      "project_name, name), project_name), ",
      "'left_names=project_id', 
    'right_names=project_id', 
    'keep_dimensions=0'",
      ")"
    )
    res = iquery(con$db, 
           query = query3,
           return = T,
           only_attributes=T)
    
    res = plyr::rename(res, 
                       c("dataset_id"      = "study_id",
                         "dataset_version" = "study_version", 
                         "val"             = "filter_value",
                         "key"             = "filter_column"))
    def = get_definitions(con = con)
    res2 = merge(
      res, 
      def[, c('dataset_id', 'attribute_name', 'controlled_vocabulary')], 
      by.x = c('study_id', 'filter_column'), 
      by.y = c('dataset_id', 'attribute_name'), 
      all.x = TRUE)
    controlled_idx = which(!is.na(res2$controlled_vocabulary))
    if (length(controlled_idx) > 0) {
      controlled_idx_ont_ids = res2[controlled_idx, 'filter_value']
      ont_df = get_ontology(ontology_id = unique(controlled_idx_ont_ids), con = con)
      res2[controlled_idx, 'filter_value'] = 
        ont_df[match(controlled_idx_ont_ids, ont_df$ontology_id), ]$term
    }
    
    ont_df = get_ontology(ontology_id = unique(res2$sample_molecule_type), con = con)
    res2$sample_molecule_type = 
      ont_df[match(res2$sample_molecule_type, ont_df$ontology_id), ]$term
    res2$controlled_vocabulary = NULL
    head(res2)
    res2 = res2[, c('study_id', 'study_version', 'project_id', 'project_name', 'study_name',
                    'filter_column', 'filter_value', 'sample_molecule_type', 'total')]
    
    return(res2)
    
    if (FALSE) {
      # code to add `sample_molecule_type = NA` when sample not tagged`
      ont_df = get_ontology(con = con)
      ont_df[grep("sample_molecule_type", ont_df$category), c("ontology_id", "term", "category")]
      #    ontology_id         term             category
      # 66          66          RNA sample_molecule_type
      # 67          67         mRNA sample_molecule_type
      # 68          68          DNA sample_molecule_type
      # 69          69 blood_plasma sample_molecule_type
      # 70          70           BP sample_molecule_type
      # 71          71           NA sample_molecule_type      
      res0 = iquery(con$db, "limit(op_count(merge(redimension(project(apply(aggregate(gh_secure.BIOSAMPLE_INFO, count(*), dataset_version, dataset_id, biosample_id), key_id, int64(10000), sample_molecule_type, string(-10001)), key_id, sample_molecule_type),<sample_molecule_type:string> [dataset_version=0:*:0:100; dataset_id=0:*:0:10; biosample_id=0:*:0:100; key_id=0:*:0:242]),
       
       project(apply(filter(gh_secure.BIOSAMPLE_INFO, key='sample_molecule_type'), sample_molecule_type, val), sample_molecule_type))), 5)", return = T)
    }
  }
}

aggr_proj_study_by_pheno_v2 = function(
  filter_column, con = NULL
) {
}
