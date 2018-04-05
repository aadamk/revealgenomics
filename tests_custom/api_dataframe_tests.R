reference_object = list(biosample_ref = data.frame(name = c('a', 'b', 'c'),
                                                   bioample_id = 132:134,
                                                   stringsAsFactors = FALSE),
                        feature_ref = data.frame(name = c('EGFR', 'MYC', 'P53'),
                                                 feature_id = 2062:2064,
                                                 stringsAsFactors = FALSE),
                        measurementset = data.frame(name = c('Cufflinks Isoform'),
                                                    measurementset_id = 15,
                                                    measurement_entity = 'RNAQUANTIFICATION',
                                                    measurement_subtype = 'RNASeq',
                                                    stringsAsFactors = FALSE))

data_df_var = data.frame(biosample_name = c('c', 'b', 'b'),
                         gene = c('MYC', 'KRAS', 'RFFE'),
                         ref = c('A', 'A', 'C'),
                         alt = c('T', 'T', 'G'),
                         stringsAsFactors = FALSE)
data_df_rnaseq = data.frame(biosample_name = c('a', 'b'),
                            gene = c('EGFR', 'KRAS'),
                            value = c(1.003, 0.332),
                            stringsAsFactors = FALSE)


x1 = createDataLoader(measurement_data_tag = 'RNAQUANTIFICATION / RNASeq / Cufflinks_Isoform',
                      data_df = data_df_rnaseq,
                      reference_object = reference_object)
x2 = createDataLoader(measurement_data_tag = 'RNAQUANTIFICATION / RNASeq / Cufflinks_Gene',
                      data_df = data_df_rnaseq,
                      reference_object = reference_object)
x3 = createDataLoader(measurement_data_tag = 'VARIANT / SNV / GEMINI',
                      data_df = data_df_var,
                      reference_object = reference_object)
