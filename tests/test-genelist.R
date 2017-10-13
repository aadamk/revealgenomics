rm(list=ls())
source('~/.ga4gh_config.R')

library(scidb4gh)

# Register genelists
gh_connect(username = "secure_user", password = secure_password)
id1 = register_genelist(genelist_name = "secure_user list 1", 
                  genelist_description = "secure_user list 1 (private)", 
                  isPublic = FALSE)
id1

gh_connect(username = "public_user", password = public_password)
id2 = register_genelist(genelist_name = "test2", 
                  genelist_description = "test2", 
                  isPublic = TRUE)
id2

gh_connect(username = "secure_user", password = secure_password)
id3 = register_genelist(genelist_name = "secure_user list 2", 
                        genelist_description = "secure_user list 1 (public)", 
                        isPublic = TRUE)
id3

# Retrieve genelists
gh_connect(username = "secure_user", password = secure_password)
stopifnot(nrow(get_genelist()) >= 3)

gh_connect(username = "public_user", password = public_password)
stopifnot(nrow(get_genelist()) >= 2)
stopifnot(nrow(get_genelist(id1)) == 0)

# Register gene symbols in a genelist
gh_connect(username = "secure_user", password = secure_password)
id_s1 = register_genelist_gene(genelist_id = id1, # must exist in `genelist` table
            gene_symbols = c('TSPAN6', 'KCNIP2'))

gh_connect(username = "public_user", password = public_password)
id_s2 = register_genelist_gene(genelist_id = id2, # must exist in `genelist` table
                       gene_symbols = c('EGFR', 'KRAS', 'CFAP58', 'GOT1', 'CPN1', 'PSIP1P1'))

gh_connect(username = "secure_user", password = secure_password)
id_s3 = register_genelist_gene(genelist_id = id3, # must exist in `genelist` table
                       gene_symbols = c('MYC', 'A1BG'))

# Retrieve gene symbols from gene list
gh_connect(username = "secure_user", password = secure_password)
sym1 = search_genelist_gene(genelist = get_genelist(genelist_id = id1))
stopifnot(nrow(sym1) == 2)

sym1b = search_genelist_gene(genelist_id = id1)
stopifnot(identical(sym1, sym1b))

gh_connect(username = "public_user", password = public_password)
stopifnot(class(try({search_genelist_gene(genelist = get_genelist(genelist_id = id1))}, 
                    silent = TRUE)) == 'try-error')

stopifnot(class(try({search_genelist_gene(genelist_id = id1)}, 
                    silent = TRUE)) == 'try-error')
