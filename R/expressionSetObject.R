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

#' Variant access object 
#' 
#' @export
expressionSetObject <- R6::R6Class("expressionSetObject",
                               public = list(
                                 
                                 phenotypeData=NULL,
                                 featureData=NULL,
                                 expressionMatrix=NULL,
                                 
                                 initialize = function(phenotypeData, featureData, expressionMatrix) {
                                   self$phenotypeData <- phenotypeData
                                   self$featureData <- featureData
                                   self$expressionMatrix <- expressionMatrix
                                 },
                                 
                                 ########################
                                 # general DB functions #
                                 ########################
                                 
                                 # Object Getters/Setters
                                 getPhenotypeData = function() {
                                   self$phenotypeData
                                 },
                                 setPhenotypeData = function(phenotypeData) {
                                   self$phenotypeData <- phenotypeData
                                 },
                                 
                                 getFeatureData = function() {
                                   self$featureData
                                 },
                                 setFeatureData = function(featureData) {
                                   self$featureData <- featureData
                                 },
                                 
                                 getExpressionMatrix = function() {
                                   self$expressionMatrix
                                 },
                                 setExpressionMatrix = function(expressionMatrix) {
                                   self$expressionMatrix <- expressionMatrix
                                 },
                                 
                                 # Object Methods
                                 getGeneCount = function() {
                                   nrow(self$featureData)	
                                 },
                                 
                                 getLongFormat = function() {
                                   dat.long<-reshape::melt(t(self$expressionMatrix))
                                   colnames(dat.long)<-c("sample.id","feature","value")
                                   
                                   # dat.long<-merge(self$featureData, dat.long, by="hugo_gene_symbol")
                                   # dat.long<-merge(self$phenotypeData, dat.long, by="sampleID")
                                   
                                   dat.long<-merge(data.frame(feature=rownames(self$featureData),self$featureData,stringsAsFactors=F),dat.long,by="feature")
                                   dat.long<-merge(data.frame(sample.id=rownames(self$phenotypeData),self$phenotypeData,stringsAsFactors=F),dat.long,by="sample.id")
                                   
                                   dat.long
                                 }, 
                                 
                                 # 
                                 # getDataPivotedByGene = function(genes=NULL) {
                                 #   dat.long<-reshape::melt(t(self$expressionMatrix))
                                 #   colnames(dat.long)<-c("sampleID","hugo_gene_symbol","value")
                                 #   
                                 #   dat.long<-merge(self$featureData, dat.long, by="hugo_gene_symbol")
                                 #   dat.long<-merge(self$phenotypeData, dat.long, by="sampleID")
                                 #   # dat.long<-merge(data.frame(feature=rownames(self$featureData),self$featureData,stringsAsFactors=F),dat.long,by="feature")
                                 #   # dat.long<-merge(data.frame(sample.id=rownames(self$phenotypeData),self$phenotypeData,stringsAsFactors=F),dat.long,by="sample.id")
                                 #   dat.long[dat.long==""] <- NA
                                 #   
                                 #   if(is.null(genes)) {
                                 #     d2<-dat.long
                                 #   } else {
                                 #     d2<-subset(dat.long, hugo_gene_symbol %in% genes)
                                 #   }
                                 #   # d3<-d2[,-which(colnames(d2) %in% c("feature","genename"))]
                                 #   df<-subset(d2, select = -c(geneid))
                                 #   d4<-dcast(df, sampleID + ... ~ hugo_gene_symbol, value.var = "value")
                                 # },
                                   
                                 getDataPivotedByGene = function(gene.symbol.variable="hugo_gene_symbol") {
                                   dat<-t(self$expressionMatrix)
                                   colnames(dat)<-self$featureData[,gene.symbol.variable]
                                   exprs.data<-data.frame(sample.id=rownames(dat),dat,stringsAsFactors=F)
                                   pheno<-data.frame(sample.id=rownames(self$phenotypeData),self$phenotypeData,stringsAsFactors=F)
                                   merge(pheno,exprs.data,by="sample.id")
                                 },
                                 
                                 # returns the names of the columns from the phenotype data that are 
                                 # of class numeric or integer
                                 getPhenotypeDataNumericVariables = function() {
                                   v<-colnames(self$getPhenotypeData())
                                   isColNumeric<-sapply(self$getPhenotypeData(),class, simplify=FALSE) %in% c("numeric","integer")
                                   
                                   idx<-which(isColNumeric)
                                   
                                   if(length(idx)>0) {
                                     return(sort(v[idx]))
                                   }
                                   return(NULL)
                                 },
                                 
                                 # returns the names of the columns from the phenotype data that are 
                                 # of class character or factor
                                 getPhenotypeDataCategoricalVariables = function() {
                                   v<-colnames(self$getPhenotypeData())
                                   isColCategorical<-sapply(self$getPhenotypeData(),class, simplify=FALSE) %in% c("character","factor")
                                   
                                   idx<-which(isColCategorical)
                                   
                                   if(length(idx)>0) {
                                     return(sort(v[idx]))
                                   }
                                   return(NULL)
                                 },
                                 
                                # returns the names of the columns from the phenotype data that are 
                                # of class numeric or integer, and only those whose entries are not unique
                               	getPhenotypeDataNumericNotUniqueVariables = function() {
                               	  v<-colnames(self$getPhenotypeData())
                               	  isColNumeric<-sapply(self$getPhenotypeData(),class, simplify=FALSE) %in% c("numeric","integer")
                               	  isColNotUniqueVals<-unlist(sapply(self$getPhenotypeData(),function(x){ length(unique(x)) != length(x) }, simplify=FALSE))
                               	      
                               	  idx<-which(isColNumeric & isColNotUniqueVals)
                            
                               	  if(length(idx)>0) {
                               	    return(sort(v[idx]))
                               	  }
                               	  return(NULL)
                               	},
                             
                             
                                # returns the names of the columns from the phenotype data that are 
                                # of class character of factor, and only those whose entries are not unique
                                getPhenotypeDataCategoricalNotUniqueVariables = function() {
                                  v<-colnames(self$getPhenotypeData())
                                  isColCategorical<-sapply(self$getPhenotypeData(),class, simplify=FALSE) %in% c("character","factor")
                                  isColNotUniqueVals<-unlist(sapply(self$getPhenotypeData(),function(x){ length(unique(x)) != length(x) }, simplify=FALSE))
                                  
                                  idx<-which(isColCategorical & isColNotUniqueVals)
                                  
                                  if(length(idx)>0) {
                                    return(sort(v[idx]))
                                  }
                                  return(NULL)
                                }
                            )
)
