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

#From https://gist.github.com/armish/564a65ab874a770e2c26
memoSort <- function(M) {
  geneOrder <- sort(rowSums(M), decreasing=TRUE, index.return=TRUE)$ix;
  scoreCol <- function(x) {
    score <- 0;
    for(i in 1:length(x)) {
      if(x[i]) {
        score <- score + 2^(length(x)-i);
      }
    }
    return(score);
  }
  scores <- apply(M[geneOrder, ], 2, scoreCol);
  sampleOrder <- sort(scores, decreasing=TRUE, index.return=TRUE)$ix;
  return(M[geneOrder, sampleOrder]);
}


#From https://gist.github.com/armish/564a65ab874a770e2c26
oncoPrintSimple <- function(M, sort=TRUE) {
  if(sort) {
    alts <- memoSort(M);		
  } else {
    alts <- M;
  }
  
  ngenes <- nrow(alts);
  nsamples <- ncol(alts);
  coverage <- sum(rowSums(alts) > 0);
  
  ### OncoPrint
  numOfOncos <- ngenes*nsamples;
  oncoCords <- matrix( rep(0, numOfOncos * 5), nrow=numOfOncos );
  colnames(oncoCords) <- c("xleft", "ybottom", "xright", "ytop", "altered");
  
  xpadding <- .01;
  ypadding <- .01;
  cnt <- 1;
  for(i in 1:ngenes) {
    for(j in 1:nsamples) {
      xleft <- j-1 + xpadding;
      ybottom <- ((ngenes-i+1) -1) + ypadding;
      xright <- j - xpadding;
      ytop <- (ngenes-i+1) -ypadding;
      altered <- alts[i, j];
      
      oncoCords[cnt, ] <- c(xleft, ybottom, xright, ytop, altered);
      cnt <- cnt+1;
    }
  }
  
  colors <- rep("lightgray", cnt);
  colors[ which(oncoCords[, "altered"] == 1) ] <- "black";
  plot(c(0, nsamples), c(0, ngenes), type="n", main=sprintf("Gene set altered in %.2f%%: %d of %d cases", coverage/nsamples*100, coverage, nsamples), xlab="Samples", ylab="", yaxt="n");
  rect(oncoCords[, "xleft"], oncoCords[, "ybottom"],oncoCords[, "xright"], oncoCords[, "ytop"], col=colors, border="white");
  axis(2, at=(ngenes:1)-.5, labels=rownames(alts), las=2);
}



#' Variant access object 
#' 
#' @export
dnaVariantsObject <- R6::R6Class("dnaVariantsObject",
                             public = list(
                               
                               variantData=NULL,
                               featureData=NULL,
                               sampleData=NULL,
                               
                               initialize = function(variantData, featureData,sampleData) {
                                 self$variantData <- variantData
                                 self$featureData <- featureData
                                 self$sampleData<-sampleData
                               },
                               
                               # Object Getters/Setters
                               getVariantData = function() {
                                 self$variantData
                               },
                               setVariantData = function(variantData) {
                                 self$variantData <- variantData
                               },
                               
                               getFeatureData = function() {
                                 self$featureData
                               },
                               setFeatureData = function(featureData) {
                                 self$featureData <- featureData
                               },
                               getSampleData = function() {
                                 self$sampleData
                               },
                               setSampleData = function(sampleData) {
                                 self$sampleData <- sampleData
                               },
                               #########################
                               # Object Methods
                               getGeneCount = function() {
                                 length(unique(self$variantData$entrez_gene_id))	
                               },
                               getRowCount = function() {
                                 nrow(self$variantData)	
                               },
                               #return a data frame of all variants merged with gene symbol and description
                               getTable = function() {
                                 mydat=self$variantData
                                 igenes=as.data.table(self$featureData) #a data frame of gene annotations
                                 mydat=as.data.table(mydat)
                                 mydat$entrez_gene_id=as.character(mydat$entrez_gene_id)
                                 igenes$value=as.character(igenes$value)
                                 mydat$entrez_gene_id=as.character(mydat$entrez_gene_id)
                                 mydat=merge(mydat,igenes,by.x="entrez_gene_id",by.y="value",all.x=TRUE)
                                 # mydat[,by="sample_name",attribute_sampleLoad := length(classification),]
                                 mydat 
                               },
                               #returns vector of unique sample names
                               getSamples=function(){
                                 unique(self$sampleData$sample_name)	
                               },
                               getSampleIds=function(){
                                 unique(self$sampleData$sample_id)	
                               },
                               getGenes=function(){
                                 unique(self$variantData$entrez_gene_id)
                               },
                               geneFrequency=function(){
                                 df=self$getTable()
                                 sampleinfo=self$sampleData
                                 df=as.data.table(df)
                                 classifier=c("label","name","entrez_gene_id")
                                 subsetTotal=length(sampleinfo$sample_id)
                                 stotal=length(sampleinfo$sample_id)
                                 df[,by=classifier,list(
                                   Variants=length(variant_aa),
                                   No_unique_variants=length(unique(variant_aa)),
                                   No_patients_with_variant=length(unique(sample_name)),
                                   No_patients=stotal,
                                   Frequency=as.numeric(sprintf("%.3f",length(unique(sample_name))*100/stotal)),
                                   Mean_variant_coverage=as.double(sprintf("%.2f",mean(as.numeric(coverage)))),
                                   Mean_subject_allele_frequency=as.double(sprintf("%.2f",mean(as.numeric(allele_fraction))))
                                 )  
                                 ][order(No_unique_variants)]		
                                 
                               },
                               #Data matrix of mutations
                               mutationMatrix=function(customgenes=NULL){
                                 df=self$getTable()
                                 df$label=factor(df$label)
                                 df$sample_name=factor(df$sample_name)
                                 mat=as.matrix(dcast.data.table(df,label~sample_name,value.var="classification",drop=F,fun.aggregate=function(x) paste(unique(x),collapse=";")   ))
                                 mat[is.na(mat)] = ""
                                 rownames(mat) = mat[, 1]
                                 mat = mat[, -1]
                                 #subset by genes 
                                 if (!is.null(customgenes)){
                                   mat[customgenes,] 
                                 }else {
                                   mat
                                 }
                                 
                               },
                               sortedAlterationMatrix=function(doSort=TRUE,customgenes=NULL){
                                 mydf=self$getTable() 
                                 mydf$label=as.factor(mydf$label)
                                 mydf$sample_name=as.factor(mydf$sample_name)
                                 #Create sorted matrix first 
                                 
                                 #Below is where we define a score for each mutation class
                                 mydf$mutscore=0
                                 mydf[grep("frameshift_variant|nonsense|stop_gained",classification),mutscore:=11]
                                 mydf[grep("missense",classification),mutscore:=6]
                                 mydf[grep("silent",classification),mutscore:=0]
                                 mydf[grep("UTR",classification),mutscore:=0]
                                 mydf[grep("intron",classification),mutscore:=0]
                                 mydf[grep("stop_retained",classification),mutscore:=1]
                                 mydf[grep("splice",classification),mutscore:=1]
                                 
                                 dc=dcast.data.table(mydf,label~sample_name,value.var="mutscore",fun.aggregate=length,drop=F)
                                 dc=as.data.frame(dc)
                                 #Subset matrix to our shortlist of genes
                                 if (!is.null(customgenes))
                                   dc=subset(dc,label %in% customgenes) 
                                 lab=dc$label
                                 dc$label=NULL
                                 mat=data.matrix(dc)
                                 rownames(mat)=lab
                                 mat[is.na(mat)] = 0
                                 
                                 if (doSort==TRUE) {
                                   #Sort the matrix
                                   memoSort(mat)
                                 }else {
                                   mat 
                                 }
                               },
                               #Summary of mutation types per sample 
                               sampleMutationLoad=function(){
                                 data=self$getTable()
                                 data=as.data.table(data)
                                 classifier=c("sample_name","sample_id")
                                 old=		data[,by="sample_name",list(altered_genes=length(unique(entrez_gene_id)))]
                                 data$ref_alt=paste0(data$reference_allele,">",data$variant_allele)
                                 data$vclass="unknown"
                                 data$gene=data$label
                                 data[ grepl("^C>T$|^T>C$|^A>G$|^G>A$",ref_alt), vclass:="transition"]
                                 data[ grepl("^A>T$|^T>A$|^C>G$|^G>C$|^A>C$|^C>A$|^G>T$|^T>G$",ref_alt), vclass:="transversion"]
                                 #unique sample-variant combinations
                                 data=unique(data,by=c("sample_name","variant_aa"))
                                 mTotal=length(data$sample_name) #Total number of all mutations
                                 
                                 
                                 sload=data[,by=classifier,list(
                                   AllVariants=length(variant_aa),
                                   UniqueVariants=length(unique(variant_aa)),
                                   Somatic_Mutations= length(grep("other|^syn|silent",invert=TRUE,classification,value=T,ignore.case=T)),
                                   Missense_Mutations= length(grep("missense",classification,value=T,ignore.case=T)),
                                   Synonymous_Mutations= length(grep("^syn|silent",classification,value=T,ignore.case=T)),
                                   Number_of_Samples=length(unique(sample_name)),
                                   Frame_Affecting_Indels= length(grep("frame|shift|deletion",classification,value=T,ignore.case=T)),
                                   Transitions= length(grep("transition",vclass,value=T,ignore.case=T)),
                                   Transversions= length(grep("transver",vclass,value=T,ignore.case=T)),
                                   Number_of_genes=length(unique(gene))
                                 )]
                                 sload$sampleLoad=sload$Somatic_Mutations/sload$AllVariants
                                 sload$percentGenes=100*sload$Number_of_genes/length(unique(data$label))
                                 sload$impactScore=10*sload$Frame_Affecting_Indels+2*sload$Missense_Mutations
                                 
                                 return(sload) 
                                 
                               },
                               #Simple count of mutation classes in this set
                               mutationClasses=function(){
                                 data=self$getTable()
                                 data=as.data.table(data)
                                 table(data$classification)
                               }
                               
                             )
)

#get some test data from informe db
getDNAObject<-function(){
  #Connect to informe 
  dao<-DAO$new() # create a global DAO object
  allgenes<-dao$getGenes("")
  samplesdf=dao$getTotalVariantSamplesWithAtributes(88)
  variantdf= dao$get_informe_SNVs(studyid=88,genes="all")
  # create object
  dnaobject<-dnaVariantsObject$new(
    variantData=variantdf,
    featureData=allgenes,
    sampleData=samplesdf
  )
  return(dnaobject)
  #saveRDS(dnaobject,"dnavariantsobject_example.rds")
}



dvTestRun<-function(){
  ######### TESTING THE OBJECT SECTION ###################
  #dao<-DAO$new()
  #allgenes<-dao$getGenes("")
  #samplesdf=dao$getTotalVariantSamplesWithAtributes(88)
  #variantdf= dao$get_informe_SNVs(studyid=88,genes="all")
  #saveRDS(samplesdf,"samplesdf.rds")
  #saveRDS(allgenes,"testdata/allgenes.rds")
  
  #Some example data in the testdata/ folder
  samplesdf=readRDS("testdata/samplesdf.rds")
  variantdf=readRDS("testdata/testvariantdf.rds")
  allgenes=readRDS("testdata/allgenes.rds")
  
  # create object
  dv<-dnaVariantsObject$new(
    variantData=variantdf,
    featureData=allgenes,
    sampleData=samplesdf
  ) 
  
  #print(head(dv$getFeatureData()))
  print(dv$getSampleIds())
  print("-Sample Count-")
  print(length(dv$getSampleIds()))
  
  #print(head(dv$getTable()))
  #print(dv$getGeneCount())
  print("-Gene Freq-")
  print(dv$geneFrequency())
  #print("-Samples-")
  #print(dv$sampleData)
  print(dv$sampleMutationLoad())
  print("Summary of mutation classes")
  print(dv$mutationClasses())
}



#tu
######### END OF TESTS ######