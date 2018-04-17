cd /v/region/na/appl/banking/pbg_analytics_prql/data/prod_cde/PreAcxiom/InputTables/

/ms/dist/R/PROJ/core/3.2.3/bin/R


library("msversion")

addpkg('R6', '2.2.2')
addpkg('Rcpp', '0.12.14')
addpkg('assertthat', '0.2.0')
addpkg('bindrcpp', '0.2')
addpkg('bindr', '0.1')
addpkg('glue', '1.1.1')
addpkg('magrittr', '1.5')
addpkg('pkgconfig', '2.0.1')
addpkg('rlang', '0.1.2')
addpkg('tibble', '1.3.3')
library(R6)
library(Rcpp)
library(assertthat)
library(bindr)
library(bindrcpp)
library(glue)
library(magrittr)
library(pkgconfig)
library(rlang)
library(tibble)


addpkg('dplyr', '0.7.4')
library(dplyr)