#!/usr/bin/env Rscript

# INFO -------------------------------------------------------------------------
# Autor: Alceu Eilert Nascimento (alceuenascimento@gmail.com)
# Instituição: Observatório do Poder Judiciário (OAB Paraná)
# Data: 2026-01-10
# Objetivo: pegar numero dos processos para lançar no MNI
# Requisitos: os parquet de ingestão terem sido criados pelo `datajud_parse.R`
# Execução: `Rscript bin/R`
# Notas: 
#   - é necessário um Rproj para o projeto 
#   - execute este script a partir do diretório raiz do projeto
#   - os parquet devem estar em `data/parsed/` e seguir a estrutura de nomes:
#      - parquet com os dados básicos dos processos: processos.parquet
#      - parquet com os movimentos dos processos: movimentos.parquet
#      - parquet com os assuntos dos processos: assuntos.parquet

## LOG AND ERROR CONTROL -------------------------------------------------------
start_time <- Sys.time()

ROOT <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
pacman::p_load(
  duckdb,
  tidyverse,
  dplyr,
  httr,
  purrr,
  jsonlite,
  lubridate
)

## funcoes ----


# CONEXÃO E CONSULTA -----------------------------------------------------
con <- dbConnect(duckdb())

df_processos <- dbGetQuery(con,"
    SELECT 
    *
    FROM read_parquet('data/parsed/processos.parquet')
")


dbDisconnect(con, shutdown = TRUE)

# EXTRAIR NUMERO DOS PROCESSOS -----------------------------------------

glimpse(df_processos)

numero_processo <- df_processos |> 
  select(processo)

glimpse(processos)  


# Contagem de duplicatas
numero_processo |> 
  count(processo) |> 
  filter(n > 1) |> 
  arrange(desc(n))
# Sumário rápido
cat("Total registros:", nrow(processos), "\n")
cat("Processos únicos:", n_distinct(processos$processo), "\n")
cat("Duplicatas:", nrow(processos) - n_distinct(processos$processo), "\n")


# removendo duplicados
numero_processo <- df_processos |> 
  select(processo) |> 
  distinct(processo)

glimpse(processos)  

# exportando em csv  
write_csv(numero_processo, "data/processos.csv")

#eof----------------------------------------------------------------------------