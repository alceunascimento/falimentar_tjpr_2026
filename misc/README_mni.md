# Pipeline MNI — TJPR Falimentar 2026

Documentação do pipeline de coleta e transformação de dados judiciais via **MNI** (Modelo Nacional de Interoperabilidade) do TJPR.

---

## Visão Geral

O MNI é o protocolo padronizado do CNJ para intercâmbio de dados processuais entre sistemas judiciais. O TJPR expõe um endpoint SOAP/XML que permite consultar processos individuais retornando metadados, movimentações e complementos em estrutura hierárquica. Este pipeline transforma esses dados brutos em artefatos analíticos no formato Parquet, prontos para análise com DuckDB, R ou Python.

```
MNI (SOAP/XML)
    │
    ▼
mni_01_getdata_batch.py       ← coleta em lote → JSON organizados
    │
    ▼
data_mni/processed/*.json
    │
    ├──▶ mni_02_to_parquet_processos.R   → processos_mni.parquet
    │
    ├──▶ mni_03_to_parquet_movimentos.R  → movimentos_mni.parquet
    │
    └──▶ mni_04_movimentos_parse_partes.R
              │
              ├──▶ movimentos_mni_partes.parquet   (todos os movimentos + campos extraídos)
              └──▶ partes_mni.parquet               (filtrado: apenas movimentos com partes identificadas)
```

---

## Estrutura de Diretórios

```
falimentar_tjpr_2026/
├── bin/
│   ├── mni_01_getdata_batch.py
│   ├── mni_02_to_parquet_processos.R
│   ├── mni_03_to_parquet_movimentos.R
│   └── mni_04_movimentos_parse_partes.R
├── data_mni/
│   ├── processed/           ← JSONs organizados por processo
│   ├── processos_mni.parquet
│   ├── movimentos_mni.parquet
│   ├── movimentos_mni_partes.parquet
│   └── partes_mni.parquet
└── logs/                    ← logs timestamped de cada execução
```

---

## Scripts

### `mni_01_getdata_batch.py` — Coleta em Lote

**Linguagem:** Python  
**Função:** Consulta o endpoint MNI do TJPR via SOAP para uma lista de números de processos, salva a resposta XML deserializada como JSON organizado em `data_mni/processed/`.

**Saída por processo:**
```
{numero_processo}_organized_{YYYYMMDD_HHMMSS}.json
```

**Estrutura do JSON:**
```json
{
  "metadados": {
    "numero_processo": "00000013119688160019",
    "data_consulta":   "20260225_003024",
    "data_formatada":  "25/02/2026 00:30:24",
    "arquivo_origem":  "..._raw_....json"
  },
  "resumo":   { ... },
  "processo": { "dados_basicos": { ... }, "movimentos": [ ... ] },
  "resposta": { ... }
}
```

---

### `mni_02_to_parquet_processos.R` — Processos

**Motor:** DuckDB (`:memory:`) + `read_json()` glob  
**Input:** `data_mni/processed/*.json`  
**Output:** `data_mni/processos_mni.parquet`

**Schema:**

| Coluna | Tipo | Origem |
|---|---|---|
| `numero_processo` | VARCHAR | `metadados.numero_processo` |
| `data_consulta` | VARCHAR | `metadados.data_consulta` |
| `data_formatada` | VARCHAR | `metadados.data_formatada` |
| `arquivo_origem` | VARCHAR | `metadados.arquivo_origem` |
| `classe_processual` | VARCHAR | `resumo.classe_processual` |
| `data_ajuizamento` | VARCHAR | `resumo.data_ajuizamento` |
| `valor_causa` | DOUBLE | `resumo.valor_causa` |
| `orgao_julgador_nome` | VARCHAR | `resumo.orgao_julgador` |
| `situacao` | VARCHAR | `resumo.situacao` |
| `status` | VARCHAR | `resumo.status` |
| `total_movimentos` | INTEGER | `resumo.total_movimentos` |
| `ultimo_movimento_data` | VARCHAR | `resumo.ultimo_movimento.data` |
| `ultimo_movimento_descricao` | VARCHAR | `resumo.ultimo_movimento.descricao` |
| `competencia` | VARCHAR | `processo.dados_basicos.competencia` |
| `codigo_localidade` | VARCHAR | `processo.dados_basicos.codigoLocalidade` |
| `juizo_100_digital` | BOOLEAN | `processo.dados_basicos.juizo100Digital` |
| `nivel_sigilo` | INTEGER | `processo.dados_basicos.nivelSigilo` |
| `custas_recolhidas` | DOUBLE | `processo.dados_basicos.custasRecolhidas` |
| `codigo_orgao` | VARCHAR | `processo.dados_basicos.orgaoJulgador.atributos.codigoOrgao` |
| `codigo_municipio_ibge` | VARCHAR | `processo.dados_basicos.orgaoJulgador.atributos.codigoMunicipioIBGE` |
| `instancia` | VARCHAR | `processo.dados_basicos.orgaoJulgador.atributos.instancia` |

---

### `mni_03_to_parquet_movimentos.R` — Movimentos

**Motor:** DuckDB (`:memory:`) + `UNNEST(processo.movimentos)`  
**Input:** `data_mni/processed/*.json`  
**Output:** `data_mni/movimentos_mni.parquet`

Cada movimento do array `processo.movimentos` vira uma linha — relação 1:N com `processos_mni`.

**Schema:**

| Coluna | Tipo | Descrição |
|---|---|---|
| `numero_processo` | VARCHAR | FK para `processos_mni` |
| `classe_processual` | VARCHAR | Código da classe (denormalizado) |
| `movimento_sequencia` | INTEGER | Posição ordinal do movimento |
| `movimento_data_hora` | TIMESTAMP | Data/hora do movimento (parsed) |
| `movimento_data_hora_raw` | VARCHAR | Valor bruto original |
| `movimento_codigo` | VARCHAR | Código CNJ do movimento |
| `movimento_nome` | VARCHAR | Descrição do tipo de movimento |
| `complemento_projudi_nome` | VARCHAR | Título do complemento Projudi |
| `complemento_projudi_descricao` | VARCHAR | Texto completo do complemento |
| `complemento_projudi_movimentado_por` | VARCHAR | String bruta "Nome - Cargo" |
| `complemento_projudi_movimentado_por_nome` | VARCHAR | Nome extraído (lower) |
| `complemento_projudi_movimentado_por_cargo` | VARCHAR | Cargo extraído (lower) |

O campo `complemento_projudi_descricao` contém o texto semântico rico dos movimentos — intimações, habilitações/desabilitações de partes, remessas ao MP, leituras de intimação, etc. É a principal entrada do script seguinte.

---

### `mni_04_movimentos_parse_partes.R` — Parsing Semântico de Partes

**Motor:** DuckDB (`:memory:`) para extração via regex RE2 + R/`stringr` para normalização  
**Input:** `data_mni/movimentos_mni.parquet`  
**Outputs:**
- `data_mni/movimentos_mni_partes.parquet` — todos os movimentos com 3 colunas adicionais
- `data_mni/partes_mni.parquet` — projeção filtrada: apenas movimentos com pelo menos um campo extraído não-nulo

**Flag de teste:** `Rscript bin/mni_04_movimentos_parse_partes.R --test`  
Opera sobre `movimentos_mni_test.parquet` e grava `*_test.parquet`, sem tocar nos arquivos de produção.

#### Campos Extraídos

| Coluna (`partes_mni`) | Tipo | Descrição |
|---|---|---|
| `parte_nome` | VARCHAR | Nome da parte processual, normalizado |
| `parte_especie` | VARCHAR | Papel processual: Promovente, Promovido, Terceiro, etc. |
| `administrador_judicial` | VARCHAR | Nome do AJ ou síndico, quando identificado |

#### Lógica de Extração

A extração opera sobre `complemento_projudi_descricao` em dois ramos mutuamente exclusivos, baseados no padrão textual do campo:

**Ramo 1 — HAB/DESAB** (`HABILITAÇÃO`/`DESABILITAÇÃO`/`DESHABILITAÇÃO DE PARTE EM PROCESSO`):

O regex âncora `(?i)(?:DESH?)?ABILIT.+?Parte:\s+` detecta o ramo. Dentro dele, um `COALESCE` de 5 sub-padrões trata as variantes de posicionamento do cargo:

| Sub-padrão | Exemplo | `parte_nome` | `administrador_judicial` |
|---|---|---|---|
| A | `ADMINISTRADOR JUDICIAL (Nome) (Espécie)` | `nome` | `nome` |
| B | `(ADMINISTRADOR JUDICIAL) Nome (Espécie)` | `nome` | `nome` |
| C | `Nome (SÍNDICO DO(A) Empresa) (Espécie)` | `nome` | `nome` |
| D | `Nome (ADMINISTRADOR JUDICIAL) (Espécie)` | `nome` | `nome` |
| E | `Nome (Espécie)` — sem cargo | `nome` | `NULL` |

**Ramo 2 — Intimações** (`Para advogados/...`, `P/ advgs.`, `Pelo advogado/...`):

Captura o destinatário da intimação. Se o destinatário for síndico (`SÍNDICO DO(A)`), separa nome e preenche `administrador_judicial`. `MASSA FALIDA` no texto de intimação é tratado como nome da parte — não dispara `administrador_judicial`.

#### Pós-processamento em R/stringr

Após a extração SQL, `parte_nome` passa por uma cadeia de normalização aplicada via `stringr` (necessário pois `\b` e `$` em strings SQL DuckDB causam erros de parsing no Windows):

1. `& Cia Ltda` → `e cia ltda`
2. `& Cia S/A` → `e cia sa`
3. `Cia.` → `cia`
4. `S/A`, `S.A.`, `S.A` → `sa`
5. `Ltda.`, `LTDA` → `ltda`
6. Vírgulas removidas
7. `str_squish()` — colapso de espaços

**Schema de `partes_mni.parquet`:**

| Coluna | Tipo |
|---|---|
| `numero_processo` | VARCHAR |
| `classe_codigo` | VARCHAR |
| `movimento_sequencia` | INTEGER |
| `movimento_data_hora` | TIMESTAMP |
| `movimento_codigo` | VARCHAR |
| `movimento_nome` | VARCHAR |
| `parte_nome` | VARCHAR |
| `parte_especie` | VARCHAR |
| `administrador_judicial` | VARCHAR |
| `complemento_projudi_descricao` | VARCHAR |
| `complemento_projudi_movimentado_por_nome` | VARCHAR |
| `complemento_projudi_movimentado_por_cargo` | VARCHAR |

---

## Execução

```bash
# 1. Coleta (Python)
python bin/mni_01_getdata_batch.py

# 2. Processos → parquet
Rscript bin/mni_02_to_parquet_processos.R

# 3. Movimentos → parquet (UNNEST)
Rscript bin/mni_03_to_parquet_movimentos.R

# 4. Parsing semântico de partes
Rscript bin/mni_04_movimentos_parse_partes.R

# Modo teste (opera sobre *_test.parquet, não altera produção)
Rscript bin/mni_04_movimentos_parse_partes.R --test
```

Os scripts R devem ser executados a partir da raiz do projeto (onde está o `.Rproj`). Cada script verifica a presença do `.Rproj` e aborta com mensagem clara se executado fora do contexto correto.

---

## Dependências

### Python
```
requests
zeep          # cliente SOAP
```

### R
```r
pacman        # gerenciamento de pacotes (bootstrap automático)
duckdb        # motor SQL/parquet
arrow         # leitura/escrita parquet
dplyr         # manipulação tabular
stringr       # normalização regex
glue          # interpolação de strings SQL
logger        # logging estruturado com dual appender
benchmarkme   # detecção de RAM para SET memory_limit
```

---

## Logs

Cada script R grava um log timestamped em `logs/`:

```
logs/mni_movimentos_parse_partes_20260225_094619.log
```

O log é simultâneo: console com cores ANSI (`layout_glue_colors`) e arquivo plano (`layout_glue`). Prefixos de contexto: `SETUP`, `PROC`, `PARTES`, `NORM`, `VALID`.

---

## Notas Técnicas

**DuckDB em `:memory:`:** todos os scripts usam `dbdir = ":memory:"` — nenhum arquivo `.duckdb` é criado em disco. O `memory_limit` é definido dinamicamente como 80% da RAM física detectada via `benchmarkme::get_ram()`.

**Escaping de regex no DuckDB/Windows:** o motor RE2 do DuckDB recebe padrões como strings SQL single-quoted. Dentro de `glue("...")` no R, cada barra real no SQL requer 4 barras no source R (`\\\\`). `\b` (word boundary) e `$` (fim de string) causam erros de parsing no DuckDB quando passados via SQL string — por isso a normalização de sufixos societários foi movida para R/`stringr`, onde esses tokens funcionam normalmente. No DuckDB, `\z` substitui `$` como âncora de fim de string no RE2.

**Lock de arquivo no Windows:** o DuckDB mantém memory-mapped I/O nos arquivos Parquet que escreveu. Tentativas de `write_parquet()` sobre o mesmo arquivo antes do `dbDisconnect()` resultam em erro 1224 (`user-mapped section open`). O script 04 contorna isso: (a) fecha a conexão principal antes de ler o arquivo de saída com `arrow`, (b) grava a versão normalizada em arquivo temporário `*_tmp.parquet`, (c) renomeia atomicamente sobre o destino final.

**Separação de responsabilidades (scripts 03 e 04):** o script 03 realiza ETL puro (JSON → Parquet flat, sem regex semântico). O script 04 opera exclusivamente sobre o Parquet já gerado. Essa separação permite reajustar os padrões de extração sem reprocessar os JSONs originais.