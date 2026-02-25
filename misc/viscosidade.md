# apuração da viscosidade processual no TJPR

fonte:
Estudo sobre varas empresariais na Comarca de São Paulo
Associação Brasileira de Jurimetria
2016-11-28
https://github.com/abjur/tjspBook
Finalização do projeto: Dezembro/2016 
Base de dados: Processos distribuídos nas Varas Cíveis do Foro Central entre 2013 e 2015 
Coordenador(es): Marcelo Guedes Nunes 
ISBN: 978-65-80612-06-2


## descrição da apuração

### 3.2 Mensuração do esforço do juiz em cada processo
Definido um critério para identificação de processos empresariais e estimação da cifra oculta, o desafio passa
a ser o de mensurar quantitativamente a razão do esforço despendido por um juiz para tratar de processos
empresariais com relação a procedimentos comuns. Essa taxa é útil pois possibilita a comparação das cargas
de trabalho nos dois regimes.

A mensuração do esforço está ligada ao tema de complexidade processual, um tópico de pesquisa com muitos
eixos de investigação. Até o momento, não há consenso sobre quais metodologias são mais adequadas em
cálculos dessa natureza, mas NUNES (2016) oferece algumas sugestões tais como a avaliação do tempo,
quantidade de movimentações, quantidade de recursos, partes e valores envolvidos. Essas sugestões são
consolidadas no conceito de viscosidade processual:

>[…] A viscosidade processual pode ser definida como o conjunto de características estruturais de
um processo, capazes de afetar a sua velocidade. Insistindo na analogia com os fluidos, se um
observador separar dois copos, um cheio de mel e outro de água, e virá-los simultaneamente de
ponta cabeça, a água cairá mais rápido do que o mel. A maior velocidade da água decorre não
da resistência oferecida por um obstáculo externo ao seu deslocamento, mas de diferenças na
estrutura íntima de cada substância: o mel é viscoso e avança mais vagarosamente do que a água,
que é mais fluida.
Seguindo na analogia, também alguns processos são mais viscosos que outros. Processos que
envolvam matérias complexas, múltiplas partes ou a produção de provas técnicas elaboradas pos-
suem uma estrutura íntima mais complexa e tendem a avançar mais lentamente do que casos
simples, com duas partes e que envolvam a produção apenas de prova documental. Essa com-
plexidade interna é o que chamamos de viscosidade processual, e sua mensuração é fundamental
para administrar a carga de trabalho e as metas dos funcionários da justiça, como, por exemplo,
na criação de regras para ponderar a distribuição de recursos para as câmaras reservadas.

Nas análises que seguem, a viscosidade não será utilizada exatamente da forma como foi definida, mas será
o ponto de partida para a quantificação do volume de trabalho em processos empresariais e comuns. Como
a viscosidade de um fluido está associada à velocidade com que ele escorre em um determinado meio, no
contexto jurídico ela estaria associada ao tempo de tramitação dos processos.

Para refinar o conceito, separamos a viscosidade em dois componentes: 
* i) o tempo associado às movimentações serventuárias e 
* ii) o tempo associado às decisões judiciais e outras intervenções dos magistrados.

Justificamos essa separação supondo que o tempo gasto pelos magistrados em uma determinada classe de
processos é mais informativa com relação à sua dificuldade fática e de direito do que o tempo total de
tramitação, sujeito a todo tipo de interferência externa.

Seguindo essa linha de raciocínio e considerando a finalidade desse estudo, a criação de varas especializada,
vamos definir viscosidade processual como o tempo gasto para a tomada de decisão dos magistrados. 
Em termos mais precisos, considere $( T_i )$, $( i = 1, \ldots, n )$ as datas das movimentações 
disponíveis no sistema e-SAJ de um certo processo, sendo $(n)$ o seu número de movimentações. 
Considere também uma variável $( D_i )$, $( i = 1, \ldots, n )$ que assume valor $1$ se 
a i-ésima movimentação é uma decisão, um despacho ou uma sentença e $0$ em caso contrário. 
Definimos a viscosidade como

$$
V = \sum_{i=2}^{n} D_i \times (T_i - T_{i-1}),
$$

que pode ser simplificada quando consideramos apenas os termos em que ( D_i > 0 )

$$
V = \sum_{\text{decisões}} \left( T_{\text{decisão}} - T_{\text{última movimentação}} \right).
$$



## apuração cumputacional 

Duas fases:
- adicionar ao dataframe de dados datajud sobre movimento (df_movimentos) dados da API CNJ-TPU
- apurar a viscosidade

PRIMEIRA FASE - Método:
- carregar o df_movimentos
  - "\data\cleaned\df_movimentos.rds"
  - assuma que há um Rproct file e vou chamar o script como `Rscript bin/api_tpu_movimentos.R`
- criar variaveis:
  - movimento_codigo_1
  - movimento_codigo_2
  - movimento_codigo_3
  - movimento_codigo_4
  - movimento_codigo_5
  - movimento_nome_1
  - movimento_nome_2
  - movimento_nome_3
  - movimento_nome_4
  - movimento_nome_5
- preencher as variaveis com os dados da API CNJ-TPU Consulta Detalhada 
  - GET /api/v1/publico/consulta/detalhada/movimentos Consulta movimentos processuais cadastrados no Sistema de Gestão de Tabelas Unificadas
  - `curl -X GET "https://gateway.cloud.pje.jus.br/tpu/api/v1/publico/consulta/detalhada/movimentos?codigo={movimento_codigo}" -H  "accept: */*"`
  - se utilizar o `movimento_codigo = 221`, ele dá esse response body:

```
[
  {
    "data_versao": "2025-09-11 16:34:41",
    "cod_item_pai": 385,
    "nome": "Procedência em Parte",
    "descricao_glossario": "",
    "justica_estadual_1grau": "S",
    "justica_estadual_2grau": "S",
    "justica_estadual_juizado_especial": "S",
    "justica_estadual_juizado_especial_turmas_recursais": "N",
    "justica_estadual_juizado_especial_fazenda_publica": "S",
    "justica_estadual_turma_estadual_uniformizacao": "N",
    "justica_estadual_militar_1grau": "S",
    "justica_estadual_militar_2grau": "S",
    "justica_federal_1grau": "S",
    "justica_federal_2grau": "S",
    "justica_federal_juizado_especial": "S",
    "justica_federal_turmas_recursais": "N",
    "justica_federal_nacional_uniformizacao": "N",
    "justica_federal_regional_uniformizacao": "N",
    "justica_federal_cjf": "N",
    "justica_trabalho_1grau": "S",
    "justica_trabalho_2grau": "S",
    "justica_trabalho_tst": "S",
    "justica_trabalho_csjt": "S",
    "justica_militar_uniao_1grau": "S",
    "justica_militar_uniao_stm": "S",
    "justica_militar_estadual_1grau": "S",
    "justica_militar_estadual_tjm": "S",
    "justica_eleitoral_zonas": "S",
    "justica_eleitoral_tre": "S",
    "justica_eleitoral_tse": "S",
    "outras_justicas_stf": "S",
    "outras_justicas_stj": "S",
    "outras_justicas_cnj": "S",
    "norma": "",
    "artigo": "",
    "sigiloso": "N",
    "situacao": "A",
    "data_inclusao": "2008-09-30 15:52:47",
    "usuario_inclusao": "",
    "data_alteracao": null,
    "usuario_alteracao": null,
    "complementos": [],
    "id": 221,
    "colegiado": "S",
    "dispositivoLegal": null,
    "flgEletronico": "S",
    "flgPapel": "S",
    "glossario": "<p>\n\tRegistra a solu&ccedil;&atilde;o do processo NO JU&Iacute;ZO ORIGIN&Aacute;RIO. Tratando-se de ju&iacute;zo recursal, registrar em Conhecido o recurso de parte e provido em parte.</p>\n",
    "monocratico": "N",
    "movimento": "Julgado procedente em parte o pedido",
    "presidenteVice": "N",
    "visibilidadeExterna": "S"
  }
]
```
- variaveis para serem obtidas:
  - "id"
  - "cod_item_pai"
  - "nome"
  - "descricao_glossario"
  - "glossario"
  - "norma"
  - "artigo"
  - "dispositivoLegal"
  - "data_inclusao"
  - "data_versao"
  - "data_alteracao"


- sendo que o `id` é o `movimento_codigo_{numero mais baixo na arvore de movimentos}
- nesse caso o `"id": 221` é o movimento que buscamos
- o movimento_codigo imediatamente anterior (um nivel acima) é o `"cod_item_pai": 385`
- só é possível saber qual é o nível, quando se volta até o primeiro nivel e se conta quantos niveis, pois existem movimentos que são de 2º, 3º, 4º e 5º nivel.
- nosso objetivo é complementar os dados que já temos, mas, especialmente, ter, na variavel `movimento_codigo_1` o dado se aquele movimento é de "Magistrado" ou de "Serventuário".

Estrutura hierárquica dos movimentos
- Nível 1 (raiz)
  - Contém apenas dois elementos.
  - Esses elementos não possuem pai (cod_item_pai = null).

  - { "id": 1,  "nome": "Magistrado",   "cod_item_pai": null }
  - { "id": 14, "nome": "Serventuário", "cod_item_pai": null }

- Nível 2 (subtipos vinculados ao nível 1)

  - Movimentos de Magistrado (cod_item_pai = 1)
    - { "id": 15185, "nome": "Cooperação Judiciária", "cod_item_pai": 1 }
    - { "id": 3,     "nome": "Decisão",               "cod_item_pai": 1 }
    - { "id": 11009, "nome": "Despacho",              "cod_item_pai": 1 }
    - { "id": 193,   "nome": "Julgamento",            "cod_item_pai": 1 }
    - { "id": 14092, "nome": "Voto",                  "cod_item_pai": 1 }

  - Movimentos de Serventuário (cod_item_pai = 14)
    - { "id": 865,   "nome": "Arquivista",                                         "cod_item_pai": 14 }
    - { "id": 12522, "nome": "Auxiliar da Justiça",                                "cod_item_pai": 14 }
    - { "id": 15,    "nome": "Contador",                                           "cod_item_pai": 14 }
    - { "id": 18,    "nome": "Distribuidor",                                       "cod_item_pai": 14 }
    - { "id": 48,    "nome": "Escrivão/Diretor de Secretaria/Secretário Jurídico", "cod_item_pai": 14 }
    - { "id": 104,   "nome": "Oficial de Justiça",                                 "cod_item_pai": 14 }


- salvar como `data/cleaned/df_movimentos_tpu.rds`

SEGUNDA FASE - Método:
- carregar o `data/cleaned/df_movimentos_tpu.rds`
- filtrar:
  - movimentos em que `movimento_codigo_1 = 1` (estamos querendo observar os movimento de "Magistrado") E
  - todos os movimentos anteriores ao `movimento_codigo_1 = 1` que forem `movimento_codigo_3 = 51` (é o movimento_nome_3 de "Conclusão")(queremos observar quando o processo foi concluso) mas pegando apenas a ocorrência mais recente do `movimento_codigo_3 = 51`
- criar uma variavel de `delta_magistrado` sendo a differença temporal entre:
  - `movimento_data_hora` do `movimento_codigo_1 = 1` 
  - `movimento_data_hora` do `movimento_codigo_3 = 51`
- apresentar 2 tabelas (todos os processos, processos das varas especializadas) com:
  - quantidade de processos analisada:
  - quantidade de decisões de magistrado:
  - viscosidade processual (geral): soma do `delta_magistrado`
- apresente 2 graficos ggplot (todos os processos, processos das varas especializadas)
  - viscosidade processual (serie temporal A - data_ajuizamento): soma do `delta_magistrado` por processos agregados por mes_ajuizamento (quero observar para aquela safra de processos ajuizados naquele mes, qual foi a viscosidade e ver se há variação temporal nela, com decomposição da serie)
  - viscosidade processual (serie temporal B - movimento_data_hora): soma do `delta_magistrado` por processos agregados por floor(movimento_data_hora),month (quero observar para aqueles processos decididos no mes, qual foi a viscosidade e ver se há variação temporal nela, com decomposição da serie)

`varas_especializadas <- c(26404, 26422, 13951, 13952, 13950, 13752, 8816, 8809)`

# todo
Escreva o código

## Referencias

PDPJ - Módulo de consulta às tabelas processuais unificadas, 1.4.3 
[ Base URL: gateway.cloud.pje.jus.br/tpu ]
https://gateway.cloud.pje.jus.br/tpu/v2/api-docs
Serviço estruturante que provê as consulta às tabelas processuais unificadas. Versao: 1.4.3
https://gateway.cloud.pje.jus.br/tpu/swagger-ui.html#/

GET /api/v1/publico/consulta/detalhada/movimentos Consulta movimentos processuais cadastrados no Sistema de Gestão de Tabelas Unificadas

`curl -X GET "https://gateway.cloud.pje.jus.br/tpu/api/v1/publico/consulta/detalhada/movimentos?codigo={movimento_codigo}" -H  "accept: */*"`

Response code 200  ("Sucesso na operação ou consulta. Consultas com resultado vazio também retornam este código")
Obtém um Example value:

```
[
  {
    "artigo": "string",
    "cod_item_pai": 0,
    "colegiado": "S",
    "complementos": [
      {
        "complementoDscComplemento": "string",
        "complementoDscObservacao": "string",
        "complementoId": 0,
        "complementoTabelado": [
          {
            "complementoDscComplemento": "string",
            "complementoDscObservacao": "string",
            "complementoId": 0,
            "complementoTipoComplemento": 0,
            "dscValorTabelado": "string",
            "id": 0
          }
        ],
        "complementoTipoComplemento": 0,
        "id": 0
      }
    ],
    "data_alteracao": "2026-02-25T14:18:52.401Z",
    "data_inclusao": "2026-02-25T14:18:52.401Z",
    "data_versao": "2026-02-25T14:18:52.401Z",
    "descricao_glossario": "string",
    "dispositivoLegal": "string",
    "flgEletronico": "S",
    "flgPapel": "S",
    "glossario": "string",
    "id": 0,
    "informacaoId": "string",
    "justica_eleitoral_tre": "S",
    "justica_eleitoral_tse": "S",
    "justica_eleitoral_zonas": "S",
    "justica_estadual_1grau": "S",
    "justica_estadual_2grau": "S",
    "justica_estadual_juizado_especial": "S",
    "justica_estadual_juizado_especial_fazenda_publica": "S",
    "justica_estadual_juizado_especial_turmas_recursais": "S",
    "justica_estadual_militar_1grau": "S",
    "justica_estadual_militar_2grau": "S",
    "justica_estadual_turma_estadual_uniformizacao": "S",
    "justica_federal_1grau": "S",
    "justica_federal_2grau": "S",
    "justica_federal_cjf": "S",
    "justica_federal_juizado_especial": "S",
    "justica_federal_nacional_uniformizacao": "S",
    "justica_federal_regional_uniformizacao": "S",
    "justica_federal_turmas_recursais": "S",
    "justica_militar_estadual_1grau": "S",
    "justica_militar_estadual_tjm": "S",
    "justica_militar_uniao_1grau": "S",
    "justica_militar_uniao_stm": "S",
    "justica_trabalho_1grau": "S",
    "justica_trabalho_2grau": "S",
    "justica_trabalho_csjt": "S",
    "justica_trabalho_tst": "S",
    "monocratico": "S",
    "movimento": "string",
    "nome": "string",
    "norma": "string",
    "outras_justicas_cnj": "S",
    "outras_justicas_stf": "S",
    "outras_justicas_stj": "S",
    "presidenteVice": "S",
    "sigiloso": "S",
    "situacao": {},
    "usuario_alteracao": "string",
    "usuario_inclusao": "string",
    "visibilidadeExterna": "S"
  }
]
```