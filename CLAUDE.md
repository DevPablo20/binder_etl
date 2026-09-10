# binder_etl

ETL interno: extrai dados de plataformas de mídia via Airbyte, transforma em camadas medallion
no MinIO com Spark + Delta, e expõe uma FastAPI de catálogo para o backend descobrir
identidades do lake.

**Fluxo:** plataformas → Airbyte → `raw` → Spark `bronze`/`silver`/`gold` → o backend
consulta o catálogo para configurar o Bridge → o DAG busca a publicação de enriquecimento e
materializa o **gold enriquecido**.

## Estado desta branch

`arch/bridge-enrichment` reorganiza a documentação para a arquitetura decidida. **O código
ainda é o antigo.** Três coisas mudam aqui, nesta ordem:

| # | Mudança | Situação |
|---|---|---|
| 1 | `dedupe_columns` reduzido à chave natural mínima | pendente — **pré-requisito do 2** |
| 2 | Bronze acumula: union com o existente antes do dedupe | pendente |
| 3 | Reprocessar e verificar a recuperação de linhas no gold | pendente |
| 8 | Gold enriquecido: fetch da publicação, três `LEFT JOIN`, coluna `MAP` | pendente |

Detalhe e armadilhas: [docs/architecture.md](docs/architecture.md).

## Problema conhecido: o gold perde ~52% das linhas

`ads_reports_daily` tem 8.676 linhas; `ads_daily_metrics` tem 4.154.

A causa é uma assimetria no Airbyte: as **dimensões** (`advertisers`, `campaigns`,
`ad_groups`, `ads`) vêm em Full Refresh + Overwrite — um único objeto no raw, do último sync
— enquanto **`ads_reports_daily`** vem em Incremental + Append e acumula. Todo objeto que
existia antes e não existe hoje tem métrica sem linha de dimensão, e o `inner join` do gold
descarta.

**Isso bloqueia o resto.** Não adianta enriquecer corretamente uma base incompleta.

## Arquitetura de enriquecimento (invariantes compartilhadas)

Valem nos três repositórios. Contradizer uma delas é bug, não escolha de implementação.

1. **Um fato: ad × dia.** `campaign`, `ad_group` e `ad` são níveis de *declaração*, não grãos
   de dado. Tudo resolve até a linha ad × dia.
2. **Um atributo, um nível.** Cada atributo é declarado em exatamente um nível e propaga para
   baixo. Sem override, sem declaração dupla — por isso não existe `coalesce` nem precedência.
3. **O nível é do negócio, não da plataforma.** Onde a plataforma guarda um dado é irrelevante.
4. **A amarração é do banco.** Integridade vira constraint no Postgres do backend, não
   validação imperativa.
5. **Nada some por enriquecimento.** Soma sem filtro no fato enriquecido é idêntica à soma no
   fato cru. Todo join é `LEFT`; ausência vira categoria explícita.
6. **SCD tipo 1.** A verdade é a configuração atual; corrigir reescreve o histórico. Por isso
   o `Overwrite` full do gold enriquecido é a semântica **correta**, não uma limitação.

| Atributo | Declarado em | Propaga para | Origem |
|---|---|---|---|
| Cliente | account | tudo abaixo | configuração |
| Campanha de negócio | campaign | ad_group, ad | configuração |
| Channel | campaign | ad_group, ad | configuração manual |
| Buying type | campaign | ad_group, ad | plano de mídia |
| Território, Persona, … | ad_group | ad | configuração |
| Format / Sub-format | ad | — | traduzido do nativo |

> Este bloco é espelhado em `binder_app_backend/CLAUDE.md` e `binder_app_frontend/CLAUDE.md`.
> Ao mudar, mude nos três.

## Camadas medallion

| Camada | Caminho | Formato | Responsabilidade |
|---|---|---|---|
| Raw | `raw/airbyte/{platform}/{stream}/` | Parquet | Landing do Airbyte — nunca editar à mão. **Pode ser sobrescrito** |
| Bronze | `bronze/{platform}/{table}/` | Delta | **Camada acumuladora.** Union + dedupe pela chave natural |
| Silver | `silver/{platform}/{table}/` | Delta | Renomes e tipos corretos |
| Gold | `gold/{platform}/{fact}/` | Delta | Fatos no grão de negócio |

`_airbyte_extracted_at` sobrevive até o silver e vale como **last seen** — é o filtro de
"ainda existe na plataforma" para a tela de configuração, sem coluna nova.

## Regras duras

- **Não escrever linhas do Bridge a partir daqui.** O Bridge é SSOT do backend.
- **O backend nunca faz I/O no MinIO.** I/O de lake fica no ETL; o backend consulta por HTTP.
- **Cast de IDs de plataforma para `string`** no silver — casa com `varchar(255)` do Bridge.
- **Preservar colunas de linhagem** do Airbyte: `_airbyte_raw_id`, `_airbyte_extracted_at`,
  `_airbyte_meta`.
- **Não guardar entidades de negócio do Binder no MinIO** — isso vive no Postgres.
- **Chave de dedupe é a chave natural mínima** (`ad_id`, não `advertiser_id + campaign_id +
  adgroup_id + ad_id`). Com bronze acumulando, chave composta demais duplica métrica.
- A FastAPI de catálogo precisa inicializar o `SparkSession` no lifespan **antes** de aceitar
  tráfego. Trabalho Spark/MinIO é síncrono — rode fora do event loop.

## Comandos

```bash
python -m src.pipelines.run medallion tiktok    # bronze → silver → gold
python -m src.pipelines.run bronze tiktok       # camada isolada
docker compose up                               # MinIO + Postgres (meta Airflow) + Airflow
docker compose --profile dev up spark-dev       # sandbox Spark
uvicorn src.api.main:app                        # FastAPI de catálogo
pytest tests/                                   # smoke tests
```

## Documentação

| Arquivo | Quando ler |
|---|---|
| [docs/architecture.md](docs/architecture.md) | acumulação no bronze, gold enriquecido, contrato de publicação, plano de migração |
| [docs/project-structure.md](docs/project-structure.md) | árvore de diretórios, convenções de nome, split layer/transform |
| [docs/tech-stack.md](docs/tech-stack.md) | versões e práticas por biblioteca |

Skill `add-platform` (`.claude/skills/add-platform/`) para integrar uma plataforma nova.
