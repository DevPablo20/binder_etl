# Estrutura do projeto

ETL medallion + FastAPI de catálogo. Fluxo de dados e decisões:
[architecture.md](architecture.md). Versões: [tech-stack.md](tech-stack.md).

## Raiz

```
binder_etl/
├── CLAUDE.md                # contexto sempre carregado
├── docs/                    # referência sob demanda
│   └── plans/               # trabalho em andamento: iniciativas ativas + backlog
├── .claude/skills/          # workflows invocáveis
├── docker-compose.yaml      # MinIO + Postgres (meta Airflow) + Airflow (+ perfil spark-dev)
├── requirements/
│   ├── spark.txt
│   └── airflow.txt
├── airbyte/README.md        # instalação abctl + conexões TikTok, Facebook Pages e Instagram
├── dags/
│   ├── tiktok/tiktok_daily.py
│   ├── facebook_organic/facebook_organic_daily.py
│   └── instagram_organic/
│       ├── instagram_organic_daily.py
│       └── instagram_stories_daily.py   # alvo: stories têm extração e DAG próprios
├── src/
│   ├── config/settings.py   # MinIO e demais settings
│   ├── spark_session.py     # Spark + Delta + S3A
│   ├── api/                 # FastAPI de catálogo
│   │   ├── main.py          # app + lifespan async (Spark antes de servir)
│   │   └── routes/catalog.py
│   ├── airbyte/             # alvo: cliente da API pública (token, disparo, poll de job)
│   ├── io/
│   │   ├── reader.py        # lê Parquet/Delta do MinIO
│   │   └── writer.py        # escreve Delta no MinIO
│   ├── pipelines/run.py     # CLI: run {bronze|silver|gold|medallion} {platform}
│   └── transformers/
│       ├── base.py          # base compartilhada fina — inclui dedupe()
│       ├── snapshots.py     # foto diária e delta entre fotos — plataformas orgânicas
│       ├── organic_gold.py  # contrato da gold orgânica compartilhada (gold/organic/)
│       └── tiktok/
│           ├── tables.py    # SSOT: streams, dedupe, contrato de join, entidades de catálogo
│           ├── bronze.py    # orquestrador (I/O + ETL_STRICT + acumulação)
│           ├── silver.py
│           ├── gold.py
│           └── transforms/  # lógica pura de DataFrame
│               ├── silver/  # um módulo por stream + registry SILVER_TRANSFORMS
│               ├── gold/    # um módulo por fato + registry GOLD_TRANSFORMS
│               └── catalog/ # silver → formato de linha da API
│       ├── facebook_organic/ # mesmo molde, sem catalog/ — ver seção própria
│       └── instagram_organic/ # idem
├── dev/                     # sandbox Spark, não é código de produção
│   ├── examples/            # templates de inspeção versionados
│   └── sandbox/             # scripts pessoais (gitignored)
├── infra/
│   ├── airflow/Dockerfile
│   └── spark-dev/Dockerfile
└── tests/
    ├── test_pipeline.py         # CLI/registry — não é de uma plataforma
    ├── test_spark_minio.py      # plumbing Spark + MinIO/S3A
    ├── test_snapshots.py        # regras do delta entre fotos — compartilhado pelas orgânicas
    └── transformers/
        └── tiktok/               # espelha src/transformers/tiktok/
            ├── test_conservation.py  # invariante de conservação — cruza camadas
            ├── bronze/
            │   ├── test_smoke.py       # lê MinIO de verdade
            │   └── test_accumulate.py  # unitário, sem I/O
            ├── silver/
            │   ├── test_smoke.py
            │   └── test_*.py            # um por transform com lógica não-trivial
            ├── gold/
            │   ├── test_smoke.py
            │   └── test_*.py            # um por fato com lógica não-trivial
            └── catalog/
                └── test_smoke.py
        ├── facebook_organic/     # espelha src/transformers/facebook_organic/ (sem catalog/)
        └── instagram_organic/    # idem
```

## Convenções de nome

| Item | Padrão | Exemplo |
|---|---|---|
| Pasta de plataforma | slug minúsculo | `tiktok`, `meta`, `google_ads` |
| DAG id | `{platform}_daily` | `tiktok_daily` |
| Tarefas Airflow | `{layer}_{platform}` | `bronze_tiktok`, `gold_tiktok` |
| Caminho raw | `airbyte/{platform}/{stream}/` | `airbyte/tiktok/campaigns/` |
| Caminho Delta | `{layer}/{platform}/{table}/` | `silver/tiktok/campaigns/` |
| CLI | `run {layer} {platform}` | `run gold tiktok` |
| SSOT de metadados | `src/transformers/{platform}/tables.py` | streams, dedupe, join keys |
| Transform silver | `transforms/silver/{stream}.py` | renomes e casts |
| Transform gold | `transforms/gold/{fact}.py` | joins e grão de negócio |
| Transform catálogo | `transforms/catalog/{entity}.py` | linhas de hierarquia para a API |
| Teste | `tests/transformers/{platform}/{layer}/test_*.py` | espelha a árvore de `src/` — 1:1 por plataforma e camada |

## Testes

`tests/` espelha `src/transformers/{platform}/` para escalar com o número de plataformas —
sem isso, o diretório vira uma lista plana de dezenas de arquivos `test_{layer}_{platform}.py`
assim que a segunda ou terceira plataforma entrar.

- **`test_smoke.py`** por camada (`bronze/`, `silver/`, `gold/`, `catalog/`): lê/escreve no
  MinIO de verdade, roda o transformer real. É o mesmo teste de sempre, só que com o nome
  simplificado porque a pasta já diz plataforma e camada — não precisa mais do sufixo
  `_tiktok` no nome do arquivo.
- **Um arquivo extra por transform com lógica não-trivial** (filtro, join, acumulação):
  unitário, sem MinIO, com DataFrames sintéticos — ver `bronze/test_accumulate.py`,
  `silver/test_ads_reports_daily_filter.py`, `gold/test_ads_daily_metrics.py` como exemplo.
  Nem todo transform precisa de um; só os que têm uma regra que vale a pena provar isolada.
- **`test_conservation.py`** fica na raiz da plataforma (`tiktok/`, não dentro de uma
  camada), porque cruza bronze → silver → gold.
- **Todo diretório tem `__init__.py`**, incluindo `tests/` e `tests/transformers/`. Sem isso,
  dois arquivos com o mesmo nome em camadas diferentes (`bronze/test_smoke.py` e
  `gold/test_smoke.py`) colidem no modo de import padrão do pytest.
- **O que não é de uma plataforma fica na raiz de `tests/`** — `test_pipeline.py` (CLI e
  registry) e `test_spark_minio.py` (plumbing Spark + MinIO).

Ao adicionar uma plataforma, copie `tests/transformers/tiktok/` como copia
`src/transformers/tiktok/` — mesma pasta, mesmos nomes de arquivo.

## Split camada vs transform

| Tipo | Local | Responsabilidade |
|---|---|---|
| Orquestrador | `{platform}/bronze.py`, `silver.py`, `gold.py` | I/O no MinIO, loop de streams/fatos, `ETL_STRICT`, acumulação no bronze |
| Metadados | `{platform}/tables.py` | caminhos, chaves de dedupe, config de fato gold, entidades de catálogo, join keys do Bridge |
| Transforms | `{platform}/transforms/{layer}/` | lógica pura: silver é `DataFrame → DataFrame`; gold recebe `dict` de DataFrames já lidos; catálogo recebe Spark |
| API | `src/api/` | FastAPI; Spark no lifespan; rotas chamam transforms |

Bronze não tem módulos de transform — o dedupe é genérico via `base.dedupe()` + chaves do
`tables.py`. **A acumulação também mora no orquestrador**, não num transform. Catálogo não é
camada de CLI — só API + transforms.

## Entrypoints

| Arquivo | Papel |
|---|---|
| `src/pipelines/run.py` | CLI de bronze, silver, gold, medallion |
| `src/api/main.py` | FastAPI; lifespan async inicializa Spark antes de servir |
| `src/api/routes/catalog.py` | `GET` catálogo para descoberta do Bridge |
| `src/spark_session.py` | Sessão Spark com Delta + MinIO S3A |
| `src/transformers/{platform}/tables.py` | SSOT de metadados da plataforma |
| `dags/{platform}/{platform}_daily.py` | Orquestração Airflow: dispara a extração, aguarda, monta o medallion |
| `src/airbyte/` | Cliente da API pública do Airbyte, compartilhado pelos DAGs (alvo) |
| `dev/` + perfil `dev` do Compose | Sandbox para explorar o lake e prototipar |

## Contrato da Catalog API

Somente leitura, para descoberta do Bridge. O `SparkSession` precisa estar pronto no lifespan
**antes** de a app aceitar tráfego. Handlers podem ser async, mas trabalho Spark/MinIO é
síncrono e roda fora do event loop.

Fontes (entidades do silver):

| Tabela silver | `object_type` |
|---|---|
| `{platform}/advertisers` | `account` |
| `{platform}/campaigns` | `campaign` |
| `{platform}/ad_groups` | `ad_group` |
| `{platform}/ads` | `ad` |

Nulabilidade da resposta:

| Campo | Nulo quando |
|---|---|
| `platform` | nunca (vem do slug do transformer, não de coluna do silver) |
| `object_type` | nunca |
| `account_id` / `account_name` | nunca |
| `campaign_id` / `campaign_name` | só se `object_type = account` |
| `ad_group_id` / `ad_group_name` | se `object_type` em (`account`, `campaign`) |
| `ad_id` / `ad_name` | se `object_type` em (`account`, `campaign`, `ad_group`) |

## TikTok (referência)

Streams: `advertisers`, `campaigns`, `ad_groups`, `ads`, `ads_reports_daily`.
Fato gold: `ads_daily_metrics` no grão `ad_id + date`.

| Campo raw | Coluna silver |
|---|---|
| `advertiser_id` | `ad_account_id` |
| `campaign_id` | `campaign_id` |
| `adgroup_id` | `ad_group_id` |
| `ad_id` | `ad_id` |

"Ad group" do TikTok corresponde ao nível `ad_group` do Bridge.

## Facebook orgânico

Slug `facebook_organic`. Fluxo e regras: [architecture.md](architecture.md#facebook-orgânico-facebook_organic).
Registrado em `TRANSFORMERS` e `PLATFORMS`, **fora** de `CATALOG_BY_PLATFORM` — a Catalog API
responde 404 para ele.

Difere do molde do TikTok em três pontos:

- **`tables.py` declara o schema de cada stream** (`spark.read.schema(...)`), além de
  `SILVER_TABLES` e das listas de métricas.
- **O silver não é 1:1 com o bronze.** `SilverTableConfig.bronze_sources` lista os streams de
  cada tabela, e o transform recebe um `dict` de DataFrames, como no gold.
- **O bronze acrescenta `page_id` e `snapshot_date`** antes do dedupe (`bronze.prepare`).

| Camada | Tabelas |
|---|---|
| Bronze | `page`, `post`, `post_insights`, `page_insights` — chave `id + snapshot_date` |
| Silver | `pages`, `page_followers_snapshot`, `posts`, `post_metrics_daily`, `page_insights_daily` |
| Gold | partição `facebook` de `gold/organic/`: `content`, `content_daily`, `account_daily` |

DAG `facebook_organic_daily` às 08:00 UTC — depois do sync da 01:00 de São Paulo e antes do
corte das 06:00.

## Instagram orgânico

Slug `instagram_organic`. Fluxo e regras: [architecture.md](architecture.md#instagram-orgânico-instagram_organic).
Mesmo molde do `facebook_organic` e o mesmo registro: `TRANSFORMERS` e `PLATFORMS`, fora de
`CATALOG_BY_PLATFORM`.

- **A chave de dedupe varia por stream** (`InstagramStream.dedupe_columns`): foto diária,
  série nativa (`user_insights`) ou última leitura (stories). `required_column` descarta as
  linhas vazias do `user_insights`.
- **Não há `page_id` no caminho** — todo registro traz `business_account_id`.

| Camada | Tabelas |
|---|---|
| Bronze | `users`, `user_insights`, `user_lifetime_insights`, `media`, `media_insights`, `stories`, `story_insights` |
| Silver | `accounts`, `account_followers_snapshot`, `account_insights_daily`, `follower_demographics_snapshot`, `media`, `media_metrics_daily`, `stories` |
| Gold | partição `instagram` de `gold/organic/`: `content`, `content_daily`, `account_daily`, `stories` |

DAG `instagram_organic_daily` às 08:00 UTC.

## Plataformas orgânicas: um DAG por extração

Um DAG por conexão de extração, não por plataforma: o Instagram tem dois (principal e stories)
porque são duas conexões com horários diferentes, e os dois chamam o mesmo medallion de
plataforma. O Facebook tem um, que dispara em paralelo quantas conexões de página existirem e
consolida tudo numa passada — o curinga do `raw_path` é o que permite isso. O desenho e o porquê
estão em [architecture.md](architecture.md#orquestração-airflow--airbyte).

## Plataformas orgânicas: código compartilhado

`src/transformers/snapshots.py` guarda o que o Facebook e o Instagram fazem igual no bronze e
no silver: o `snapshot_date` (dia que a foto fecha), `latest_per` (última foto por chave) e
`add_lifetime_deltas` (delta entre fotos, `baseline_kind`, `gap_days`).

`src/transformers/organic_gold.py` é o contrato da gold compartilhada: nomes, ordem e tipos de
`content`, `content_daily`, `account_daily` e `stories`, e o `conform` que coloca a saída de
cada rede nesse schema. A gold fica em `gold/organic/{tabela}` — fora do padrão
`{layer}/{platform}/{table}` —, particionada por `platform`; cada orquestrador grava só a sua
partição (`write_delta(..., replace_where="platform = '...'")`).

Os orquestradores de camada continuam copiados por plataforma, no molde do TikTok.

## Guardrail de complexidade

`binder_data_hub` é inspiração, não modelo a copiar. Este repo escolheu deliberadamente:
uma `base.py` em vez de três base classes por camada; um `run.py` em vez de `run_layer` com
aliases legados; imagem Docker compartilhada onde possível.

Ao adicionar plataforma, **copie o padrão da pasta TikTok** — não refatore código
compartilhado a menos que a duplicação passe de ~30 linhas.
