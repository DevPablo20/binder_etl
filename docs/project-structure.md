# Estrutura do projeto

ETL medallion + FastAPI de catálogo. Fluxo de dados e decisões:
[architecture.md](architecture.md). Versões: [tech-stack.md](tech-stack.md).

## Raiz

```
binder_etl/
├── CLAUDE.md                # contexto sempre carregado
├── docs/                    # referência sob demanda
├── .claude/skills/          # workflows invocáveis
├── docker-compose.yaml      # MinIO + Postgres (meta Airflow) + Airflow (+ perfil spark-dev)
├── requirements/
│   ├── spark.txt
│   └── airflow.txt
├── airbyte/README.md        # instalação abctl + conexão TikTok
├── dags/
│   └── tiktok/tiktok_daily.py
├── src/
│   ├── config/settings.py   # MinIO e demais settings
│   ├── spark_session.py     # Spark + Delta + S3A
│   ├── api/                 # FastAPI de catálogo
│   │   ├── main.py          # app + lifespan async (Spark antes de servir)
│   │   └── routes/catalog.py
│   ├── io/
│   │   ├── reader.py        # lê Parquet/Delta do MinIO
│   │   └── writer.py        # escreve Delta no MinIO
│   ├── pipelines/run.py     # CLI: run {bronze|silver|gold|medallion} {platform}
│   └── transformers/
│       ├── base.py          # base compartilhada fina — inclui dedupe()
│       └── tiktok/
│           ├── tables.py    # SSOT: streams, dedupe, contrato de join, entidades de catálogo
│           ├── bronze.py    # orquestrador (I/O + ETL_STRICT + acumulação)
│           ├── silver.py
│           ├── gold.py
│           └── transforms/  # lógica pura de DataFrame
│               ├── silver/  # um módulo por stream + registry SILVER_TRANSFORMS
│               ├── gold/    # um módulo por fato + registry GOLD_TRANSFORMS
│               └── catalog/ # silver → formato de linha da API
├── dev/                     # sandbox Spark, não é código de produção
│   ├── examples/            # templates de inspeção versionados
│   └── sandbox/             # scripts pessoais (gitignored)
├── infra/
│   ├── airflow/Dockerfile
│   └── spark-dev/Dockerfile
└── tests/
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

## Split camada vs transform

| Tipo | Local | Responsabilidade |
|---|---|---|
| Orquestrador | `{platform}/bronze.py`, `silver.py`, `gold.py` | I/O no MinIO, loop de streams/fatos, `ETL_STRICT`, acumulação no bronze |
| Metadados | `{platform}/tables.py` | caminhos, chaves de dedupe, config de fato gold, entidades de catálogo, join keys do Bridge |
| Transforms | `{platform}/transforms/{layer}/` | `DataFrame → DataFrame` puro (gold e catálogo recebem Spark + fontes) |
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
| `dags/{platform}/{platform}_daily.py` | Orquestração Airflow |
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

## Guardrail de complexidade

`binder_data_hub` é inspiração, não modelo a copiar. Este repo escolheu deliberadamente:
uma `base.py` em vez de três base classes por camada; um `run.py` em vez de `run_layer` com
aliases legados; imagem Docker compartilhada onde possível.

Ao adicionar plataforma, **copie o padrão da pasta TikTok** — não refatore código
compartilhado a menos que a duplicação passe de ~30 linhas.
