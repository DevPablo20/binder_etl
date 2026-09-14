# binder_etl

ETL interno: extrai dados de plataformas de mídia via Airbyte, transforma em camadas medallion
no MinIO com Spark + Delta, e expõe uma FastAPI de catálogo para o backend descobrir
identidades do lake.

**Fluxo:** plataformas → Airbyte → `raw` → Spark `bronze`/`silver`/`gold` → o backend
consulta o catálogo para configurar o Bridge → o DAG busca a publicação de enriquecimento e
materializa o **gold enriquecido**.

## Estado desta branch

`arch/bridge-enrichment` reorganiza a documentação para a arquitetura decidida. **O código
ainda é o antigo.** O que muda aqui, nesta ordem:

| # | Mudança | Situação |
|---|---|---|
| 0 | Extração completa no Airbyte (deletados incluídos, `start_date` 2025-01-01) | **feito** |
| 1 | Gold parte do fato: `LEFT JOIN` nas dimensões, chaves vindas do fato | pendente — é o que `tests/test_conservation_tiktok.py` cobra |
| 2 | `dedupe_columns` reduzido à chave natural mínima, inclusive no fato | **feito** |
| 3 | Bronze acumula: union com o existente antes do dedupe | pendente — baixa prioridade, defesa contra retenção do raw |
| 8 | Gold enriquecido: fetch da publicação, três `LEFT JOIN`, coluna `MAP` | pendente |

Detalhe e armadilhas: [docs/architecture.md](docs/architecture.md).

## Diagnóstico da perda de linhas no gold

Medido em 11/09/2026. O `inner join` do gold descarta linhas do fato cuja campanha não está
na dimensão `campaigns`.

- **A causa principal era a extração incompleta, não o modo de sync.** As streams já eram
  Incremental + Append, e `advertisers` Full Refresh + Append; o raw acumula. O conector,
  porém, só trazia objetos modificados desde o `start_date` (2026-01-01) e omitia
  deletados. Com isso, 54 de 68 campanhas do fato não tinham dimensão, e o gold perdia
  R$ 30.065,43 de spend.
- **Correção aplicada no Airbyte:** ligar *Include Deleted Data*, `start_date` 2025-01-01 e
  **Clear data** dos streams. Limpar o raw **não** reseta o cursor do incremental.
- **Hoje:** spend, impressões e cliques batem entre silver e gold (R$ 9.073.343,49). Ainda
  se perdem 958 de 22.739 linhas, de 85 campanhas modificadas antes de 2025, e **todas** as
  59 métricas dessas linhas são zero. O passo 1 fecha isso.

**Linha não é dinheiro.** 66% do fato (14.967 linhas) são ad × dia com todas as métricas
zeradas: o conector emite uma linha por ad por dia mesmo sem entrega. Meça a invariante por
soma de métrica, não por contagem.

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
| Raw | `raw/airbyte/{platform}/{stream}/` | Parquet | Landing do Airbyte — nunca editar à mão. Acumula um arquivo por sync |
| Bronze | `bronze/{platform}/{table}/` | Delta | Dedupe pela chave natural sobre todo o raw; alvo: acumular também |
| Silver | `silver/{platform}/{table}/` | Delta | Renomes e tipos corretos |
| Gold | `gold/{platform}/{fact}/` | Delta | Fatos no grão de negócio |

**Existência de objeto vem do status, não de `_airbyte_extracted_at`.** No incremental, um
objeto só é reextraído quando muda, então um `extracted_at` antigo não significa que ele
sumiu. Deletados chegam explicitamente com `*_STATUS_DELETE` em `secondary_status`.

## Regras duras

- **Não escrever linhas do Bridge a partir daqui.** O Bridge é SSOT do backend.
- **O backend nunca faz I/O no MinIO.** I/O de lake fica no ETL; o backend consulta por HTTP.
- **Cast de IDs de plataforma para `string`** no silver — casa com `varchar(255)` do Bridge.
- **Preservar colunas de linhagem** do Airbyte: `_airbyte_raw_id`, `_airbyte_extracted_at`,
  `_airbyte_meta`.
- **Não guardar entidades de negócio do Binder no MinIO** — isso vive no Postgres.
- **Chave de dedupe é a chave natural mínima** (`ad_id`, não `advertiser_id + campaign_id +
  adgroup_id + ad_id`). O raw acumula versões; chave composta demais faz um objeto que mudou
  de pai virar duas linhas e duplica métrica.
- **O fato não traz id de conta.** `advertiser_id` existe no schema do `ads_reports_daily`
  mas vem `NULL` do conector. `ad_account_id` deriva da hierarquia, pela dimensão `ads`.
- **Todo join do gold é `LEFT`, partindo do fato.** A extração nunca é garantidamente
  completa; o join não pode depender dela.
- **Mudar `start_date` ou *Include Deleted* no Airbyte exige Clear data dos streams.**
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
pytest tests/test_conservation_tiktok.py        # invariante de conservação (rode o medallion antes)
```

## Documentação

| Arquivo | Quando ler |
|---|---|
| [docs/architecture.md](docs/architecture.md) | extração, conservação, gold enriquecido, contrato de publicação, plano de migração |
| [docs/project-structure.md](docs/project-structure.md) | árvore de diretórios, convenções de nome, split layer/transform |
| [docs/tech-stack.md](docs/tech-stack.md) | versões e práticas por biblioteca |

Skill `add-platform` (`.claude/skills/add-platform/`) para integrar uma plataforma nova.
