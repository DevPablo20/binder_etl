---
name: add-platform
description: Integra uma nova plataforma de mídia ao binder_etl — streams do Airbyte, tabelas medallion bronze/silver/gold, transforms de catálogo, DAG do Airflow e registro em PLATFORMS. Use ao adicionar Meta, Google Ads, Kwai ou qualquer plataforma nova ao pipeline.
---

# Adicionar uma plataforma

Copie o padrão da pasta `src/transformers/tiktok/`. Não refatore código compartilhado a menos
que a duplicação passe de ~30 linhas — a simplicidade deste repo é deliberada.

Leia antes: [docs/architecture.md](../../../docs/architecture.md) para o contrato de join com
o Bridge, e [docs/project-structure.md](../../../docs/project-structure.md) para as convenções
de nome.

## Checklist

```
- [ ] 1. Conexão Airbyte documentada em airbyte/README.md
- [ ] 2. src/transformers/{platform}/tables.py
- [ ] 3. bronze.py, silver.py, gold.py
- [ ] 4. transforms/silver/, transforms/gold/, transforms/catalog/
- [ ] 5. Normalizar as colunas canônicas do silver
- [ ] 6. Registrar em PLATFORMS
- [ ] 7. dags/{platform}/{platform}_daily.py
- [ ] 8. tests/transformers/{platform}/ — copie a árvore de tests/transformers/tiktok/
- [ ] 9. Cadastrar catalog_key da plataforma no backend
```

## 1. Airbyte

Documente a conexão em `airbyte/README.md`. Prefira **Incremental + Append** se o conector
oferecer — reduz chamada de API e churn no raw. Evite Overwrite: hoje é o raw que acumula o
histórico, um arquivo por sync. Se o conector só oferecer Overwrite, o bronze passa a
**precisar** acumular (ver passo 3).

- **Inclua deletados**, se o conector tiver a opção. Sem ela, objetos deletados nunca chegam
  às dimensões, mas as métricas deles chegam ao relatório.
- **Escolha um `start_date` que cubra o histórico desejado.** Streams incrementais só trazem
  objetos modificados desde essa data.
- **Mudar qualquer um dos dois depois exige Clear data dos streams.** O cursor do
  incremental vive no Airbyte; limpar o raw não o reseta.

## 2. `tables.py` — o SSOT da plataforma

Define streams, caminhos raw, chaves de dedupe, contrato de join com o Bridge e entidades de
catálogo.

⚠️ **A chave de dedupe é a chave natural mínima.** `ad_id`, não
`advertiser_id + campaign_id + adgroup_id + ad_id`. Como o bronze acumula, chave composta
demais faz um objeto que mudou de pai virar duas linhas — e a métrica duplica no gold.

| Stream | Chave de dedupe |
|---|---|
| contas/advertisers | id da conta |
| campanhas | id da campanha |
| ad groups | id do ad group |
| ads | id do ad |
| relatório diário | id do ad + data |

## 3. Orquestradores de camada

Estendem `BaseTransformer`. Cuidam de I/O no MinIO, loop de streams, `ETL_STRICT` e — no
bronze — do dedupe.

Com Append no raw, ler o raw inteiro e fazer dedupe já preserva o histórico. Se o raw for
Overwrite, ou tiver retenção, o bronze **precisa** acumular: union com o Delta existente,
`dedupe()`, `.cache()` + `.count()`, e só então `write_delta(mode="overwrite")`. Padrão em
[docs/architecture.md](../../../docs/architecture.md).

## 4. Transforms

Lógica pura de DataFrame, um módulo por stream/fato/entidade, registrados nos registries
`SILVER_TRANSFORMS` e `GOLD_TRANSFORMS`.

## 5. Colunas canônicas do silver

Toda plataforma normaliza para os mesmos nomes, **todos string**:

| Coluna | Alvo no Bridge |
|---|---|
| `ad_account_id` | `platform_account.external_account_id` |
| `campaign_id` | `platform_campaign_binding.external_campaign_id` |
| `ad_group_id` | `platform_ad_group_classification.external_ad_group_id` |
| `ad_id` | `platform_ad_classification.external_ad_id` |

Preserve `_airbyte_raw_id`, `_airbyte_extracted_at` e `_airbyte_meta`. No incremental,
`_airbyte_extracted_at` é a última *modificação vista*, não prova de existência — se o
objeto ainda existe na plataforma, quem diz é o status de deletado.

Se o fato da plataforma não trouxer o id da conta (o TikTok emite `advertiser_id` nulo no
relatório), derive `ad_account_id` pela hierarquia, a partir da dimensão de ads.

Se a plataforma expõe formato nativo do criativo (equivalente ao `ad_format` do TikTok),
mantenha a coluna no silver: ela alimenta a tradução `(platform, native_value) → format`
do backend, evitando classificação manual por ad. Valor nativo nulo é legítimo (no TikTok,
posts autorizados) — não descarte a linha.

## 6. Registrar em `PLATFORMS`

A Catalog API passa a servir a plataforma automaticamente.

## 7. DAG

`dags/{platform}/{platform}_daily.py`, tarefas `{layer}_{platform}`, terminando em
`gold_{platform}`.

## 8. Testes

Copie `tests/transformers/tiktok/` para `tests/transformers/{platform}/` — mesma árvore,
`bronze/`, `silver/`, `gold/`, `catalog/`, cada uma com `__init__.py` e um `test_smoke.py`
que rode o medallion sobre uma amostra e verifique as colunas canônicas. Um `test_*.py`
extra só nos transforms com lógica não-trivial (filtro, join, acumulação) — sem MinIO, com
DataFrames sintéticos, no molde de `gold/test_ads_daily_metrics.py`. E um teste de
conservação na raiz da plataforma, no molde de `tiktok/test_conservation.py`.

## 9. Backend

A plataforma precisa de uma linha em `platform` com `catalog_key` igual ao slug usado aqui —
é por ela que o backend encontra o catálogo (`GET /catalog/{catalog_key}`).

## Verificação

```bash
python -m src.pipelines.run medallion {platform}
pytest tests/
```

Confirme: as quatro tabelas de dimensão existem no silver com as colunas canônicas como
string; o fato gold está no grão ad × dia; a Catalog API responde para os quatro
`object_type`; e — a invariante que mais importa — **a soma das métricas no gold é igual à
soma no relatório diário do silver**. Se for menor, um `inner join` está descartando linhas
— e vale checar se a extração trouxe todas as dimensões. Se for maior, é fan-out por chave
de dedupe composta demais.
