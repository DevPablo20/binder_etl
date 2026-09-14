# Iniciativa: bridge-enrichment

> **Estado de trabalho, não referência.** Este arquivo diz o que falta, o que está em aberto
> e o que aconteceu no caminho. Como o sistema funciona está no `CLAUDE.md` e em `docs/` de
> cada repositório. Quando a iniciativa encerrar, este arquivo é apagado — o histórico fica
> no git.

Plano único dos três repositórios (`binder_etl`, `binder_app_backend`,
`binder_app_frontend`). Os `CLAUDE.md` do backend e do frontend apontam para cá.

## Próxima ação

Decidir **D9** (ordem de execução dos passos 4–9) e **D1–D3** (constraints do binding).
Nenhuma entidade nova no backend antes disso.

## Objetivo

Dar significado de negócio aos fatos do lake sem perder dado. O Bridge (backend) classifica
IDs nativos com o vocabulário do Binder e publica um snapshot; o ETL materializa o gold
enriquecido, com a soma de qualquer métrica idêntica à do fato cru.

**Fora do escopo:** segmentações (age, gender, region), override de eixo no ad, histórico de
classificação. Os motivos estão em "Fora de escopo" de `binder_app_backend/docs/architecture.md`.

## Passos

| # | Passo | Repo | Status |
|---|---|---|---|
| 0 | Extração completa no Airbyte e diagnóstico medido | etl | feito 11/09 |
| 1 | Gold parte do fato: `LEFT JOIN`, chaves do fato, conta derivada de `ads` | etl | feito 14/09 |
| 1a | Silver descarta ad × dia sem nenhuma métrica | etl | feito 14/09 |
| 2 | `dedupe_columns` na chave natural mínima, inclusive no fato | etl | feito 14/09 |
| 3 | Bronze acumula: union com o existente antes do dedupe | etl | feito 14/09 |
| 4 | Tabelas novas do Bridge, índices de apoio e FKs compostas | backend | pendente — bloqueado por D1–D3, D9 |
| 5 | Migrar dados de `platform_object_map` para as tabelas novas | backend | pendente |
| 6 | Tradução de formato + fila de pendências | backend | pendente — bloqueado por D5, D8 |
| 7 | `enrichment_publication`, snapshot e endpoint | backend | pendente — bloqueado por D4 |
| 8 | Gold enriquecido: fetch, três `LEFT JOIN`, coluna `MAP`, teste de invariante | etl | pendente — bloqueado por D7 |
| 9 | Telas de configuração, alerta de não materializado, card de cobertura | frontend | pendente — card bloqueado por D6 |
| 10 | Remover `platform_object_map` e `assertLevelFields` | backend | pendente |

Nas conversas e commits de 14/09, "Fase 1 / Etapas 1–4" corresponde aos passos 2, 1a, 1 e 3,
nessa ordem. Daqui em diante, só a numeração desta tabela.

## Estado atual × alvo

### binder_etl

| | Existe hoje | Alvo |
|---|---|---|
| Gold base | `ads_daily_metrics`, partindo do fato com `LEFT JOIN` | — (pronto) |
| Gold enriquecido | não existe | fetch da publicação, três `LEFT JOIN`, coluna `MAP` de eixos |

### binder_app_backend

| | Existe hoje no código | Alvo |
|---|---|---|
| Bridge | `PlatformObjectMap` — tabela larga com `object_type` e validação em `assertLevelFields` | 4 tabelas, uma por nível, com as regras em constraint |
| Campanha de negócio | coluna `campaign_id` digitada em todo nível | derivada por FK composta a partir do binding |
| Formato | classificação manual por ad | traduzido de `ad_format` nativo; manual só como exceção |
| Publicação | não existe | snapshot imutável consumido pelo DAG |

### binder_app_frontend

As telas atuais falam com `PlatformObjectMap`.

| Fluxo | Hoje | Alvo |
|---|---|---|
| Contas | mapeia conta → cliente | sem mudança |
| Campanhas | `PlatformObjectMap` `campaign` + channel + buying type | `platform_campaign_binding` — mesma UX, endpoint novo |
| Ad groups | `ObjectMatchingPage` legado, alvos nos filtros do topo | tela própria no fluxo de diálogo, um seletor **single-select por eixo** |
| Ads | classificar formato ad a ad (adiado, sem UI) | manter as traduções da plataforma, mais exceção pontual |
| Publicação | não existe | alerta de alterações não materializadas + botão Publicar |
| Cobertura | não existe | card de "% do investimento classificado" nos relatórios |

## Notas por passo

- **4** — DDL em `binder_app_backend/docs/architecture.md`; use a skill `create-entities`.
  Revise a migration gerada: chave composta é onde o TypeORM mais erra. Precisa do índice
  único de apoio `sub_grouping (grouping_id, id)`.
- **5** — Vai revelar sujeira. Ao transformar `campaign_id` de digitado em derivado, maps de
  ad apontando para campanha diferente da do ad_group vão bater na FK. É o sistema
  funcionando: trate como relatório a resolver, não como falha do script. Enquanto a tela de
  campanhas não trocar de endpoint (passo 9), a UI continua gravando em `platform_object_map`
  — migrar e trocar a tela de cada nível juntos evita a dupla escrita.
- **6** — Ver D5 e D8 antes de desenhar a fila.
- **7** — Transporte por HTTP (`GET /enrichment/publications/pending`), simétrico ao
  catalog-api. As transições de status dependem de D4.
- **8** — `'Não informado'` entra aqui: o gold base guarda `NULL` quando falta dimensão, e o
  enriquecido transforma em balde explícito. Estender o teste de conservação para o gold
  enriquecido.
- **9** — A Catalog API agora oferece objetos deletados e campanhas antigas sem gasto: as
  listas de "Disponíveis" precisam mostrar o status do objeto, e as filas de pendências
  precisam ser ordenadas por investimento, senão enterram o que importa. Ad groups e ads
  precisam sair do `ObjectMatchingPage` antes de ganhar feature.
- **10** — Só depois de o gold enriquecido estar reconciliando.

## Decisões em aberto

| # | Decisão | Contexto | Bloqueia |
|---|---|---|---|
| D1 | Unicidade do binding com conta compartilhada: `UNIQUE (platform, external_account_id, external_campaign_id)`? | `platform_account` tem uma linha por cliente. Com `UNIQUE (platform_account_id, external_campaign_id)`, a mesma campanha pode ser vinculada a dois clientes — e o `LEFT JOIN` do enriquecido, pela chave externa, duplica a métrica. Pergunta de negócio: uma campanha da plataforma pertence a exatamente um cliente? | 4 |
| D2 | Como amarrar o eixo à campanha vinculada | A DDL garante que o valor pertence ao eixo, mas não que o eixo (`grouping.campaign_id`) é da campanha do binding. Opções: copiar `campaign_id` na classificação amarrado por FK composta ao binding (cópia que não diverge), ou trigger | 4 |
| D3 | FK `(channel_id, buying_type_id) → channel_buying_type` e cliente da campanha = cliente da conta | Hoje essas regras só existem em texto; a invariante 4 pede constraint | 4 |
| D4 | Contrato de status da publicação entre DAG e backend | O DAG precisa marcar `processing`/`materialized`/`failed`; falta endpoint e autenticação do DAG no backend | 7 |
| D5 | De onde vem o investimento por `ad_format` | A fila de formatos não traduzidos é ordenada por investimento, que está no lake; o backend não lê o MinIO. Candidato: endpoint novo no catalog-api | 6 |
| D6 | Caminho de serving do card de cobertura | Não existe serving do gold para o frontend. Candidato sem serving: o DAG grava estatísticas de cobertura na própria publicação | 9 |
| D7 | Chave do join de enriquecimento: `campaign_id` sozinho ou `(ad_account_id, campaign_id)` | O fato não traz conta (vem de `ads`); IDs do TikTok são únicos globalmente | 8 |
| D8 | `ad_format` nulo e sub-formato | `native_value` é `NOT NULL`, e posts autorizados vêm com `ad_format` nulo: o ETL emite um valor explícito? `ad_format` não carrega duração: de onde vem o sub-formato (15s, 30s) que `sub_format_id NOT NULL` exige? | 6 |
| D9 | Ordem dos passos 4–9 | Linear como na tabela, ou em fatias verticais: nível campanha de ponta a ponta (4→5→7→8→9, só binding), depois ad_group com eixos, depois formato. Fatias expõem integração cedo e evitam a dupla escrita do passo 5 | 4 |
| D10 | Recuar o `start_date` do Airbyte antes de 2025 | Traria 85 campanhas sem gasto e o histórico anterior. Decisão de negócio, não de correção | — |

## Diário

Registro datado, só acrescentado. Números medidos moram aqui, não na referência.

- **10/09** — Arquitetura de enriquecimento adotada nos três repositórios; `.cursor/rules`
  migradas para `CLAUDE.md` + `docs/` + skills.
- **11/09** — **Diagnóstico da perda de linhas no gold.** Com `start_date` 2026-01-01 e sem
  deletados, o fato tinha 8.744 linhas e o gold 4.216. Todas as perdidas tinham `campaign_id`
  fora da dimensão `campaigns` (54 de 68 campanhas). Em dinheiro, R$ 30.065,43 (1,45% do
  spend), concentrados em 01–02/01/2026. Descartados: fan-out, hierarquia divergente no join,
  duplicata no fato. A hipótese registrada em 10/09 (dimensões em Full Refresh + Overwrite)
  estava errada: o raw já acumulava um arquivo por sync; faltava a extração trazer os
  objetos.
- **11/09** — **Extração refeita** (Include Deleted, `start_date` 2025-01-01, Clear data). O
  primeiro sync depois de mudar a configuração foi incremental, porque limpar o raw não
  reseta o cursor. Depois do Clear data:

  | | Silver (fato) | Gold |
  |---|---|---|
  | Linhas | 22.739 | 21.781 |
  | Spend | R$ 9.073.343,49 | R$ 9.073.343,49 |

  As 958 linhas restantes eram de 85 campanhas modificadas antes de 2025, todas com as 59
  métricas zeradas. 66% do fato (14.967 linhas) era ad × dia sem nenhuma métrica.
  `advertiser_id` veio nulo em 100% do relatório. "14 campanhas para 17 advertisers" era
  extração incompleta: passaram a ser 70.
- **11/09** — `ad_format` em 675 ads: `SINGLE_VIDEO` 603 (R$ 8,01 mi), `CAROUSEL_ADS` 62
  (R$ 952 mil; 7 ACO), nulo 10 (R$ 115 mil; todos `BC_AUTH_TT`). Três valores, não ~6.
- **14/09** — Passo 2: dedupe na chave natural mínima. Nenhum número mudou — nenhum objeto
  tinha trocado de pai.
- **14/09** — Passo 1a: filtro de linhas sem atividade no silver. Fato de 22.798 para 7.797
  linhas, spend de R$ 9.091.703,44 idêntico em bronze, silver e gold. Com isso o teste de
  conservação silver × gold passou a passar mesmo com `inner join`, porque os órfãos eram
  todos vazios — por isso o passo 1 ganhou teste sintético próprio.
- **14/09** — Passo 1: gold com `LEFT JOIN` partindo do fato, transform de gold virou função
  pura. **Decidido:** dimensão ausente vira `NULL` no gold base; `'Não informado'` fica para
  o enriquecido.
- **14/09** — Passo 3: bronze acumula. Duas rodadas seguidas deram contagens idênticas; o
  read-modify-write no mesmo caminho Delta com `.cache()` funcionou em execução real.
- **14/09** — Infra fora da numeração: testes reorganizados em
  `tests/transformers/{platform}/{layer}/`; a suíte usa uma `SparkSession` só
  (`tests/conftest.py`); o catalog-api roda em `local[2]` (`CATALOG_SPARK_MASTER`). A
  flakiness de "Python worker exited" vinha da churn de sessões e da disputa de cores.
- **14/09** — Documentação reorganizada: estado de trabalho saiu de `CLAUDE.md` e `docs/`
  para este arquivo, nos três repositórios.
