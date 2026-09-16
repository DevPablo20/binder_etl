# Iniciativa: bridge-enrichment

> **Estado de trabalho, não referência.** Este arquivo diz o que falta, o que está em aberto
> e o que aconteceu no caminho. Como o sistema funciona está no `CLAUDE.md` e em `docs/` de
> cada repositório. Quando a iniciativa encerrar, este arquivo é apagado — o histórico fica
> no git.

Plano único dos três repositórios (`binder_etl`, `binder_app_backend`,
`binder_app_frontend`). Os `CLAUDE.md` do backend e do frontend apontam para cá.

## Próxima ação

Começar o **passo 4** — nenhuma decisão o bloqueia, e a fatia 1 está livre até o gold. D5, D6,
D8 e D10 só aparecem nas fatias 3 e no card de cobertura.

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
| 4 | Tabelas novas do Bridge, índices de apoio e FKs compostas | backend | pendente — desbloqueado |
| 5 | Migrar dados de `platform_object_map` para as tabelas novas | backend | vazio — nada a migrar |
| 6 | Tradução de formato + fila de pendências | backend | pendente — bloqueado por D5, D8 |
| 7 | `enrichment_publication`, `enrichment_run`, snapshot e endpoints | backend | pendente — desbloqueado |
| 8 | Gold enriquecido: fetch, três `LEFT JOIN`, coluna `MAP`, teste de invariante | etl | pendente — desbloqueado |
| 9 | Telas de configuração, alerta de não materializado, card de cobertura | frontend | pendente — card bloqueado por D6 |
| 10 | Remover `platform_object_map` e `assertLevelFields` | backend | pendente |

Nas conversas e commits de 14/09, "Fase 1 / Etapas 1–4" corresponde aos passos 2, 1a, 1 e 3,
nessa ordem. Daqui em diante, só a numeração desta tabela.

## Ordem de execução

Passo 4 inteiro primeiro, depois três fatias verticais. O passo 4 **não** se fatia: a cadeia de
FKs compostas foi desenhada como um todo, e quebrá-la em três migrations multiplica a chance de
o TypeORM errar uma chave composta — o ponto mais frágil do passo.

| Fatia | Nível | Entrega | Passos |
|---|---|---|---|
| **1** | campanha | gasto por campanha de negócio, channel e buying type no gold enriquecido | 7, 8, 9 |
| **2** | ad_group | eixos (Território, Persona) propagando até ad × dia | 8, 9 |
| **3** | formato | formato e sub-formato traduzidos do nativo | 6, 8, 9 |

A fatia 1 atravessa o trecho mais incerto da iniciativa — publicação → fetch do DAG → join no
Spark → teste de conservação — com a carga mais simples possível. É ela que prova o mecanismo,
e é apresentável sozinha.

As cinco regras de escopo acompanham o corte: regras 1, 2 e 3 são do binding (fatia 1), a regra
5 é do eixo (fatia 2), a regra 4 é do formato (fatia 3).

O gold enriquecido é tocado três vezes, ganhando um `LEFT JOIN` por fatia. O teste de
conservação roda em cada uma — delta menor para depurar quando não fechar.

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
  único de apoio `sub_grouping (grouping_id, id)`. O baseline já traz a unicidade de D1
  (`platform_account` em `UNIQUE (platform_id, external_account_id)`) — é ela que faz
  `UNIQUE (platform_account_id, external_campaign_id)` bastar no binding. Em base com dado,
  antes de aplicar essa unicidade, rodar:

  ```sql
  SELECT platform_id, external_account_id, COUNT(DISTINCT client_id)
  FROM platform_account GROUP BY 1,2 HAVING COUNT(DISTINCT client_id) > 1;
  ```

  Tem que voltar vazio; cada linha é uma decisão manual de qual cliente fica.

  **Amarração de escopo (D2/D3).** As cinco regras da tabela em `architecture.md` entram
  juntas, pela mesma técnica: coluna de escopo copiada na linha + FK composta para a origem.
  São 4 colunas copiadas (`client_id` e `platform_id` no binding, `campaign_id` na
  classificação e na atribuição) e 7 índices únicos de apoio. Dois pontos de atenção na
  implementação:

  - O DTO de classificação de ad_group **não expõe `campaignId`**. O backend lê do binding e
    grava a cópia. Se o operador pudesse digitar, a cópia deixaria de ser cópia.
  - Trocar `campaign_id` de um binding classificado exige apagar as
    `platform_ad_group_classification` daquele binding na mesma transação, antes do update —
    senão a FK `(campaign_id, grouping_id) → grouping` barra a operação. Mesmo padrão do
    `updateMany` de `platform-account.service.ts`, que já apaga os filhos ao trocar o cliente
    de uma conta.

  **Migration.** Não há base além do Postgres local em docker: em vez de migration
  incremental, derrubar o banco, regenerar o baseline `default` do zero e rodar os seeds — o
  mesmo caminho usado ao fechar a D1. Uma migration só para revisar, com a cadeia de FKs
  compostas inteira à vista. Enquanto isso valer, é o caminho preferido; quando existir base
  com dado, volta a ser migration incremental.

  Junto com o passo 4, mover `Grouping`/`SubGrouping` de `src/media/grouping/` para
  `src/business/grouping/`. Sem mudança de schema — só tira a única FK de Media para Business.
- **5** — **Não há o que migrar.** `platform_object_map` está vazio, ninguém classificou nada
  ainda e não existe instância além do Postgres local. O passo deixa de ser script de migração
  e vira uma checagem antes do passo 4:

  ```sql
  SELECT count(*) FROM platform_object_map;
  ```

  Voltando zero, os dois custos que o passo carregava desaparecem: não há relatório de sujeira
  a resolver nem janela de dupla escrita. Se um dia vier diferente de zero, o raciocínio
  antigo volta a valer — ao transformar `campaign_id` de digitado em derivado, maps de ad
  apontando para campanha diferente da do ad_group batem na FK, e isso é relatório a resolver,
  não falha do script.
- **6** — Ver D5 e D8 antes de desenhar a fila.
- **7** — Desenho fechado na D4, em `binder_app_backend/docs/architecture.md`: duas tabelas
  (`enrichment_publication` e `enrichment_run`), snapshot em tabelas próprias com valores
  resolvidos, e duas chamadas HTTP do DAG — leitura no começo, resultado no fim. Sem
  `processing`. Guard próprio com chave de API em header; nada de usuário de serviço.
- **8** — `'Não informado'` entra aqui: o gold base guarda `NULL` quando falta dimensão, e o
  enriquecido transforma em balde explícito. Estender o teste de conservação para o gold
  enriquecido — e testar os **dois lados**: `LEFT` protege contra perder linha, a chave do join
  protege contra duplicar. Chave e unicidades em `binder_app_backend/docs/architecture.md`.
- **9** — O filtro de campanha da tela de ad groups é **navegação, não dado**: serve para
  achar os ad groups, e nada dele é gravado na linha. Hoje o `ObjectMatchingPage` grava o
  filtro do topo em `platform_object_map.campaign_id` — a tela nova não repete isso, porque a
  campanha passa a ser derivada do binding.
  A Catalog API agora oferece objetos deletados e campanhas antigas sem gasto: as
  listas de "Disponíveis" precisam mostrar o status do objeto, e as filas de pendências
  precisam ser ordenadas por investimento, senão enterram o que importa. Ad groups e ads
  precisam sair do `ObjectMatchingPage` antes de ganhar feature.
- **10** — Só depois de o gold enriquecido estar reconciliando.

## Decisões em aberto

| # | Decisão | Contexto | Bloqueia |
|---|---|---|---|
| D5 | De onde vem o investimento por `ad_format` | A fila de formatos não traduzidos é ordenada por investimento, que está no lake; o backend não lê o MinIO. Candidato: endpoint novo no catalog-api | 6 |
| D6 | Caminho de serving do card de cobertura | Não existe serving do gold para o frontend. A D4 resolveu metade: cobertura é métrica de **rodada**, não de publicação, e cabe em `enrichment_run` — o DAG grava ao fechar a rodada e o backend serve sem tocar no lake. Falta decidir o resto do serving de relatório | 9 |
| D8 | `ad_format` nulo e sub-formato | `native_value` é `NOT NULL`, e posts autorizados vêm com `ad_format` nulo: o ETL emite um valor explícito? `ad_format` não carrega duração: de onde vem o sub-formato (15s, 30s) que `sub_format_id NOT NULL` exige? | 6 |
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
- **16/09** — **D1 fechada: conta de plataforma pertence a exatamente um cliente.** A
  unicidade que protege a conservação mora em `platform_account`
  (`UNIQUE (platform_id, external_account_id)`), não numa chave nova no binding: com ela
  `platform_account_id` vira bijeção com as coordenadas externas, e o
  `UNIQUE (platform_account_id, external_campaign_id)` que já está na DDL passa a bastar —
  sem desnormalizar `external_account_id` no binding. A razão é que o fato do lake não
  carrega cliente: só as coordenadas externas chegam ao join, então duas linhas para a mesma
  conta duplicariam métrica, contra a invariante 5.

  Descoberto no caminho: o `@Unique(['platform','externalAccountId','client'])` de 24/07 não
  resolvia o caso que o motivou — campanha Always On com uma conta veiculando até março/2026
  e outra de março em diante. Duas contas para o **mesmo** cliente nunca esbarraram na chave
  de duas colunas; o que ela proíbe é a mesma conta repetida. O modelo alvo já cobre esse
  caso sem multi-cliente: `campaign_id` no binding não é único, então as duas contas ganham
  bindings próprios apontando para a mesma campanha de negócio.

  Revertido nos três repositórios: constraint + migration, DTO bulk (`clientIds[]` →
  `clientId`) e `createMany` sem produto cartesiano, `AccountMatchingPage` com tabela única
  e diálogo single-select, e as afirmações de multi-cliente em `CLAUDE.md`,
  `docs/bridge-matching.md` e `docs/domain-model.md`. Deixado de fora de propósito:
  `CampaignMatchingPage` ainda tem a seção "dois ou mais clientes" — vira código morto e
  morre na troca de endpoint do passo 9, junto com o resto da tela.
- **16/09** — **D2 e D3 fechadas juntas: eram a mesma decisão.** Ao procurar como amarrar o
  eixo à campanha, apareceram cinco validações imperativas no
  `platform-object-map.service.ts` com a mesma forma — `assertClientAlignment`,
  `assertChannelPlatform`, `assertBuyingTypeOnChannel`,
  `assertFormatSubFormatConsistency` e `loadSubGroupingsForCampaign`. Todas dizem *a
  classificação escolhida tem que pertencer ao escopo que a identificação de cima
  estabeleceu*. A D3 era as regras 1–3 dessa lista; a D2, a regra 5.

  **Decidido:** as cinco viram constraint no passo 4, pela técnica única de coluna de escopo
  copiada + FK composta para a origem. A cópia não diverge porque o banco não deixa. Custo
  medido: 4 colunas, 7 índices únicos de apoio. Ganho: as cinco validações somem do
  TypeScript junto com `assertLevelFields`.

  **Decidido:** trocar a campanha de negócio de um binding classificado apaga as
  classificações de ad_group daquele binding, em cascata na mesma transação. Segue o
  precedente de trocar o cliente de uma conta.

  Vocabulário novo, que faltava para explicar por que cada nível tem as colunas que tem:
  **identificação** (a que entidade de negócio o objeto pertence — declarada uma vez, herdada
  para baixo) × **classificação** (que atributos o objeto tem). O nível de ad_group só tem
  classificação, porque herda a identificação da campanha. Subiu para o bloco compartilhado
  dos três `CLAUDE.md`.

  Duas coisas que a discussão revelou e entraram no passo 4 e no passo 9: o DTO de ad_group
  não pode expor `campaignId` (senão a cópia deixa de ser cópia), e o filtro de campanha da
  tela nova é navegação e não dado — hoje o `ObjectMatchingPage` grava o filtro do topo na
  linha, que é a raiz da confusão entre cadastro e atribuição.

  `Grouping`/`SubGrouping` vão de Media para Business: eram a única entidade de Media com FK
  para Business, e descrevem como o cliente fatia a campanha dele, não como a mídia foi
  comprada.
- **16/09** — **D9 fechada: passo 4 inteiro, depois três fatias verticais.** O passo 4 não se
  fatia porque a cadeia de FKs compostas foi desenhada como um todo — três migrations
  triplicariam a chance de errar uma chave composta. De 5 a 9, o corte é por nível: campanha,
  ad_group, formato. A fatia 1 existe para atravessar cedo o trecho mais incerto (publicação →
  fetch do DAG → join no Spark → conservação) com a carga mais simples possível.

  **Descoberto ao decidir: o passo 5 não tem trabalho.** `platform_object_map` está com zero
  linhas, ninguém classificou nada e não há instância além do Postgres local em docker. Some o
  script de migração, some o relatório de sujeira e some a janela de dupla escrita — que era o
  argumento principal a favor de fatiar. Fatiar continua valendo, agora só pelo feedback de
  integração.

  **Enquanto não houver base com dado**, mudança de schema é derrubar o banco e regenerar o
  baseline `default`, não migration incremental. Vale para o passo 4.

  D4 e D7 ficam para fechar no papel, antes da fatia 1 chegar ao gold.
- **16/09** — **D4 fechada.** O desenho completo está em
  `binder_app_backend/docs/architecture.md`; aqui fica o que a discussão revelou.

  **Os dois relógios.** O gold enriquecido é produto do fato (muda todo dia) e da configuração
  (muda quando alguém publica). Por isso ele é reconstruído a cada rodada diária, mesmo sem
  publicação nova. Consequência: uma publicação é criada uma vez e usada em **muitas rodadas**.

  **Erro de modelagem corrigido.** `status` estava na publicação, espremendo N rodadas num
  campo: a linha era reescrita todo dia, uma publicação materializada voltava a `failed` por
  erro transitório de Spark, e não dava para distinguir "falhou uma vez" de "falha há 30 dias".
  Falha passou para `enrichment_run`, tabela nova. A publicação ficou com
  `pending | materialized | superseded`.

  **`processing` foi removido.** A operação é idempotente e o Airflow roda uma instância por
  vez — não há corrida a proteger, e some o risco de publicação presa se o DAG morrer no meio.

  **Sem limite de tentativas.** O receio de "loop infinito" não se aplica: é uma tentativa por
  dia, e a reconstrução diária aconteceria de qualquer forma por causa do fato novo. Travar
  depois de N falhas congelaria o gold também em relação ao fato. O remédio é visibilidade —
  contar falhas consecutivas em `enrichment_run`.

  **Snapshot em tabelas próprias, com valores resolvidos.** Guardar `sub_grouping_id` não
  congela nada: renomear o valor no dia seguinte mudaria o resultado de uma publicação
  supostamente imutável. Resolver no momento da publicação é o que torna a rodada reprodutível.

  **Autenticação por chave de API em header, com guard próprio.** O `AuthGuard` global busca um
  usuário real no banco e monta `UserSignature` com role e empresas — molde de pessoa, não de
  robô. Regra nova no `CLAUDE.md` do backend.

  **Sem publicação, o gold enriquecido é construído vazio.** Snapshot vazio é snapshot com zero
  linhas; os `LEFT JOIN` dão `NULL` e viram `'Não informado'`. Menos código que pular, e o
  resultado é passa-through puro — mesma soma e mesma contagem do gold base, o canário mais
  sensível a fan-out que existe.

  **D6 ficou meio resolvida:** cobertura é métrica de rodada, não de publicação, e cabe em
  `enrichment_run`.

  Confusão desfeita no caminho, que vale registrar: a tela de **configuração** nunca depende da
  materialização — ela compara o catálogo do lake contra as tabelas do Bridge, ambos alcançáveis
  pelo backend. Só a tela de **relatório** depende do gold, e essa ainda não existe (D6).
- **16/09** — **D7 fechada: o join de enriquecimento casa pelo id do objeto, sem a conta.**
  `campaign_id`, `ad_group_id` e `ad_id` vêm do fato e estão sempre presentes; `ad_account_id`
  é derivado da dimensão `ads` e pode faltar — e `NULL` não casa com `NULL`. Incluir a conta
  amarraria a cobertura do enriquecimento à completude de uma dimensão que o `LEFT JOIN` do
  gold base existe para sobreviver sem.

  **Medido no gold atual** (7.819 linhas, R$ 9.114.522,76): 0 linhas sem `ad_account_id`, 0 sem
  `campaign_id`, e **0 objetos sob mais de uma conta** nos três níveis — 55 campanhas, 314
  ad_groups, 558 ads. Nenhuma divergência de hierarquia. Hoje as duas chaves se comportam
  igual; a decisão é sobre o modo de falha quando o dado deixar de ser perfeito.

  **Preço da chave frouxa, e como foi pago.** Sem a conta na chave, dois candidatos para o
  mesmo id fariam a linha do fato duplicar — dinheiro dobrado, invariante 5 quebrada para cima.
  Entram três unicidades por coordenada externa no Bridge:
  `UNIQUE (platform_id, external_campaign_id | external_ad_group_id | external_ad_id)`. Com
  elas o fan-out é estruturalmente impossível e o erro aparece na tela ao salvar, não de
  madrugada no teste de conservação. É a mesma forma da D1: a unicidade tem que morar onde o
  join enxerga.

  **Custo não previsto:** as unicidades de ad_group e de ad exigiram `platform_id` nas duas
  classificações. As cópias de escopo passaram de 4 para 6.

  **Conta ausente não é remendada.** `ad_account_id` nulo continua nulo no enriquecido. É coluna
  técnica de rastreio; cliente e campanha de negócio vêm do snapshot e não dependem dela.
  Preencher a partir do binding criaria segunda fonte para o mesmo atributo, contra a
  invariante 2, sem ganho de negócio.
