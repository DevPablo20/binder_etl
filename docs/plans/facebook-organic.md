# Iniciativa: facebook-organic

> **Estado de trabalho, não referência.** Este arquivo diz o que falta, o que está em aberto
> e o que aconteceu no caminho. Como o sistema funciona está no `CLAUDE.md` e em `docs/`.
> Quando a iniciativa encerrar, o desenho implementado sobe para `docs/architecture.md` e
> `docs/project-structure.md`, e este arquivo é apagado — o histórico fica no git.

Iniciativa só do `binder_etl`. Nada aqui toca o backend nem o Bridge.

## Próxima ação

D1 medida e **fechada em 06/10**: não há corte por aniversário de 2 anos, e a regra que a D1
propunha não descreve o que acontece (diário). No lugar dela entra a **D3**: levar o horizonte
por métrica — hoje só no `dashboards/texaco_rj/extract.py` — para dentro do pipeline, marcando
"não medido" em vez de zero. Com a D3 fechada, apagar este plano e tirar o ponteiro do
`CLAUDE.md`.

## Objetivo

Levar ao lake o conteúdo das páginas do Facebook — posts e página — em camadas
bronze/silver/gold, reaproveitando a estrutura medallion do TikTok, **com o conector oficial do
Airbyte como está**. O ganho central é a **evolução diária das métricas de post**, que a fonte
só entrega como total acumulado.

**Fora do escopo:**

- **Cópia ou conector próprio.** Decisão tomada: trabalhar com o que o conector oficial
  entrega. Por isso não há `breakdown=is_from_ads` nem `since`/`until`.
- **Orgânico × pago nas visualizações.** Sem breakdown, não se separa. O gold chama as coisas
  pelo que são: desempenho total.
- **Marcar post impulsionado.** Nenhum campo do conector indica. Vai para o backlog via join
  com o Meta Ads.
- **Integração com o backend** (Catalog API, Bridge, telas). A plataforma não tem hierarquia
  campanha → anúncio.
- **Instagram.** Os posts da página são publicação cruzada do Instagram; o Facebook é só uma
  fatia do desempenho. Backlog.

## Passos

| # | Passo | Status |
|---|---|---|
| 0 | Extração, seleção de campos e diagnóstico com o Business Suite | feito 30/09 |
| 1 | `tables.py`: streams, schemas explícitos, chaves, métricas | feito 30/09 |
| 2 | Bronze: foto diária de todas as streams | feito 30/09 |
| 3 | Silver: dimensões, séries e página diária | feito 30/09 |
| 4 | Gold: `post_daily_metrics` e `page_daily_metrics` | feito 30/09 — substituída pela gold orgânica compartilhada (ver diário) |
| 5 | Testes: unitários por transform + conservação por post | feito 30/09 |
| 6 | Registro em `TRANSFORMERS`/`PLATFORMS` | feito 30/09 — a rota do catálogo já devolvia 404 (o `KeyError` de `get_catalog_transforms` vira 404); não precisou mudar |
| 7 | DAG `facebook_organic_daily` | feito 30/09 |
| 8 | Desenho implementado sobe para `docs/`; plano apagado | desenho em `docs/` feito 30/09; plano fica até fechar D3 |

## Onde está o desenho

- Conexão, seleção de campos e agendamento: `airbyte/README.md`, seção Facebook Pages.
- Fluxo, regras de cada camada e conservação: `docs/architecture.md`, seção Facebook orgânico.
- Estrutura e diferenças em relação ao molde do TikTok: `docs/project-structure.md`.
- Regras que valem sempre: `CLAUDE.md` (sessão Spark em UTC; uma foto por dia no bronze).

## Decisões em aberto

| # | Decisão | Como fecha |
|---|---|---|
| D1 | **O `lifetime` perde o que tem mais de 2 anos?** A doc de Insights diz que `lifetime` é o período disponível, até 2 anos. Se sim, o delta fica negativo no aniversário de 2 anos e a regra passa a ser: delta só enquanto `snapshot_date < created_date + 24 meses`, com `is_beyond_retention` | Post 6 (`280663778456254_122161958420250905`, publicado 03/10/2024, 353 visualizações em 30/09) no sync de 04/10. Se cair, a regra entra; se não, sai |

## Diário

**29/09** — Primeira extração (sync 27): 1 página, 692 posts, 3.924 linhas de
`post_insights`, 15 de `page_insights`. Stream `post` só com `id`, `from`, `name`,
`description`, e `name`/`description` nulos. `page_insights` sem id de página.

**29–30/09** — Mais 3 syncs (28, 33, 34). O arquivo do sync 27 foi removido à mão antes de
mudar as colunas. Campos novos do `post` a partir do sync 33 (29/09 17:35). O sync 34 rodou às
01:01 (São Paulo), com o cron à 01:00. Os mesmos 692 posts em todos os syncs, publicados de
30/06/2024 a 29/09/2026. Nenhum delta negativo. Variação total entre 17:35 e 01:01: +115
visualizações nos 692 posts; o post novo de 29/09 foi 10 → 13 → 18 → 40.

**30/09** — Conferência com o Business Suite (aba Facebook):

- `page_media_view` com `end_time` 27/09 07:00Z = 842.533 = Business Suite **26/09**. Com 28/09
  07:00Z = 485.620 = **27/09**. Regra de −1 dia confirmada.
- Post publicado às 14:50:46 na API = 11:50 no Business Suite. `created_time` em UTC confirmado.
- Posts `…122270722622250905`, `…122270779214250905`, `…122141010062250905`,
  `…122161958420250905`: visualizações, espectadores e reações idênticos à API.
- Post `…122205957698250905`: 84.090 visualizações = **781 orgânicas + 83.309 de anúncios**.
  O `post_media_view` de post impulsionado inclui distribuição paga.
- Página, 25–29/09: 2.813.446 visualizações, sendo **6.400 orgânicas**.
- Espectadores maiores que visualizações acontece também no Business Suite (49 e 58 no post de
  29/09) — comportamento da Meta.
- A lista "Todos os conteúdos" soma Facebook e Instagram nos posts cruzados; comparar sempre
  pela aba Facebook do post.
- Espectadores muito abaixo das visualizações em posts de 2024 (178/3, 353/1). Não é o corte de
  2 anos — o post de 03/10/2024 ainda não tem 2 anos. Hipótese: `post_total_media_view_unique`
  é métrica nova (entrou no conector em 17/08/2026) e subestima posts antigos. Documentar na
  coluna; não corrigir.

**30/09** — Decidido não criar cópia do conector. Seleção de campos refeita e nova extração
disparada.

**30/09** — Extração refeita (sync 38, 11:16 São Paulo, manual) com a seleção de campos nova:

- `page` com `fan_count` = `followers_count` = 1.783; `username` nulo (a página não tem).
- `page_insights` com `id` (`{page_id}/insights/{metric}/{period}`). Os arquivos anteriores não
  têm a coluna: ler a pasta sem schema explícito escolheu o schema de um arquivo antigo e o
  `id` sumiu sem erro — a regra do schema explícito provada na prática.
- `post`: `is_published` true, `is_hidden` e `is_expired` false e `from` = a própria página nos
  692 posts. `shares` é string JSON `{"count": N}`; o post de 29/09 tem 1, igual à prévia do
  Business Suite. `attachments` veio `{}` em todos.
- **A janela do `page_insights` anda.** O sync 38 trouxe `end_time` 28 e 29/09 (dias 27 e 28);
  28/09 = 483.992, igual ao Business Suite. Às 01:01 o dia 28 ainda não existia; às 11:16 já.
  A Meta publica o dia entre 04:00 e 14:00 UTC do segundo dia seguinte. Antecipar o atraso
  exigiria mover o cron para o fim da manhã, desalinhando a foto dos posts do fechamento do dia.
  Mantido 01:00.
- Post 6 (`…122161958420250905`) ainda em 353. Post de 29/09: 40 → 51. Nenhum delta negativo
  entre os syncs 34 e 38.

**30/09** — Passos 1 a 7 implementados na branch `arch/facebook-organic` e rodados contra o
MinIO de `192.168.10.80` (o de desenvolvimento local não tem o raw do Facebook):

- Bronze: 2 fotos por stream — 29/09 (sync 34 venceu os syncs 28 e 33) e 30/09 (sync 38).
  `page_insights` com o `id` reconstruído nas 15 linhas da foto de 29/09.
- Silver: 692 posts (373 reels, 315 fotos, 3 status, 1 vídeo); 1.384 fotos de post;
  45 linhas de página (26, 27 e 28/09 × 15), sem buraco; 1 foto de seguidores (30/09, 1.783).
- Gold `post_daily_metrics`: 1.384 linhas. Em 29/09, 691 `pre_existing` e 1 `new_post` (o
  post de 29/09, delta 40). Em 30/09, +17 visualizações e +18 de alcance novo nos 692 posts.
- Gold `page_daily_metrics`: 26–28/09 igual ao Business Suite (842.533, 485.620, 483.992). O
  dia 28/09 tem o primeiro seguidor orgânico (`fan_adds_unpaid` = 1). Seguidores ainda nulos:
  a foto de 30/09 só casa com o `metric_date` 30/09, que chega por volta de 02/10.
- Testes: 28 do facebook_organic verdes contra o MinIO remoto (conservação incluída); suíte
  inteira verde no MinIO local (50 passed, 2 skipped — a conservação do facebook, sem dados ali).
- D2 fechada: seguidores entram por `snapshot_date = metric_date` (a foto que fecha o dia D
  traz os seguidores ao fim de D).
- A sessão Spark passou a ser fixada em UTC para todo o repo: a máquina de dev está em
  America/Sao_Paulo e os containers em UTC, e as datas derivadas dependiam disso.

**30/09** — **Gold reorganizada como camada de consumo, compartilhada com o Instagram.** A gold
antiga expunha a mecânica de construção (placar acumulado, `baseline_kind`, horas entre fotos)
e parecia bronze. Decisão:

- A série diária (total e delta por foto) passa a ser padronização e vai para o silver
  (`post_metrics_daily`), com as colunas de controle para auditoria.
- A gold vira `gold/organic/`, com as duas redes nas mesmas tabelas, nomes de negócio e
  `metrics_scope` (`organic` × `total`): `content` (dimensão do post, sem métrica),
  `content_daily` (cada métrica em par: o do dia e `_total` até o dia), `account_daily` e
  `stories`. Cada rede grava só a sua partição (`replaceWhere` em `platform`).
- O total atual de um post é o `_total` na última data do post; ele não pode vir da soma dos
  dias, que só conta a atividade desde o início do acompanhamento.

Implementado e rodado contra o MinIO de `192.168.10.80`: 692 posts em `content`, 1.384 linhas em
`content_daily`, 3 dias em `account_daily`. Testes: 54 orgânicos verdes contra o remoto
(conservação incluída); suíte inteira verde no local (73 passed, 6 skipped). As tabelas antigas
continuam no MinIO e não são mais escritas: `gold/facebook_organic/post_daily_metrics`,
`gold/facebook_organic/page_daily_metrics` e `silver/facebook_organic/post_insights_snapshot`.

**06/10** — **D1 medida nos 7 dias de fotos (29/09 a 05/10) e fechada.** O que o lake mostra,
lido do `silver/facebook_organic/post_metrics_daily`:

- O post-testemunha (`…122161958420250905`, publicado 03/10/2024) caiu de **353 para 13
  visualizações entre as fotos de 29/09 e 30/09** — e ficou em 13 até 05/10. Ele completou 2
  anos em 03/10, três dias **depois** da queda: não foi o aniversário.
- `media_views` teve **39 quedas, todas na foto de 30/09**, e nenhuma nas fotos de 01 a 05/10.
  Numa janela móvel por post, cairia gente todo dia; não cai.
- Os 39 estão concentrados em quem publicou de **2024-06 a 2024-10** (nada de 2024-11 em
  diante), mas são **só ~1/3 do acervo dessa faixa** — a mediana de `media_views` dos posts de
  2024-07, 08, 09 e 10 continua em 135, 139, 103 e 115 na foto de 05/10, contra 142–497 nos
  meses de 2025–2026. **O `media_views` do Facebook não tem corte de 2 anos: ele sobrevive aos
  27 meses do post mais antigo da página.** A queda de 30/09 foi um recálculo em lote, que
  levou de 16% a 75% do valor de quem atingiu.
- `reach` e `reactions_total` **não tiveram uma única queda**. O `reach` de post antigo já era
  baixo por outro motivo, o da nota de 30/09: a mediana por mês de publicação é 1–4 até
  2025-04 e salta para 154+ a partir de 2025-05 — comportamento de métrica nova
  (`post_total_media_view_unique`, no conector desde 17/08/2026), não de retenção.

Conclusão: **a regra proposta na D1 estaria errada.** Marcar `is_beyond_retention` por
`created_date + 24 meses` esconderia posts de 27 meses com visualização boa e não pegaria o
que de fato aconteceu — uma queda em lote, numa data só, visível apenas como delta negativo
grande. O sinal confiável é a própria queda, não a idade. Vira D3.
