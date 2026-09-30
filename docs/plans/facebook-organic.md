# Iniciativa: facebook-organic

> **Estado de trabalho, não referência.** Este arquivo diz o que falta, o que está em aberto
> e o que aconteceu no caminho. Como o sistema funciona está no `CLAUDE.md` e em `docs/`.
> Quando a iniciativa encerrar, o desenho implementado sobe para `docs/architecture.md` e
> `docs/project-structure.md`, e este arquivo é apagado — o histórico fica no git.

Iniciativa só do `binder_etl`. Nada aqui toca o backend nem o Bridge.

## Próxima ação

Fechar D1 com o sync de 04/10: rodar o medallion e ver se o `media_views_lifetime` do post
`280663778456254_122161958420250905` (353 em 30/09) cai no dia em que ele completa 2 anos.
Depois, apagar este plano e tirar o ponteiro do `CLAUDE.md`.

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
| 4 | Gold: `post_daily_metrics` e `page_daily_metrics` | feito 30/09 |
| 5 | Testes: unitários por transform + conservação por post | feito 30/09 |
| 6 | Registro em `TRANSFORMERS`/`PLATFORMS` | feito 30/09 — a rota do catálogo já devolvia 404 (o `KeyError` de `get_catalog_transforms` vira 404); não precisou mudar |
| 7 | DAG `facebook_organic_daily` | feito 30/09 |
| 8 | Desenho implementado sobe para `docs/`; plano apagado | desenho em `docs/` feito 30/09; plano fica até fechar D1 |

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
