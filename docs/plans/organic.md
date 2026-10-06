# Iniciativa: organic

> **Estado de trabalho, não referência.** Este arquivo diz o que falta, o que está em aberto
> e o que aconteceu no caminho. Como o sistema funciona está no `CLAUDE.md` e em `docs/`.
> Quando a iniciativa encerrar, o desenho implementado sobe para `docs/architecture.md` e
> `docs/project-structure.md`, e este arquivo é apagado — o histórico fica no git.

Iniciativa só do `binder_etl`. Nada aqui toca o backend nem o Bridge.

Facebook e Instagram orgânicos são **uma** iniciativa, não duas: compartilham as tabelas de
`gold/organic/`, o `src/transformers/snapshots.py` e a orquestração. Os planos separados de
`facebook-organic`, `instagram-organic` e `organic-orchestration` foram fundidos aqui.

## Próxima ação

Passo 0 da orquestração: `AIRBYTE_API_URL` no `.env`/`.env.example`, as três variáveis
`AIRBYTE_*` no `airflow-common-env` do compose e os campos em `src/config/settings.py`.
Hoje o container do Airflow alcança o Airbyte mas não recebe credencial nenhuma.

## Objetivo

Levar ao lake o conteúdo orgânico de páginas do Facebook e contas de Instagram, com o conector
oficial do Airbyte como está, e **garantir que as tabelas de `gold/organic/` estejam atualizadas
todo dia por dependência, não por horário combinado**.

As duas camadas medallion estão implementadas e rodando. O que falta é a orquestração: hoje o
`sync_raw` dos DAGs é um `EmptyOperator`, e se a extração atrasa ou falha o medallion roda sobre
o raw de ontem e **termina verde**.

**Fora do escopo:**

- **Cópia ou conector próprio.** Métricas e campos são os que o conector oficial pede — sem
  `breakdown=is_from_ads`, sem `since`/`until`, sem `views` de imagem e carrossel.
- **Orgânico × pago.** Sem breakdown, não se separa: o gold marca `metrics_scope`
  (`total` no Facebook, `organic` no Instagram) e chama as coisas pelo que são.
- **Marcar post impulsionado** e **demografia de seguidores no gold** — backlog.
- **Ligar post do Instagram a post do Facebook.** Objetos diferentes, sem id em comum; a
  ligação disponível é por conta (`users.page_id` = `page_id` do Facebook).
- **Integração com o backend** (Catalog API, Bridge, telas). As plataformas não têm hierarquia
  campanha → anúncio.
- **Plataformas pagas na orquestração nova.** Mantêm o cron do Airbyte até terem o próprio DAG
  no mesmo molde. A migração é por conexão, não big bang.
- **Webhook do Airbyte chamando a API do Airflow.** Mais fiel ao "após a extração", mas espalha
  o tratamento de falha entre dois sistemas por um ganho de minutos.
- **Consolidação por stream.** O runner é `<layer> <platform>`; o DAG de stories reexecuta o
  medallion inteiro do Instagram (~7 min, idempotente). Dar seleção por stream ao runner só se
  o tempo incomodar.

## Passos

Medallion das duas redes — bronze, silver, gold, testes, DAGs — **implementado e em produção**.
O desenho está em `docs/`; o detalhe de como foi feito está no git.

Orquestração:

| # | Passo | Status |
|---|---|---|
| 0 | `AIRBYTE_*` chegando ao container do Airflow, via `.env` e compose | a fazer |
| 1 | `src/airbyte/`: token, `POST /jobs`, poll de `GET /jobs/{id}`, timeout | a fazer |
| 2 | Pools `airbyte_sync` (2 slots) e `spark_medallion` (1 slot); `spark.driver.memory` 4g | a fazer |
| 3 | `instagram_organic_daily`: disparo + espera + medallion, às 02:00 SP | a fazer |
| 4 | `instagram_stories_daily`: DAG novo, às 09:30 SP | a fazer |
| 5 | `facebook_organic_daily`: fan-out por conexão descoberta, às 01:00 SP | a fazer |
| 6 | Checagem de conexão órfã: conexão `Organic` que nenhum DAG reivindica falha | a fazer |
| 7 | Asserção de frescor: `max(snapshot_date)` do gold é o dia esperado | a fazer |
| 8 | Alerta de falha (hoje é `retries: 1` e silêncio) | a fazer |
| 9 | As três conexões orgânicas para `scheduleType: "manual"` no Airbyte | a fazer, **por último** |
| 10 | Rótulos "alvo" saem de `docs/`; plano apagado | a fazer |

O passo 9 é o último de propósito: enquanto os DAGs não estiverem verdes, o cron do Airbyte é a
rede de segurança. Enquanto os dois convivem, o disparo do Airflow pode cair em cima do job do
cron — o Airbyte não roda dois jobs da mesma conexão ao mesmo tempo.

O passo 3 prova o padrão com uma conexão só; o 5 é o único com fan-out. Daí a ordem.

## Onde está o desenho

- Fluxo, regras de cada camada, conservação e **orquestração**: `docs/architecture.md`.
- Estrutura, chaves por stream e o módulo compartilhado: `docs/project-structure.md`.
- Conexões, tags, `namespaceFormat`, seleção de campos e agendamento: `airbyte/README.md`.
- Regras que valem sempre: `CLAUDE.md`.

As seções de orquestração em `docs/` estão marcadas **alvo**: são desenho decidido, não código
em produção. O rótulo sai no passo 10.

## Decisões em aberto

| # | Decisão | Como fecha |
|---|---------|-----------|
| D1 | **Horizonte por métrica dentro do pipeline.** Métrica de insight de conteúdo antigo é resíduo, não desempenho, e cada métrica morre numa idade diferente (diário de 06/10). Hoje quem protege disso é o dashboard do cliente, por heurística; o lake entrega o número cru | Decidir onde mora: coluna de qualidade no silver (`lifetime_truncated_at` por conteúdo × métrica, detectada pela queda) e/ou horizonte por `platform` × métrica no gold, com "não medido" distinto de zero. Enquanto estiver aberta, todo consumidor repete a heurística |
| D2 | **Revisão tardia das métricas da conta do Instagram.** O conector relê só o dia anterior e a Meta avisa revisão até 48h, então cada dia para de ser lido muito antes de parar de mudar | Medir a diferença entre a penúltima e a última leitura de cada dia. Mensurável: há fotos desde 29/09 e o cron principal (02:00 SP) não mudou. Ao medir, refazer a conta de quantas horas depois do fim do dia vem a última leitura |
| D3 | **A espera do job prende um slot do LocalExecutor.** Poll bloqueante é simples e aguenta sync de minutos; com o fan-out de páginas, N esperas simultâneas ocupam N slots | Medir a duração real de um sync de página. Passando de ~15 min, trocar o poll por `@task.sensor` em modo `reschedule`, que solta o slot entre tentativas |
| D4 | **Onde mora o horário depois da migração.** Com o disparo no Airflow, mudar hora de extração vira commit — bom para rastreabilidade, ruim para experimentar (a troca do cron de stories em 06/10 foi pelo painel) | Decidir ao fim do passo 9: aceitar o commit, ou ler o horário de uma Variable do Airflow |
| D5 | **Teto do pool `airbyte_sync`.** 2 slots agora; a RAM aguentaria ~5, a CPU (4 núcleos) não | Na terceira conexão de página: medir a duração de um sync sozinho contra dois simultâneos. Só subir para 3 se o tempo individual não piorar |

## Decisões fechadas que o código ainda não reflete

| # | Decisão | Fechada em |
|---|---------|-----------|
| **Retenção** | **Não existe corte por aniversário de 2 anos.** A regra que os planos antigos propunham (`is_beyond_retention` por `created_date + 24 meses`) está errada nos dois sentidos: esconderia post de 27 meses com número bom e não pegaria a queda real, que vem em lote e só aparece como delta muito negativo. Vira a D1 | 06/10 |
| **Stories** | **Cron em 09:30 São Paulo** (`0 30 9 * * ?`), pouco antes da primeira janela de publicação. Stories da manhã passam a ser lidos com ~23h de vida; os da noite seguem com ~16h, e fechar isso pede uma segunda leitura no fim da tarde, não uma hora diferente | 06/10 |
| **Fan-out** | **O fan-out do Facebook é da extração, não do medallion.** N syncs em paralelo, **um** medallion depois de todos | 06/10 |
| **Falha de página** | **Página que falha não bloqueia o medallion** (`all_done`). O silver já modela buraco com `gap_days`; a asserção de frescor diz qual página ficou atrás | 06/10 |
| **Lista de conexões** | **Instagram declarado por id, Facebook descoberto** por tag `Organic` + `namespaceFormat` começando com `facebook_organic/` | 06/10 |

## Diário

O diário de 29/09 a 01/10 — primeiras extrações, conferência contra o Business Suite, decisões
de campo e a reorganização da gold — foi para o git junto com os planos antigos de
`facebook-organic` e `instagram-organic`. Ficam aqui as medições que ainda sustentam decisão
aberta.

**06/10** — **Retenção medida em 7 fotos (29/09 a 05/10) e a hipótese de 2 anos derrubada.**
Do silver das duas redes:

- Testemunha do Facebook (`…122161958420250905`, publicado 03/10/2024): **353 → 13
  visualizações entre as fotos de 29 e 30/09**, e ficou em 13. Completou 2 anos em 03/10,
  três dias **depois** da queda.
- Testemunha do Instagram (`18153850864324974`, publicado 30/09/2024): **1.666 → 81 de alcance
  na foto de 02/10**, dois dias **depois** do aniversário. E o `17845158231310366`
  (05/10/2024) caiu no mesmo 02/10, com **23,9 meses** — antes do aniversário.
- As quedas são **lote numa data só**: Facebook `media_views` com 39 quedas, todas em 30/09;
  Instagram `reach` com 727, todas em 02/10. **Nenhuma** nos dias seguintes — numa janela móvel
  por post, cairia gente todo dia.
- **Cada métrica morre numa idade diferente.** `reach` do Instagram tem degrau compatível com
  ~24 meses (mediana 251 em 2024-10 contra 4.299 em 2024-11); `media_views` do Facebook **não
  tem degrau** e sobrevive aos 27 meses do post mais antigo da página; `reach` do Facebook é
  inútil antes de 2025-05, por ser métrica nova no conector, não por retenção. Curtidas,
  comentários, reações e compartilhamentos não perdem nada.

**06/10** — **Cron dos stories de horário para 09:30 São Paulo.** Decisão do Pablo: sem campanha
exigindo acompanhamento horário, e o Append horário multiplicava o raw por 24. Medido:

- A troca deixou **~33h sem leitura** (última horária em 03/10 21:00 UTC, primeira diária em
  06/10 06:00 UTC). Story publicado nesse intervalo expirou sem leitura e não existe mais.
- Na hora provisória (06:00 UTC), o `hours_live_at_last_read` caiu para **9,5–16,1h**, contra
  **23,0–24,0** nos dias de sync horário. Daí a escolha de 09:30.
- O rótulo de `snapshot_date` mudou de lado: `SNAPSHOT_CUTOFF_HOURS` é 6, então leitura antes
  das 06:00 São Paulo fecha o dia anterior e às 09:30 fecha o próprio dia. A data de negócio do
  gold vem de `published_date`, então consumidor nenhum sente; só auditoria foto a foto cruza
  a troca.
- E stories passam a entrar no bronze um dia depois enquanto o DAG rodar às 05:00 SP — o passo 4
  resolve.

**06/10** — **Levantamento para a orquestração**, com a API pública do Airbyte:

- O container do scheduler alcança o Airbyte em `192.168.10.80:8080` (HTTP 200); `localhost` e
  `host.docker.internal` não resolvem — o compose não tem `extra_hosts` e o Airbyte vive no
  kind do abctl. E o `airflow-common-env` **não passa nenhuma variável `AIRBYTE_*`**.
- O provider `apache-airflow-providers-airbyte` não está instalado. A API pública resolve com
  token + `POST /jobs` + poll, sem dependência nova nem risco de versão de provider.
- As tags já são consistentes (`Organic`, `Paid Media`, `Other`) e o `namespaceFormat` declara
  o caminho do raw — o da Texaco é `facebook_organic/280663778456254`. É o que viabiliza a
  descoberta por tag.
- A máquina tem **4 núcleos** e 30 GiB (12 em uso), com o `airbyte-abctl-control-plane` em
  6,1 GiB residentes. O `spark_session.py` usa `local[*]` e **não define
  `spark.driver.memory`** — roda no default de 1 GB. A CPU é a restrição, não a RAM.
- Os dois DAGs de hoje rodam no mesmo minuto (`0 8 * * *`) e escrevem as mesmas tabelas de
  `gold/organic/` com `replaceWhere` em `platform`. Escapam porque as tarefas de gold caem em
  minutos diferentes — sorte, não desenho. O escalonamento 01:00 / 02:00 / 09:30 acaba com a
  corrida, e o pool `spark_medallion` cobre o retry fora de hora.

**06/10** — Consequência de consumo a acertar quando o passo 4 entrar: o refresh do dashboard
da Texaco roda ~08:04 São Paulo e passaria a ver stories de ontem. Depois das 09:45.
