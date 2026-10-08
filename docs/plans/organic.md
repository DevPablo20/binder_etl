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

Observar o ciclo de 08/10 à noite e o semanal de 14/10 com os dois ajustes de hoje no ar. O
passo 14 espera o ETL de Facebook Marketing.

## Objetivo

Levar ao lake o conteúdo orgânico de páginas do Facebook e contas de Instagram, com o conector
oficial do Airbyte como está, e **garantir que as tabelas de `gold/organic/` estejam atualizadas
todo dia por dependência, não por horário combinado**.

As duas camadas medallion e a orquestração estão implementadas. Falta a entrega: desligar o
cron das conexões que o Airflow já dispara, e um alerta de falha. Até lá os dois convivem, com
o cron do Airbyte como rede de segurança.

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
| 0 | `AIRBYTE_*` chegando ao container do Airflow, via `.env` e compose | feito 06/10 |
| 1 | `src/airbyte/`: token, `POST /jobs`, poll de `GET /jobs/{id}`, timeout | feito 06/10 |
| 2 | Pools `airbyte_sync` (2 slots) e `spark_medallion` (1 slot); `spark.driver.memory` 4g | feito 06/10 |
| 3 | Asserção de frescor no gold, antes da escrita | feito 06/10 |
| 4 | `instagram_organic_daily`: disparo + espera + medallion, às 02:00 SP | feito 06/10 |
| 5 | `instagram_stories_daily`: DAG novo, às 09:30 SP | feito 06/10 |
| 6 | `facebook_organic_daily`: fan-out por conexão descoberta, às 01:00 SP | feito 06/10 |
| 7 | Checagem de conexão órfã: conexão `Organic` que nenhum DAG reivindica falha | feito 06/10 |
| 8 | Alerta de falha | fechado 07/10: **acompanhamento manual** na UI, com o log de tarefa persistido para que a falha possa ser diagnosticada; disparo automático no backlog |
| 9 | As três conexões orgânicas para `scheduleType: "manual"` no Airbyte | feito 07/10 pelo Pablo |
| 10 | Rótulos "alvo" saem de `docs/`; plano apagado | a fazer |

A asserção de frescor subiu de 7 para 3: ela é só `src/`, não depende de DAG, e com ela no
lugar desde o começo o primeiro DAG testado já exercita o caminho inteiro.

O passo 9 é o último de propósito: enquanto os DAGs não estiverem verdes, o cron do Airbyte é a
rede de segurança. Enquanto os dois convivem, o disparo do Airflow pode cair em cima do job do
cron — o Airbyte não roda dois jobs da mesma conexão ao mesmo tempo.

O passo 4 prova o padrão com uma conexão só; o 6 é o único com fan-out. Daí a ordem.

## Mudança de escopo em 07/10: página diária, post semanal

A equipe de monitoramento fechou os dois níveis, e eles separam **exatamente na linha do
custo** — o que precisa ser diário é barato, o que é caro basta semanal.

**Página, diário, em todas as contas das duas redes.** Curtidas, posts, seguidores, alcance e o
que mais permita acompanhar o movimento da página. São 1 a 2 chamadas por página; `page` e
`page_insights` do Facebook, `users` e `user_insights` do Instagram.

**Post, semanal, recorte quarta → terça.** Interações, comentários, curtidas,
compartilhamentos, alcance. É onde está o custo: uma chamada de insights **por post**, em todo
sync, sem incremental no conector.

O recorte cai de graça na mecânica que já existe: sync na quarta às 01:00 fecha o
`snapshot_date` na terça (corte das 06:00), e o delta entre duas quartas cobre quarta → terça.
O silver marca `gap_days` 7 e o gold expõe `days_covered` 7 — a coluna existe para isso.
**Nada muda na matemática do medallion**, que é agnóstica ao intervalo entre fotos.

Efeito colateral a documentar, não a corrigir: o `content_daily` passa a ter grão semanal para
post de Facebook e de Instagram (uma linha por post por foto), com `days_covered` dizendo o
alcance. O nome da tabela fica um pouco largo; renomear a gold custa mais do que a confusão
que evita.

**Contagem de posts da semana** sai do `published_date` da dimensão `content`, agrupando por
semana que começa na quarta — melhor que um contador de página, porque dá o número da semana
em vez do acumulado. Duas ressalvas: a contagem só conhece posts até o último sync de `post`
(com cadência semanal, atrasa até 7 dias), e post apagado sai da listagem mas **permanece na
dimensão** com `last_seen_date` antigo — quem conta decide se inclui.

### Passos

| # | Passo | Status |
|---|---|---|
| 11 | Dividir cada conexão em duas: página (diária) e post (semanal, quarta). Stories seguem diários — insight de story vive 24h | feito 07/10 |
| 12 | Tags de condutor (`daily`/`weekly`/`stories`) somadas à `Organic`; descoberta por tag em todos os DAGs, sem id no repositório | feito 07/10 |
| 13 | DAGs semanais (quarta, 01:00 e 02:00 SP) disparando as conexões de post e chamando o mesmo medallion | feito 07/10 |
| 14 | Marcação de impulsionado pelo join com o Meta Ads | **a fazer depois do ETL de Facebook Marketing** — decisão do Pablo em 07/10: a marcação depende do transformer do Meta Ads existir |
| 15 | ~~Fechar o caso da CAIXA~~ | **fechado 07/10:** `page_size` 5 resolveu a extração; a página está dormente, não truncada (diário) |

O passo 14 está mais perto do que o backlog supunha: o **Meta Ads já cai no lake**
(`raw/airbyte/meta/`, 9 contas, com o stream `ads`), só não tem transformer. O
`effective_object_story_id` do criativo tem o formato `{page_id}_{post_id}`, então um
`LEFT JOIN` marca quais posts foram impulsionados e quanto gastaram.

## Onde está o desenho

- Fluxo, regras de cada camada, conservação e **orquestração**: `docs/architecture.md`.
- Estrutura, chaves por stream e o módulo compartilhado: `docs/project-structure.md`.
- Conexões, tags, `namespaceFormat`, seleção de campos e agendamento: `airbyte/README.md`.
- Regras que valem sempre: `CLAUDE.md`.

A seção de orquestração do `docs/architecture.md` está marcada **parcialmente alvo**: os DAGs
disparam e esperam, mas as conexões ainda têm cron no Airbyte (passo 9) e não há alerta
(passo 8). O rótulo sai no passo 10.

## Decisões em aberto

| # | Decisão | Como fecha |
|---|---------|-----------|
| D1 | **Horizonte por métrica dentro do pipeline.** Métrica de insight de conteúdo antigo é resíduo, não desempenho, e cada métrica morre numa idade diferente (diário de 06/10). Hoje quem protege disso é o dashboard do cliente, por heurística; o lake entrega o número cru | Decidir onde mora: coluna de qualidade no silver (`lifetime_truncated_at` por conteúdo × métrica, detectada pela queda) e/ou horizonte por `platform` × métrica no gold, com "não medido" distinto de zero. Enquanto estiver aberta, todo consumidor repete a heurística |
| D2 | **Revisão tardia das métricas da conta do Instagram.** O conector relê só o dia anterior e a Meta avisa revisão até 48h, então cada dia para de ser lido muito antes de parar de mudar | Medir a diferença entre a penúltima e a última leitura de cada dia. Mensurável: há fotos desde 29/09 e o cron principal (02:00 SP) não mudou. Ao medir, refazer a conta de quantas horas depois do fim do dia vem a última leitura |
| ~~D6~~ | ~~**Conector próprio com filtro de data**~~ | **Fechada em 07/10 sem precisar de conector:** com `page_size` 5 a CAIXA extraiu inteira em 41 min, numa tentativa. O `page_size` é a alavanca e o valor é **por página**, proporcional ao tamanho dela — Texaco 25, Loterias 10, CAIXA 5. Não há histórico a cortar, porque a extração completa. Se uma página maior que a CAIXA aparecer e 5 não bastar, a decisão reabre |
| D7 | **O que a gold orgânica faz quando o fato não tem dimensão.** O `test_every_daily_post_is_in_content` assume que todo post com métrica tem linha em `content`, e a fonte não garante isso: post com `is_published = false` é descartado pelo `own_published` do `silver/posts`, e post que só aparece em `post_insights` nunca chega a `posts` (diário de 08/10). Hoje a inconsistência é 12 posts e 0,000% da métrica, então é decisão de desenho, não incêndio | Escolher uma das duas: **(a)** filtrar o fato como a dimensão é filtrada — a métrica do post não publicado desaparece; **(b)** fazer a dimensão aceitar todo post que tem fato, com a ausência como categoria explícita. A regra "todo join do gold é `LEFT`, partindo do fato" e a invariante "nada some" apontam para (b), e aí o teste muda de "fato ⊆ dimensão" para "a ausência está modelada". Decidir antes de mexer no teste: silenciá-lo sem decidir esconde as duas causas |
| D4 | **Onde mora o horário depois da migração.** Com o disparo no Airflow, mudar hora de extração vira commit — bom para rastreabilidade, ruim para experimentar (a troca do cron de stories em 06/10 foi pelo painel) | Decidir ao fim do passo 9: aceitar o commit, ou ler o horário de uma Variable do Airflow |
| ~~D5~~ | ~~**Teto do pool `airbyte_sync`**~~ | **Fechada em 07/10, com a premissa derrubada.** O pool **não** limita sync simultâneo e não tem como: o disparo segura o slot por um segundo e a espera em `reschedule` solta o slot entre os pokes. Com três páginas, os três jobs começaram no mesmo segundo. E não precisa limitar: a Texaco levou **4m00s com três syncs em paralelo**, dentro da faixa dela sozinha (2m53s a 5m46s), com load 2,74 em quatro núcleos e 15 GiB livres — sync é espera de rede, não CPU. Se um dia precisar, o lugar é o limite de concorrência do próprio Airbyte, não o Airflow |

## Decisões fechadas que o código ainda não reflete

| # | Decisão | Fechada em |
|---|---------|-----------|
| ~~D3~~ | **A espera é sensor em `reschedule`, não poll bloqueante** — fechada pela medição, não pelo palpite: o sync principal do Instagram leva 23 a 34 min, bem acima do limite de ~15 min que a decisão previa. Já implementado | 06/10 |
| **Retenção** | **Não existe corte por aniversário de 2 anos.** A regra que os planos antigos propunham (`is_beyond_retention` por `created_date + 24 meses`) está errada nos dois sentidos: esconderia post de 27 meses com número bom e não pegaria a queda real, que vem em lote e só aparece como delta muito negativo. Vira a D1 | 06/10 |
| **Stories** | **Cron em 09:30 São Paulo** (`0 30 9 * * ?`), pouco antes da primeira janela de publicação. Stories da manhã passam a ser lidos com ~23h de vida; os da noite seguem com ~16h, e fechar isso pede uma segunda leitura no fim da tarde, não uma hora diferente | 06/10 |

As três decisões de orquestração que estavam nesta tabela — fan-out da extração, página que
falha não bloqueia, lista de conexões — saíram: estão no código e descritas em
`docs/architecture.md`.

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

**06/10** — **Passos 0 a 7 implementados.** O que o caminho ensinou, além do previsto:

- **A API de jobs do Airbyte ordena ascendente.** `GET /jobs?limit=1` devolve o job mais
  **antigo** da conexão, não o mais recente. O cliente pede `orderBy=createdAt|DESC`
  explicitamente; sem isso, procurar o job em andamento numa conexão com histórico leria os
  primeiros jobs de sempre. Pego antes de rodar, por desconfiança da ordem — não por falha.
- **O heap default do Spark é 1 GiB, medido.** `spark.driver.memory` pelo builder do PySpark
  **funciona** (a dúvida era se só valeria via `spark-submit`): com a config, heap real de
  4,00 GiB; sem ela, 1,00 GiB. Agora é `settings.spark_driver_memory`.
- **`ETL_STRICT` no Airflow é `ETL_STRICT_AIRFLOW` no compose**, com default `true`, para não
  arrastar a CLI e o catalog-api — que seguem tolerantes com o `ETL_STRICT` do `.env`.
- **A espera em `reschedule` se provou no primeiro DAG:** o poke das 20:54 marcou
  `UP_FOR_RESCHEDULE` e soltou o slot, e o job foi reconhecido no poke seguinte. Um poll
  bloqueante teria segurado o slot do LocalExecutor o sync inteiro.

Medições do dia, para a D5 e a D3: sync de stories ~1,5 min; Facebook 3,5–6 min; Instagram
principal 23–34 min.

**06/10** — **Verificação dos DAGs, rodando à mão com os crons do Airbyte ainda ligados.**

- `instagram_stories_daily` verde duas vezes (a segunda provou idempotência). Resultado que
  importa: `gold/organic/stories` passou a ter **stories do próprio dia** — antes deste DAG a
  leitura das 09:30 só era transformada na rodada seguinte.
- `facebook_organic_daily` verde no caminho completo do fan-out: descoberta achou a Texaco pelo
  `namespaceFormat`, disparo mapeado, espera, gate, bronze, silver, gold. A gold ganhou a foto
  de 06/10 (695 posts, 51 visualizações e 41 de alcance novo no dia).
- `instagram_organic_daily` exercitou a espera longa: 14 pokes em `reschedule`, soltando o slot
  entre cada um.
- **Pool provado com dois DAGs diferentes:** `spark_medallion` em `em_uso=1, na_fila=1`, com o
  medallion do Facebook esperando o do stories.
- **Gate provado no caminho negativo**, sem criar DagRun: sem nenhuma espera (descoberta
  falhou), uma falha, duas falhas → falha com a mensagem certa; uma de duas extraiu, duas de
  duas → segue.
- **Conexão inexistente falha alto:** `AirbyteError` com HTTP 404 e corpo da resposta, com o
  erro do disparo preservado em vez do erro da investigação.

**06/10** — **A suíte inteira é frágil quanto à ordem, e isso apareceu aqui.** Rodando `pytest
tests/` com um sync do Airbyte em andamento, o `test_conservation.py` do Instagram falhou:
gold 18.819 linhas contra silver 19.409. Não era bug de código.

O pytest coleta as pastas em ordem alfabética — `bronze`, **`gold`**, **`silver`**,
`test_conservation.py` — e os `test_smoke.py` **rodam os transformers de verdade e escrevem no
MinIO**. Então o gold é materializado a partir do silver anterior, e só depois o silver é
atualizado. Normalmente inofensivo; com raw novo aparecendo no meio (o sync em andamento tinha
escrito 590 linhas parciais de `media_insights`), o teste compara silver fresco com gold velho.

Duas consequências práticas: **não rodar a suíte com sync em andamento**, e a ordem merece
correção (fora desta iniciativa).

**07/10** — **Primeiro ciclo agendado, nas agendas novas, verde nos três.** Facebook 7m33s,
Instagram principal 33m26s, stories 6m16s.

O ciclo real produziu a evidência que nenhum teste manual conseguia: **os crons do Airbyte e os
disparos do Airflow colidiram de verdade**, porque estão na mesma hora.

- Facebook, 04:00:02 — `POST /jobs` devolveu **HTTP 409 "A sync is already running"** e o DAG
  passou a acompanhar o job 204, que o cron tinha começado um ou dois segundos antes.
- Instagram principal, 05:00:01 — mesma coisa, job 206.
- Stories, 12:30:01 — aqui o DAG ganhou a corrida e disparou o job 209.

Sem o tratamento do "job já em andamento", dois dos três DAGs teriam falhado na primeira noite.
Depois do passo 9 a corrida deixa de existir: o disparo do Airflow passa a ser o único.

**A hora dos stories se confirmou no horário certo** (o teste de 06/10 foi às 20:54, fora de
hora, e pegava os stories com 1 a 7h de vida). No ciclo das 09:30: stories publicados entre
10:55 e 12:11 lidos com **21,3 a 22,6h**; o das 16:38 com 16,9h; o das 18:54 com 14,6h. O story
da Texaco que em 06/10 saiu com 7h e 138 visualizações fechou com 22,6h e 279.

**07/10** — **A única run que já falhou na história do Airflow está explicada.** Em 02/10 os
DAGs do Facebook e do Instagram foram disparados à mão com **2 segundos de diferença**
(17:51:08 e 17:51:10): o do Facebook levou 14,3 min — o dobro do normal — e o do Instagram
falhou no bronze depois de 6,9 min. Às 18:29, sozinho, o mesmo Instagram rodou em 3,8 min.

Dois medallions `local[*]` em quatro núcleos, cada um com o heap default de 1 GiB. É exatamente
o modo de falha que o pool `spark_medallion` de um slot e o heap de 4 GiB passaram a impedir —
e que foi visto funcionando em 06/10, com o medallion do Facebook esperando na fila o do
stories. A causa não pode ser **provada**: o log da tarefa não existe mais, porque
`/opt/airflow/logs` não é volume montado e foi apagado ao recriar os containers.

**07/10** — **Log de tarefa do Airflow passou a ser persistido**, em
`./infra/airflow/logs:/opt/airflow/logs`, seguindo o padrão dos outros bind mounts do repo
(`infra/airflow/postgres_data`, `infra/minio/minio_data`). A pasta precisa pertencer a
`${AIRFLOW_UID}:0`.

Sem esse bind, o log vivia só dentro do container: o histórico de runs sobrevivia (está no
Postgres, que já tem bind mount), mas o **motivo** de cada falha desaparecia a cada
`docker compose up -d`. Com o acompanhamento de falha sendo manual pela UI, isso era o furo
central — ver a run vermelha e abrir um log vazio.

**O preço de arrumar foi perder o log das runs de hoje**: montar sobre `/opt/airflow/logs`
esconde o que estava na camada do container, e recriar o container apagou. O que importava
daquelas runs já estava transcrito aqui (os três disparos e os dois HTTP 409).

`airflow tasks test` não serve para verificar isso — ele escreve só no stdout, sem arquivo de
log. A verificação é pelo executor de verdade: `airflow tasks clear` de uma tarefa barata, e o
log aparecendo em `infra/airflow/logs/dag_id=…/run_id=…/task_id=…`.

**07/10** — **Passo 9 feito: as três conexões orgânicas em `scheduleType: "manual"`**, com
`status` seguindo `active` (o erro que seria desligar a conexão também para a API não
aconteceu). As três pagas mantiveram o cron.

Primeiro disparo depois da virada, no DAG de stories: `Sync disparado: job 211`, **sem o
HTTP 409** que aparecia quando o cron competia. Run verde nas cinco tarefas. A cadeia agora tem
um dono só.

**Respingo a resolver:** a conexão `Kwai Ads - Always On → Google Sheets` também foi para
manual, e ela estava em `Every 24 HOURS`. Não é orgânica (tag `Other`), nenhum DAG a dispara, e
a checagem de órfã não a vê porque olha só a tag `Organic` — então aquela exportação está
congelada até alguém devolver o agendamento dela.

**07/10** — **Para entrar com página nova de Facebook não muda código nenhum**: não há id de
página em `src/` nem em `dags/`, tudo agrupa e junta por `page_id`, e o DAG descobre por tag
mais namespace. O que precisa de cuidado é o `namespaceFormat`, e por uma razão mais forte do
que o curinga do `raw_path`: **ele é a única fonte do `page_id` em todo o pipeline do Facebook**
(`page_id_from_path()` extrai do caminho do arquivo com regex). Namespace default esconde a
página; namespace com nome em vez do id numérico grava `account_id` errado na gold, em silêncio.

**07/10** — **CAIXA resolvida, e o diagnóstico anterior estava errado.** Com `page_size` 5 o
sync completou em **41m02s, numa única tentativa**, 36.029 linhas. O escalonamento fecha a
questão: Texaco 696 posts em 25, Loterias 1.302 em 10, CAIXA 5.148 em 5 — o valor é por
página, proporcional ao tamanho. A D6 (conector próprio) fecha sem ser necessária.

**O que eu vinha chamando de "a listagem da CAIXA não completa" não existia.** Depois do sync
bem-sucedido, o post mais novo dela **continua 22/07/2026** — e a razão é outra: a página
**está dormente**. Posts por mês: 18 a 68 de janeiro/2025 a maio/2026, 2 em junho, 20 em
julho, **nenhum depois de 22/07**. E o `page_insights` dela, agora que chega, devolve **zero em
tudo** nos dias 04 e 05/10 — views, alcance, interações, novos seguidores — com breakdown
`{total: 0, paid: 0, unpaid: 0}`, para uma página de 4,7 milhões de seguidores.

O zero vem da API, não do transform: no silver o `value` é 0 e o map de breakdown é
explicitamente zerado. O que a parcial escondia era o **histórico antigo** (o post mais antigo
foi de 2011-08-01 para 2011-01-12, e entraram 75 posts), não o recente — a paginação da
listagem caminha para trás.

**Confirmado pelo Pablo: a página está parada de propósito, por causa das eleições e do
período defeso.** Então a página do Facebook da CAIXA aparece zerada no lake e **esse é o
número certo** — não há nada a corrigir no pipeline.

Isso tem uma consequência para a checagem por página que ficou adiada: ela precisa distinguir
**sem foto** (falha de extração, problema) de **foto com zero** (página quieta de verdade, não
problema). A checagem como proposta já faz isso — ela exige que exista foto do dia, não que
haja atividade —, mas é a diferença que importa quando alguém for alertar sobre "página ficou
quieta".

**07/10** — **Erro silencioso por página fechado.** O desenho "página que falha não bloqueia as
outras" (D2) produzia run **verde** com páginas defasadas, e as três guardas existentes não
pegavam porque todas são globais: a dependência do DAG passa pelo gate com ≥1 sucesso, o
`ETL_STRICT` vê silver cheio das outras páginas, e o `assert_photo_reached_gold` compara o
máximo global, que vem de quem funcionou. Aconteceu duas vezes no dia em que o fan-out entrou:
run com 1 de 3 páginas extraídas e run com 2 de 3, ambas `success`.

`require_all_pages_extracted` entra como **folha** depois da gold, com o limiar oposto ao do
gate — falha se alguma ficou de fora. Lê o estado das instâncias de `wait_for_sync` no banco do
Airflow, sem sessão Spark e sem chamada ao Airbyte, e nomeia as conexões pelo XCom da
descoberta. Provada nos seis cenários (3 de 3, 1 de 3, 2 de 3, 0 de 3, conexão única, e zero
esperas mapeadas) e com o encanamento exercitado sob contexto real do Airflow.

Dois detalhes que só apareceram ao escrever o teste: com **zero** esperas mapeadas a checagem
passaria — "tudo bem porque nada aconteceu" —, agora falha; e a mensagem dizia que a gold tinha
sido escrita mesmo quando o gate havia barrado o medallion, o que seria mentir para quem lê o
erro de madrugada.

O que ela não pega, de propósito: sync que termina bem e escreve pouco. Para isso seria preciso
exigir foto do dia por `page_id` no lake, ao custo de uma leitura Spark. A evidência de 07/10 é
que job bem-sucedido entregou dado e job cancelado falhou a espera.

**07/10** — **Passos 11 a 13 feitos: a estrutura por cadência está no ar.** Nove conexões
`Organic` ativas, cada uma com exatamente um condutor:

| DAG | Conexões | Horário |
|---|---|---|
| `facebook_organic_daily` | 3 de página | 01:00 |
| `facebook_organic_weekly` | 3 de posts | quarta, 01:00 |
| `instagram_organic_daily` | 1 de conta | 02:00 |
| `instagram_organic_weekly` | 1 de mídia | quarta, 02:00 |
| `instagram_stories_daily` | 1 de stories | 09:30 |

As conexões antigas combinadas ficaram em `status: inactive`, como rollback — e invisíveis para
a descoberta e para a checagem, que só olham ativas.

Os cinco DAGs são a mesma função (`organic_flow`); o que muda é a coordenada. **Nenhum id de
conexão mora mais no repositório** — os dois do Instagram, que eram declarados, saíram. A
checagem virou mais forte: em vez de "alguém reivindica esta conexão?", agora é "toda conexão
`Organic` ativa tem exatamente um condutor e um namespace com DAG?".

Três coisas que o caminho ensinou:

- **`stories` precisou ser um condutor, não uma cadência.** As três conexões de Instagram
  compartilham o namespace `instagram_organic`, então cadência não separa conta de stories —
  as duas seriam `daily`. A tag diz *qual DAG dirige*, que é a pergunta que a descoberta faz.
- **O dry run pagou por si.** A primeira versão do script de divisão ia desativar a conexão de
  stories junto com as outras, porque a regra "divide e desativa" não previa a exceção. Rodar
  em modo de listagem antes mostrou isso sem custo.
- **`selectedFields: []` no GET significa "todos os campos", e o POST recusa como "nenhum".**
  O `user_insights` e o `user_lifetime_insights` caem nesse caso; a criação falhou com
  `No fields selected for stream user_insights` **depois** de a conexão antiga já ter sido
  desativada, deixando o Instagram diário sem conexão por alguns minutos. A correção é omitir
  a chave quando vem vazia. Numa próxima migração, criar tudo antes de desativar nada.

**08/10** — **Primeiro ciclo na estrutura nova. Os três diários verdes, um semanal vermelho —
e o vermelho foi a checagem nova fazendo o trabalho dela.**

| Run | Estado | Duração |
|---|---|---|
| `facebook_organic_daily` | success | 8 min |
| `instagram_organic_daily` | success | 11 min |
| `instagram_stories_daily` | success | 6 min |
| `instagram_organic_weekly` | success | 24 min |
| `facebook_organic_weekly` | **failed** | 125 min |

No semanal do Facebook: 2 de 3 extrações concluíram, o gate deixou passar, o medallion rodou, e
`require_all_extractions` **derrubou a run** nomeando a conexão que faltou. Ontem a situação
idêntica terminou verde. A checagem funcionou na primeira vez que teve chance.

**Achado 1: o timeout de 2h é curto para a cadência semanal.** O job de posts da Loterias levou
**5h25m50s** e terminou **bem**, com 14.908 linhas — o mesmo trabalho levou 16m58s dois dias
antes. A espera estourou as 2h e falhou; o dado chegou às 02:14 e entrou no lake pela rodada
diária das 04:00. Então a run ficou vermelha por um motivo real (o DAG não pôde confirmar) com
o dado íntegro no fim. Hipótese do 20× mais lento: limite de taxa da Meta depois do volume do
dia (os syncs de teste, os 41 min da CAIXA e quatro syncs simultâneos à noite). Não é fato
provado.

**Achado 2: "succeeded com 0 linhas" existe, e passa pela checagem.** Dois jobs terminaram bem
sem escrever nada: `Facebook Pages - Texaco (posts)` e `Instagram Pages - All (mídia)`, ambos em
~16 min. É exatamente o furo que `require_all_extractions` não cobre por desenho.

O que isso deixou no lake:

- **Facebook: em ordem.** Nível de página e de post com foto de 07/10.
- **Instagram mídia: uma foto atrás** (06/10 em vez de 07/10), porque o sync de 0 linhas não
  produziu foto. Não é perda: a cadência é semanal, e o primeiro delta vai cobrir 8 dias em vez
  de 7, com `days_covered` dizendo isso. Nível de conta do Instagram está em 07/10.
- **`account_daily` do Facebook em 05/10** — é o atraso de ~2 dias do `page_insights`, por
  desenho, não falha.

**08/10** — **Os dois achados do primeiro ciclo foram corrigidos.**

**Prazo de espera por cadência**, em vez de 2h para tudo:

| Condutor | Prazo | Por quê |
|---|---|---|
| `daily`, `stories` | 4h | `max_active_runs=1` faz run pendurada engolir o dia seguinte, e o `page_insights` só serve 2 dias. São ~50× o pior sync diário medido (5 min), com o dia inteiro de folga para retry à mão |
| `weekly` | 12h | A próxima run é em 7 dias, não há o que atropelar — e o sync de posts da Loterias levou 5h26 e terminou bem. Doze horas cobrem o dobro do pior caso |

Implementado com `wait_for_sync.override(timeout=…)` por DAG, a partir de um mapa por condutor.

**"Terminou bem sem trazer nada" virou falha**, dentro da própria espera: ela passou a ler o
job inteiro (`client.job`) e não só o estado, e falha quando o job fecha em `succeeded` com
`rowsSynced` 0. Sem isso o Airbyte diz "deu tudo certo", nenhum arquivo vai para o raw, a foto
do dia não existe e o DAG fica verde — foi o que aconteceu em 07/10 com `Texaco (posts)` e
`Instagram (mídia)`, ambos `succeeded` em ~16 min com zero linhas.

Duas escolhas de desenho:

- **Mora na espera, não numa tarefa nova.** Tratar o sync vazio como falha faz a conexão entrar
  na contagem de `require_all_extractions` e aparecer pelo nome, reaproveitando a máquina que
  já existe. E resolve mais barato que a alternativa — a checagem de foto por página no lake,
  que custaria uma leitura Spark por DAG.
- **`AirflowFailException`, sem retry.** Repetir o poke daria o mesmo zero.

A exceção é `stories`, com `expect_rows=False`: dia sem story vivo devolve zero
legitimamente, e alarmar ali faria a checagem gritar em dia calmo.

Provado em seis cenários (rodando, com linhas, zero linhas, campo ausente, zero em stories,
cancelado) e conferido no grafo: 4h nos três diários, 12h nos dois semanais, `expect_rows`
falso só nos stories.

**08/10** — **12 posts no `content_daily` sem linha no `content`, e são duas causas somadas.**
O `test_every_daily_post_is_in_content` falha. Medido no lake: 12 `content_id` do Facebook
(9 da CAIXA, 3 das Loterias) estão no fato e não na dimensão, **todos com `date` 07/10** — o
dia do enxame de tentativas da CAIXA. Nenhum do Instagram.

As duas causas não se parecem:

- **2 posts: `is_published = false`.** São posts da própria página (`from.id = page_id`),
  presentes no bronze e descartados pelo filtro `own_published` do `silver/posts`. O
  `post_metrics_daily` não aplica filtro nenhum — então a métrica sobrevive e o atributo não.
  É assimetria de código, dentro do nosso controle.
- **10 posts nunca entraram no raw de `post`.** Conferido contra o raw, não só contra o
  bronze: existem em `post_insights` e não existem em `post` em **nenhuma** foto. As Loterias
  tiveram um sync de `post` em 08/10 que também não os trouxe, o que enfraquece a hipótese de
  flush interrompido e aponta para posts que o `/feed` não devolve. Por que, não está medido.

Peso: **0,000%** de `views_total` (47 de 89.799.479) e de `reach_total` (94 de 37.142.846).
Os 6.420 likes são internamente inconsistentes — 6.167 likes com 94 de alcance —, o que é a
assinatura de resíduo da D1: conteúdo antigo cujo alcance já morreu e cujas reações
sobreviveram.

**Não é regressão de commit.** O teste e o filtro nasceram no mesmo commit
(`3f5d48e`); o que mudou foi o dado — a assimetria chegou com a extração de 07/10.

**08/10 (II)** — **Os 10 insight-only não são artefato do enxame: `post` e `post_insights`
discordam de forma reprodutível.** A hipótese de flush interrompido está derrubada pela
medição.

Rastreando os insights dos órfãos até o arquivo de origem no raw, por sync:

| página | syncs que trouxeram insight dos órfãos | linhas por sync |
|---|---|---|
| CAIXA (155187484530317) | **15 de 15** | **42, sempre** (7 posts × 6 linhas) |
| Loterias (204170826380793) | 3 de 4, inclusive o de 08/10 | 18, 6, 18 |

Contagem perfeitamente estável em toda tentativa. Flush interrompido daria número errático.
E o `post` da CAIXA, no mesmo dia, variou de 4.477 a 5.240 linhas por sync — a variação é das
tentativas que morreram no meio; as **duas que completaram devolveram 5.240 cada**, e nenhuma
das duas trouxe os 7. O sync de `post` das Loterias de 08/10 também não trouxe os 3.

Descartado também que seja formato de id nosso: os 8.138 ids do `bronze/post` têm todos duas
partes (`{page_id}_{post_id}`), e **nenhum** post do bronze tem o mesmo sufixo de um órfão com
prefixo diferente. Os 10 têm as 5 métricas de insight normalmente.

Então o `post_insights` conhece, em todo sync, posts que o `post` nunca devolve. Isso é
propriedade do conector ou da Graph API, não transiente e não nosso.

**O que falta para fechar a D7:** saber qual endpoint cada stream chama. Dois caminhos, nenhum
feito — o token da Graph vem mascarado na API do Airbyte, e extraí-lo do secret store não se
justifica para isto: (a) ler o manifest do conector `source-facebook-pages`, que não precisa de
credencial nenhuma; (b) consultar um dos ids na Graph API, que o Pablo roda com o token dele.

**08/10 (III)** — **Mesmo endpoint, mesmo paginador: a divergência está no `fields`.** Lido do
manifest do conector `source-facebook-pages:2.1.4`, já presente no cluster (nenhum download,
nenhuma credencial).

Fato, não hipótese:

| | `post` | `post_insights` |
|---|---|---|
| path | `/{page_id}/feed` | `/{page_id}/feed` — **o mesmo** |
| paginador | `facebook_post_paginator` | o mesmo |
| `fields` | `QueryProperties` com **44 campos** | `insights.metric(5 métricas)` |
| extrator | `data` | `data.*.insights.data.*` |

`url_base` é `graph.facebook.com/v24.0` nos dois. Então a hipótese de endpoints diferentes
está **errada** — os dois percorrem o mesmo feed, com o mesmo cursor.

O que difere é o peso do pedido. `post_query_properties` é um `QueryProperties` com
`property_selector`, ou seja, **property chunking do CDK**: a lista de 44 campos é quebrada em
vários requests por página do feed, e os registros parciais são remontados pela primary key.
O `post_insights` faz um request leve por página, de um campo só.

A inferência que resta — e é inferência — é que algum dos 44 campos faz a Graph omitir ou
falhar o item para esses posts, ou que a remontagem por chunk perde registro ausente de um
chunk. Candidatos pelo histórico de restrição da Graph: `instagram_eligibility`,
`allowed_advertising_objects`, `video_buying_eligibility`, `targeting`, `feed_targeting`,
`privacy`, `promotable_id`, `scheduled_publish_time`.

Isso também explica o `page_size`: o stream caro **é** o `post`, e é caro por causa dos 44
campos em chunks — não por causa do volume de posts. É a mesma causa do limite de 5 na CAIXA.

**Teste que fecha:** pedir um dos ids órfãos à Graph duas vezes, com lista mínima e com os 44
campos. Se a mínima responde e a de 44 falha, o erro nomeia o campo. Exige **Page token** — o
token de usuário é recusado com `code 190 / subcode 2069032` na nova experiência de Páginas,
que é por que a primeira tentativa falhou.
