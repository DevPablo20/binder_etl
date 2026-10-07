"""Tarefas compartilhadas pelos DAGs do orgânico: disparar a extração, esperar, consolidar.

Os DAGs do orgânico seguem a mesma forma — **dispara o Airbyte, aguarda o job, monta o
medallion**. Horário combinado não é dependência: com o medallion agendado por relógio, uma
extração que atrasa ou falha deixa o pipeline rodar sobre o raw de ontem e terminar verde.

Mora em `dags/` e não em `src/` porque importa Airflow, que só existe na imagem do Airflow —
o container do Spark e o da Catalog API não têm. O Airflow põe a pasta de DAGs no `sys.path`,
então `import organic_tasks` funciona de qualquer arquivo de DAG.

**Nenhuma conexão está escrita aqui.** Cada DAG descobre as suas por duas coordenadas: a tag de
condutor (quem dirige) e o prefixo de `namespaceFormat` (onde escreve no raw). Conexão nova
entra sem commit, e conexão sem condutor — ou com dois — falha a checagem.
"""
import logging
from datetime import timedelta

from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.sensors.base import PokeReturnValue
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

from src.airbyte import client

logger = logging.getLogger(__name__)

# Concorrência. Criados no `airflow-init`.
#
# `spark_medallion` com um slot é o que importa: o medallion roda `local[*]` e dois ao mesmo
# tempo não dividem quatro núcleos — além de disputarem a escrita das mesmas tabelas de
# `gold/organic/`. Funciona porque a tarefa de camada segura o slot de ponta a ponta.
#
# `airbyte_sync` **não** limita sync simultâneo, e não tem como: o disparo segura o slot pelo
# tempo de um POST, e a espera em `reschedule` solta o slot entre os pokes — que é o que a
# torna barata. N conexões disparam N syncs, com qualquer número de slots. Medido com três
# páginas: os três jobs começaram no mesmo segundo. Fica no disparo como teto nominal e para
# dar um lugar onde mexer se um dia a espera virar bloqueante; quem limita de fato é o Airbyte.
POOL_SYNC = "airbyte_sync"
POOL_MEDALLION = "spark_medallion"

# Folga sobre o pior sync medido (a CAIXA leva ~41 min; o principal do Instagram, 23 a 34).
SYNC_TIMEOUT = timedelta(hours=2)
POKE_INTERVAL_SECONDS = 60

# As checagens contam as instâncias destas tarefas, então os nomes precisam casar com os
# `task_id` reais.
WAIT_TASK_ID = "wait_for_sync"
DISCOVER_TASK_ID = "discover_connections"

# Tag de escopo: marca o que esta iniciativa dirige. Conexão de mídia paga não tem.
ORGANIC_TAG = "Organic"

# Tags de condutor: qual DAG dispara a conexão. `stories` não é cadência — é o terceiro grupo,
# que existe porque as três conexões de Instagram compartilham o namespace e a cadência não as
# separa (conta e stories são as duas diárias).
DAILY_TAG = "daily"
WEEKLY_TAG = "weekly"
STORIES_TAG = "stories"
DRIVER_TAGS = (DAILY_TAG, WEEKLY_TAG, STORIES_TAG)

FACEBOOK_NAMESPACE = "facebook_organic/"
INSTAGRAM_NAMESPACE = "instagram_organic"

# O mapa completo de quem dirige o quê. A checagem usa isto para provar que nenhuma conexão
# `Organic` ficou sem DAG — e que nenhuma tem dois condutores.
DRIVEN_BY_DAG: tuple[tuple[str, str], ...] = (
    (DAILY_TAG, FACEBOOK_NAMESPACE),
    (WEEKLY_TAG, FACEBOOK_NAMESPACE),
    (DAILY_TAG, INSTAGRAM_NAMESPACE),
    (WEEKLY_TAG, INSTAGRAM_NAMESPACE),
    (STORIES_TAG, INSTAGRAM_NAMESPACE),
)

DEFAULT_ARGS = {
    "owner": "binder",
    "depends_on_past": False,
    "retries": 1,
}


def _drivers_of(connection: dict) -> list[str]:
    """As tags de condutor da conexão. Vazio ou mais de uma é configuração errada."""
    names = {t.get("name") for t in connection.get("tags") or []}
    return [driver for driver in DRIVER_TAGS if driver in names]


def _describe(connection: dict) -> str:
    return (
        f"{connection.get('name')} ({connection['connectionId']}, "
        f"namespace `{connection.get('namespaceFormat')}`)"
    )


@task
def discover_connections(driver: str, namespace: str) -> list[str]:
    """As conexões que este DAG dirige: tag `Organic` + tag de condutor + namespace.

    O `namespaceFormat` **é** o caminho que a conexão escreve no raw, e no Facebook é de onde
    sai o `page_id` (`page_id_from_path`). Filtrar por ele é o mesmo critério que o curinga do
    transformer usa para ler — o que o DAG dispara e o que o medallion consolida não podem
    divergir.

    Lista vazia é erro: significa que nada chegaria ao raw hoje, e seguir para o medallion só
    reescreveria a gold de ontem.
    """
    connections = [
        c
        for c in client.list_connections(tag=ORGANIC_TAG, namespace_prefix=namespace)
        if _drivers_of(c) == [driver]
    ]
    if not connections:
        raise AirflowFailException(
            f"nenhuma conexão ativa com tag `{ORGANIC_TAG}` + `{driver}` e namespace "
            f"`{namespace}*`. Conexão nova precisa nascer com as duas tags e com o "
            f"`namespaceFormat` certo, senão nenhum DAG a dispara e ela não chega ao lake"
        )

    for connection in connections:
        logger.info("A extrair: %s", _describe(connection))
    return [connection["connectionId"] for connection in connections]


@task(pool=POOL_SYNC)
def trigger_sync(connection_id: str) -> int:
    """Dispara o sync e devolve o `jobId` que a espera vai acompanhar.

    Separado da espera de propósito: numa tarefa só, o retry da espera dispararia um segundo
    sync em cima do primeiro.
    """
    return client.trigger_sync(connection_id)


@task.sensor(
    poke_interval=POKE_INTERVAL_SECONDS,
    timeout=SYNC_TIMEOUT.total_seconds(),
    mode="reschedule",
    pool=POOL_SYNC,
)
def wait_for_sync(job_id: int) -> PokeReturnValue:
    """Espera o job terminar. `reschedule` solta o slot entre as tentativas.

    Não é detalhe de estilo: o sync principal do Instagram leva mais de vinte minutos e o da
    CAIXA uns quarenta, e um poll bloqueante seguraria um slot do LocalExecutor todo esse
    tempo. Estado terminal ruim levanta (`job_is_done`) em vez de esperar o timeout.
    """
    status = client.job_status(job_id)
    return PokeReturnValue(is_done=client.job_is_done(status), xcom_value=status)


def _extraction_outcome() -> tuple[list, list]:
    """As esperas desta run, separadas em sucesso e falha, com o nome da conexão quando dá.

    Lê o estado das instâncias de `wait_for_sync`, não XCom e não o Airbyte. XCom de tarefa
    mapeada que falhou não existe, e perguntar ao Airbyte o último job de cada conexão
    responderia à pergunta errada: um job que terminou bem **ontem** também é "succeeded". O
    que importa é o que esta run conseguiu.
    """
    context = get_current_context()
    waits = [
        instance
        for instance in context["dag_run"].get_task_instances()
        if instance.task_id == WAIT_TASK_ID
    ]

    # O `map_index` segue a ordem da descoberta, então dá para nomear quem falhou. Se o XCom
    # não estiver lá (descoberta falhou), o índice sozinho já serve.
    try:
        discovered = context["ti"].xcom_pull(task_ids=DISCOVER_TASK_ID) or []
    except Exception:  # noqa: BLE001 - nomear é conveniência, não pode derrubar a checagem
        discovered = []

    def label(instance) -> str:
        i = instance.map_index
        if 0 <= i < len(discovered):
            return f"{discovered[i]} (índice {i})"
        return f"índice {i}" if i >= 0 else "conexão única"

    ok = [i for i in waits if i.state == TaskInstanceState.SUCCESS]
    failed = [(i, label(i), i.state) for i in waits if i.state != TaskInstanceState.SUCCESS]
    return ok, failed


@task(trigger_rule=TriggerRule.ALL_DONE)
def require_any_fresh_extraction() -> None:
    """Deixa o medallion seguir se **alguma** conexão extraiu; falha se nenhuma.

    Conexão que falha não bloqueia as outras: o silver modela buraco explicitamente
    (`gap_days`, `hours_since_prev`), então a página sem sync apenas não ganha foto no dia.
    Mas se nenhuma extraiu, rodar o medallion só reescreveria a gold de ontem — e é
    exatamente esse "verde sem dado novo" que esta iniciativa existe para matar.

    Quem avisa que *alguma* ficou de fora é `require_all_extractions`, depois da gold.
    """
    ok, failed = _extraction_outcome()

    for _, label, state in failed:
        logger.error("Extração sem sucesso em %s: %s", label, state)

    if not ok:
        raise AirflowFailException(
            f"nenhuma extração concluída nesta run ({len(ok) + len(failed)} tentativas) — "
            "o medallion não roda para não reescrever a gold de ontem"
        )

    logger.info("Extrações concluídas: %d de %d", len(ok), len(ok) + len(failed))


@task(trigger_rule=TriggerRule.ALL_DONE)
def require_all_extractions() -> None:
    """Falha a run se **alguma** conexão ficou de fora — depois de a gold já estar escrita.

    O limiar oposto ao do gate, e de propósito na outra ponta do DAG. Sem esta tarefa, o
    desenho "conexão que falha não bloqueia as outras" produz uma run **verde** com páginas
    defasadas: o vermelho fica numa tarefa mapeada no meio do grafo, e quem acompanha pela
    lista de runs não vê nada. Aconteceu duas vezes no dia em que o fan-out entrou — uma run
    com 1 de 3 páginas extraídas e outra com 2 de 3, ambas `success`.

    Depois da gold, não antes: o que funcionou entrega o dia (é a decisão de não penalizar as
    outras), e só então a run fica vermelha para ser vista.

    **Pergunta pela extração, não pela métrica.** Página legitimamente quieta — fora de
    campanha, em período eleitoral — entrega foto com zero, e isso não é problema. Problema é
    não ter foto. Alarmar por métrica zerada faria a checagem gritar todo dia numa página
    parada de propósito, e alerta que grita sempre ninguém olha.

    Não abre sessão Spark: lê o estado das tarefas no banco do Airflow. A contrapartida é não
    detectar sync que termina bem e escreve pouco — para isso seria preciso exigir foto do dia
    por `page_id` no lake, ao custo de uma leitura Spark.
    """
    ok, failed = _extraction_outcome()

    # Nenhuma espera é o caso em que a descoberta nem chegou a mapear tarefa. Passar aqui
    # seria o mesmo erro silencioso visto do outro lado: "tudo bem" porque nada aconteceu.
    if not ok and not failed:
        raise AirflowFailException(
            "nenhuma extração foi sequer tentada nesta run — a descoberta de conexões não "
            "mapeou nada, então não há como afirmar que alguma página está em dia"
        )

    if failed:
        detail = "; ".join(f"{label}: {state}" for _, label, state in failed)
        # Sem nenhuma extração o gate já barrou o medallion, e dizer que a gold saiu seria
        # mentir para quem lê o erro às três da manhã.
        outcome = (
            "a gold do dia saiu com as que funcionaram, mas estas ficaram defasadas"
            if ok
            else "nenhuma extraiu, então o medallion não rodou e a gold segue a de ontem"
        )
        raise AirflowFailException(
            f"{len(failed)} de {len(ok) + len(failed)} extrações falharam nesta run — "
            f"{outcome}: {detail}. Duas falhas seguidas na mesma página abrem buraco "
            f"permanente no nível diário dela, que a fonte só serve por 2 dias"
        )

    logger.info("Todas as %d extrações desta run concluíram", len(ok))


@task
def assert_connections_are_claimed() -> None:
    """Toda conexão `Organic` ativa tem exatamente um condutor e um namespace com DAG.

    É o preço da descoberta. Sem esta checagem, conexão criada no painel sem a tag de
    condutor — ou com `namespaceFormat` errado — fica fora de todo DAG e **nunca extrai**,
    sem nenhum sinal, agora que os crons do Airbyte estão desligados.

    Roda em paralelo ao medallion, não antes: ela denuncia configuração esquecida, não impede
    a gold do dia.
    """
    problems: list[str] = []
    for connection in client.list_connections(tag=ORGANIC_TAG):
        drivers = _drivers_of(connection)
        namespace = connection.get("namespaceFormat") or ""

        if len(drivers) > 1:
            problems.append(f"{_describe(connection)} tem dois condutores: {drivers}")
            continue
        if not drivers:
            problems.append(
                f"{_describe(connection)} não tem tag de condutor "
                f"(uma de {list(DRIVER_TAGS)})"
            )
            continue
        if not any(
            drivers[0] == driver and namespace.startswith(prefix)
            for driver, prefix in DRIVEN_BY_DAG
        ):
            problems.append(
                f"{_describe(connection)} é `{drivers[0]}` num namespace que nenhum DAG dirige"
            )

    if problems:
        raise AirflowFailException(
            "conexão orgânica que nenhum DAG dispara: " + "; ".join(problems)
        )

    logger.info("Toda conexão `%s` ativa tem DAG.", ORGANIC_TAG)


def medallion_tasks(platform: str) -> list[BashOperator]:
    """`bronze → silver → gold` da plataforma, serializados no pool do Spark.

    Nomes na convenção da casa (`{layer}_{platform}`, terminando em `gold_{platform}`), três
    tarefas em vez de um `medallion` só para a camada que falhou aparecer no grafo.
    """
    return [
        BashOperator(
            task_id=f"{layer}_{platform}",
            bash_command=f"python -m src.pipelines.run {layer} {platform}",
            pool=POOL_MEDALLION,
        )
        for layer in ("bronze", "silver", "gold")
    ]


def chain_medallion(upstream, platform: str) -> BashOperator:
    """Liga `upstream >> bronze >> silver >> gold` e devolve a última tarefa."""
    previous = upstream
    for layer_task in medallion_tasks(platform):
        previous >> layer_task
        previous = layer_task
    return previous


def organic_flow(driver: str, namespace: str, platform: str) -> None:
    """O DAG inteiro de um grupo de conexões: descobre, dispara, espera, consolida, confere.

    Os cinco DAGs do orgânico são esta função com coordenadas diferentes — o que muda entre
    eles é qual grupo de conexões dirigem, em que horário rodam e qual medallion chamam.
    """
    connection_ids = discover_connections(driver, namespace)
    waits = wait_for_sync.expand(job_id=trigger_sync.expand(connection_id=connection_ids))

    gate = require_any_fresh_extraction()
    waits >> gate

    # A gold do dia sai com o que extraiu; a run só fica vermelha depois, se faltou alguém.
    chain_medallion(gate, platform) >> require_all_extractions()
