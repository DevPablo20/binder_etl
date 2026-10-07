"""Tarefas compartilhadas pelos DAGs do orgânico: disparar a extração, esperar, consolidar.

Os três DAGs do orgânico seguem a mesma forma — **dispara o Airbyte, aguarda o job, monta o
medallion**. Horário combinado não é dependência: com o medallion agendado por relógio, uma
extração que atrasa ou falha deixa o pipeline rodar sobre o raw de ontem e terminar verde.

Mora em `dags/` e não em `src/` porque importa Airflow, que só existe na imagem do Airflow —
o container do Spark e o da Catalog API não têm. O Airflow põe a pasta de DAGs no `sys.path`,
então `import organic_tasks` funciona de qualquer arquivo de DAG.

Para o fan-out de páginas do Facebook, ver `facebook_organic_daily.py`: o paralelo é da
extração, nunca do medallion.
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
# torna barata. N páginas disparam N syncs, com qualquer número de slots. Medido com três
# páginas: os três jobs começaram no mesmo segundo. Fica no disparo como teto nominal e para
# dar um lugar onde mexer se um dia a espera virar bloqueante; quem limita de fato é o Airbyte.
POOL_SYNC = "airbyte_sync"
POOL_MEDALLION = "spark_medallion"

# Folga sobre o pior sync medido (o principal do Instagram leva de 23 a 34 min).
SYNC_TIMEOUT = timedelta(hours=2)
POKE_INTERVAL_SECONDS = 60

# As checagens contam as instâncias destas tarefas, então os nomes precisam casar com os
# `task_id` reais.
WAIT_TASK_ID = "wait_for_sync"
DISCOVER_TASK_ID = "discover_facebook_connections"

# Conexões declaradas por id. As duas do Instagram compartilham o `namespaceFormat`
# `instagram_organic` e nenhum metadado as separa — descoberta não resolve.
INSTAGRAM_MAIN_CONNECTION = "e86ceec3-237c-4173-9298-2512ae388ef8"
INSTAGRAM_STORIES_CONNECTION = "38f9bb84-15b6-4c98-8157-85df3f22680d"

# As de página do Facebook são descobertas: página nova entra sem commit, e o critério é o
# mesmo que o curinga do `raw_path` do transformer usa para ler.
ORGANIC_TAG = "Organic"
FACEBOOK_NAMESPACE_PREFIX = "facebook_organic/"

DEFAULT_ARGS = {
    "owner": "binder",
    "depends_on_past": False,
    "retries": 1,
}


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

    Não é detalhe de estilo: o sync principal do Instagram leva mais de vinte minutos, e um
    poll bloqueante seguraria um slot do LocalExecutor todo esse tempo. Estado terminal ruim
    levanta (`job_is_done`) em vez de esperar o timeout.
    """
    status = client.job_status(job_id)
    return PokeReturnValue(is_done=client.job_is_done(status), xcom_value=status)


@task
def discover_facebook_connections() -> list[str]:
    """Conexões de página do Facebook, por tag mais prefixo de `namespaceFormat`.

    Lista vazia é erro: significa que nenhuma página chegaria ao raw hoje, e seguir para o
    medallion só reescreveria a gold de ontem.
    """
    connections = client.list_connections(
        tag=ORGANIC_TAG, namespace_prefix=FACEBOOK_NAMESPACE_PREFIX
    )
    if not connections:
        raise AirflowFailException(
            f"nenhuma conexão ativa com tag `{ORGANIC_TAG}` e namespace "
            f"`{FACEBOOK_NAMESPACE_PREFIX}*` — página nova precisa nascer com "
            f"`namespaceFormat = {FACEBOOK_NAMESPACE_PREFIX}{{page_id}}`, senão os arquivos "
            "caem fora do curinga do transformer e a página desaparece do lake sem erro"
        )

    for connection in connections:
        logger.info(
            "Página a extrair: %s (%s) -> %s",
            connection.get("name"),
            connection["connectionId"],
            connection.get("namespaceFormat"),
        )
    return [connection["connectionId"] for connection in connections]


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
    # não estiver lá (descoberta falhou, DAG sem fan-out), o índice sozinho já serve.
    try:
        descobertas = context["ti"].xcom_pull(task_ids=DISCOVER_TASK_ID) or []
    except Exception:  # noqa: BLE001 - nomear é conveniência, não pode derrubar a checagem
        descobertas = []

    def nome(instance) -> str:
        i = instance.map_index
        if 0 <= i < len(descobertas):
            return f"{descobertas[i]} (índice {i})"
        return f"índice {i}" if i >= 0 else "conexão única"

    ok = [i for i in waits if i.state == TaskInstanceState.SUCCESS]
    falhas = [(i, nome(i), i.state) for i in waits if i.state != TaskInstanceState.SUCCESS]
    return ok, falhas


@task(trigger_rule=TriggerRule.ALL_DONE)
def require_any_fresh_extraction() -> None:
    """Deixa o medallion seguir se **alguma** página extraiu; falha se nenhuma.

    Página que falha não bloqueia as outras: o silver modela buraco explicitamente
    (`gap_days`, `hours_since_prev`), então a página sem sync apenas não ganha foto no dia.
    Mas se nenhuma extraiu, rodar o medallion só reescreveria a gold de ontem — e é
    exatamente esse "verde sem dado novo" que esta iniciativa existe para matar.

    Quem avisa que *alguma* ficou de fora é `require_all_pages_extracted`, depois da gold.
    """
    ok, falhas = _extraction_outcome()

    for _, nome, estado in falhas:
        logger.error("Extração sem sucesso em %s: %s", nome, estado)

    if not ok:
        raise AirflowFailException(
            f"nenhuma extração concluída nesta run ({len(ok) + len(falhas)} tentativas) — "
            "o medallion não roda para não reescrever a gold de ontem"
        )

    logger.info("Páginas extraídas: %d de %d", len(ok), len(ok) + len(falhas))


@task(trigger_rule=TriggerRule.ALL_DONE)
def require_all_pages_extracted() -> None:
    """Falha a run se **alguma** página ficou de fora — depois de a gold já estar escrita.

    O limiar oposto ao do gate, e de propósito na outra ponta do DAG. Sem esta tarefa, o
    desenho "página que falha não bloqueia as outras" produz uma run **verde** com páginas
    defasadas: o vermelho fica numa tarefa mapeada no meio do grafo, e quem acompanha pela
    lista de runs não vê nada. Aconteceu duas vezes no dia em que o fan-out entrou — uma run
    com 1 de 3 páginas extraídas e outra com 2 de 3, ambas `success`.

    Depois da gold, não antes: as páginas que funcionaram entregam o dia (é a decisão de não
    penalizar as outras), e só então a run fica vermelha para ser vista.

    **Pergunta pela extração, não pela métrica.** Página legitimamente quieta — fora de
    campanha, em período eleitoral — entrega foto com zero, e isso não é problema. Problema é
    não ter foto. Alarmar por métrica zerada faria a checagem gritar todo dia numa página
    parada de propósito, e alerta que grita sempre ninguém olha.

    Não abre sessão Spark: lê o estado das tarefas no banco do Airflow. A contrapartida é não
    detectar sync que termina bem e escreve pouco — para isso seria preciso exigir foto do dia
    por `page_id` no lake, ao custo de uma leitura Spark.
    """
    ok, falhas = _extraction_outcome()

    # Nenhuma espera é o caso em que a descoberta nem chegou a mapear tarefa. Passar aqui
    # seria o mesmo erro silencioso visto do outro lado: "tudo bem" porque nada aconteceu.
    if not ok and not falhas:
        raise AirflowFailException(
            "nenhuma extração foi sequer tentada nesta run — a descoberta de conexões não "
            "mapeou nada, então não há como afirmar que alguma página está em dia"
        )

    if falhas:
        detalhe = "; ".join(f"{nome}: {estado}" for _, nome, estado in falhas)
        # Sem nenhuma extração o gate já barrou o medallion, e dizer que a gold saiu seria
        # mentir para quem lê o erro às três da manhã.
        contexto = (
            "a gold do dia saiu com as que funcionaram, mas estas ficaram defasadas"
            if ok
            else "nenhuma extraiu, então o medallion não rodou e a gold segue a de ontem"
        )
        raise AirflowFailException(
            f"{len(falhas)} de {len(ok) + len(falhas)} extrações falharam nesta run — "
            f"{contexto}: {detalhe}. Duas falhas seguidas na mesma página abrem buraco "
            f"permanente no nível diário dela, que a fonte só serve por 2 dias"
        )

    logger.info("Todas as %d extrações desta run concluíram", len(ok))


@task
def assert_no_orphan_connections(claimed: list[str]) -> None:
    """Conexão com tag `Organic` que nenhum DAG dispara é erro.

    É o preço da descoberta. Sem esta checagem, uma conexão criada no painel com o namespace
    errado — ou uma rede nova ligada e esquecida — fica fora de todo DAG e, depois que os
    crons do Airbyte forem desligados, **nunca mais extrai**, sem nenhum sinal.
    """
    known = set(claimed) | {INSTAGRAM_MAIN_CONNECTION, INSTAGRAM_STORIES_CONNECTION}
    orphans = [
        f"{c.get('name')} ({c['connectionId']}, namespace `{c.get('namespaceFormat')}`)"
        for c in client.list_connections(tag=ORGANIC_TAG)
        if c["connectionId"] not in known
    ]
    if orphans:
        raise AirflowFailException(
            "conexão orgânica que nenhum DAG dispara: "
            + "; ".join(orphans)
            + " — ou ela entra num DAG, ou perde a tag `Organic`"
        )


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
