"""Cliente da API pública do Airbyte: disparar sync e acompanhar job.

Os DAGs do orgânico não confiam em horário combinado — eles disparam a extração e esperam o
job terminar antes de montar o medallion. Este módulo é a única porta para isso.

**stdlib, de propósito.** São três chamadas HTTP; `requests` existe na imagem do Airflow
(dependência dele) mas não em `requirements/spark.txt`, e adicionar dependência em duas
imagens por isso não se paga.

Todo I/O passa por `_request` — é o único ponto que os testes trocam.
"""
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request

from src.config import settings

logger = logging.getLogger(__name__)

API_PREFIX = "/api/public/v1"
TIMEOUT_SECONDS = 30

# O token do Airbyte vive 900s. Renovamos com folga: uma tarefa que começa com token de 870s
# de idade não pode descobrir no meio que ele venceu.
TOKEN_TTL_SECONDS = 600

# Estados de job. `incomplete` é o nome que o Airbyte dá a job que terminou sem sucesso
# (parcial ou abortado) — terminal e ruim, não "ainda rodando".
STATUS_SUCCESS = "succeeded"
STATUS_PENDING = frozenset({"pending", "running"})
STATUS_FAILED = frozenset({"failed", "cancelled", "incomplete"})

_token_cache: tuple[str, float] | None = None


class AirbyteError(RuntimeError):
    """Falha de comunicação ou de contrato com a API do Airbyte."""


def _request(
    method: str,
    path: str,
    payload: dict | None = None,
    token: str | None = None,
) -> dict:
    """Única porta de I/O do módulo. Devolve o JSON da resposta.

    `path` é relativo ao prefixo da API pública. Erro HTTP vira `AirbyteError` com o corpo
    da resposta anexado — sem ele, um 409 ou 403 chega ao log do Airflow como um número solto.
    """
    url = f"{settings.airbyte_api_url}{API_PREFIX}{path}"
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"

    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:500]
        raise AirbyteError(f"{method} {path} -> HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise AirbyteError(f"{method} {path} -> sem resposta: {exc.reason}") from exc

    if not body:
        return {}
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise AirbyteError(f"{method} {path} -> resposta não é JSON") from exc


def token(force: bool = False) -> str:
    """Token de acesso, cacheado no processo pela TTL.

    Cada tarefa do Airflow é um processo novo, então o cache serve a uma tarefa só — é o
    suficiente: o que ele evita é um token por poke dentro da mesma execução.
    """
    global _token_cache

    if not force and _token_cache is not None:
        value, fetched_at = _token_cache
        if time.monotonic() - fetched_at < TOKEN_TTL_SECONDS:
            return value

    if not settings.airbyte_client_id or not settings.airbyte_client_secret:
        raise AirbyteError(
            "AIRBYTE_CLIENT_ID/AIRBYTE_CLIENT_SECRET ausentes — dentro do container elas "
            "chegam pelo `environment` do compose, não pelo `.env`"
        )

    response = _request(
        "POST",
        "/applications/token",
        payload={
            "client_id": settings.airbyte_client_id,
            "client_secret": settings.airbyte_client_secret,
        },
    )
    value = response.get("access_token")
    if not value:
        raise AirbyteError("resposta de token sem `access_token`")

    _token_cache = (value, time.monotonic())
    return value


def list_connections(
    tag: str | None = None,
    namespace_prefix: str | None = None,
) -> list[dict]:
    """Conexões ativas, opcionalmente filtradas por tag e por prefixo de `namespaceFormat`.

    O `namespaceFormat` da conexão **é** o caminho que ela escreve no raw — o da Texaco é
    `facebook_organic/280663778456254`. Por isso filtrar por ele é o mesmo critério que o
    curinga do `raw_path` do transformer usa para ler: o que o DAG dispara e o que o
    medallion consolida não podem divergir.
    """
    access = token()
    page_size = 100
    offset = 0
    out: list[dict] = []

    while True:
        query = urllib.parse.urlencode({"limit": page_size, "offset": offset})
        page = _request("GET", f"/connections?{query}", token=access).get("data") or []
        out.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    def matches(connection: dict) -> bool:
        if connection.get("status") != "active":
            return False
        if tag is not None:
            names = {t.get("name") for t in connection.get("tags") or []}
            if tag not in names:
                return False
        if namespace_prefix is not None:
            namespace = connection.get("namespaceFormat") or ""
            if not namespace.startswith(namespace_prefix):
                return False
        return True

    return [c for c in out if matches(c)]


def jobs(connection_id: str, limit: int = 20) -> list[dict]:
    """Jobs da conexão, **do mais recente para o mais antigo**.

    O `orderBy` não é enfeite: o default da API é ascendente, então `limit=1` devolve o job
    mais **antigo** da conexão. Sem ele, procurar o job em andamento numa conexão com
    histórico leria os primeiros jobs de sempre e nunca o atual.
    """
    query = urllib.parse.urlencode(
        {
            "connectionId": connection_id,
            "limit": limit,
            "orderBy": "createdAt|DESC",
        }
    )
    return _request("GET", f"/jobs?{query}", token=token()).get("data") or []


def last_job(connection_id: str) -> dict | None:
    """Job mais recente da conexão, em qualquer estado."""
    found = jobs(connection_id, limit=1)
    return found[0] if found else None


def running_job(connection_id: str) -> dict | None:
    """Job ainda não terminal da conexão, se houver."""
    for job in jobs(connection_id):
        if job.get("status") in STATUS_PENDING:
            return job
    return None


def trigger_sync(connection_id: str) -> int:
    """Dispara o sync e devolve o `jobId` a acompanhar.

    **Job já em andamento não é erro.** Enquanto o cron do Airbyte conviver com o disparo do
    Airflow, o normal é o DAG chegar e encontrar o job do cron rodando; o Airbyte não roda
    dois jobs da mesma conexão ao mesmo tempo e recusa o pedido. Nesse caso devolvemos o job
    existente, para a espera acompanhar o que está de fato rodando em vez de falhar.
    """
    try:
        response = _request(
            "POST",
            "/jobs",
            payload={"connectionId": connection_id, "jobType": "sync"},
            token=token(),
        )
    except AirbyteError as exc:
        # Se a própria consulta falhar (conexão que não existe, por exemplo), o erro que
        # interessa é o do disparo — não o da investigação.
        try:
            existing = running_job(connection_id)
        except AirbyteError:
            raise exc from None
        if existing is None:
            raise
        logger.warning(
            "Disparo recusado para %s (%s) — acompanhando o job %s que já estava rodando",
            connection_id,
            exc,
            existing.get("jobId"),
        )
        return int(existing["jobId"])

    job_id = response.get("jobId")
    if job_id is None:
        raise AirbyteError(f"disparo de {connection_id} sem `jobId` na resposta")

    logger.info("Sync disparado para %s: job %s", connection_id, job_id)
    return int(job_id)


def job(job_id: int) -> dict:
    """O job inteiro. Além do `status`, traz `rowsSynced` — e um sync que termina bem sem
    trazer linha nenhuma é um caso real: visto em 07/10 em duas conexões, com `succeeded` e
    `rowsSynced` 0, sem escrever arquivo no raw."""
    response = _request("GET", f"/jobs/{job_id}", token=token())
    if not response.get("status"):
        raise AirbyteError(f"job {job_id} sem `status` na resposta")
    return response


def job_status(job_id: int) -> str:
    """Estado atual do job."""
    return job(job_id)["status"]


def job_is_done(status: str) -> bool:
    """`True` quando o job terminou bem. Estado terminal ruim levanta; rodando devolve `False`.

    Concentrar a classificação aqui evita que cada DAG reinvente a lista de estados — e um
    estado novo na API vira erro explícito em vez de espera infinita.
    """
    if status == STATUS_SUCCESS:
        return True
    if status in STATUS_FAILED:
        raise AirbyteError(f"job terminou em `{status}`")
    if status in STATUS_PENDING:
        return False
    raise AirbyteError(f"estado de job desconhecido: `{status}`")
