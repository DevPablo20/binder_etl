"""Unitário e isolado (sem rede, sem Spark): o contrato do cliente do Airbyte.

`src/airbyte/client.py` é a porta por onde os DAGs do orgânico disparam a extração e esperam
o job. Os testes provam o que dá segurança a essa espera: token cacheado, job já em andamento
tratado como caso normal, estado terminal ruim que levanta em vez de esperar para sempre, e o
filtro de conexões que precisa casar com o curinga do transformer.

Todo I/O do módulo passa por `_request` — é o único ponto trocado aqui.
"""
import time

import pytest

from src.airbyte import client
from src.airbyte.client import AirbyteError

CONNECTION = "6e52d410-693f-41b5-ba78-298644818118"
TOKEN_RESPONSE = {"access_token": "tok-1", "token_type": "Bearer", "expires_in": 900}


@pytest.fixture(autouse=True)
def _clear_token_cache():
    """O cache é módulo-global: sem limpar, um teste herda o token do anterior."""
    client._token_cache = None
    yield
    client._token_cache = None


@pytest.fixture(autouse=True)
def _credentials(monkeypatch):
    monkeypatch.setattr(client.settings, "airbyte_client_id", "id")
    monkeypatch.setattr(client.settings, "airbyte_client_secret", "secret")
    monkeypatch.setattr(client.settings, "airbyte_api_url", "http://airbyte.test")


def fake_request(monkeypatch, handler):
    """Troca `_request` por `handler(method, path, payload, token)` e registra as chamadas."""
    calls = []

    def _request(method, path, payload=None, token=None):
        calls.append((method, path, payload))
        return handler(method, path, payload, token)

    monkeypatch.setattr(client, "_request", _request)
    return calls


def test_token_e_reaproveitado_dentro_da_ttl(monkeypatch):
    calls = fake_request(monkeypatch, lambda *_: TOKEN_RESPONSE)

    assert client.token() == "tok-1"
    assert client.token() == "tok-1"

    assert len(calls) == 1, "o segundo pedido deveria sair do cache"


def test_token_renova_quando_a_ttl_vence(monkeypatch):
    calls = fake_request(monkeypatch, lambda *_: TOKEN_RESPONSE)
    # Envelhecer o cache é mais honesto que trocar o relógio do processo.
    client._token_cache = ("tok-velho", time.monotonic() - client.TOKEN_TTL_SECONDS - 1)

    assert client.token() == "tok-1"

    assert len(calls) == 1, "token vencido tem de ser buscado de novo"


def test_token_sem_credencial_diz_onde_elas_moram(monkeypatch):
    monkeypatch.setattr(client.settings, "airbyte_client_id", "")
    fake_request(monkeypatch, lambda *_: TOKEN_RESPONSE)

    with pytest.raises(AirbyteError, match="compose"):
        client.token()


def test_trigger_sync_devolve_o_job_id(monkeypatch):
    def handler(method, path, payload, _token):
        if path == "/applications/token":
            return TOKEN_RESPONSE
        if method == "POST" and path == "/jobs":
            assert payload == {"connectionId": CONNECTION, "jobType": "sync"}
            return {"jobId": 91, "status": "pending"}
        raise AssertionError(f"chamada inesperada: {method} {path}")

    fake_request(monkeypatch, handler)

    assert client.trigger_sync(CONNECTION) == 91


def test_trigger_sync_acompanha_job_que_ja_estava_rodando(monkeypatch):
    """O caso normal enquanto o cron do Airbyte conviver com o disparo do Airflow."""

    def handler(method, path, _payload, _token):
        if path == "/applications/token":
            return TOKEN_RESPONSE
        if method == "POST" and path == "/jobs":
            raise AirbyteError("POST /jobs -> HTTP 409: sync already running")
        if method == "GET" and path.startswith("/jobs?"):
            assert "DESC" in path, "sem orderBy, a API devolve os jobs mais ANTIGOS"
            # Mais recente primeiro, como a API devolve com `orderBy=createdAt|DESC`.
            return {
                "data": [
                    {"jobId": 81, "status": "running"},
                    {"jobId": 80, "status": "succeeded"},
                ]
            }
        raise AssertionError(f"chamada inesperada: {method} {path}")

    fake_request(monkeypatch, handler)

    assert client.trigger_sync(CONNECTION) == 81


def test_trigger_sync_propaga_erro_quando_nao_ha_job_rodando(monkeypatch):
    def handler(method, path, _payload, _token):
        if path == "/applications/token":
            return TOKEN_RESPONSE
        if method == "POST" and path == "/jobs":
            raise AirbyteError("POST /jobs -> HTTP 403: forbidden")
        if method == "GET" and path.startswith("/jobs?"):
            return {"data": [{"jobId": 80, "status": "succeeded"}]}
        raise AssertionError(f"chamada inesperada: {method} {path}")

    fake_request(monkeypatch, handler)

    with pytest.raises(AirbyteError, match="403"):
        client.trigger_sync(CONNECTION)


def test_trigger_sync_preserva_o_erro_do_disparo(monkeypatch):
    """Conexão que não existe: a consulta de "tem job rodando?" também falha, e o erro que
    chega ao log do Airflow tem de ser o do disparo, não o da investigação."""

    def handler(method, path, _payload, _token):
        if path == "/applications/token":
            return TOKEN_RESPONSE
        if method == "POST" and path == "/jobs":
            raise AirbyteError("POST /jobs -> HTTP 404: resource-not-found")
        raise AirbyteError("GET /jobs -> HTTP 404: resource-not-found")

    fake_request(monkeypatch, handler)

    with pytest.raises(AirbyteError, match="POST /jobs"):
        client.trigger_sync(CONNECTION)


def test_trigger_sync_sem_job_id_na_resposta(monkeypatch):
    def handler(_method, path, _payload, _token):
        if path == "/applications/token":
            return TOKEN_RESPONSE
        return {}

    fake_request(monkeypatch, handler)

    with pytest.raises(AirbyteError, match="jobId"):
        client.trigger_sync(CONNECTION)


def test_last_job_pede_o_mais_recente(monkeypatch):
    """O default da API é ascendente: `limit=1` sem `orderBy` traz o job mais antigo da
    conexão, que para uma conexão com histórico é sempre a resposta errada."""

    def handler(_method, path, _payload, _token):
        if path == "/applications/token":
            return TOKEN_RESPONSE
        assert "limit=1" in path and "DESC" in path, path
        return {"data": [{"jobId": 185, "status": "succeeded"}]}

    fake_request(monkeypatch, handler)

    assert client.last_job(CONNECTION)["jobId"] == 185


def test_last_job_sem_historico(monkeypatch):
    def handler(_method, path, _payload, _token):
        if path == "/applications/token":
            return TOKEN_RESPONSE
        return {"data": []}

    fake_request(monkeypatch, handler)

    assert client.last_job(CONNECTION) is None


def test_job_status_le_o_estado(monkeypatch):
    def handler(_method, path, _payload, _token):
        if path == "/applications/token":
            return TOKEN_RESPONSE
        assert path == "/jobs/91"
        return {"jobId": 91, "status": "running", "duration": "PT2M"}

    fake_request(monkeypatch, handler)

    assert client.job_status(91) == "running"


@pytest.mark.parametrize("status", ["pending", "running"])
def test_job_is_done_falso_enquanto_roda(status):
    assert client.job_is_done(status) is False


def test_job_is_done_verdadeiro_no_sucesso():
    assert client.job_is_done("succeeded") is True


@pytest.mark.parametrize("status", ["failed", "cancelled", "incomplete"])
def test_job_is_done_levanta_em_estado_terminal_ruim(status):
    """`incomplete` é terminal e ruim — tratá-lo como "ainda rodando" faria a espera
    estourar o timeout em vez de dizer o que houve."""
    with pytest.raises(AirbyteError, match=status):
        client.job_is_done(status)


def test_job_is_done_levanta_em_estado_desconhecido():
    with pytest.raises(AirbyteError, match="desconhecido"):
        client.job_is_done("teleported")


CONNECTIONS = [
    {
        "connectionId": "fb-texaco",
        "name": "Facebook Pages - Texaco → S3",
        "status": "active",
        "tags": [{"name": "Organic"}],
        "namespaceFormat": "facebook_organic/280663778456254",
    },
    {
        "connectionId": "fb-outra",
        "name": "Facebook Pages - Outra → S3",
        "status": "active",
        "tags": [{"name": "Organic"}],
        "namespaceFormat": "facebook_organic/999",
    },
    {
        "connectionId": "fb-pausada",
        "name": "Facebook Pages - Pausada → S3",
        "status": "inactive",
        "tags": [{"name": "Organic"}],
        "namespaceFormat": "facebook_organic/111",
    },
    {
        "connectionId": "ig-all",
        "name": "Instagram Pages - All → S3",
        "status": "active",
        "tags": [{"name": "Organic"}],
        "namespaceFormat": "instagram_organic",
    },
    {
        "connectionId": "kwai",
        "name": "Kwai Ads → S3",
        "status": "active",
        "tags": [{"name": "Paid Media"}],
        "namespaceFormat": "kwai",
    },
]


def _connections_handler(pages):
    def handler(_method, path, _payload, _token):
        if path == "/applications/token":
            return TOKEN_RESPONSE
        assert path.startswith("/connections?"), path
        offset = int(dict(p.split("=") for p in path.split("?")[1].split("&"))["offset"])
        return {"data": pages.get(offset, [])}

    return handler


def test_list_connections_filtra_por_tag_e_namespace(monkeypatch):
    fake_request(monkeypatch, _connections_handler({0: CONNECTIONS}))

    found = client.list_connections(
        tag="Organic", namespace_prefix="facebook_organic/"
    )

    assert [c["connectionId"] for c in found] == ["fb-texaco", "fb-outra"]


def test_list_connections_ignora_conexao_pausada(monkeypatch):
    fake_request(monkeypatch, _connections_handler({0: CONNECTIONS}))

    found = client.list_connections(tag="Organic")

    assert "fb-pausada" not in [c["connectionId"] for c in found]


def test_list_connections_sem_filtro_devolve_as_ativas(monkeypatch):
    fake_request(monkeypatch, _connections_handler({0: CONNECTIONS}))

    found = client.list_connections()

    assert len(found) == 4


def test_list_connections_pagina(monkeypatch):
    """O `instagram_organic` não se separa por namespace, então a paginação não pode
    perder conexão: uma página cheia obriga a pedir a próxima."""
    primeira = [dict(c, connectionId=f"c{i}") for i, c in enumerate(CONNECTIONS * 20)]
    assert len(primeira) == 100
    calls = fake_request(
        monkeypatch, _connections_handler({0: primeira, 100: [CONNECTIONS[0]]})
    )

    found = client.list_connections(tag="Organic", namespace_prefix="facebook_organic/")

    assert len(found) == 41, "40 da primeira página mais 1 da segunda"
    assert sum(1 for c in calls if c[1].startswith("/connections?")) == 2
