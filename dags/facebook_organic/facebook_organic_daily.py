"""Facebook orgânico: extração de todas as páginas em paralelo e **um** medallion depois.

O fan-out é da extração, não do medallion. Cada página escreve em
`raw/airbyte/facebook_organic/{page_id}/{stream}` e o transformer lê com curinga
(`facebook_organic/*/{stream}`), então um medallion já consolida todas as páginas numa
passada. Um medallion por página reprocessaria todas as páginas de qualquer forma e N runs
disputariam a partição `platform='facebook'` da gold compartilhada.

As conexões não estão escritas aqui: saem da API por tag e prefixo de namespace, então página
nova entra sem commit. O preço é a checagem de órfã — conexão `Organic` que nenhum DAG dispara
falha o DAG.
"""
import pendulum
from airflow import DAG
from organic_tasks import (
    DEFAULT_ARGS,
    assert_no_orphan_connections,
    chain_medallion,
    discover_facebook_connections,
    require_all_pages_extracted,
    require_any_fresh_extraction,
    trigger_sync,
    wait_for_sync,
)

PLATFORM = "facebook_organic"

# 01:00 São Paulo: antes do corte das 06:00 que define o `snapshot_date`, então a foto fecha
# o dia anterior. Escalonado uma hora antes do Instagram — as duas redes escrevem as mesmas
# tabelas de `gold/organic/`, e o pool do medallion é a rede de segurança para o retry.
with DAG(
    dag_id=f"{PLATFORM}_daily",
    default_args=DEFAULT_ARGS,
    description="Facebook Pages orgânico: dispara cada página, aguarda, monta o medallion",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=[PLATFORM, "medallion", "organic"],
) as dag:
    connection_ids = discover_facebook_connections()

    job_ids = trigger_sync.expand(connection_id=connection_ids)
    waits = wait_for_sync.expand(job_id=job_ids)

    gate = require_any_fresh_extraction()
    waits >> gate

    # A gold do dia sai com o que extraiu; a run só fica vermelha depois, se faltou alguém.
    # Sem a segunda checagem, página defasada vira run verde — ver `require_all_pages_extracted`.
    chain_medallion(gate, PLATFORM) >> require_all_pages_extracted()

    # A checagem de órfã não é pré-requisito do medallion: ela denuncia conexão esquecida,
    # não impede a gold do dia. Depende só da descoberta, que é quem já lista as conexões.
    assert_no_orphan_connections(connection_ids)
