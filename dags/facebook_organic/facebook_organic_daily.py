"""Facebook orgânico, nível de página: extração diária e medallion em cima dela.

`page` e `page_insights` são 1 a 2 chamadas por página — o oposto do nível de post. E o
`page_insights` só serve os **2 últimos dias**, com ~2 dias de atraso: duas falhas seguidas
na mesma página abrem buraco permanente. Por isso este é o DAG mais crítico do orgânico.

As conexões não estão escritas aqui: saem da API por tag `Organic` + `daily` e prefixo de
namespace. Página nova entra sem commit; conexão sem condutor falha a checagem.
"""
import pendulum
from airflow import DAG
from organic_tasks import (
    DAILY_TAG,
    DEFAULT_ARGS,
    FACEBOOK_NAMESPACE,
    assert_connections_are_claimed,
    organic_flow,
)

PLATFORM = "facebook_organic"

# 01:00 São Paulo: antes do corte das 06:00 que define o `snapshot_date`, então a foto fecha
# o dia anterior. Escalonado uma hora antes do Instagram — as duas redes escrevem as mesmas
# tabelas de `gold/organic/`, e o pool do medallion é a rede de segurança para o retry.
with DAG(
    dag_id=f"{PLATFORM}_daily",
    default_args=DEFAULT_ARGS,
    description="Facebook Pages, nível de página: dispara, aguarda, monta o medallion",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=[PLATFORM, "medallion", "organic", "daily"],
) as dag:
    organic_flow(DAILY_TAG, FACEBOOK_NAMESPACE, PLATFORM)

    # Só um DAG precisa vigiar o inventário inteiro de conexões, e é este — o que roda
    # primeiro no dia.
    assert_connections_are_claimed()
