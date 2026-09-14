import pytest

from src.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark():
    """Uma única SparkSession para a suíte inteira.

    Antes, cada arquivo de teste criava (e derrubava) a sua própria sessão — em ~9
    módulos numa mesma execução do pytest, isso significava recriar o JVM e o pool de
    workers Python repetidas vezes seguidas. Sob essa churn, o worker Python às vezes
    morria no meio de uma tarefa (`Python worker exited unexpectedly`, `EOFException`,
    `SocketException: Connection reset`) — flakiness intermitente, sem relação com a
    lógica testada. Uma sessão só, compartilhada por toda a suíte, elimina a causa.

    `local[2]`, não `local[*]`: os dados de teste são pequenos (sintéticos ou algumas
    centenas de linhas reais) — não há ganho em disputar dezenas de workers por eles, só
    mais contenção com o catalog-api e com qualquer outra coisa rodando na máquina.
    """
    session = get_spark_session(app_name="binder-etl-test-suite", master="local[2]")
    session.conf.set("spark.sql.shuffle.partitions", "4")
    yield session
    session.stop()
