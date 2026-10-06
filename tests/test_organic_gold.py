"""A conservação de frescor da gold orgânica: a foto mais nova do silver chega à ponta.

`assert_photo_reached_gold` é o que impede uma gold velha de ser publicada em silêncio — o
caso em que um join, um filtro ou uma janela mal fechada derruba o dia mais recente e o
pipeline termina verde. Roda antes da escrita, dentro da sessão Spark que o transformer já
tem aberta, e por isso vale também para quem roda o medallion pela CLI.
"""
from datetime import date

import pytest

from src.transformers.organic_gold import assert_photo_reached_gold

SILVER_SCHEMA = "content_id string, snapshot_date date, reach_lifetime bigint"
GOLD_SCHEMA = "content_id string, date date, reach_new bigint"


def silver(spark, days):
    return spark.createDataFrame(
        [("c1", day, 10) for day in days], schema=SILVER_SCHEMA
    )


def gold(spark, days):
    return spark.createDataFrame([("c1", day, 1) for day in days], schema=GOLD_SCHEMA)


D1, D2, D3 = date(2026, 10, 3), date(2026, 10, 4), date(2026, 10, 5)


def test_passa_quando_a_gold_tem_a_foto_mais_nova(spark):
    assert_photo_reached_gold(
        gold(spark, [D1, D2, D3]),
        {"post_metrics_daily": silver(spark, [D1, D2, D3])},
        "content_daily",
    )


def test_levanta_quando_a_gold_perdeu_o_dia_mais_novo(spark):
    with pytest.raises(RuntimeError) as exc:
        assert_photo_reached_gold(
            gold(spark, [D1, D2]),
            {"post_metrics_daily": silver(spark, [D1, D2, D3])},
            "content_daily",
        )

    message = str(exc.value)
    assert "content_daily" in message
    assert str(D3) in message, "a mensagem tem de dizer até onde o silver ia"
    assert str(D2) in message, "e onde a gold parou"


def test_account_daily_fica_de_fora(spark):
    """A data do `account_daily` é a da métrica na fonte, que atrasa por desenho — o
    `page_insights` do Facebook chega com ~2 dias. Checar frescor ali daria falso positivo
    todo dia."""
    assert_photo_reached_gold(
        gold(spark, [D1]),
        {"page_insights_daily": silver(spark, [D1, D2, D3])},
        "account_daily",
    )


def test_tabela_sem_grao_de_dia_fica_de_fora(spark):
    assert_photo_reached_gold(
        spark.createDataFrame([("c1",)], schema="content_id string"),
        {"posts": silver(spark, [D1, D2, D3])},
        "content",
    )


def test_silver_sem_coluna_de_foto_nao_e_comparado(spark):
    """`content` e `accounts` entram como source de dimensão, sem `snapshot_date`. Sem
    nenhuma referência de foto, não há o que afirmar — e inventar uma seria pior."""
    assert_photo_reached_gold(
        gold(spark, [D1]),
        {"accounts": spark.createDataFrame([("a1",)], schema="account_id string")},
        "content_daily",
    )


def test_a_referencia_e_o_source_mais_novo(spark):
    """Com mais de um source datado, vale o mais novo: a gold tem de alcançar o mais
    recente que o lake extraiu, não a média deles."""
    with pytest.raises(RuntimeError, match="post_metrics_daily"):
        assert_photo_reached_gold(
            gold(spark, [D1]),
            {
                "pages": silver(spark, [D1]),
                "post_metrics_daily": silver(spark, [D1, D3]),
            },
            "content_daily",
        )


def test_gold_adiante_do_silver_nao_levanta(spark):
    """Reprocessamento parcial pode deixar a gold à frente de um source; o que a invariante
    proíbe é a gold ficar **atrás**."""
    assert_photo_reached_gold(
        gold(spark, [D1, D2, D3]),
        {"post_metrics_daily": silver(spark, [D1, D2])},
        "content_daily",
    )
