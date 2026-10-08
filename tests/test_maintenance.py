"""Manutenção do lake: a trava de retenção e a descoberta de tabelas.

O que vale testar aqui é a **decisão**, não o VACUUM em si: se o recolhimento de arquivo
morto funciona é problema do Delta, mas quando a trava de retenção cai — e se ela cai por
acidente — é decisão nossa, e é a única com consequência irreversível.
"""
import pytest

from src.io import maintenance
from src.pipelines import vacuum as vacuum_cli


class TestRetentionGuard:
    def test_default_does_not_need_override(self):
        """O default do módulo é o piso do Delta: não desliga trava nenhuma."""
        assert not maintenance.requires_check_override(maintenance.DEFAULT_RETAIN_HOURS)

    @pytest.mark.parametrize("hours", [0, 1, 72, 167])
    def test_below_delta_floor_needs_override(self, hours):
        assert maintenance.requires_check_override(hours)

    @pytest.mark.parametrize("hours", [168, 169, 336])
    def test_at_or_above_floor_does_not(self, hours):
        assert not maintenance.requires_check_override(hours)


class TestCli:
    def test_dry_run_is_the_default(self):
        """A ação que apaga precisa ser pedida: `--apply` ausente é relatório."""
        args = vacuum_cli._parse_args([])
        assert args.apply is False
        assert args.retain_hours == maintenance.DEFAULT_RETAIN_HOURS

    def test_short_retention_is_refused_without_explicit_flag(self, capsys):
        """Sem `--allow-short-retention`, retenção curta sai pelo código 2 e não sobe Spark.

        É a proteção que importa: `--retain-hours 0` digitado sozinho não pode virar uma
        limpeza que apaga o chão de um leitor concorrente.
        """
        code = vacuum_cli.main(["--apply", "--retain-hours", "0"])
        assert code == 2
        assert "allow-short-retention" in capsys.readouterr().err

    def test_short_retention_passes_with_the_flag(self):
        args = vacuum_cli._parse_args(
            ["--apply", "--retain-hours", "0", "--allow-short-retention"]
        )
        assert args.allow_short_retention is True
        assert maintenance.requires_check_override(args.retain_hours)

    def test_rejects_unknown_layer(self):
        with pytest.raises(SystemExit):
            vacuum_cli._parse_args(["--layers", "raw"])


class TestVacuumLake:
    def test_raw_is_not_a_delta_layer(self, spark):
        """`raw` é Parquet solto do Airbyte, não tabela Delta — e volume de raw é outro
        problema. Pedir VACUUM nele é erro de chamada, não no-op silencioso."""
        with pytest.raises(ValueError, match="raw"):
            maintenance.vacuum_lake(spark, layers=("raw",))


class TestDiscovery:
    def test_finds_the_tables_the_medallion_writes(self, spark):
        """Smoke contra o MinIO de verdade: a descoberta tem que achar as tabelas que as
        plataformas gravam, e devolver URI que o Delta consegue abrir."""
        tables = maintenance.discover_delta_tables(spark, "bronze")
        assert tables, "nenhuma tabela Delta encontrada em bronze — rode um medallion antes"
        assert all(uri.startswith("s3a://") for uri in tables)
        assert any(uri.endswith("/tiktok/ads_reports_daily") for uri in tables)

    def test_does_not_descend_into_a_delta_table(self, spark):
        """Tabela Delta não contém outra: nenhum resultado é prefixo de outro — senão o
        `_delta_log` de uma tabela viraria 'tabela' e o VACUUM rodaria duas vezes nela."""
        tables = maintenance.discover_delta_tables(spark, "bronze")
        for uri in tables:
            assert maintenance.LOG_DIR not in uri
            assert not any(other.startswith(uri + "/") for other in tables)


class TestListingParallelism:
    def test_vacuum_lake_tames_the_listing_parallelism(self, spark):
        """O default do Spark (10.000) transforma cada tabela em 10.000 tasks e a passada
        inteira em mais de uma hora de escalonamento em `local[*]`. `vacuum_lake` tem que
        baixar isso antes de listar — senão o DAG semanal fica inviável."""
        spark.conf.unset(maintenance.LISTING_PARALLELISM_CONF)
        maintenance.vacuum_lake(spark, layers=("gold",), apply=False)
        assert (
            spark.conf.get(maintenance.LISTING_PARALLELISM_CONF)
            == maintenance.LISTING_PARALLELISM
        )
