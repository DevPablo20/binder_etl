import subprocess
import sys
from pathlib import Path

import pytest

from src.transformers import CATALOG_BY_PLATFORM, PLATFORMS, get_catalog_transforms

ROOT = Path(__file__).resolve().parents[1]


def test_platforms_contains_tiktok():
    assert "tiktok" in PLATFORMS


def test_facebook_organic_is_a_platform_without_catalog():
    """Sem hierarquia campanha → anúncio, não há o que descobrir no Bridge. A rota do
    catálogo (`src/api/routes/catalog.py`) converte o KeyError em 404."""
    assert "facebook_organic" in PLATFORMS
    assert "facebook_organic" not in CATALOG_BY_PLATFORM
    with pytest.raises(KeyError):
        get_catalog_transforms("facebook_organic")


def test_instagram_organic_is_a_platform_without_catalog():
    assert "instagram_organic" in PLATFORMS
    assert "instagram_organic" not in CATALOG_BY_PLATFORM
    with pytest.raises(KeyError):
        get_catalog_transforms("instagram_organic")


def test_pipeline_cli_prints_usage_without_args():
    result = subprocess.run(
        [sys.executable, "-m", "src.pipelines.run"],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert result.returncode == 1
    assert "Usage:" in result.stdout


def test_pipeline_cli_lists_medallion_layer():
    result = subprocess.run(
        [sys.executable, "-m", "src.pipelines.run"],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert result.returncode == 1
    assert "medallion" in result.stdout
