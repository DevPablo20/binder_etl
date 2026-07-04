import subprocess
import sys
from pathlib import Path

from src.transformers import PLATFORMS

ROOT = Path(__file__).resolve().parents[1]


def test_platforms_contains_tiktok():
    assert "tiktok" in PLATFORMS


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
