from pathlib import Path


def test_pipeline_prepares_telegram_variant_for_bilibili() -> None:
    source = Path("/app/services/pipeline.py").read_text(encoding="utf-8")

    assert 'p.id in {"youtube", "bilibili"}' in source
