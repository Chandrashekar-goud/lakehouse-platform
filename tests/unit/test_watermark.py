from lakehouse.ingestion.watermark import WatermarkStore


def test_watermark_roundtrip_and_default(tmp_path):
    wm = WatermarkStore(tmp_path / "wm.json")
    assert wm.get("weather", "2026-01-01") == "2026-01-01"
    wm.set("weather", "2026-06-01")
    wm.set("crimes", "2026-06-02T00:00:00")
    assert wm.get("weather", "x") == "2026-06-01"
    assert wm.get("crimes", "x") == "2026-06-02T00:00:00"
