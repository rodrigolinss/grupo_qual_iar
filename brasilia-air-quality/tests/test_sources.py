from br.aqi.sources import get_sources, Source


def test_get_sources_returns_sources() -> None:
    sources = get_sources()
    assert isinstance(sources, list)
    assert all(isinstance(s, Source) for s in sources)
    assert len(sources) >= 1