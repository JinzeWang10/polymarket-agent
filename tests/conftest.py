import json
from pathlib import Path

import pytest


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir():
    return FIXTURES_DIR


def load_fixture(name: str) -> list | dict:
    path = FIXTURES_DIR / name
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return []
