import os
import tempfile
from pathlib import Path

import pytest

# Must be set before Airflow's config is first loaded (imports below).
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-tests-"))

from airflow.dag_processing.dagbag import DagBag  # noqa: E402
from airflow.utils.db import initdb  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run integration tests (requires Docker, e.g. RustFS/S3)",
    )
    parser.addoption(
        "--dlt-profile",
        action="store",
        default=None,
        help="dlt run-context profile to activate for the test session",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-integration"):
        return
    skip = pytest.mark.skip(reason="requires --run-integration")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session", autouse=True)
def airflow_db():
    """Initialize the Airflow metadata database once per test session."""
    initdb()
    yield


@pytest.fixture(scope="session", autouse=True)
def dlt_profile(request):
    """Activate the dlt run-context profile requested via ``--dlt-profile``."""
    profile = request.config.getoption("--dlt-profile")
    if not profile:
        yield None
        return

    from dlt.common.runtime.run_context import switch_context, switched_run_context

    with switched_run_context(switch_context(run_dir=None, profile=profile)) as ctx:
        yield ctx


@pytest.fixture
def local_bucket(tmp_path, monkeypatch):
    """Point the dlt filesystem destination at a throwaway local bucket."""
    bucket = tmp_path / "bucket"
    bucket.mkdir()
    monkeypatch.setenv("DESTINATION__FILESYSTEM__BUCKET_URL", f"file://{bucket}")
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / ".dlt"))
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")
    return bucket


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR))
