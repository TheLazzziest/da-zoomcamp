import time
from dataclasses import dataclass

import httpx
import pytest
from testcontainers.core.container import DockerContainer

RUSTFS_IMAGE = "rustfs/rustfs:1.0.0-rc.6"


@dataclass
class RustFS:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str


@pytest.fixture(scope="session")
def rustfs():
    """Start a RustFS (S3-compatible) container and yield its connection settings."""
    container = (
        DockerContainer(RUSTFS_IMAGE)
        .with_env("RUSTFS_ADDRESS", "0.0.0.0:9000")
        .with_env("RUSTFS_CONSOLE_ADDRESS", "0.0.0.0:9001")
        .with_env("RUSTFS_ACCESS_KEY", "rustfsadmin")
        .with_env("RUSTFS_SECRET_KEY", "rustfsadmin")
        .with_exposed_ports(9000)
    )
    with container:
        endpoint = (
            f"http://{container.get_container_host_ip()}:"
            f"{container.get_exposed_port(9000)}"
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                if httpx.get(f"{endpoint}/health", timeout=2).status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(1)
        else:
            raise RuntimeError("RustFS did not become healthy in time")

        yield RustFS(
            endpoint=endpoint,
            access_key="rustfsadmin",
            secret_key="rustfsadmin",
            bucket="bronze",
        )
