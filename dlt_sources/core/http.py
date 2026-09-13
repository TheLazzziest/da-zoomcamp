from __future__ import annotations

from collections.abc import Mapping

import httpx

try:  # pragma: no cover - import guard for environments without the Rust backend
    from pyreqwest.compatibility.httpx.transport import SyncHttpxTransport

    _RUST_TRANSPORT = True
except ImportError:  # pragma: no cover
    _RUST_TRANSPORT = False


def build_client(
    *,
    timeout: float,
    base_url: str,
    headers: Mapping[str, str] | None = None,
) -> httpx.Client:
    """Build an ``httpx.Client`` backed by the Rust ``reqwest`` transport.

    The ``pyreqwest`` transport keeps the familiar httpx API while delegating
    connection pooling, HTTP/2 negotiation and (de)compression to Rust. When the
    backend is unavailable it falls back to httpx's native transport with HTTP/2.
    ``timeout`` and ``base_url`` are supplied by the caller
    (e.g. ``settings.http.timeout`` and the source's ``base_url``).
    """
    if _RUST_TRANSPORT:
        return httpx.Client(
            base_url=base_url,
            headers=headers,
            timeout=timeout,
            transport=SyncHttpxTransport(),
        )
    return httpx.Client(
        base_url=base_url,
        headers=headers,
        timeout=timeout,
        http2=True,
    )
