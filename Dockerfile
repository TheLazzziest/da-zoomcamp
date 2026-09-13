# Single build for both targets:
#   Astro(default): docker build .
#   Airflow: docker build --build-arg BASE_IMAGE=apache/airflow:3.3.1-python3.13 --build-arg RUN_USER=airflow .
ARG BASE_IMAGE=quay.io/astronomer/astro-runtime:3.3-7-python-3.13
ARG RUN_USER=astro

# --- build the project package ---
FROM ${BASE_IMAGE} AS builder

ARG RUN_USER
USER root
RUN mkdir -p /wheels && chown -R ${RUN_USER}:0 /wheels
USER ${RUN_USER}
COPY --chown=${RUN_USER}:0 pyproject.toml README.md /build/
COPY --chown=${RUN_USER}:0 dlt_sources /build/dlt_sources
RUN pip wheel --no-cache-dir --wheel-dir /wheels /build

# --- runtime stage: install the prebuilt wheels only ---
FROM ${BASE_IMAGE} AS runtime

ARG RUN_USER
USER ${RUN_USER}
COPY --from=builder --chown=${RUN_USER}:0 /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels da-zoomcamp \
    && rm -rf /wheels/*
