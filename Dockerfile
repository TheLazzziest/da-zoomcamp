FROM quay.io/astronomer/astro-runtime:3.3-7-python-3.13

# Install the project (dlt sources + pipelines) as a library into the image,
# so DAGs can `from dlt_sources import pipelines`. Version comes from git (hatch-vcs).
COPY --chown=astro:0 pyproject.toml README.md /tmp/build/
COPY --chown=astro:0 dlt_sources /tmp/build/dlt_sources
RUN pip install --no-cache-dir /tmp/build && rm -rf /tmp/build
