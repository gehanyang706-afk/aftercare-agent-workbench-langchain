ARG BASE_IMAGE=python:3.13.14-slim-bookworm@sha256:67a1e1f215ccda113cfc024e8639049257e88f273898f595b61476d128d387e8
FROM ${BASE_IMAGE}
USER root
ARG REUSE_LOCKED_RUNTIME=false
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.lock pyproject.toml ./
COPY scripts/backend_runtime_lock.py ./scripts/backend_runtime_lock.py
# Default: clean, hash-checked installation. Local reuse is explicit and fails
# before pruning if any native-platform locked dependency is missing/mismatched.
# Metadata version checks of a cached runtime do not revalidate wheel hashes.
RUN python -c "import sys; assert sys.version_info[:3] == (3, 13, 14)" \
    && case "$REUSE_LOCKED_RUNTIME" in \
        false) python -m pip install --require-hashes -r requirements.lock ;; \
        true) python scripts/backend_runtime_lock.py --prune-extras ;; \
        *) echo "REUSE_LOCKED_RUNTIME_MUST_BE_TRUE_OR_FALSE"; exit 2 ;; \
    esac
# COPY merges directories. Remove stale application assets from a reused base
# inside this new image layer so deleted source files cannot survive the update.
RUN rm -rf /app/src /app/configs /app/data /app/migrations
COPY src ./src
COPY configs ./configs
COPY data ./data
COPY migrations ./migrations
COPY alembic.ini ./
RUN python -m pip install --no-deps --no-build-isolation -e . \
    && python scripts/backend_runtime_lock.py --require-project \
    && python -m pip check \
    && if id aftercare >/dev/null 2>&1; then \
        test "$(id -u aftercare)" = 10001; \
    else useradd --uid 10001 --create-home aftercare; fi \
    && mkdir -p /app/runtime/mock-warehouse \
    && chown -R 10001:10001 /app/runtime
USER aftercare
EXPOSE 8000
CMD ["python", "-m", "aftercare", "serve", "--offline", "--container-network"]
