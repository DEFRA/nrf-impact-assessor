ARG PARENT_VERSION=2.7.0-python3.14.6
ARG PORT=8085
ARG PORT_DEBUG=8086

FROM defradigital/python-development:${PARENT_VERSION} AS development

ENV PATH="/home/nonroot/.venv/bin:${PATH}"
ENV LOG_CONFIG="logging-dev.json"

WORKDIR /home/nonroot

COPY --chown=nonroot:nonroot --chmod=444 pyproject.toml .
COPY --chown=nonroot:nonroot --chmod=444 uv.lock .
COPY --chown=nonroot:nonroot --chmod=555 app/ ./app/
# root-owned deliberately: a nonroot-owned 555 file is still writable by
# nonroot, which can chmod its own file. Root ownership makes the mode stick.
# 555 not 444 -- --chmod also applies to the `scripts/` directory COPY creates,
# and without the execute bit nonroot could not traverse into it.
COPY --chown=root:root --chmod=555 scripts/install_ostn15.py ./scripts/
COPY --chmod=444 .git-has[h] ./

RUN --mount=type=cache,target=/home/nonroot/.cache/uv,uid=1000,gid=1000 \
    uv sync --locked --link-mode=copy

# PROJ ships no UK datum-shift grid, and without one EPSG:4326 <-> EPSG:27700
# silently degrades to a ~3m-inaccurate Helmert approximation. See the script.
RUN python scripts/install_ostn15.py

COPY --chown=nonroot:nonroot logging-dev.json .

ARG PORT=8085
ARG PORT_DEBUG=8086
ENV PORT=${PORT}
EXPOSE ${PORT} ${PORT_DEBUG}

CMD [ "-m", "app.consumer" ]

FROM defradigital/python:${PARENT_VERSION} AS production

ENV PATH="/home/nonroot/.venv/bin:${PATH}"
ENV LOG_CONFIG="logging.json"

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gdal-bin \
    libgdal36 \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

USER nonroot

WORKDIR /home/nonroot

COPY --from=development /home/nonroot/pyproject.toml .
COPY --chown=nonroot:nonroot README.md .
COPY --from=development /home/nonroot/uv.lock .
COPY --from=development /home/nonroot/app ./app
COPY --from=development --chown=root:root --chmod=555 /home/nonroot/scripts/install_ostn15.py ./scripts/
COPY --from=development --chmod=444 /home/nonroot/.git-has[h] ./

COPY logging.json .

RUN --mount=type=cache,target=/home/nonroot/.cache/uv,uid=1000,gid=1000 \
    --mount=from=development,source=/home/nonroot/.local/bin/uv,target=/home/nonroot/.local/bin/uv \
    uv sync --locked --compile-bytecode --link-mode=copy --no-dev

# This stage builds its own virtualenv, so the grid installed in `development`
# is not inherited -- PROJ's data directory here is a different path.
RUN python scripts/install_ostn15.py

ARG PORT
ENV PORT=${PORT}
EXPOSE ${PORT}

CMD [ "-m", "app.consumer" ]
