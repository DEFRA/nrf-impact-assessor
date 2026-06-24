ARG PARENT_VERSION=2.6.0-python3.14.5
ARG PORT=8085
ARG PORT_DEBUG=8086

FROM defradigital/python-development:${PARENT_VERSION} AS development

ENV PATH="/home/nonroot/.venv/bin:${PATH}"
ENV LOG_CONFIG="logging-dev.json"

WORKDIR /home/nonroot

COPY --chown=nonroot:nonroot --chmod=444 pyproject.toml .
COPY --chown=nonroot:nonroot --chmod=444 uv.lock .
COPY --chown=nonroot:nonroot --chmod=555 app/ ./app/
COPY --chmod=444 .git-has[h] ./

RUN --mount=type=cache,target=/home/nonroot/.cache/uv,uid=1000,gid=1000 \
    uv sync --locked --link-mode=copy

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
COPY --from=development --chmod=444 /home/nonroot/.git-has[h] ./

COPY logging.json .

RUN --mount=type=cache,target=/home/nonroot/.cache/uv,uid=1000,gid=1000 \
    --mount=from=development,source=/home/nonroot/.local/bin/uv,target=/home/nonroot/.local/bin/uv \
    uv sync --locked --compile-bytecode --link-mode=copy --no-dev

ARG PORT
ENV PORT=${PORT}
EXPOSE ${PORT}

CMD [ "-m", "app.consumer" ]
