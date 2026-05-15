ARG BASE_VERSION=3.14.3-slim-trixie
ARG PYTHON_VERSION=3.14.3
ARG PORT=8085
ARG PORT_DEBUG=8086

FROM python:${BASE_VERSION} AS base

ARG PYTHON_VERSION

ENV PATH="/home/nonroot/.venv/bin:/home/nonroot/.local/bin:${PATH}"
ENV PYTHONUNBUFFERED=1
ENV UV_PYTHON=${PYTHON_VERSION}
ENV UV_MANAGED_PYTHON=0
ENV UV_PYTHON_DOWNLOADS=0

RUN apt-get update && apt-get upgrade -y --no-install-recommends \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade --force-reinstall pip

RUN addgroup --gid 1000 nonroot \
    && adduser nonroot \
        --uid 1000 \
        --gid 1000 \
        --home /home/nonroot \
        --shell /bin/bash

FROM base AS development

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHON_ENV=development
ENV LOG_CONFIG="logging-dev.json"

RUN python -m pip install uv debugpy

USER nonroot
WORKDIR /home/nonroot

COPY --chown=nonroot:nonroot pyproject.toml .
COPY --chown=nonroot:nonroot README.md .
COPY --chown=nonroot:nonroot uv.lock .
COPY --chown=nonroot:nonroot app/ ./app/
COPY --chmod=444 .git-has[h] ./

RUN --mount=type=cache,target=/home/nonroot/.cache/uv,uid=1000,gid=1000 \
    uv sync --locked --link-mode=copy

COPY --chown=nonroot:nonroot logging-dev.json .

ARG PORT=8085
ARG PORT_DEBUG=8086
ENV PORT=${PORT}
EXPOSE ${PORT} ${PORT_DEBUG}

ENTRYPOINT ["python"]
CMD ["-m", "app.consumer"]

FROM base AS production

ENV PYTHON_ENV=production
ENV LOG_CONFIG="logging.json"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gdal-bin \
    htop \
    libgdal36 \
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
    --mount=from=development,source=/usr/local/bin/uv,target=/usr/local/bin/uv \
    uv sync --locked --compile-bytecode --link-mode=copy --no-dev

ARG PORT
ENV PORT=${PORT}
EXPOSE ${PORT}

ENTRYPOINT ["python"]
CMD ["-m", "app.consumer"]
