# syntax=docker/dockerfile:1
# Stdlib-only app: no third-party packages, so the image is small and has little to patch.
FROM python:3.15-slim@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928 AS build
WORKDIR /src
COPY pyproject.toml README.md ./
COPY sparqify ./sparqify
RUN pip install --no-cache-dir --prefix=/install .

FROM python:3.15-slim@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=random
# Pull in Debian security fixes published after the base image was built (e.g. the zlib1g HIGH CVEs the CI scan flagged)
RUN apt-get update && apt-get upgrade -y --no-install-recommends && rm -rf /var/lib/apt/lists/*
RUN groupadd -g 10001 sparqify && useradd -u 10001 -g 10001 -M -s /usr/sbin/nologin sparqify
COPY --from=build /install /usr/local
COPY examples /opt/sparqify/examples
USER 10001:10001
WORKDIR /tmp
EXPOSE 8765
HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import urllib.request as u; u.urlopen('http://127.0.0.1:8765/healthz', timeout=2)"
# The web UI needs a token when it listens on a non-loopback address: set SPARQIFY_TOKEN (the manifests use a Secret).
ENTRYPOINT ["sparqify"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8765"]
