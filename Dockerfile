# syntax=docker/dockerfile:1
# Stdlib-only app: no third-party packages, so the image is small and has little to patch.
FROM python:3.12-slim AS build
WORKDIR /src
COPY pyproject.toml README.md ./
COPY sparqix ./sparqix
RUN pip install --no-cache-dir --prefix=/install .

FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=random
RUN groupadd -g 10001 sparqix && useradd -u 10001 -g 10001 -M -s /usr/sbin/nologin sparqix
COPY --from=build /install /usr/local
COPY examples /opt/sparqix/examples
USER 10001:10001
WORKDIR /tmp
EXPOSE 8765
HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import urllib.request as u; u.urlopen('http://127.0.0.1:8765/healthz', timeout=2)"
# The web UI needs a token when it listens on a non-loopback address: set SPARQIX_TOKEN (the manifests use a Secret).
ENTRYPOINT ["sparqix"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8765"]
