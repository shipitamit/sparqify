# syntax=docker/dockerfile:1
# Version-agnostic image: YOU choose the Spark image (any Spark/Scala/Java build) and the matching Gluten bundle jar.
#
#   docker build -f deploy/gluten/Dockerfile \
#     --build-arg BASE_IMAGE=<your spark image with python3, e.g. one you already run on your cluster> \
#     --build-arg GLUTEN_JAR_URL=<url of the Gluten Velox bundle built for that Spark/Scala/OS>   (or put the jar in the build context, see below) \
#     -t myorg/spark-gluten:custom .
#
# Pick the bundle that matches the Spark version, Scala version and Linux distribution of BASE_IMAGE: a mismatched jar fails at start-up.
ARG BASE_IMAGE
FROM python:3.12-slim AS wheel
WORKDIR /src
COPY pyproject.toml README.md ./
COPY sparqify ./sparqify
RUN pip wheel --no-cache-dir --no-deps -w /wheels .

FROM ${BASE_IMAGE}
ARG GLUTEN_JAR_URL=""
ARG GLUTEN_JAR_SHA256=""
ARG GLUTEN_OFFHEAP=4g
USER root
COPY --from=wheel /wheels /tmp/wheels
# Optional: a jar placed at deploy/gluten/jars/gluten.jar in the build context is used when no URL is given.
COPY deploy/gluten/jars/ /tmp/jars/
RUN set -eux; mkdir -p /opt/gluten; \
    if [ -n "$GLUTEN_JAR_URL" ]; then \
        (command -v curl >/dev/null && curl -fsSL "$GLUTEN_JAR_URL" -o /opt/gluten/gluten.jar) || python3 -c "import sys,urllib.request as u; u.urlretrieve(sys.argv[1], '/opt/gluten/gluten.jar')" "$GLUTEN_JAR_URL"; \
    else cp /tmp/jars/gluten.jar /opt/gluten/gluten.jar; fi; \
    if [ -n "$GLUTEN_JAR_SHA256" ]; then echo "$GLUTEN_JAR_SHA256  /opt/gluten/gluten.jar" | sha256sum -c -; fi; \
    chmod 0444 /opt/gluten/gluten.jar; \
    python3 -m pip install --no-cache-dir --no-deps /tmp/wheels/*.whl; \
    SPARK_CONF_DIR="${SPARK_CONF_DIR:-${SPARK_HOME:-/opt/spark}/conf}"; mkdir -p "$SPARK_CONF_DIR"; \
    python3 -m sparqify gluten-conf --jar /opt/gluten/gluten.jar --offheap "$GLUTEN_OFFHEAP" >> "$SPARK_CONF_DIR/spark-defaults.conf"; \
    rm -rf /tmp/wheels /tmp/jars
# Back to the base image's own user and entrypoint: nothing else is changed.
ARG RUN_AS_USER=185
USER ${RUN_AS_USER}
