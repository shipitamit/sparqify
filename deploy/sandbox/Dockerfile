# Image for `sparqify agent ... --sandbox-image sparqify-sandbox --sandbox-data ./sample` (run-and-compare of the original and the converted code).
# Python, pandas, PySpark and a Java runtime cover pandas / notebooks / SQL; polars, dask and duckdb are installed for polars, dask and SQL originals.
# R originals need R: build with `--build-arg WITH_R=1` (adds r-base-core, roughly 300 MB). sparqify itself is not needed inside the image.
# The agent starts it with --network none --read-only, a 256 MB /tmp, no capabilities and a non-root user, so everything writable must live in /tmp.
#   docker build -t sparqify-sandbox deploy/sandbox            (add --build-arg WITH_R=1 to verify R scripts)
# Not built or run where this was written (no Docker daemon): check the first run and adjust versions if needed.
FROM python:3.15-slim@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928
ARG WITH_R=0
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \
    && if [ "$WITH_R" = "1" ]; then apt-get install -y --no-install-recommends r-base-core; fi \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir "pandas>=2" "pyspark>=3.4,<5" numpy polars "dask[dataframe]" duckdb
ENV HOME=/tmp SPARK_LOCAL_DIRS=/tmp SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON=python JAVA_TOOL_OPTIONS="-Djava.io.tmpdir=/tmp -Duser.home=/tmp"
