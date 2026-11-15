# Gluten + Velox image (any Spark, Scala or Java version)

Nothing in sparqix is tied to one release. You supply two things that must match each other:

1. **A Spark base image** (`BASE_IMAGE`): whatever Spark/Java you already run, with `python3` and `pip`.
2. **The Gluten bundle jar** built for that Spark version, Scala version and Linux distribution (`GLUTEN_JAR_URL`, or a file at `deploy/gluten/jars/gluten.jar`).
   Optionally pass `GLUTEN_JAR_SHA256` so the build fails on a wrong or tampered file.

```
docker build -f deploy/gluten/Dockerfile \
  --build-arg BASE_IMAGE=<your spark image> \
  --build-arg GLUTEN_JAR_URL=<jar url> --build-arg GLUTEN_JAR_SHA256=<sha256> \
  --build-arg GLUTEN_OFFHEAP=8g --build-arg RUN_AS_USER=<uid of the base image's spark user> \
  -t myorg/spark-gluten:custom .
```

The build installs sparqix into the image and appends the Gluten settings to `spark-defaults.conf` with `sparqix gluten-conf`,
so the driver and every executor started from the image get the plugin, the jar on the classpath, off-heap memory and the shuffle manager.
Everything else (entrypoint, user, Spark layout) stays as in your base image.

Settings differ between Gluten releases. Override or add any of them without editing code:

    sparqix gluten-conf --conf spark.shuffle.manager=<class for your version> --conf spark.some.other=value
    sparqix app.py --target gluten --gluten-conf spark.some.key=value --gluten-jar /opt/gluten/gluten.jar

Check after the first run: the Spark UI has a Gluten tab / `WholeStageTransformer` stages; operators that fell back are listed there.
`job.example.yaml` is a single-pod example; use your normal submission method for a cluster.

Not tested here: there was no Docker daemon, Spark or Gluten build available. Jar/Spark mismatches show up as start-up errors in the driver log.

## Other engines
The same Dockerfile works for Comet (pass the Comet jar as `GLUTEN_JAR_URL`, then use `sparqix gluten-conf --engine comet` in place of the Gluten line, via a small edit of the `RUN` step)
or the ClickHouse backend (add `libch.so` to the image and use `--backend clickhouse --libpath /opt/gluten/libch.so`). Fabric and Photon are managed services: there is no image to build,
use `--target fabric` / `--target photon` for the settings.
