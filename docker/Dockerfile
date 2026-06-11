# Reproducible Titian environment: Spark 4.1.2 + JDK 17 + Python 3 + the Titian jar
# built from source, with the three end-to-end notebooks.
#
#   docker build -t titian -f docker/Dockerfile .
#   docker run --rm -p 8888:8888 titian            # JupyterLab at :8888
#   docker run --rm titian validate                # execute all notebooks headlessly

# ---- build stage: compile the Titian jar from source -------------------------
FROM eclipse-temurin:17-jdk-jammy AS build
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
RUN curl -sL https://github.com/sbt/sbt/releases/download/v1.10.7/sbt-1.10.7.tgz | tar xz
COPY build.sbt .
COPY project project
COPY src src
RUN ./sbt/bin/sbt -batch package \
    && cp "$(find /root/.cache/coursier -name 'fastutil-8.5.15.jar' | head -1)" \
          /build/fastutil.jar

# ---- runtime stage ------------------------------------------------------------
FROM eclipse-temurin:17-jdk-jammy
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/local/bin/python
RUN pip3 install --no-cache-dir pyspark==4.1.2 jupyterlab nbconvert

WORKDIR /opt/titian
COPY --from=build /build/target/scala-2.13/*.jar target/scala-2.13/
COPY --from=build /build/fastutil.jar jars/fastutil.jar
COPY python python
COPY notebooks notebooks
COPY src/test/resources src/test/resources
COPY examples/data examples/data
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV TITIAN_HOME=/opt/titian \
    FASTUTIL_JAR=/opt/titian/jars/fastutil.jar \
    PYTHONPATH=/opt/titian/python

EXPOSE 8888
ENTRYPOINT ["/entrypoint.sh"]
CMD ["lab"]
