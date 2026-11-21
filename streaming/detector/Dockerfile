FROM ghcr.io/astral-sh/uv:0.9.18 AS uv
FROM flink:1.20.5-scala_2.12-java17

COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/cg-python \
    PATH=/opt/cg-detector/bin:$PATH \
    PYFLINK_CLIENT_EXECUTABLE=/opt/cg-detector/bin/python \
    PYTHONUNBUFFERED=1 \
    CG_ROOT=/app \
    PYTHONPATH=/app/streaming/detector/src

RUN set -eux; \
    wget -q -P "$FLINK_HOME/lib" https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar; \
    test -f "$FLINK_HOME/opt/flink-python-1.20.5.jar"; \
    for plugin in s3-fs-hadoop metrics-prometheus; do \
      jar="flink-$plugin-1.20.5.jar"; \
      target="$FLINK_HOME/plugins/$plugin/$jar"; \
      test -f "$target" || install -Dm644 "$FLINK_HOME/opt/$jar" "$target"; \
    done

WORKDIR /app
COPY streaming/requirements.txt ./streaming/requirements.txt
COPY libs/common ./libs/common
RUN uv python install 3.11 && \
    uv venv --python 3.11 /opt/cg-detector && \
    uv pip install --no-cache --python /opt/cg-detector/bin/python \
      -r streaming/requirements.txt apache-flink==1.20.5 ./libs/common

COPY config ./config
COPY contracts ./contracts
COPY streaming/detector/src ./streaming/detector/src
