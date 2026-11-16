# Build from repo root: make image-ingest
FROM python:3.11-slim
WORKDIR /app
COPY libs/common /app/libs/common
RUN pip install --no-cache-dir /app/libs/common
COPY ingest/requirements.txt /app/ingest/requirements.txt
RUN pip install --no-cache-dir -r /app/ingest/requirements.txt
COPY config /app/config
COPY contracts /app/contracts
COPY ingest/src /app/ingest/src
ENV CG_ROOT=/app PYTHONPATH=/app/ingest/src PYTHONUNBUFFERED=1
EXPOSE 9108
ENTRYPOINT ["python", "-m"]
CMD ["collectors.main", "--source", "binance"]
