# API + sink image. Build from repo root: make image-serving
FROM python:3.11-slim
WORKDIR /app
COPY libs/common /app/libs/common
RUN pip install --no-cache-dir /app/libs/common
COPY serving/requirements.txt /app/serving/requirements.txt
RUN pip install --no-cache-dir -r /app/serving/requirements.txt
COPY config /app/config
COPY contracts /app/contracts
COPY serving/src /app/serving/src
ENV CG_ROOT=/app PYTHONPATH=/app/serving/src PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "cg_serving.api:app", "--host", "0.0.0.0", "--port", "8000"]
