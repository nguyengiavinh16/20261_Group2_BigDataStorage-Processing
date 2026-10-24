FROM python:3.10-slim
WORKDIR /app
COPY pyproject.toml requirements.txt ./
COPY src ./src
RUN pip install --no-cache-dir -e .
COPY app ./app
COPY artifacts ./artifacts
ENV PYTHONPATH=/app/src
EXPOSE 8000
CMD ["uvicorn", "fraud_detection.serving.api:app", "--host", "0.0.0.0", "--port", "8000"]
