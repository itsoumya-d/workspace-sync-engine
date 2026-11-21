FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

COPY src/ src/
RUN pip install --no-cache-dir -e .

EXPOSE 8000

CMD ["uvicorn", "workspace_sync_engine.api:app", "--host", "0.0.0.0", "--port", "8000"]
