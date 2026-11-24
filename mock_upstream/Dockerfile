# Build context is the repo root (see docker-compose.yml).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /srv

RUN pip install --no-cache-dir "fastapi>=0.115" "uvicorn[standard]>=0.30"
COPY mock_upstream ./mock_upstream

EXPOSE 8000
CMD ["uvicorn", "mock_upstream.main:app", "--host", "0.0.0.0", "--port", "8000"]
