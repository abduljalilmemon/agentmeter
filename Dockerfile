FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    TIKTOKEN_CACHE_DIR=/opt/tiktoken
WORKDIR /srv

COPY pyproject.toml README.md requirements.lock ./
COPY app ./app
COPY worker ./worker
RUN pip install --no-cache-dir -c requirements.lock .

# Fetch the tokenizer files at build time so containers never download them at runtime.
# o200k_base: gpt-4o family and the fallback; cl100k_base: gpt-4 / gpt-3.5.
RUN python -c "import tiktoken; [tiktoken.get_encoding(e) for e in ('o200k_base', 'cl100k_base')]" \
    && test -n "$(ls -A "$TIKTOKEN_CACHE_DIR")"

COPY agentmeter.yaml ./
ENV AGENTMETER_CONFIG=/srv/agentmeter.yaml

EXPOSE 8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
