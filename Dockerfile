FROM python:3.11-slim

# No root needed: the data volume is chowned to this uid/gid in compose.
RUN groupadd --system --gid 10001 app \
 && useradd --system --uid 10001 --gid app --home-dir /app --shell /usr/sbin/nologin app

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

ENV DATA_VOLUME=/data \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN mkdir -p /data && chown -R app:app /app /data
USER app

EXPOSE 8000

# Single instance only: the in-process worker and the SQLite WAL assume one writer.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
