FROM python:3.12-slim

LABEL maintainer="Netflix Cookie Checker Bot"
LABEL description="High-speed Netflix cookie checker Telegram bot"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r botuser && useradd -r -g botuser -d /app -s /sbin/nologin botuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY bot.py checker.py proxy_manager.py user_store.py \
     mongodb_store.py stats.py dashboard.py password_changer.py ./

RUN mkdir -p /data && chown botuser:botuser /data

ENV DB_DIR=/data

USER botuser

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

CMD ["python", "-u", "test_mongo.py"]
