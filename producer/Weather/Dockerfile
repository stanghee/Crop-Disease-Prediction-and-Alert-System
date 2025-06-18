FROM python:3.9-slim

# Imposta le variabili d'ambiente per ottimizzare Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Installa solo le dipendenze di sistema necessarie
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copia e installa le dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && rm -rf ~/.cache/pip/*

# Copia solo i file necessari
COPY producer.py .
COPY wait_for_kafka.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Utente non-root per sicurezza
RUN useradd -m appuser && chown -R appuser /app
USER appuser

CMD ["./entrypoint.sh"]