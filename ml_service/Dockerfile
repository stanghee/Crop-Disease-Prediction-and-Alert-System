FROM python:3.9-slim

# Imposta le variabili d'ambiente per ottimizzare Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Installa dipendenze di sistema necessarie per ML
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    g++ \
    libblas-dev \
    liblapack-dev \
    tzdata \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Crea directory per i modelli
RUN mkdir -p /app/models

# Copia e installa le dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && rm -rf ~/.cache/pip/*

# Copia il codice del servizio
COPY model_trainer.py .
COPY anomaly_detector.py .
COPY ml_service.py .
COPY entrypoint.sh .

# Utente non-root per sicurezza
RUN useradd -m mluser
RUN chmod +x entrypoint.sh && chown -R mluser /app
USER mluser

# Espone una porta per potenziali API future
EXPOSE 8080

CMD ["python", "ml_service.py"] 