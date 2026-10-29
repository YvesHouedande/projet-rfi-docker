# Image d'ingestion « rfi-ingest » : lancée par le DockerOperator (un conteneur par tâche).
# Pas d'ENTRYPOINT : la commande (fetch, load, find...) vient de l'opérateur.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# Utilisateur non-root (uid 1000) et dossier de données : un volume nommé vide hérite
# de ces droits au premier montage.
RUN useradd --uid 1000 --create-home rfi \
    && mkdir -p /data \
    && chown 1000:1000 /data

USER 1000:1000
