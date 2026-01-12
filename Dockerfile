# Utilise une image légère avec Python
FROM python:3.11-slim

# Répertoire de travail
WORKDIR /app

# Copie des fichiers requis
COPY requirements.txt .

# Installation des dépendances
RUN apt-get update && apt-get install -y \
    build-essential \
    libglib2.0-0 \
    libgl1 \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copie du reste de l’application
COPY . .

# Création du dossier de logs si nécessaire
RUN mkdir -p /app/logs

# Port exigé par Cloud Run
ENV PORT=8080
EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]