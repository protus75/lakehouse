FROM python:3.11-slim

WORKDIR /workspace

# Pre-create mount point directories so bind mounts work reliably
# on Docker Desktop for Windows (WSL2 backend)
RUN mkdir -p /workspace/cache

RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Base packages — heavy, rarely changes (cached layer)
COPY requirements-base.txt .
RUN pip install -r requirements-base.txt

# Additional packages — lightweight, changes frequently (fast rebuild)
COPY requirements.txt .
RUN pip install -r requirements.txt

EXPOSE 8889 8501