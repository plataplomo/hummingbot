# Multi-stage build for CyberDeltaEngine
FROM python:3.13-slim AS builder

# Set working directory
WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    build-essential \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Bun
RUN curl -fsSL https://bun.sh/install | bash

# Copy project files for building
COPY pyproject.toml setup.py README.md ./
COPY cyberdelta ./cyberdelta

# Build the wheel
RUN pip install --no-cache-dir build && \
    python -m build --wheel

# Runtime stage
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy and install the built wheel
COPY --from=builder /build/dist/*.whl ./
RUN pip install --no-cache-dir *.whl && rm *.whl

# Install additional runtime dependencies not in wheel
RUN pip install --no-cache-dir uvloop

# Create necessary directories
RUN mkdir -p /app/data/state_backups /app/logs /app/config

# Copy application files
COPY main.py ./
COPY cyberdelta ./cyberdelta

# Create non-root user
RUN useradd -m -u 1000 trader && \
    chown -R trader:trader /app

# Switch to non-root user
USER trader

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8050', timeout=5)" || exit 1

# Expose dashboard port
EXPOSE 8050

# Default command
ENTRYPOINT ["python", "-u", "main.py"]