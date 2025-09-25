# Use a small, recent Python
FROM python:3.11-slim

# System deps (certs, build tools if wheels aren't available)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# App dir
WORKDIR /app

# Copy dependency manifest first to leverage layer caching
COPY requirements.txt .

# Install deps
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Non-root user for security
RUN useradd -m appuser
USER appuser

# Environment defaults
ENV PORT=8080 \
    FLASK_ENV=production \
    PYTHONUNBUFFERED=1 \
    GUNICORN_CMD_ARGS="--workers 2 --threads 8 --timeout 120" \
    # Optional feature flags / secrets (override at runtime)
    ALLOW_GTM_WRITES=0 \
    MCP_SHARED_KEY=""

# Expose port
EXPOSE 8080

# Healthcheck
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD curl -fsS http://127.0.0.1:${PORT}/health || exit 1

# Entrypoint: run with Gunicorn
# (app.py exposes `app = Flask(__name__)`)
CMD gunicorn -b 0.0.0.0:${PORT} app:app
