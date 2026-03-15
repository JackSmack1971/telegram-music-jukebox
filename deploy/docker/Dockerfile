# Multi-stage build: Builder stage
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies and FFmpeg
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install dependencies
COPY requirements.txt requirements-phase2-3.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r requirements-phase2-3.txt

# Install yt-dlp in venv
RUN pip install --no-cache-dir yt-dlp

# Multi-stage build: Runtime stage
FROM python:3.12-slim AS runtime

WORKDIR /app

# Install only runtime libraries (not dev packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libffi7 \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy virtualenv from builder
COPY --from=builder /opt/venv /opt/venv

# Copy FFmpeg from builder
COPY --from=builder /usr/bin/ffmpeg /usr/bin/ffmpeg
COPY --from=builder /usr/bin/ffprobe /usr/bin/ffprobe
COPY --from=builder /usr/lib/*-linux-gnu/lib*.so* /usr/lib/

# Copy application code
COPY src/ ./src/
COPY tests/ ./tests/

# Create non-root user
RUN groupadd -r jukebox && \
    useradd -r -g jukebox -d /app -s /bin/bash jukebox && \
    mkdir -p /var/cache/jukebox /dev/shm/jukebox /var/log/jukebox && \
    chown -R jukebox:jukebox /app /var/cache/jukebox /var/log/jukebox

# Set environment
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app" \
    PYTHONUNBUFFERED=1

USER jukebox

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:3000/health || exit 1

# Run application
CMD ["python", "-m", "src.main"]
