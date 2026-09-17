# ================================================================
# ETL Bronze v4.0 — Docker Image
# Base: GDAL (includes ogr2ogr, ogrinfo)
# ================================================================

FROM ghcr.io/osgeo/gdal:ubuntu-small-latest

# Avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install Python 3, pip, and build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        python3-dev \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first (Docker cache layer)
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

# Copy application code
COPY . .

# Create uploads directory
RUN mkdir -p /app/uploads

# Environment
ENV UPLOAD_FOLDER=/app/uploads
ENV PYTHONUNBUFFERED=1

# Expose port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/')" || exit 1

# Run the application
CMD ["python3", "app.py"]
