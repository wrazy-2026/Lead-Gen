# Dockerfile for Google Cloud Run
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies (including Playwright browser deps)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium browser + all system deps
RUN playwright install --with-deps chromium

# Copy application code
COPY . .

# Set environment variables
ENV PORT=8080
ENV PYTHONUNBUFFERED=1
ENV FIREBASE_PROJECT_ID=lively-paratext-487716-r8
ENV FIRESTORE_DATABASE_ID=leadgen

# Expose port
EXPOSE 8080

# Run with gunicorn (longer timeout for SSE streams)
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 600 app_flask:app
