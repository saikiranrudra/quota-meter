FROM python:3.11-slim

# Install curl
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./app /app
COPY entrypoint.sh /entrypoint.sh

# Make entrypoint executable
RUN chmod +x /entrypoint.sh

# Set Flask app environment variable
ENV FLASK_APP=app.py
ENV FLASK_ENV=development

EXPOSE 5000

ENTRYPOINT ["/entrypoint.sh"]
