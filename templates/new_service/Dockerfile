# Use base image of python 3.9
FROM python:3.9-slim

# Setting the working directory
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy the requirements.txt file and install the dependencies
COPY {{SERVICE_NAME}}/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the folder common
COPY common /app/common

# Copy the code in the working dir
COPY {{SERVICE_NAME}} /app/

# Set PYTHONPATH to /app so that 'common' is importable as 'common'
ENV PYTHONPATH="/app"

# Expose the port
EXPOSE {{SERVICE_PORT}}

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:{{SERVICE_PORT}}/health || exit 1

CMD ["gunicorn", "--bind", "0.0.0.0:{{SERVICE_PORT}}", "--workers", "4", "--timeout", "300", "app:create_app()"]
