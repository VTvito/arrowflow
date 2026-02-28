# Use base image of python 3.9
FROM python:3.9-slim

# Setting the working directory
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy the requirements.txt file and install the dependencies
COPY load-data-service/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the folder common
COPY common /app/common

# Copy the code in the working dir
COPY load-data-service /app/

# Set PYTHONPATH to /app so that 'common' is importable as 'common'
ENV PYTHONPATH="/app"

# Expose the 5009 port -- this microservice used this port
EXPOSE 5009

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:5009/health || exit 1

CMD ["gunicorn", "--bind", "0.0.0.0:5009", "--workers", "4", "--timeout", "300", "app:create_app()"]