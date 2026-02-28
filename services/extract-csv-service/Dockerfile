# Use base image of python 3.9
FROM python:3.9-slim

# Setting the working directory
WORKDIR /app

# Install curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy the requirements.txt file and install the dependencies
COPY extract-csv-service/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the folder common
COPY common /app/common

# Copy the code in the working dir
COPY extract-csv-service /app/

# Set PYTHONPATH to /app so that 'common' is importable as 'common'
ENV PYTHONPATH="/app"

# Expose the 5001 port -- this microservice used this port
EXPOSE 5001

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:5001/health || exit 1

# Use gunicorn for production (4 workers)
CMD ["gunicorn", "--bind", "0.0.0.0:5001", "--workers", "4", "--timeout", "300", "app:create_app()"]