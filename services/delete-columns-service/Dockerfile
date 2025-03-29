# Use base image of python 3.9
FROM python:3.9-slim

# Setting the working directory
WORKDIR /app

# Copy the requirements.txt file and install the dependencies
COPY delete-columns-service/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the folder common
COPY common /app/common

# Copy the code in the working dir
COPY delete-columns-service /app/

# Set PYTHONPATH to /app so that 'common' is importable as 'common'
ENV PYTHONPATH="/app"

# Expose the 5004 port -- this microservice used this port
EXPOSE 5004

# Command to start the microservice
CMD ["python", "run.py"]