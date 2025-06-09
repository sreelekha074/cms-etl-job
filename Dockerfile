# Use slim Python base image
FROM python:3.9-slim

# Set working directory in the container
WORKDIR /app

# Copy files into the container
COPY run_daily_cms_etl.py ./
COPY requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Default command
CMD ["python", "run_daily_cms_etl.py"]

