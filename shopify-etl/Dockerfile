# Start from a Python 3 base image
FROM python:3.11-slim

# Create a directory for your app
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the source
COPY . .

# By default, we run main.py
CMD ["python", "main.py"]