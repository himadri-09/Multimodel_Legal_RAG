# Use a lightweight Python base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Combine apt-get commands for efficiency
RUN apt-get update && apt-get install -y --no-install-recommends \
    # PyMuPDF dependencies
    libmupdf-dev \
    libfreetype6-dev \
    libjpeg-dev \
    zlib1g-dev \
    # Build dependencies
    gcc \
    g++ \
    make \
    libffi-dev \
    libssl-dev \
    # Optional: curl for health checks
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Create the uploads directory (required by Flask)
RUN mkdir -p uploads

# Expose the port the app runs on
EXPOSE 5000

# Command to run the application
CMD ["python", "app.py"]