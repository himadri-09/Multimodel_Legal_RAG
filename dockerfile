# Use a lightweight Python base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies in a single layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    # PyMuPDF (fitz) dependencies
    libmupdf-dev \
    libfreetype6-dev \
    libjpeg-dev \
    libpng-dev \
    zlib1g-dev \
    # PIL/Pillow dependencies  
    libtiff5-dev \
    libwebp-dev \
    libopenjp2-7-dev \
    # Build dependencies
    gcc \
    g++ \
    make \
    libffi-dev \
    libssl-dev \
    # Utility tools
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

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
CMD ["python", "app.py", "--host", "0.0.0.0", "--port", "5000"]
