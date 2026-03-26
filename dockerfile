# Use Playwright official image (already has Chromium + deps)
FROM mcr.microsoft.com/playwright/python:v1.58.0-jammy

WORKDIR /app

# Install only Python deps (fast because system deps already exist)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Create uploads dir
RUN mkdir -p uploads

# Expose port
EXPOSE 8000

# Run FastAPI
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]