FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and config
COPY src/ ./src/
COPY config.json ./

# Run the Translation Layer
CMD ["python", "src/main.py"]
