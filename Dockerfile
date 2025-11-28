FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /app/
COPY entrypoint.sh /app/

# Create directory for output data (to be mounted)
RUN mkdir -p /data && chmod 777 /data

# Make entrypoint executable
RUN chmod +x /app/entrypoint.sh

# Expose the web server port
EXPOSE 8080

# Run the orchestration script
CMD ["/app/entrypoint.sh"]
