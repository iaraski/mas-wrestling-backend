FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Expose port
EXPOSE 8000

# Command to run the application (production)
# Default to 4 workers; can be overridden via GUNICORN_WORKERS build/run env
ENV GUNICORN_WORKERS=4
CMD ["sh", "-lc", "gunicorn -k uvicorn.workers.UvicornWorker -w ${GUNICORN_WORKERS} -b 0.0.0.0:8000 --keep-alive 30 --timeout 60 app.main:app"]
