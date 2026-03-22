FROM python:3.12-slim

WORKDIR /app

# Install only the Flask app dependencies (not playwright/scraper deps)
COPY requirements.txt .
RUN pip install --no-cache-dir flask flask-cors

# Copy application code and static assets
COPY app.py .
COPY static/ static/

EXPOSE 5111

# Bind to 0.0.0.0 so Docker port mapping works
CMD ["python", "-c", "from app import app; app.run(host='0.0.0.0', port=5111, threaded=True)"]
