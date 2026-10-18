# Official Playwright Python image (browsers + deps pre-installed)
FROM mcr.microsoft.com/playwright/python:v1.52.0-noble

WORKDIR /app

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Bot code
COPY . .

# Railway sets PORT dynamically
ENV BOT_PORT=${PORT:-8099}

CMD ["python", "bot_time_new.py"]
