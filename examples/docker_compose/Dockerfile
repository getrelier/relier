FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Override per service in docker-compose.yml
CMD ["celery", "-A", "celery_app", "worker", "--loglevel=info"]
