FROM python:3.11-slim

WORKDIR /app

COPY loader/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY loader/load_to_redis.py .

CMD ["python", "load_to_redis.py"]
