FROM python:3.11-slim

WORKDIR /app

COPY loader/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["tail", "-f", "/dev/null"]