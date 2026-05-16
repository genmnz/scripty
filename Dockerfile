FROM python:3.12-slim

WORKDIR /app

# install deps first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# default db inside container — override with a volume mount for persistence
ENV DB_PATH=/data/seen.db

RUN mkdir -p /data

CMD ["python", "main.py"]
