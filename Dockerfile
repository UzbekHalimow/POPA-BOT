FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Папка для базы данных
RUN mkdir -p /app/data

CMD ["python", "main.py"]
