FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY binance_producer.py .
CMD ["python", "binance_producer.py"]