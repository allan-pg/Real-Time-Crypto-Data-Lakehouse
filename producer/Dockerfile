FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY producer/binance_websocket.py .

CMD ["python", "binance_websocket.py"]