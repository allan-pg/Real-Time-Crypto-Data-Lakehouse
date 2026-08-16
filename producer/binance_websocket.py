import json
import logging
import os
import time
from datetime import datetime, timezone

import websocket
from confluent_kafka import Producer
from dotenv import load_dotenv


# 1. Load environment variables

load_dotenv()


# 2. Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# 3. Application configuration

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS"
)

KAFKA_TOPIC_CRYPTO = os.getenv(
    "KAFKA_TOPIC_CRYPTO",
    "crypto_prices"
)

CRYPTO_SYMBOLS = os.getenv(
    "CRYPTO_SYMBOLS",
    "btcusdt,ethusdt"
).split(",")


# 4. Build Binance WebSocket URL

streams = "/".join(
    f"{symbol.strip().lower()}@trade"
    for symbol in CRYPTO_SYMBOLS
)

BINANCE_WS_URL = (
    f"wss://stream.binance.us:9443/stream?streams={streams}"
)


# 5. Kafka Producer configuration
producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "crypto-websocket-producer",
    "acks": "1"
}


kafka_producer = Producer(producer_conf)


# 6. Kafka delivery callback
def delivery_report(err, msg):

    if err is not None:

        logging.error(
            f"Kafka delivery failed: {err}"
        )

    else:

        logging.info(
            f"Kafka delivered | "
            f"topic={msg.topic()} | "
            f"partition={msg.partition()} | "
            f"offset={msg.offset()} | "
            f"key={msg.key().decode('utf-8')}"
        )

# 7. Process incoming Binance message

def on_message(ws, message):

    try:

        # 1. Convert JSON string → Python dictionary

        raw_data = json.loads(message)

        
        # 2. Extract the actual event data
        data = raw_data.get("data", raw_data)

       # 3. Validate the message

        if not isinstance(data, dict):
            return

        if data.get("e") != "trade":
            return

       
        # 4. Extract trade fields
        symbol = data.get("s")
        trade_id = data.get("t")
        price = data.get("p")
        quantity = data.get("q")
        event_time = data.get("E")
        trade_time = data.get("T")
        is_buyer_maker = data.get("m")

        # 5. Validate required fields
        
        if symbol is None:
            return

        if price is None:
            return

        if quantity is None:
            return

        # 6. Create our standardized event
        
        payload = {
            "symbol": symbol,
            "trade_id": trade_id,
            "price": float(price),
            "quantity": float(quantity),
            "exchange": "binance",
            "event_time": event_time,
            "trade_time": trade_time,
            "is_buyer_maker": is_buyer_maker,
        }

        # 7. Send event to Kafka

        kafka_producer.produce(
            topic=KAFKA_TOPIC_CRYPTO,
            key=symbol.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            callback=delivery_report
        )

        # Process Kafka delivery callbacks
        kafka_producer.poll(0)

        
        # 8. Log the event

        logging.info(
            f"Trade streamed | "
            f"symbol={symbol} | "
            f"price={payload['price']} | "
            f"quantity={payload['quantity']} | "
            f"trade_id={payload['trade_id']}"
        )

    except Exception as e:

        logging.exception(
            f"Error processing Binance trade: {e}"
        )





# 8. WebSocket error handler

def on_error(ws, error):

    logging.error(
        f"WebSocket error: {repr(error)}"
    )



# 9. WebSocket close handler


def on_close(
    ws,
    close_status_code,
    close_msg
):

    logging.warning(
        f"WebSocket closed | "
        f"status={close_status_code} | "
        f"message={close_msg}"
    )



# 10. WebSocket open handler


def on_open(ws):

    logging.info(
        "Connected to Binance WebSocket"
    )

    logging.info(
        f"Streaming symbols: {CRYPTO_SYMBOLS}"
    )



# 11. Start ingestion service

def start_ingestion_stream():

    retry_interval = 2

    while True:

        try:

            logging.info(
                "Connecting to Binance WebSocket..."
            )

            ws = websocket.WebSocketApp(

                BINANCE_WS_URL,

                on_open=on_open,

                on_message=on_message,

                on_error=on_error,

                on_close=on_close
            )

            ws.run_forever(
                ping_interval=30,
                ping_timeout=10
            )

        except KeyboardInterrupt:

            logging.info(
                "Producer stopped by user."
            )

            break

        except Exception as e:

            logging.exception(
                f"Connection error: {e}"
            )

        logging.warning(
            f"Reconnecting in "
            f"{retry_interval} seconds..."
        )

        time.sleep(retry_interval)

        retry_interval = min(
            retry_interval * 2,
            60
        )



# 12. Application entry point

if __name__ == "__main__":

    start_ingestion_stream()
