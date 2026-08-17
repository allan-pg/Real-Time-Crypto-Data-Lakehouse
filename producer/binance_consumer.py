import json
import logging
import os

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv


# ---------------------------------------------------------
# 1. Load environment variables
# ---------------------------------------------------------

load_dotenv()


# ---------------------------------------------------------
# 2. Logging configuration
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ---------------------------------------------------------
# 3. Application configuration
# ---------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS"
)

KAFKA_TOPIC_CRYPTO = os.getenv(
    "KAFKA_TOPIC_CRYPTO",
    "crypto_prices"
)

KAFKA_CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "crypto-price-consumer"
)


# ---------------------------------------------------------
# 4. Kafka Consumer configuration
# ---------------------------------------------------------

consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_CONSUMER_GROUP,

    # Start from the earliest available message
    # if this consumer group has no committed offset.
    "auto.offset.reset": "earliest",

    # We will explicitly commit after processing.
    "enable.auto.commit": False,

    "client.id": "crypto-price-consumer"
}


# ---------------------------------------------------------
# 5. Create Kafka consumer
# ---------------------------------------------------------

kafka_consumer = Consumer(consumer_conf)


# ---------------------------------------------------------
# 6. Process a Kafka message
# ---------------------------------------------------------

def process_message(message):

    try:

        # -------------------------------------------------
        # BEFORE PROCESSING
        # -------------------------------------------------

        logging.info(
            f"Kafka message received | "
            f"topic={message.topic()} | "
            f"partition={message.partition()} | "
            f"offset={message.offset()}"
        )

        # -------------------------------------------------
        # 1. Get message value
        # -------------------------------------------------

        raw_value = message.value()

        if raw_value is None:
            logging.warning(
                "Kafka message has no value"
            )
            return False

        # -------------------------------------------------
        # 2. Decode JSON
        # -------------------------------------------------

        payload = json.loads(
            raw_value.decode("utf-8")
        )

        logging.info(
            f"Kafka message parsed | "
            f"symbol={payload.get('symbol')} | "
            f"trade_id={payload.get('trade_id')}"
        )

        # -------------------------------------------------
        # 3. Extract fields
        # -------------------------------------------------

        symbol = payload.get("symbol")
        trade_id = payload.get("trade_id")
        price = payload.get("price")
        quantity = payload.get("quantity")
        exchange = payload.get("exchange")
        event_time = payload.get("event_time")
        trade_time = payload.get("trade_time")
        is_buyer_maker = payload.get("is_buyer_maker")

        # -------------------------------------------------
        # 4. Validate required fields
        # -------------------------------------------------

        required_fields = {
            "symbol": symbol,
            "trade_id": trade_id,
            "price": price,
            "quantity": quantity,
            "exchange": exchange,
            "event_time": event_time,
            "trade_time": trade_time
        }

        missing_fields = [
            field
            for field, value in required_fields.items()
            if value is None
        ]

        if missing_fields:

            logging.error(
                f"Trade validation failed | "
                f"missing_fields={missing_fields} | "
                f"offset={message.offset()}"
            )

            return False

        logging.info(
            f"Trade validation successful | "
            f"symbol={symbol} | "
            f"trade_id={trade_id}"
        )

        # -------------------------------------------------
        # 5. Process the trade
        # -------------------------------------------------

        logging.info(
            f"Processing trade | "
            f"symbol={symbol} | "
            f"price={price} | "
            f"quantity={quantity} | "
            f"trade_id={trade_id}"
        )

        # -------------------------------------------------
        # AFTER PROCESSING
        # -------------------------------------------------

        logging.info(
            f"Trade processed successfully | "
            f"symbol={symbol} | "
            f"trade_id={trade_id}"
        )

        return True

    except json.JSONDecodeError:

        logging.exception(
            "Failed to decode Kafka message as JSON"
        )

        return False

    except Exception:

        logging.exception(
            "Unexpected error while processing Kafka message"
        )

        return False


# ---------------------------------------------------------
# 7. Start consumer
# ---------------------------------------------------------

def start_consumer():

    try:

        logging.info(
            "Starting Kafka consumer..."
        )

        logging.info(
            f"Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}"
        )

        logging.info(
            f"Kafka topic: {KAFKA_TOPIC_CRYPTO}"
        )

        logging.info(
            f"Consumer group: {KAFKA_CONSUMER_GROUP}"
        )

        # -------------------------------------------------
        # Subscribe to topic
        # -------------------------------------------------

        kafka_consumer.subscribe(
            [KAFKA_TOPIC_CRYPTO]
        )

        logging.info(
            f"Subscribed to topic: "
            f"{KAFKA_TOPIC_CRYPTO}"
        )

        # -------------------------------------------------
        # Consumer loop
        # -------------------------------------------------

        while True:

            message = kafka_consumer.poll(
                timeout=1.0
            )

            # No message available
            if message is None:
                continue

            # Kafka error
            if message.error():

                raise KafkaException(
                    message.error()
                )

            # -------------------------------------------------
            # Process message
            # -------------------------------------------------

            processed = process_message(
                message
            )

            # -------------------------------------------------
            # Commit only after successful processing
            # -------------------------------------------------

            if processed:

                kafka_consumer.commit(
                    message=message,
                    asynchronous=False
                )

                logging.info(
                    f"Kafka offset committed | "
                    f"topic={message.topic()} | "
                    f"partition={message.partition()} | "
                    f"offset={message.offset()}"
                )

    except KeyboardInterrupt:

        logging.info(
            "Consumer stopped by user."
        )

    except Exception:

        logging.exception(
            "Consumer stopped because of an error."
        )

    finally:

        kafka_consumer.close()

        logging.info(
            "Kafka consumer closed."
        )


# ---------------------------------------------------------
# 8. Application entry point
# ---------------------------------------------------------

if __name__ == "__main__":

    start_consumer()