import logging
import os

import psycopg
from dotenv import load_dotenv


# =========================================================
# Load environment variables
# =========================================================

load_dotenv()


# =========================================================
# Logging configuration
# =========================================================

logger = logging.getLogger(__name__)


# =========================================================
# PostgreSQL configuration
# =========================================================

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")


# =========================================================
# Validate configuration
# =========================================================

def validate_postgres_config():

    required_variables = {
        "POSTGRES_HOST": POSTGRES_HOST,
        "POSTGRES_PORT": POSTGRES_PORT,
        "POSTGRES_DB": POSTGRES_DB,
        "POSTGRES_USER": POSTGRES_USER,
        "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
    }

    missing = [
        name
        for name, value in required_variables.items()
        if not value
    ]

    if missing:

        raise ValueError(
            "Missing PostgreSQL environment variables: "
            + ", ".join(missing)
        )


# =========================================================
# Create PostgreSQL connection
# =========================================================

def get_connection():

    validate_postgres_config()

    try:

        connection = psycopg.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            sslmode=POSTGRES_SSLMODE,
            connect_timeout=10
        )

        logger.info(
            "PostgreSQL connection established | "
            f"host={POSTGRES_HOST} | "
            f"database={POSTGRES_DB}"
        )

        return connection

    except Exception as e:

        logger.exception(
            f"PostgreSQL connection failed: {e}"
        )

        raise


# =========================================================
# Insert crypto trade
# =========================================================

def insert_trade(trade):

    connection = None

    try:

        connection = get_connection()

        with connection.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO crypto_trades (
                    symbol,
                    trade_id,
                    price,
                    quantity,
                    exchange,
                    event_time,
                    trade_time,
                    is_buyer_maker
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    to_timestamp(%s / 1000.0),
                    to_timestamp(%s / 1000.0),
                    %s
                )
                ON CONFLICT (
                    exchange,
                    symbol,
                    trade_id
                )
                DO NOTHING
                RETURNING id
                """,
                (
                    trade["symbol"],
                    trade["trade_id"],
                    trade["price"],
                    trade["quantity"],
                    trade["exchange"],
                    trade["event_time"],
                    trade["trade_time"],
                    trade["is_buyer_maker"],
                )
            )

            result = cursor.fetchone()

        connection.commit()

        # -------------------------------------------------
        # New trade inserted
        # -------------------------------------------------

        if result:

            logger.info(
                "Trade persisted successfully | "
                f"symbol={trade['symbol']} | "
                f"trade_id={trade['trade_id']} | "
                f"database_id={result[0]}"
            )

            return True

        # -------------------------------------------------
        # Trade already exists
        # -------------------------------------------------

        logger.warning(
            "Duplicate trade ignored | "
            f"symbol={trade['symbol']} | "
            f"trade_id={trade['trade_id']}"
        )

        return True

    except Exception as e:

        if connection:

            connection.rollback()

        logger.exception(
            "Failed to persist trade | "
            f"symbol={trade.get('symbol')} | "
            f"trade_id={trade.get('trade_id')} | "
            f"error={e}"
        )

        raise

    finally:

        if connection:

            connection.close()