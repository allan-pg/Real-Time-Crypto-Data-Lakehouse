import json
import logging

from pyflink.common import Types
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import (
    WatermarkStrategy,
    TimestampAssigner
)
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.window import SlidingEventTimeWindows


import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC_CRYPTO"
)

KAFKA_CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP"
)


# Logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Event timestamp assigner
class CryptoTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp):
        """
        Extract event_time from the Kafka record.

        event_time is expected to be an epoch timestamp
        in milliseconds.
        """

        return int(value[6])


# Main Flink application

def main():

    # 1. Create Flink execution environment
    

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(3)

    
    # 2. Kafka source

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_CONSUMER_GROUP)
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
        )
        .set_value_only_deserializer(
            JsonRowDeserializationSchema.builder()
            .type_info(
                Types.ROW_NAMED(
                    [
                        "symbol",
                        "trade_id",
                        "price",
                        "quantity",
                        "exchange",
                        "event_time",
                        "trade_time",
                        "is_buyer_maker"
                    ],
                    [
                        Types.STRING(),
                        Types.LONG(),
                        Types.DOUBLE(),
                        Types.DOUBLE(),
                        Types.STRING(),
                        Types.LONG(),
                        Types.LONG(),
                        Types.BOOLEAN()
                    ]
                )
            )
            .build()
        )
        .build()
    )

    
    # 3. Read Kafka stream

    trades = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Crypto Trades"
    )

    
    # 4. Assign event time + watermarks

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(
            Time.seconds(5)
        )
        .with_timestamp_assigner(
            CryptoTimestampAssigner()
        )
    )

    trades = trades.assign_timestamps_and_watermarks(
        watermark_strategy
    )

    
    # 5. Keep only valid records

    trades = trades.filter(
        lambda trade:
            trade[0] is not None
            and trade[2] is not None
            and trade[3] is not None
            and trade[5] is not None
    )


    # 6. Key by cryptocurrency

    keyed_trades = trades.key_by(
        lambda trade: trade[0],
        key_type=Types.STRING()
    )


    # 7. Sliding event-time window
    #
    # Window size: 1 minute
    # Slide:       10 seconds
    #
    # This creates overlapping windows.
   
    windowed_trades = keyed_trades.window(
        SlidingEventTimeWindows.of(
            Time.minutes(1),
            Time.seconds(10)
        )
    )

  
    # 8. Calculate window statistics

    def aggregate_window(key, window, values):

        prices = []
        quantities = []
        trade_count = 0

        for trade in values:

            prices.append(trade[2])
            quantities.append(trade[3])

            trade_count += 1

        if not prices:
            return None

        total_volume = sum(quantities)

        avg_price = sum(prices) / len(prices)

        min_price = min(prices)

        max_price = max(prices)

        return (
            key,
            window.start,
            window.end,
            trade_count,
            total_volume,
            avg_price,
            min_price,
            max_price
        )

    results = windowed_trades.process(
        aggregate_window,
        output_type=Types.TUPLE([
            Types.STRING(),
            Types.LONG(),
            Types.LONG(),
            Types.INT(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE(),
            Types.DOUBLE()
        ])
    )

 
    # 9. Print results

    results.print()

 
    # 10. Execute Flink job

    env.execute(
        "Crypto Sliding Window Aggregation"
    )



# Entry point

if __name__ == "__main__":
    main()
