"""
Design notes
------------
- One streaming query per topic, all sharing the same SparkSession. Each query
  has its own checkpoint directory and its own staging table (the queries run
  At the same time, staging tables must never be shared).

- Staging tables are created/recreated by Spark itself on every micro-batch
  (`write.jdbc(mode="overwrite")`), so they are intentionally NOT part of the
  versioned DDL. Only the final fact_*/dim_* tables live in db/ DDL, because
  They have the primary keys which are required by ON CONFLICT.

- Deletes are handled as a logical flag (`is_deleted`) instead of removing
  Therefore, queries in the style of history will still work.

- The merge is guarded by `event_ts_ms` so a late/replayed event can never
  overwrite newer state — combined with checkpointing, this gives
  Semantics are effectively applied end to end.

- Table/column names in the generated merge SQL come exclusively from this
  file's own configuration (never from external input), so building the
  Using f-strings in this case is safe.
"""

import os
import time

import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as spark_functions
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    BooleanType,
    DecimalType,
)
from pyspark.sql.window import Window

# Configuration via environment (defined in docker-compose.yaml)
kafka_bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
postgres_jdbc_url = os.environ["POSTGRES_URL"]
postgres_jdbc_properties = {
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
}
checkpoint_base_dir = os.environ["CHECKPOINT_DIR"]


# Row schemas per source table
orders_row_schema = StructType([
    StructField("order_id", LongType()),
    StructField("order_date", IntegerType()),       # days since epoch
    StructField("delivery_date", IntegerType()),    # days since epoch
    StructField("customer_id", LongType()),
    StructField("status", StringType()),
])

order_items_row_schema = StructType([
    StructField("order_item_id", LongType()),
    StructField("order_id", LongType()),
    StructField("product_id", LongType()),
    StructField("quantity", IntegerType()),
])

customers_row_schema = StructType([
    StructField("customer_id", LongType()),
    StructField("customer_name", StringType()),
    StructField("is_active", BooleanType()),
    StructField("customer_address", StringType()),
])

products_row_schema = StructType([
    StructField("product_id", LongType()),
    StructField("product_name", StringType()),
    StructField("barcode", StringType()),
    StructField("unity_price", DecimalType(10, 2)),
    StructField("is_active", BooleanType()),
])


# Central pipeline configuration: one entry per topic.
streaming_pipelines_config = [
    {
        "pipeline_name": "orders",
        "kafka_topic": "finance_db.operations.orders",
        "row_schema": orders_row_schema,
        "primary_key_column": "order_id",
        "date_columns": ["order_date", "delivery_date"],
        "staging_table": "analytics.stg_orders",
        "target_table": "analytics.fact_orders",
        # Append-only log of every order event, answering the historical-information requirement.
        "history_table": "analytics.orders_status_history",
    },
    {
        "pipeline_name": "order_items",
        "kafka_topic": "finance_db.operations.order_items",
        "row_schema": order_items_row_schema,
        "primary_key_column": "order_item_id",
        "date_columns": [],
        "staging_table": "analytics.stg_order_items",
        "target_table": "analytics.fact_order_items",
    },
    {
        "pipeline_name": "customers",
        "kafka_topic": "finance_db.operations.customers",
        "row_schema": customers_row_schema,
        "primary_key_column": "customer_id",
        "date_columns": [],
        "staging_table": "analytics.stg_customers",
        "target_table": "analytics.dim_customers",
    },
    {
        "pipeline_name": "products",
        "kafka_topic": "finance_db.operations.products",
        "row_schema": products_row_schema,
        "primary_key_column": "product_id",
        "date_columns": [],
        "staging_table": "analytics.stg_products",
        "target_table": "analytics.dim_products",
    },
]

# Startup guard: wait for the Debezium topics to exist.
def wait_for_kafka_topics(spark_session: SparkSession, timeout_seconds: int = 300) -> None:
    """Wait until every configured Kafka topic exists.
 
    When running `docker-compose up`, this code is starting to run before 
    the Debezium connector has been registered and created the topics, so we 
    need to poll for topic availability (prevent fail with UnknownTopicOrPartitionException)
    """
    expected_topics = [config["kafka_topic"] for config in streaming_pipelines_config]
    deadline = time.monotonic() + timeout_seconds
 
    for topic_name in expected_topics:
        while True:
            try:
                # A batch read capped at one record: succeeds when the
                # topic exists (even if empty) and raises when it does not
                # Note: batch Kafka reads require endingOffsets=latest
                (
                    spark_session.read.format("kafka")
                    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                    .option("subscribe", topic_name)
                    .option("startingOffsets", "earliest")
                    .option("endingOffsets", "latest")
                    .load()
                    .limit(1)
                    .collect()
                )
                print(f"[startup] topic available: {topic_name}")
                break
            except Exception as probe_error:
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        f"Kafka topic '{topic_name}' not available after "
                        f"{timeout_seconds}s — is the Debezium connector registered?"
                    ) from probe_error
                print(
                      f"[startup] waiting for topic: {topic_name} "
                      f"({type(probe_error).__name__})"
                )
                time.sleep(10)

# Debezium envelope parsing -> typed DataFrame
def build_parsed_stream(spark_session: SparkSession, pipeline_config: dict) -> DataFrame:
    """Read one Kafka topic and parse the Debezium envelope into typed columns."""
    row_schema = pipeline_config["row_schema"]
    primary_key_column = pipeline_config["primary_key_column"]

    debezium_envelope_schema = StructType([
            StructField("before", row_schema),
            StructField("after", row_schema),
            StructField("op", StringType()),        # c=create, u=update, d=delete, r=snapshot read
            StructField("ts_ms", LongType()),
    ])

    raw_kafka_stream = (
        spark_session.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", pipeline_config["kafka_topic"])
        .option("startingOffsets", "earliest")      # initial snapshot = historical load
        .option("maxOffsetsPerTrigger", 10000)      # backpressure: bounds the micro-batch size
        .load()
    )

    envelope_stream = (
        raw_kafka_stream
        .select(
            spark_functions.from_json(
                spark_functions.col("value").cast("string"),
                debezium_envelope_schema,
            ).alias("message")
        )
        .select("message.*")
    )

    # Data columns come from `after` (create/update/snapshot). On deletes,
    # `after` is null and the primary key must be taken from `before`.
    selected_columns = []
    for field in row_schema.fields:
        if field.name == primary_key_column:
            column_expression = spark_functions.coalesce(
                spark_functions.col(f"after.{field.name}"),
                spark_functions.col(f"before.{field.name}"),
            )
        else:
            column_expression = spark_functions.col(f"after.{field.name}")

        if field.name in pipeline_config["date_columns"]:
            # Debezium io.debezium.time.Date: int days since epoch -> DATE
            column_expression = spark_functions.date_add(
                spark_functions.lit("1970-01-01").cast("date"),
                spark_functions.coalesce(column_expression, spark_functions.lit(0)),
            )

        selected_columns.append(column_expression.alias(field.name))

    selected_columns.append((spark_functions.col("op") == "d").alias("is_deleted"))
    # Raw Debezium op (c/u/d/r) kept for the status-history append; it lands in staging but is never listed in the merge statement.
    selected_columns.append(spark_functions.col("op").alias("op"))
    selected_columns.append(spark_functions.col("ts_ms").alias("event_ts_ms"))

    return (
        envelope_stream
        .select(*selected_columns)
        .where(spark_functions.col(primary_key_column).isNotNull())
    )


# Merge SQL generated from the schema (single source of truth for the column list — adding a column only touches schema + DDL)
def build_merge_statement(pipeline_config: dict) -> str:
    """Build the idempotent INSERT ... ON CONFLICT merge from staging to target."""
    primary_key_column = pipeline_config["primary_key_column"]
    staging_table = pipeline_config["staging_table"]
    target_table = pipeline_config["target_table"]

    all_column_names = (
        [field.name for field in pipeline_config["row_schema"].fields]
        + ["is_deleted", "event_ts_ms"]
    )
    columns_list = ", ".join(all_column_names)
    update_assignments = ", ".join(
        f"{column_name} = EXCLUDED.{column_name}"
        for column_name in all_column_names
        if column_name != primary_key_column
    )

    # The WHERE guard prevents a late or replayed event from overwriting
    # newer state (e.g. after a restart replays the last micro-batch).
    return f"""
        INSERT INTO {target_table} ({columns_list})
        SELECT {columns_list} FROM {staging_table}
        ON CONFLICT ({primary_key_column}) DO UPDATE SET
            {update_assignments}
        WHERE {target_table}.event_ts_ms <= EXCLUDED.event_ts_ms
    """


# foreachBatch handler: dedup -> staging (auto-created) -> merge
def build_upsert_handler(pipeline_config: dict):
    """Return a foreachBatch handler bound to one pipeline's configuration."""
    primary_key_column = pipeline_config["primary_key_column"]
    merge_statement = build_merge_statement(pipeline_config)
    history_table = pipeline_config.get("history_table")

    def upsert_micro_batch(micro_batch: DataFrame, batch_id: int) -> None:
        if micro_batch.isEmpty():
            return

        # Append-only status history
        if history_table is not None:
            (
                micro_batch.select(
                    spark_functions.col("order_id"),
                    spark_functions.col("status"),
                    spark_functions.to_timestamp(
                        spark_functions.col("event_ts_ms") / 1000.0
                    ).alias("changed_at"),
                    spark_functions.col("event_ts_ms").alias("source_ts_ms"),
                    spark_functions.col("op"),
                )
                .write.jdbc(
                    url=postgres_jdbc_url,
                    table=history_table,
                    mode="append",
                    properties=postgres_jdbc_properties,
                )
            )

        # The same key can change several times within one micro-batch:
        # keep only the latest event (highest ts_ms) per primary key.
        latest_event_window = (
            Window.partitionBy(primary_key_column)
            .orderBy(spark_functions.col("event_ts_ms").desc())
        )
        deduplicated_batch = (
            micro_batch
            .withColumn(
                "event_rank",
                spark_functions.row_number().over(latest_event_window),
            )
            .where(spark_functions.col("event_rank") == 1)
            .drop("event_rank")
        )

        # Staging table is created/recreated by Spark from the DataFrame
        # schema on every batch — hence no versioned DDL for stg_* tables.
        deduplicated_batch.write.jdbc(
            url=postgres_jdbc_url,
            table=pipeline_config["staging_table"],
            mode="overwrite",
            properties=postgres_jdbc_properties,
        )

        # write.jdbc cannot run arbitrary SQL, so the merge uses psycopg2.
        postgres_connection = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ.get("POSTGRES_PORT", "5432"),
            dbname=os.environ["POSTGRES_DB"],
            user=postgres_jdbc_properties["user"],
            password=postgres_jdbc_properties["password"],
        )
        try:
            with postgres_connection, postgres_connection.cursor() as merge_cursor:
                merge_cursor.execute(merge_statement)
        finally:
            postgres_connection.close()

    return upsert_micro_batch


# Main: start the four queries on the same SparkSession
def main() -> None:
    spark_session = (
        SparkSession.builder
        .appName("acme-analytics-streaming")
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("WARN")

    # wait for the Debezium topics to exist
    wait_for_kafka_topics(spark_session)

    for pipeline_config in streaming_pipelines_config:
        parsed_stream = build_parsed_stream(spark_session, pipeline_config)
        (
            parsed_stream.writeStream
            .queryName(pipeline_config["pipeline_name"])   # visible name in the Spark UI
            .foreachBatch(build_upsert_handler(pipeline_config))
            .option(
                "checkpointLocation",
                f"{checkpoint_base_dir}/{pipeline_config['pipeline_name']}",
            )
            .trigger(processingTime="10 seconds")
            .start()
        )

    spark_session.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()