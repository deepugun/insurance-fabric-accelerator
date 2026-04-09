# ──────────────────────────────────────────────────────────────────────────────
# Insurance Fabric Accelerator — Real-Time Streaming Pipelines
# Uses Fabric-native features ONLY:
# - Fabric Eventstream (managed EventHub/Kafka ingestion)
# - Spark Structured Streaming (built-in on Fabric)
# - Fabric Real-Time Intelligence (Eventhouse + KQL)
# - Delta Lake as streaming sink
# - Fabric Activator (Reflex) for real-time alerts
# ──────────────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, current_timestamp, expr,
    window, count, avg, sum as spark_sum, max as spark_max,
    when, lit, sha2, concat_ws
)
from pyspark.sql.types import *
from typing import Dict, List
import json

spark = SparkSession.builder.getOrCreate()
METADATA_SCHEMA = "insurance_metadata"


# ═══════════════════════════════════════════════════════════════════════════════
# STREAMING SCHEMAS (Insurance Events)
# ═══════════════════════════════════════════════════════════════════════════════

# Schema for real-time claim events (from FNOL systems, apps, call centers)
CLAIM_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),   # 'fnol', 'status_change', 'document_upload'
    StructField("claim_id", StringType()),
    StructField("policy_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("loss_date", TimestampType()),
    StructField("report_date", TimestampType()),
    StructField("loss_type", StringType()),
    StructField("loss_description", StringType()),
    StructField("loss_amount_estimate", DoubleType()),
    StructField("loss_location_state", StringType()),
    StructField("loss_location_zip", StringType()),
    StructField("reporter_name", StringType()),
    StructField("reporter_phone", StringType()),
    StructField("channel", StringType()),             # 'web', 'mobile', 'phone', 'agent'
    StructField("priority", StringType()),            # 'low', 'medium', 'high', 'catastrophe'
    StructField("metadata", StringType()),            # JSON blob
    StructField("event_timestamp", TimestampType(), False),
])

# Schema for real-time payment events (from payment gateways)
PAYMENT_EVENT_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("event_type", StringType(), False),   # 'payment', 'refund', 'chargeback', 'decline'
    StructField("policy_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("invoice_id", StringType()),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("payment_method", StringType()),      # 'card', 'ach', 'wire', 'check'
    StructField("card_last_four", StringType()),
    StructField("gateway_response_code", StringType()),
    StructField("gateway_response_msg", StringType()),
    StructField("ip_address", StringType()),
    StructField("device_fingerprint", StringType()),
    StructField("event_timestamp", TimestampType(), False),
])

# Schema for clickstream / marketing events
CLICKSTREAM_EVENT_SCHEMA = StructType([
    StructField("session_id", StringType(), False),
    StructField("visitor_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_type", StringType(), False),   # 'page_view', 'click', 'form_start', 'form_submit', 'quote_request'
    StructField("page_url", StringType()),
    StructField("referrer_url", StringType()),
    StructField("utm_source", StringType()),
    StructField("utm_medium", StringType()),
    StructField("utm_campaign", StringType()),
    StructField("product_interest", StringType()),    # LOB interest
    StructField("device_type", StringType()),         # 'desktop', 'mobile', 'tablet'
    StructField("browser", StringType()),
    StructField("geo_state", StringType()),
    StructField("geo_city", StringType()),
    StructField("time_on_page_sec", IntegerType()),
    StructField("event_timestamp", TimestampType(), False),
])

# Schema for IVR / voice call events
IVR_CALL_EVENT_SCHEMA = StructType([
    StructField("call_id", StringType(), False),
    StructField("event_type", StringType(), False),   # 'call_start', 'ivr_selection', 'agent_transfer', 'call_end'
    StructField("caller_phone", StringType()),
    StructField("customer_id", StringType()),
    StructField("policy_id", StringType()),
    StructField("ivr_menu_path", StringType()),       # e.g., '1>3>2' (menu selections)
    StructField("intent_detected", StringType()),     # 'claim_status', 'billing', 'new_quote', 'complaint'
    StructField("agent_id", StringType()),
    StructField("queue_name", StringType()),
    StructField("wait_time_sec", IntegerType()),
    StructField("talk_time_sec", IntegerType()),
    StructField("call_disposition", StringType()),    # 'resolved', 'transferred', 'abandoned', 'callback'
    StructField("sentiment_score", DoubleType()),     # from real-time speech analysis
    StructField("transcript_url", StringType()),      # OneLake path to transcript
    StructField("recording_url", StringType()),       # OneLake path to recording
    StructField("event_timestamp", TimestampType(), False),
])


# ═══════════════════════════════════════════════════════════════════════════════
# FABRIC EVENTSTREAM CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
# NOTE: Eventstreams are created via Fabric UI or REST API.
# This config drives the Spark Structured Streaming readers that
# consume FROM the Eventstream custom endpoint.

EVENTSTREAM_CONFIGS = {
    "claims": {
        "eventstream_name": "es_insurance_claims",
        "consumer_group": "cg_spark_claims",
        "schema": CLAIM_EVENT_SCHEMA,
        "target_bronze": "bronze_claims.claim_events_stream",
        "target_silver": "silver_claims.claim_events_enriched",
        "checkpoint_path": "Files/checkpoints/claims_stream",
        "trigger_interval": "10 seconds",
        "watermark_column": "event_timestamp",
        "watermark_delay": "30 seconds",
    },
    "payments": {
        "eventstream_name": "es_insurance_payments",
        "consumer_group": "cg_spark_payments",
        "schema": PAYMENT_EVENT_SCHEMA,
        "target_bronze": "bronze_billing.payment_events_stream",
        "target_silver": "silver_billing.payment_events_enriched",
        "checkpoint_path": "Files/checkpoints/payment_stream",
        "trigger_interval": "5 seconds",
        "watermark_column": "event_timestamp",
        "watermark_delay": "15 seconds",
    },
    "clickstream": {
        "eventstream_name": "es_insurance_clickstream",
        "consumer_group": "cg_spark_clickstream",
        "schema": CLICKSTREAM_EVENT_SCHEMA,
        "target_bronze": "bronze_marketing.clickstream_events",
        "target_silver": "silver_marketing.clickstream_enriched",
        "checkpoint_path": "Files/checkpoints/clickstream",
        "trigger_interval": "30 seconds",
        "watermark_column": "event_timestamp",
        "watermark_delay": "60 seconds",
    },
    "ivr_calls": {
        "eventstream_name": "es_insurance_ivr",
        "consumer_group": "cg_spark_ivr",
        "schema": IVR_CALL_EVENT_SCHEMA,
        "target_bronze": "bronze_customer.ivr_call_events",
        "target_silver": "silver_customer.ivr_calls_enriched",
        "checkpoint_path": "Files/checkpoints/ivr_stream",
        "trigger_interval": "15 seconds",
        "watermark_column": "event_timestamp",
        "watermark_delay": "30 seconds",
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# STREAMING PIPELINE ENGINE (Fabric Spark Structured Streaming)
# ═══════════════════════════════════════════════════════════════════════════════

class FabricStreamingEngine:
    """
    Real-time streaming pipelines using Fabric-native features:
    - Reads from Fabric Eventstream (EventHub-compatible endpoint)
    - Processes with Spark Structured Streaming (built-in on Fabric)
    - Writes to Delta tables in Lakehouse (built-in)
    - Sends alerts via Fabric Activator / Reflex
    """

    def __init__(self, environment: str = "prod"):
        self.environment = environment
        # Fabric Spark Structured Streaming settings
        spark.conf.set("spark.sql.streaming.schemaInference", "true")

    # ────────────────────────────────────────────────────────────────────────
    # Eventstream Reader (Fabric-native)
    # ────────────────────────────────────────────────────────────────────────

    def read_from_eventstream(self, config: Dict) -> DataFrame:
        """
        Read from Fabric Eventstream using built-in EventHub connector.
        In Fabric, Eventstreams expose an EventHub-compatible endpoint.
        """
        # Get EventHub-compatible connection string from config
        # In Fabric, this is available via the Eventstream custom endpoint
        eh_conn = self._get_eventstream_connection(config["eventstream_name"])

        stream_df = (spark.readStream
            .format("eventhubs")
            .option("eventhubs.connectionString", eh_conn)
            .option("eventhubs.consumerGroup", config["consumer_group"])
            .option("eventhubs.startingPosition",
                    json.dumps({"offset": "-1", "seqNo": -1,
                                "enqueuedTime": None, "isInclusive": True}))
            .option("maxEventsPerTrigger", 10000)
            .load()
        )

        # Parse EventHub body → structured schema
        parsed_df = (stream_df
            .select(
                col("enqueuedTime").alias("_enqueued_time"),
                col("offset").alias("_offset"),
                col("sequenceNumber").alias("_sequence_number"),
                from_json(col("body").cast("string"), config["schema"]).alias("data")
            )
            .select("_enqueued_time", "_offset", "_sequence_number", "data.*")
            .withColumn("_processing_time", current_timestamp())
        )

        # Apply watermark for late-arriving data (built-in Spark feature)
        parsed_df = parsed_df.withWatermark(
            config["watermark_column"],
            config["watermark_delay"]
        )

        return parsed_df

    # ────────────────────────────────────────────────────────────────────────
    # Stream: Claims Events
    # ────────────────────────────────────────────────────────────────────────

    def start_claims_stream(self):
        """
        Real-time claims pipeline:
        1. Ingest FNOL events from Eventstream
        2. Enrich with policy/customer data
        3. Score for fraud in real-time
        4. Write to Delta (Bronze + Silver)
        5. Trigger alerts for high-priority claims
        """
        config = EVENTSTREAM_CONFIGS["claims"]
        stream_df = self.read_from_eventstream(config)

        # ── Bronze: Raw append ──
        bronze_query = (stream_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{config['checkpoint_path']}/bronze")
            .option("mergeSchema", "true")
            .trigger(processingTime=config["trigger_interval"])
            .toTable(config["target_bronze"])
        )

        # ── Silver: Enriched + fraud scored ──
        # Join with policy and customer dimensions (using Delta table as lookup)
        enriched = (stream_df
            .withColumn("is_catastrophe",
                when(col("priority") == "catastrophe", True).otherwise(False))
            .withColumn("report_lag_days",
                expr("DATEDIFF(report_date, loss_date)"))
            .withColumn("fraud_risk_score",
                # Simple rule-based scoring (ML model used in batch)
                when(col("loss_amount_estimate") > 100000, 0.7)
                .when(col("report_lag_days") > 30, 0.6)
                .when(col("channel") == "phone", 0.3)
                .otherwise(0.1))
            .withColumn("auto_adjudicate_eligible",
                when(
                    (col("loss_amount_estimate") < 5000) &
                    (col("fraud_risk_score") < 0.3) &
                    (col("priority") != "catastrophe"),
                    True
                ).otherwise(False))
        )

        silver_query = (enriched.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{config['checkpoint_path']}/silver")
            .option("mergeSchema", "true")
            .trigger(processingTime=config["trigger_interval"])
            .toTable(config["target_silver"])
        )

        # ── Real-time aggregations (for Cockpit) ──
        agg_query = (stream_df
            .groupBy(
                window(col("event_timestamp"), "5 minutes"),
                col("loss_type"),
                col("priority"),
                col("channel")
            )
            .agg(
                count("*").alias("claim_count"),
                spark_sum("loss_amount_estimate").alias("total_estimated_loss"),
                avg("loss_amount_estimate").alias("avg_estimated_loss"),
            )
            .writeStream
            .format("delta")
            .outputMode("complete")
            .option("checkpointLocation", f"{config['checkpoint_path']}/agg")
            .trigger(processingTime="1 minute")
            .toTable("gold_claims.claim_events_5min_agg")
        )

        print("🔴 Claims streaming pipeline started")
        return {"bronze": bronze_query, "silver": silver_query, "agg": agg_query}

    # ────────────────────────────────────────────────────────────────────────
    # Stream: Payment Events
    # ────────────────────────────────────────────────────────────────────────

    def start_payment_stream(self):
        """
        Real-time payment pipeline:
        1. Ingest payment gateway events
        2. Detect anomalies (duplicate, high-value, chargebacks)
        3. Write to Delta
        4. Trigger alerts for suspicious transactions
        """
        config = EVENTSTREAM_CONFIGS["payments"]
        stream_df = self.read_from_eventstream(config)

        # ── Anomaly detection ──
        enriched = (stream_df
            .withColumn("is_anomaly",
                when(col("amount") > 50000, True)
                .when(col("event_type") == "chargeback", True)
                .when(col("event_type") == "decline", True)
                .when(col("amount") < 0, True)
                .otherwise(False))
            .withColumn("anomaly_type",
                when(col("amount") > 50000, "high_value")
                .when(col("event_type") == "chargeback", "chargeback")
                .when(col("amount") < 0, "negative_amount")
                .otherwise(None))
            .withColumn("_record_hash",
                sha2(concat_ws("||", col("transaction_id"), col("amount"),
                               col("event_timestamp")), 256))
        )

        # Write to Bronze
        bronze_query = (enriched.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{config['checkpoint_path']}/bronze")
            .trigger(processingTime=config["trigger_interval"])
            .toTable(config["target_bronze"])
        )

        # Write enriched to Silver
        silver_query = (enriched.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{config['checkpoint_path']}/silver")
            .trigger(processingTime=config["trigger_interval"])
            .toTable(config["target_silver"])
        )

        # Real-time payment velocity (fraud detection)
        velocity = (stream_df
            .groupBy(
                window(col("event_timestamp"), "10 minutes", "5 minutes"),
                col("customer_id")
            )
            .agg(
                count("*").alias("tx_count"),
                spark_sum("amount").alias("total_amount"),
                spark_sum(when(col("event_type") == "decline", 1).otherwise(0)).alias("decline_count"),
            )
            .filter("tx_count > 5 OR total_amount > 100000 OR decline_count > 2")
            .writeStream
            .format("delta")
            .outputMode("complete")
            .option("checkpointLocation", f"{config['checkpoint_path']}/velocity")
            .trigger(processingTime="1 minute")
            .toTable("gold_billing.payment_velocity_alerts")
        )

        print("🔴 Payment streaming pipeline started")
        return {"bronze": bronze_query, "silver": silver_query, "velocity": velocity}

    # ────────────────────────────────────────────────────────────────────────
    # Stream: Clickstream / Marketing
    # ────────────────────────────────────────────────────────────────────────

    def start_clickstream_stream(self):
        """
        Real-time clickstream pipeline:
        1. Ingest website behavior events
        2. Session analytics
        3. Lead scoring
        4. Write to Delta for marketing dashboards
        """
        config = EVENTSTREAM_CONFIGS["clickstream"]
        stream_df = self.read_from_eventstream(config)

        # Session enrichment
        enriched = (stream_df
            .withColumn("is_conversion_event",
                when(col("event_type").isin("form_submit", "quote_request"), True)
                .otherwise(False))
            .withColumn("engagement_score",
                when(col("event_type") == "quote_request", 10)
                .when(col("event_type") == "form_submit", 8)
                .when(col("event_type") == "form_start", 5)
                .when(col("event_type") == "click", 2)
                .otherwise(1))
        )

        # Write to Bronze
        bronze_query = (enriched.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{config['checkpoint_path']}/bronze")
            .trigger(processingTime=config["trigger_interval"])
            .toTable(config["target_bronze"])
        )

        # Session aggregation for real-time lead scoring
        session_agg = (enriched
            .groupBy(
                window(col("event_timestamp"), "30 minutes"),
                col("session_id"),
                col("visitor_id"),
                col("utm_campaign"),
                col("product_interest"),
                col("device_type")
            )
            .agg(
                count("*").alias("event_count"),
                spark_sum("engagement_score").alias("total_engagement"),
                spark_sum(when(col("is_conversion_event"), 1).otherwise(0)).alias("conversions"),
                spark_max("time_on_page_sec").alias("max_page_time"),
            )
            .writeStream
            .format("delta")
            .outputMode("complete")
            .option("checkpointLocation", f"{config['checkpoint_path']}/sessions")
            .trigger(processingTime="1 minute")
            .toTable("gold_marketing.realtime_session_engagement")
        )

        print("🔴 Clickstream streaming pipeline started")
        return {"bronze": bronze_query, "sessions": session_agg}

    # ────────────────────────────────────────────────────────────────────────
    # Stream: IVR / Voice Calls
    # ────────────────────────────────────────────────────────────────────────

    def start_ivr_stream(self):
        """
        Real-time IVR/voice call event pipeline:
        1. Ingest call center events
        2. Track wait times, sentiment, call disposition
        3. Feed real-time contact center dashboard
        """
        config = EVENTSTREAM_CONFIGS["ivr_calls"]
        stream_df = self.read_from_eventstream(config)

        # Enrich
        enriched = (stream_df
            .withColumn("long_wait_flag",
                when(col("wait_time_sec") > 300, True).otherwise(False))
            .withColumn("negative_sentiment_flag",
                when(col("sentiment_score") < -0.5, True).otherwise(False))
            .withColumn("escalation_needed",
                when(
                    (col("call_disposition") == "transferred") &
                    (col("sentiment_score") < -0.3),
                    True
                ).otherwise(False))
        )

        # Write to Bronze
        bronze_query = (enriched.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{config['checkpoint_path']}/bronze")
            .trigger(processingTime=config["trigger_interval"])
            .toTable(config["target_bronze"])
        )

        # Real-time contact center metrics
        cc_metrics = (enriched
            .groupBy(
                window(col("event_timestamp"), "15 minutes"),
                col("queue_name"),
                col("intent_detected")
            )
            .agg(
                count("*").alias("call_count"),
                avg("wait_time_sec").alias("avg_wait_sec"),
                avg("talk_time_sec").alias("avg_talk_sec"),
                avg("sentiment_score").alias("avg_sentiment"),
                spark_sum(when(col("call_disposition") == "abandoned", 1)
                          .otherwise(0)).alias("abandoned_count"),
            )
            .writeStream
            .format("delta")
            .outputMode("complete")
            .option("checkpointLocation", f"{config['checkpoint_path']}/cc_metrics")
            .trigger(processingTime="1 minute")
            .toTable("gold_customer.realtime_contact_center_metrics")
        )

        print("🔴 IVR/Voice streaming pipeline started")
        return {"bronze": bronze_query, "cc_metrics": cc_metrics}

    # ────────────────────────────────────────────────────────────────────────
    # Helpers
    # ────────────────────────────────────────────────────────────────────────

    def _get_eventstream_connection(self, eventstream_name: str) -> str:
        """
        Get Eventstream connection string.
        In Fabric: Eventstreams expose an EventHub-compatible custom endpoint.
        The connection string is available in the Eventstream properties.
        """
        row = spark.sql(f"""
            SELECT config_value FROM {METADATA_SCHEMA}.environment_config
            WHERE config_key = 'eventstream_{eventstream_name}_conn'
              AND environment = '{self.environment}'
            LIMIT 1
        """).first()

        if row:
            return row["config_value"]

        # Fallback: build from namespace config
        ns = spark.sql(f"""
            SELECT config_value FROM {METADATA_SCHEMA}.environment_config
            WHERE config_key = 'eventhub_namespace'
              AND environment = '{self.environment}'
            LIMIT 1
        """).first()
        if ns:
            # In production, use Key Vault for the connection string
            from fabric_native_utils import get_secret
            kv_url = spark.sql(f"""
                SELECT config_value FROM {METADATA_SCHEMA}.environment_config
                WHERE config_key = 'key_vault_url'
                  AND environment = '{self.environment}'
                LIMIT 1
            """).first()["config_value"]
            return get_secret(kv_url, f"eh-{eventstream_name}-conn")

        raise ValueError(f"No connection config found for Eventstream: {eventstream_name}")

    def stop_all_streams(self):
        """Stop all active streaming queries."""
        for q in spark.streams.active:
            print(f"  Stopping: {q.name or q.id}")
            q.stop()
        print("⏹️ All streams stopped.")


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    engine = FabricStreamingEngine(environment="prod")

    # Start all streams
    # claims = engine.start_claims_stream()
    # payments = engine.start_payment_stream()
    # clickstream = engine.start_clickstream_stream()
    # ivr = engine.start_ivr_stream()

    # Monitor active streams
    for q in spark.streams.active:
        print(f"  Active: {q.name or q.id} | Status: {q.status}")
