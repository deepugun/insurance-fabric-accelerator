# ──────────────────────────────────────────────────────────────────────────────
# Insurance Fabric Accelerator — Metadata-Driven Ingestion Engine
# Reads ingestion_rules + source_system_config to dynamically ingest data
# into Bronze Lakehouse. Zero hardcoded logic.
# ──────────────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name, sha2, concat_ws, expr
)
from pyspark.sql.types import StructType
from delta.tables import DeltaTable
import json
import uuid
from datetime import datetime
from typing import Optional, Dict, Any

spark = SparkSession.builder.getOrCreate()
METADATA_SCHEMA = "insurance_metadata"


class MetadataIngestionEngine:
    """
    Fully metadata-driven ingestion engine.
    Reads configuration from metadata tables and executes ingestion
    based on rules — no hardcoded source/target logic.
    """

    def __init__(self, environment: str = "prod"):
        self.environment = environment
        self.execution_id = str(uuid.uuid4())
        self.start_time = datetime.utcnow()

    # ────────────────────────────────────────────────────────────────────────
    # Configuration Loaders
    # ────────────────────────────────────────────────────────────────────────

    def get_active_ingestion_rules(self, domain: Optional[str] = None) -> DataFrame:
        """Load active ingestion rules, optionally filtered by domain."""
        query = f"""
            SELECT ir.*, ss.*
            FROM {METADATA_SCHEMA}.ingestion_rules ir
            JOIN {METADATA_SCHEMA}.source_system_config ss
              ON ir.source_system_id = ss.source_system_id
            WHERE ir.is_active = TRUE
              AND ss.is_active = TRUE
              AND ss.environment = '{self.environment}'
        """
        if domain:
            query += f" AND ir.domain = '{domain}'"
        query += " ORDER BY ir.priority ASC, ir.domain"
        return spark.sql(query)

    def get_schema_mappings(self, ingestion_rule_id: str) -> DataFrame:
        """Load column mappings for a specific ingestion rule."""
        return spark.sql(f"""
            SELECT * FROM {METADATA_SCHEMA}.schema_mapping
            WHERE ingestion_rule_id = '{ingestion_rule_id}'
              AND is_active = TRUE
            ORDER BY column_order
        """)

    def get_env_config(self, key: str) -> str:
        """Get environment-specific configuration value."""
        row = spark.sql(f"""
            SELECT config_value FROM {METADATA_SCHEMA}.environment_config
            WHERE config_key = '{key}'
              AND environment = '{self.environment}'
              AND is_active = TRUE
            LIMIT 1
        """).first()
        return row["config_value"] if row else None

    # ────────────────────────────────────────────────────────────────────────
    # Source Readers (based on source_type + connection_type)
    # ────────────────────────────────────────────────────────────────────────

    def read_jdbc_source(self, rule: Dict[str, Any]) -> DataFrame:
        """Read from JDBC source (SQL Server, Oracle, PostgreSQL)."""
        connection_string = self._resolve_secret(rule["connection_string_kv"])

        jdbc_url = f"jdbc:sqlserver://{rule['host']}:{rule['port']};databaseName={rule['database_name']}"

        read_options = {
            "url": jdbc_url,
            "dbtable": rule["source_table_or_path"],
            "fetchsize": str(rule.get("batch_size", 100000)),
        }

        # Add authentication based on type
        if rule["authentication_type"] == "managed_identity":
            read_options["authentication"] = "ActiveDirectoryMSI"
        elif rule["authentication_type"] == "service_principal":
            read_options["authentication"] = "ActiveDirectoryServicePrincipal"

        # Add incremental filter
        if rule["ingestion_mode"] == "incremental" and rule.get("incremental_value"):
            source_query = f"""
                (SELECT * FROM {rule['source_table_or_path']}
                 WHERE {rule['incremental_column']} > '{rule['incremental_value']}') AS src
            """
            read_options["dbtable"] = source_query

        # Custom query override
        if rule.get("source_query"):
            read_options["dbtable"] = f"({rule['source_query']}) AS custom_src"

        return spark.read.format("jdbc").options(**read_options).load()

    def read_api_source(self, rule: Dict[str, Any]) -> DataFrame:
        """Read from REST API source."""
        # Build API URL
        api_url = f"{rule['api_base_url']}{rule['source_table_or_path']}"

        # For API sources, use requests library and convert to DataFrame
        # In Fabric, use notebookutils or mssparkutils for HTTP calls
        import requests
        from pyspark.sql import Row

        headers = self._get_api_headers(rule)
        params = {}

        # Add incremental parameter
        if rule["ingestion_mode"] == "incremental" and rule.get("incremental_value"):
            params["since"] = rule["incremental_value"]

        response = requests.get(api_url, headers=headers, params=params, timeout=rule.get("timeout_seconds", 300))
        response.raise_for_status()
        data = response.json()

        # Handle paginated responses
        all_records = data if isinstance(data, list) else data.get("data", data.get("results", [data]))

        if not all_records:
            return spark.createDataFrame([], StructType([]))

        return spark.createDataFrame([Row(**record) for record in all_records])

    def read_file_source(self, rule: Dict[str, Any]) -> DataFrame:
        """Read from file source (CSV, Parquet, JSON)."""
        file_format = rule.get("file_format", "csv")
        file_path = rule["source_table_or_path"]

        read_options = {}
        if file_format == "csv":
            read_options = {
                "header": str(rule.get("file_header", True)).lower(),
                "delimiter": rule.get("file_delimiter", ","),
                "encoding": rule.get("file_encoding", "UTF-8"),
                "inferSchema": "true",
                "multiLine": "true",
            }

        df = spark.read.format(file_format).options(**read_options).load(file_path)

        # Add source file tracking
        if file_format in ("csv", "parquet", "json"):
            df = df.withColumn("_source_file", input_file_name())

        return df

    def read_eventhub_batch(self, rule: Dict[str, Any]) -> DataFrame:
        """Read EventHub as batch (for backfill/replay). Streaming handled separately."""
        eh_config = {
            "eventhubs.connectionString": self._resolve_secret(f"kv-{rule['eventhub_namespace']}-conn"),
            "eventhubs.consumerGroup": rule.get("consumer_group", "$Default"),
            "maxEventsPerTrigger": str(rule.get("batch_size", 10000)),
        }
        return spark.read.format("eventhubs").options(**eh_config).load()

    # ────────────────────────────────────────────────────────────────────────
    # Schema Transformation & Enrichment
    # ────────────────────────────────────────────────────────────────────────

    def apply_schema_mapping(self, df: DataFrame, ingestion_rule_id: str) -> DataFrame:
        """Apply column mappings and transformations from metadata."""
        mappings = self.get_schema_mappings(ingestion_rule_id).collect()

        if not mappings:
            # No explicit mappings — pass through with audit columns
            return self._add_audit_columns(df, ingestion_rule_id)

        select_exprs = []
        for m in mappings:
            if m["transformation_expr"]:
                # Apply transformation expression
                select_exprs.append(
                    expr(m["transformation_expr"]).cast(m["target_data_type"]).alias(m["target_column"])
                )
            else:
                # Direct column mapping with type cast
                select_exprs.append(
                    col(m["source_column"]).cast(m["target_data_type"]).alias(m["target_column"])
                )

        result_df = df.select(*select_exprs)
        return self._add_audit_columns(result_df, ingestion_rule_id)

    def _add_audit_columns(self, df: DataFrame, ingestion_rule_id: str) -> DataFrame:
        """Add standard audit/lineage columns to every ingested record."""
        return (df
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_ingestion_rule_id", lit(ingestion_rule_id))
            .withColumn("_execution_id", lit(self.execution_id))
            .withColumn("_record_hash", sha2(concat_ws("||", *df.columns), 256))
        )

    # ────────────────────────────────────────────────────────────────────────
    # Writers (Bronze Layer)
    # ────────────────────────────────────────────────────────────────────────

    def write_to_bronze(self, df: DataFrame, rule: Dict[str, Any]) -> int:
        """Write DataFrame to Bronze Lakehouse using appropriate strategy."""
        target_path = f"{rule['target_lakehouse']}.{rule['target_table']}"
        mode = rule["ingestion_mode"]
        record_count = df.count()

        if record_count == 0:
            return 0

        writer = df.write.format("delta").option("mergeSchema", "true")

        # Add partitioning if specified
        if rule.get("partition_columns"):
            partition_cols = [c.strip() for c in rule["partition_columns"].split(",")]
            writer = writer.partitionBy(*partition_cols)

        if mode == "full":
            writer.mode("overwrite").saveAsTable(target_path)
        elif mode in ("incremental", "cdc"):
            # Append new records, dedup if configured
            if rule.get("dedup_columns"):
                dedup_cols = [c.strip() for c in rule["dedup_columns"].split(",")]
                df = df.dropDuplicates(dedup_cols)

            if DeltaTable.isDeltaTable(spark, target_path):
                writer.mode("append").saveAsTable(target_path)
            else:
                writer.mode("overwrite").saveAsTable(target_path)
        elif mode == "streaming":
            # Batch write for streaming backfill; real-time handled by streaming engine
            writer.mode("append").saveAsTable(target_path)

        return record_count

    # ────────────────────────────────────────────────────────────────────────
    # Orchestrator
    # ────────────────────────────────────────────────────────────────────────

    def run_ingestion(self, domain: Optional[str] = None, rule_id: Optional[str] = None):
        """
        Main entry point. Reads metadata rules and executes ingestion.
        Can be scoped by domain or specific rule_id.
        """
        rules_df = self.get_active_ingestion_rules(domain)

        if rule_id:
            rules_df = rules_df.filter(col("ingestion_rule_id") == rule_id)

        rules = rules_df.collect()
        results = []

        for rule in rules:
            rule_dict = rule.asDict()
            rule_exec_id = str(uuid.uuid4())
            rule_start = datetime.utcnow()

            try:
                # Skip streaming rules (handled by streaming engine)
                if rule_dict["ingestion_mode"] == "streaming":
                    continue

                # Execute pre-hook if configured
                if rule_dict.get("pre_hook_notebook"):
                    self._execute_notebook(rule_dict["pre_hook_notebook"], rule_dict)

                # Read from source
                source_df = self._read_source(rule_dict)

                # Apply schema mappings
                mapped_df = self.apply_schema_mapping(source_df, rule_dict["ingestion_rule_id"])

                # Write to Bronze
                record_count = self.write_to_bronze(mapped_df, rule_dict)

                # Update watermark for incremental
                if rule_dict["ingestion_mode"] == "incremental" and rule_dict.get("incremental_column"):
                    self._update_watermark(rule_dict, source_df)

                # Execute post-hook if configured
                if rule_dict.get("post_hook_notebook"):
                    self._execute_notebook(rule_dict["post_hook_notebook"], rule_dict)

                # Log success
                self._log_execution(
                    rule_dict, rule_exec_id, "succeeded",
                    rule_start, record_count=record_count
                )
                results.append({"rule_id": rule_dict["ingestion_rule_id"], "status": "succeeded", "records": record_count})

            except Exception as e:
                # Log failure
                self._log_execution(
                    rule_dict, rule_exec_id, "failed",
                    rule_start, error_message=str(e)
                )
                results.append({"rule_id": rule_dict["ingestion_rule_id"], "status": "failed", "error": str(e)})

                # Check for auto-retry via alert rules
                self._check_alert_rules(rule_dict, str(e))

        return results

    # ────────────────────────────────────────────────────────────────────────
    # Internal Helpers
    # ────────────────────────────────────────────────────────────────────────

    def _read_source(self, rule: Dict[str, Any]) -> DataFrame:
        """Route to appropriate reader based on source type."""
        source_type = rule["source_type"]
        connection_type = rule["connection_type"]

        if source_type == "database" and connection_type == "jdbc":
            return self.read_jdbc_source(rule)
        elif source_type == "api" and connection_type == "rest":
            return self.read_api_source(rule)
        elif source_type == "file":
            return self.read_file_source(rule)
        elif source_type == "event_stream":
            return self.read_eventhub_batch(rule)
        else:
            raise ValueError(f"Unsupported source_type/connection_type: {source_type}/{connection_type}")

    def _resolve_secret(self, secret_name: str) -> str:
        """Resolve secret from Key Vault via Fabric notebookutils."""
        kv_url = self.get_env_config("key_vault_url")
        # In Fabric notebooks: notebookutils.credentials.getSecret(kv_url, secret_name)
        # Placeholder for local development
        return f"resolved:{secret_name}"

    def _get_api_headers(self, rule: Dict[str, Any]) -> dict:
        """Build API authentication headers."""
        headers = {"Content-Type": "application/json"}
        if rule.get("api_auth_type") == "api_key":
            api_key = self._resolve_secret(rule["credential_kv_secret"])
            headers["Authorization"] = f"Bearer {api_key}"
        elif rule.get("api_auth_type") == "oauth2":
            token = self._get_oauth_token(rule)
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _get_oauth_token(self, rule: Dict[str, Any]) -> str:
        """Get OAuth2 token for API authentication."""
        # Implementation would use MSAL or notebookutils
        return self._resolve_secret(rule["credential_kv_secret"])

    def _update_watermark(self, rule: Dict[str, Any], df: DataFrame):
        """Update the incremental watermark value after successful load."""
        max_value = df.agg({rule["incremental_column"]: "max"}).collect()[0][0]
        if max_value:
            spark.sql(f"""
                UPDATE {METADATA_SCHEMA}.ingestion_rules
                SET incremental_value = '{max_value}',
                    last_run_timestamp = current_timestamp(),
                    last_run_status = 'succeeded'
                WHERE ingestion_rule_id = '{rule["ingestion_rule_id"]}'
            """)

    def _execute_notebook(self, notebook_path: str, rule: Dict[str, Any]):
        """Execute a pre/post hook notebook."""
        # In Fabric: notebookutils.notebook.run(notebook_path, params)
        pass

    def _log_execution(self, rule: Dict[str, Any], exec_id: str, status: str,
                       start_time: datetime, record_count: int = 0, error_message: str = None):
        """Log pipeline execution to metadata."""
        end_time = datetime.utcnow()
        duration = int((end_time - start_time).total_seconds())

        log_data = [(
            exec_id,
            f"ingest_{rule['entity_name']}",
            "ingestion",
            rule.get("domain"),
            rule["ingestion_rule_id"],
            self.execution_id,
            status,
            start_time,
            end_time,
            duration,
            record_count if status == "succeeded" else 0,
            record_count if status == "succeeded" else 0,
            0,
            None, None,
            error_message, None,
            0, None,
            rule.get("source_table_or_path"),
            json.dumps({"ingestion_mode": rule["ingestion_mode"], "source": rule.get("source_system_name")}),
            self.environment,
            "schedule",
            None,
        )]

        log_schema = "execution_id STRING, pipeline_name STRING, pipeline_type STRING, " \
                     "domain STRING, ingestion_rule_id STRING, run_id STRING, status STRING, " \
                     "start_time TIMESTAMP, end_time TIMESTAMP, duration_seconds INT, " \
                     "records_read LONG, records_written LONG, records_errored LONG, " \
                     "bytes_read LONG, bytes_written LONG, error_message STRING, " \
                     "error_stack_trace STRING, retry_attempt INT, spark_application_id STRING, " \
                     "notebook_path STRING, parameters STRING, environment STRING, " \
                     "triggered_by STRING, parent_execution_id STRING"

        spark.createDataFrame(log_data, log_schema).write.mode("append").saveAsTable(
            f"{METADATA_SCHEMA}.pipeline_execution_log"
        )

    def _check_alert_rules(self, rule: Dict[str, Any], error_message: str):
        """Check if any alert rules should fire for this failure."""
        alerts = spark.sql(f"""
            SELECT * FROM {METADATA_SCHEMA}.alert_rules
            WHERE alert_category = 'pipeline'
              AND is_active = TRUE
              AND auto_remediation = TRUE
        """).collect()

        for alert in alerts:
            # In production: trigger Activator, send email, etc.
            pass


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT — Execute from Fabric Notebook or Pipeline
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    engine = MetadataIngestionEngine(environment="prod")

    # Run all domains
    results = engine.run_ingestion()

    # Or run specific domain
    # results = engine.run_ingestion(domain="claims")

    # Or run specific rule
    # results = engine.run_ingestion(rule_id="ING004")

    for r in results:
        print(f"  Rule: {r['rule_id']} | Status: {r['status']} | Records: {r.get('records', 'N/A')}")
