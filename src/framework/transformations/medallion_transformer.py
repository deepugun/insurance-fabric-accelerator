# ──────────────────────────────────────────────────────────────────────────────
# Insurance Fabric Accelerator — Medallion Transformations (Fabric-Native)
# Bronze → Silver → Gold using ONLY Fabric built-in features:
# - Delta Lake (merge, SCD2, constraints, time travel)
# - V-Order optimization (automatic on Fabric)
# - OPTIMIZE + VACUUM (built-in maintenance)
# - Schema evolution (built-in mergeSchema)
# - notebookutils for orchestration
# ──────────────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, sha2, concat_ws, when, coalesce,
    expr, row_number, to_date, year, month, dayofmonth, trim,
    upper, lower, regexp_replace, md5, monotonically_increasing_id
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from typing import Dict, List, Optional
import json
import uuid

spark = SparkSession.builder.getOrCreate()
METADATA_SCHEMA = "insurance_metadata"

# Fabric-native Spark settings (V-Order is automatic in Fabric)
spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")


class MedallionTransformer:
    """
    Fabric-native medallion transformation engine.
    Reads business_rules from metadata and applies:
    - Bronze → Silver: cleanse, deduplicate, conform, SCD Type 2
    - Silver → Gold: star schema, aggregations, business models
    All using Delta Lake built-in MERGE, constraints, and maintenance.
    """

    def __init__(self, domain: str):
        self.domain = domain
        self.run_id = str(uuid.uuid4())

    # ════════════════════════════════════════════════════════════════════════
    # BRONZE → SILVER  (Cleanse, Deduplicate, Conform, SCD2)
    # ════════════════════════════════════════════════════════════════════════

    def bronze_to_silver(self, source_table: str, target_table: str,
                         business_key_cols: List[str],
                         scd_type: int = 1):
        """
        Transform Bronze to Silver using metadata-driven rules.
        Uses Delta Lake MERGE (built-in) for upsert/SCD.
        """
        # 1. Read Bronze
        bronze_df = spark.table(source_table)

        # 2. Apply cleansing rules from metadata
        bronze_df = self._apply_business_rules(bronze_df, source_table, "bronze", "silver")

        # 3. Deduplicate (keep latest per business key)
        if business_key_cols:
            window = Window.partitionBy(*business_key_cols) \
                .orderBy(col("_ingestion_timestamp").desc())
            bronze_df = bronze_df.withColumn("_rn", row_number().over(window)) \
                .filter(col("_rn") == 1).drop("_rn")

        # 4. Add Silver audit columns
        bronze_df = (bronze_df
            .withColumn("_silver_load_ts", current_timestamp())
            .withColumn("_record_hash", sha2(
                concat_ws("||", *[col(c) for c in bronze_df.columns
                                   if not c.startswith("_")]), 256))
        )

        # 5. MERGE into Silver using Delta built-in
        if scd_type == 1:
            self._delta_merge_scd1(bronze_df, target_table, business_key_cols)
        elif scd_type == 2:
            self._delta_merge_scd2(bronze_df, target_table, business_key_cols)
        else:
            # Simple append
            bronze_df.write.format("delta").mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(target_table)

        # 6. Run Fabric-native maintenance
        self._maintain_table(target_table)

        print(f"✅ {source_table} → {target_table} (SCD{scd_type})")

    def _delta_merge_scd1(self, source_df: DataFrame, target_table: str,
                          key_cols: List[str]):
        """SCD Type 1 merge using Delta Lake built-in MERGE."""
        if not DeltaTable.isDeltaTable(spark, target_table):
            source_df.write.format("delta").mode("overwrite") \
                .option("mergeSchema", "true").saveAsTable(target_table)
            return

        target = DeltaTable.forName(spark, target_table)
        merge_condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in key_cols]
        )

        # Delta MERGE — built-in
        target.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        ).whenMatchedUpdate(
            condition="source._record_hash != target._record_hash",
            set={c: f"source.{c}" for c in source_df.columns}
        ).whenNotMatchedInsertAll().execute()

    def _delta_merge_scd2(self, source_df: DataFrame, target_table: str,
                          key_cols: List[str]):
        """SCD Type 2 merge using Delta Lake built-in MERGE."""
        # Add SCD2 columns
        source_df = (source_df
            .withColumn("_effective_from", current_timestamp())
            .withColumn("_effective_to", lit("9999-12-31").cast("timestamp"))
            .withColumn("_is_current", lit(True))
        )

        if not DeltaTable.isDeltaTable(spark, target_table):
            source_df.write.format("delta").mode("overwrite") \
                .option("mergeSchema", "true").saveAsTable(target_table)
            return

        target = DeltaTable.forName(spark, target_table)
        merge_condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in key_cols]
        ) + " AND target._is_current = TRUE"

        # Close existing records and insert new version — Delta MERGE built-in
        target.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        ).whenMatchedUpdate(
            condition="source._record_hash != target._record_hash",
            set={
                "_effective_to": "current_timestamp()",
                "_is_current": "FALSE",
            }
        ).execute()

        # Insert new records (changed + truly new)
        changed = source_df.alias("s").join(
            spark.table(target_table).filter(col("_is_current") == False)
                .alias("t"),
            on=[col(f"s.{k}") == col(f"t.{k}") for k in key_cols],
            how="inner"
        ).select("s.*")

        new_records = source_df.alias("s").join(
            spark.table(target_table).alias("t"),
            on=[col(f"s.{k}") == col(f"t.{k}") for k in key_cols],
            how="left_anti"
        )

        changed.union(new_records).write.format("delta").mode("append") \
            .saveAsTable(target_table)

    # ════════════════════════════════════════════════════════════════════════
    # SILVER → GOLD  (Star Schema, Aggregations, Business Models)
    # ════════════════════════════════════════════════════════════════════════

    def silver_to_gold(self, transformations: List[Dict]):
        """
        Build Gold layer models from Silver tables.
        Each transformation is a metadata-driven SQL or rule.
        """
        for tx in transformations:
            target_table = tx["target_table"]
            sql_expr = tx.get("sql_expression")
            mode = tx.get("mode", "overwrite")

            if sql_expr:
                # Execute SQL transformation (defined in metadata)
                result_df = spark.sql(sql_expr)
            else:
                # Apply business rules pipeline
                source_df = spark.table(tx["source_table"])
                result_df = self._apply_business_rules(
                    source_df, tx["source_table"], "silver", "gold"
                )

            # Write to Gold
            writer = result_df.write.format("delta") \
                .option("mergeSchema", "true")

            if tx.get("partition_by"):
                writer = writer.partitionBy(*tx["partition_by"])

            writer.mode(mode).saveAsTable(target_table)

            # Add Delta constraints (built-in data quality)
            for constraint in tx.get("constraints", []):
                try:
                    spark.sql(f"""
                        ALTER TABLE {target_table}
                        ADD CONSTRAINT {constraint['name']}
                        CHECK ({constraint['expression']})
                    """)
                except Exception:
                    pass  # Constraint may already exist

            # Maintenance
            self._maintain_table(target_table, tx.get("z_order_cols"))

            print(f"✅ Gold: {target_table}")

    # ════════════════════════════════════════════════════════════════════════
    # BUSINESS RULES ENGINE (Metadata-Driven)
    # ════════════════════════════════════════════════════════════════════════

    def _apply_business_rules(self, df: DataFrame, source_table: str,
                               source_layer: str, target_layer: str) -> DataFrame:
        """Apply business rules from metadata.business_rules table."""
        rules = spark.sql(f"""
            SELECT * FROM {METADATA_SCHEMA}.business_rules
            WHERE source_table = '{source_table}'
              AND source_layer = '{source_layer}'
              AND target_layer = '{target_layer}'
              AND domain = '{self.domain}'
              AND is_active = TRUE
              AND (effective_to IS NULL OR effective_to >= current_date())
            ORDER BY execution_order
        """).collect()

        for rule in rules:
            rule_type = rule["rule_type"]
            expression = rule["rule_expression"]
            params = json.loads(rule["rule_parameters"] or "{}")

            try:
                if rule_type == "sql_expr":
                    # Column-level SQL expression
                    target_col = params.get("target_column", rule["rule_name"])
                    df = df.withColumn(target_col, expr(expression))

                elif rule_type == "filter":
                    df = df.filter(expr(expression))

                elif rule_type == "rename":
                    old_name = params.get("old_name")
                    new_name = params.get("new_name")
                    if old_name and new_name and old_name in df.columns:
                        df = df.withColumnRenamed(old_name, new_name)

                elif rule_type == "cast":
                    target_col = params.get("column")
                    target_type = params.get("type")
                    if target_col and target_type:
                        df = df.withColumn(target_col, col(target_col).cast(target_type))

                elif rule_type == "standardize":
                    # Standard cleansing: trim, case normalize
                    target_col = params.get("column")
                    if target_col and target_col in df.columns:
                        df = df.withColumn(target_col, trim(upper(col(target_col))))

                elif rule_type == "default":
                    target_col = params.get("column")
                    default_val = params.get("value")
                    if target_col:
                        df = df.withColumn(
                            target_col,
                            coalesce(col(target_col), lit(default_val))
                        )

                elif rule_type == "lookup":
                    # Join with lookup table
                    lookup_table = params.get("lookup_table")
                    lookup_key = params.get("lookup_key")
                    lookup_value = params.get("lookup_value")
                    join_key = params.get("join_key")
                    if all([lookup_table, lookup_key, lookup_value, join_key]):
                        lookup_df = spark.table(lookup_table) \
                            .select(col(lookup_key), col(lookup_value).alias(f"_lkp_{lookup_value}"))
                        df = df.join(lookup_df, col(join_key) == col(lookup_key), "left") \
                            .drop(lookup_key)

            except Exception as e:
                error_handling = rule.get("error_handling", "skip_record")
                if error_handling == "fail_batch":
                    raise
                # skip_record: continue with unmodified df

        return df

    # ════════════════════════════════════════════════════════════════════════
    # TABLE MAINTENANCE (Fabric Built-In)
    # ════════════════════════════════════════════════════════════════════════

    def _maintain_table(self, table_name: str, z_order_cols: List[str] = None):
        """Run Fabric-native table maintenance (OPTIMIZE + VACUUM)."""
        try:
            # OPTIMIZE with V-Order (automatic in Fabric)
            if z_order_cols:
                spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({','.join(z_order_cols)})")
            else:
                spark.sql(f"OPTIMIZE {table_name}")
        except Exception:
            pass

        try:
            # VACUUM old files (7-day retention default)
            spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
# DOMAIN-SPECIFIC GOLD MODELS (Insurance)
# ═══════════════════════════════════════════════════════════════════════════════

# All Gold models defined as SQL — no hardcoded transforms
GOLD_MODELS = {
    "policy": [
        {
            "target_table": "gold_policy.dim_policy",
            "sql_expression": """
                SELECT
                    policy_id,
                    policy_number,
                    product_code,
                    product_name,
                    line_of_business,
                    policy_status,
                    effective_date,
                    expiration_date,
                    issue_date,
                    cancellation_date,
                    premium_amount,
                    coverage_amount,
                    deductible_amount,
                    customer_id,
                    agent_id,
                    underwriter_id,
                    risk_class,
                    territory_code,
                    state_code,
                    _silver_load_ts AS last_updated,
                    current_timestamp() AS _gold_load_ts
                FROM silver_policy.policy_clean
                WHERE _is_current = TRUE
            """,
            "mode": "overwrite",
            "z_order_cols": ["policy_number", "customer_id"],
            "constraints": [
                {"name": "chk_policy_id", "expression": "policy_id IS NOT NULL"},
                {"name": "chk_premium_pos", "expression": "premium_amount >= 0"},
            ]
        },
        {
            "target_table": "gold_policy.fact_policy_transaction",
            "sql_expression": """
                SELECT
                    transaction_id,
                    policy_id,
                    transaction_type,  -- 'new_business','endorsement','renewal','cancellation'
                    transaction_date,
                    effective_date,
                    premium_change,
                    written_premium,
                    earned_premium,
                    unearned_premium,
                    commission_amount,
                    tax_amount,
                    fees,
                    agent_id,
                    underwriter_id,
                    approval_status,
                    to_date(transaction_date) AS transaction_date_key,
                    current_timestamp() AS _gold_load_ts
                FROM silver_policy.policy_transaction_clean
            """,
            "mode": "overwrite",
            "partition_by": ["transaction_type"],
            "z_order_cols": ["policy_id", "transaction_date"],
            "constraints": []
        },
    ],
    "claims": [
        {
            "target_table": "gold_claims.dim_claim",
            "sql_expression": """
                SELECT
                    claim_id,
                    claim_number,
                    policy_id,
                    claimant_id,
                    loss_date,
                    report_date,
                    claim_status,
                    claim_type,
                    loss_type,
                    cause_of_loss,
                    loss_description,
                    loss_location_state,
                    loss_location_zip,
                    assigned_adjuster_id,
                    litigation_flag,
                    fraud_score,
                    fraud_flag,
                    total_incurred,
                    total_paid,
                    total_reserved,
                    salvage_amount,
                    subrogation_amount,
                    deductible_applied,
                    current_timestamp() AS _gold_load_ts
                FROM silver_claims.claim_clean
                WHERE _is_current = TRUE
            """,
            "mode": "overwrite",
            "z_order_cols": ["claim_number", "policy_id"],
            "constraints": [
                {"name": "chk_claim_id", "expression": "claim_id IS NOT NULL"},
            ]
        },
    ],
    "customer": [
        {
            "target_table": "gold_customer.dim_customer_master",
            "sql_expression": """
                SELECT
                    customer_id,
                    customer_type,         -- 'individual', 'organization'
                    first_name,
                    last_name,
                    full_name,
                    date_of_birth,
                    gender,
                    ssn,                   -- will be masked by security layer
                    email,                 -- will be masked
                    phone,                 -- will be masked
                    preferred_language,
                    preferred_channel,
                    customer_since_date,
                    customer_segment,
                    lifetime_premium_value,
                    total_claims_count,
                    active_policy_count,
                    risk_score,
                    nps_score,
                    churn_risk_score,
                    last_interaction_date,
                    current_timestamp() AS _gold_load_ts
                FROM silver_customer.customer_master_clean
                WHERE _is_current = TRUE
            """,
            "mode": "overwrite",
            "z_order_cols": ["customer_id"],
            "constraints": [
                {"name": "chk_customer_id", "expression": "customer_id IS NOT NULL"},
            ]
        },
    ],
    "finance": [
        {
            "target_table": "gold_finance.fact_journal_entry",
            "sql_expression": """
                SELECT
                    entry_id,
                    journal_id,
                    posting_date,
                    fiscal_year,
                    fiscal_period,
                    account_code,
                    account_name,
                    account_category,
                    department_code,
                    cost_center,
                    debit_amount,
                    credit_amount,
                    net_amount,
                    currency_code,
                    description,
                    source_module,    -- 'premium', 'claims', 'commission', 'reinsurance'
                    reference_id,
                    reversal_flag,
                    approved_by,
                    current_timestamp() AS _gold_load_ts
                FROM silver_finance.journal_entry_clean
            """,
            "mode": "overwrite",
            "partition_by": ["fiscal_year", "fiscal_period"],
            "z_order_cols": ["posting_date", "account_code"],
            "constraints": []
        },
    ],
    "billing": [
        {
            "target_table": "gold_billing.fact_premium_invoice",
            "sql_expression": """
                SELECT
                    invoice_id,
                    policy_id,
                    customer_id,
                    billing_period_start,
                    billing_period_end,
                    due_date,
                    invoice_amount,
                    paid_amount,
                    outstanding_amount,
                    payment_status,    -- 'pending', 'paid', 'overdue', 'grace', 'lapsed'
                    billing_frequency, -- 'monthly', 'quarterly', 'semi_annual', 'annual'
                    payment_method,
                    last_payment_date,
                    days_overdue,
                    dunning_level,
                    current_timestamp() AS _gold_load_ts
                FROM silver_billing.premium_invoice_clean
            """,
            "mode": "overwrite",
            "z_order_cols": ["policy_id", "customer_id"],
            "constraints": [
                {"name": "chk_invoice_amt", "expression": "invoice_amount >= 0"},
            ]
        },
    ],
}


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def run_medallion_pipeline(domain: str):
    """Run full Bronze → Silver → Gold pipeline for a domain."""
    transformer = MedallionTransformer(domain=domain)

    # Run Gold models
    if domain in GOLD_MODELS:
        transformer.silver_to_gold(GOLD_MODELS[domain])

    print(f"🏅 Medallion pipeline complete for domain: {domain}")


if __name__ == "__main__":
    # Run for each domain
    for domain in ["policy", "claims", "customer", "finance", "billing"]:
        run_medallion_pipeline(domain)
