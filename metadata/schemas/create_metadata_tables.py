# ──────────────────────────────────────────────────────────────────────────────
# Insurance Fabric Accelerator — Metadata Table Schemas (PySpark DDL)
# All control tables that drive the metadata-driven architecture.
# Deploy to: Lakehouse "insurance_metadata"
# ──────────────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

METADATA_SCHEMA = "insurance_metadata"

# ============================================================================
# 1. SOURCE SYSTEM CONFIGURATION
# ============================================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.source_system_config (
    source_system_id        STRING      NOT NULL,
    source_system_name      STRING      NOT NULL,
    source_type             STRING      NOT NULL,  -- 'database', 'api', 'file', 'event_stream'
    connection_type         STRING      NOT NULL,  -- 'jdbc', 'odbc', 'rest', 'sftp', 'eventhub', 'blob'
    connection_string_kv    STRING,                -- Key Vault secret name for connection string
    host                    STRING,
    port                    INT,
    database_name           STRING,
    authentication_type     STRING,                -- 'managed_identity', 'service_principal', 'key_vault'
    credential_kv_secret    STRING,                -- Key Vault secret name
    api_base_url            STRING,
    api_auth_type           STRING,                -- 'oauth2', 'api_key', 'certificate'
    file_format             STRING,                -- 'csv', 'parquet', 'json', 'xml', 'pdf'
    file_path_pattern       STRING,                -- e.g., '/raw/{source}/{entity}/{yyyy}/{MM}/{dd}/'
    eventhub_namespace      STRING,
    eventhub_name           STRING,
    consumer_group          STRING,
    max_concurrent_conn     INT         DEFAULT 5,
    retry_count             INT         DEFAULT 3,
    retry_delay_seconds     INT         DEFAULT 60,
    timeout_seconds         INT         DEFAULT 300,
    is_active               BOOLEAN     DEFAULT TRUE,
    environment             STRING      DEFAULT 'prod',  -- 'dev', 'test', 'prod'
    owner                   STRING,
    created_at              TIMESTAMP   DEFAULT current_timestamp(),
    updated_at              TIMESTAMP   DEFAULT current_timestamp(),
    created_by              STRING,
    updated_by              STRING
)
USING DELTA
COMMENT 'Master configuration for all source systems feeding the insurance platform'
TBLPROPERTIES ('quality' = 'metadata')
""")

# ============================================================================
# 2. INGESTION RULES
# ============================================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.ingestion_rules (
    ingestion_rule_id       STRING      NOT NULL,
    source_system_id        STRING      NOT NULL,
    entity_name             STRING      NOT NULL,  -- logical entity (e.g., 'policy', 'claim')
    source_table_or_path    STRING      NOT NULL,  -- table name, API endpoint, or file path
    target_lakehouse        STRING      NOT NULL,  -- 'bronze_policy', 'bronze_claims', etc.
    target_table            STRING      NOT NULL,
    ingestion_mode          STRING      NOT NULL,  -- 'full', 'incremental', 'cdc', 'streaming'
    incremental_column      STRING,                -- watermark column for incremental
    incremental_value       STRING,                -- last watermark value
    cdc_tracking_column     STRING,                -- e.g., '__$operation', 'op_type'
    partition_columns       STRING,                -- comma-separated partition columns
    load_frequency          STRING      NOT NULL,  -- 'hourly', 'daily', 'weekly', 'realtime', 'event_driven'
    schedule_cron           STRING,                -- cron expression
    batch_size              INT         DEFAULT 100000,
    source_query            STRING,                -- custom SQL or API query
    pre_hook_notebook       STRING,                -- notebook to run before ingestion
    post_hook_notebook      STRING,                -- notebook to run after ingestion
    file_header             BOOLEAN     DEFAULT TRUE,
    file_delimiter          STRING      DEFAULT ',',
    file_encoding           STRING      DEFAULT 'UTF-8',
    schema_enforcement      STRING      DEFAULT 'evolve',  -- 'strict', 'evolve', 'ignore'
    dedup_columns           STRING,                -- columns for deduplication
    priority                INT         DEFAULT 5, -- 1=highest, 10=lowest
    domain                  STRING      NOT NULL,  -- 'policy', 'claims', 'billing', etc.
    sla_minutes             INT         DEFAULT 60,
    is_active               BOOLEAN     DEFAULT TRUE,
    last_run_status         STRING,
    last_run_timestamp      TIMESTAMP,
    last_run_record_count   LONG,
    created_at              TIMESTAMP   DEFAULT current_timestamp(),
    updated_at              TIMESTAMP   DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Defines how each entity is ingested from source to Bronze layer'
TBLPROPERTIES ('quality' = 'metadata')
""")

# ============================================================================
# 3. SCHEMA MAPPING
# ============================================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.schema_mapping (
    mapping_id              STRING      NOT NULL,
    ingestion_rule_id       STRING      NOT NULL,
    source_column           STRING      NOT NULL,
    target_column           STRING      NOT NULL,
    source_data_type        STRING,
    target_data_type        STRING      NOT NULL,
    transformation_expr     STRING,        -- PySpark SQL expression for transformation
    is_nullable             BOOLEAN     DEFAULT TRUE,
    default_value           STRING,
    is_primary_key          BOOLEAN     DEFAULT FALSE,
    is_business_key         BOOLEAN     DEFAULT FALSE,
    is_partition_key        BOOLEAN     DEFAULT FALSE,
    is_pii                  BOOLEAN     DEFAULT FALSE,  -- flag for masking
    pii_category            STRING,        -- 'ssn', 'dob', 'name', 'address', 'medical', 'financial'
    masking_function        STRING,        -- 'hash', 'redact', 'partial_mask', 'tokenize'
    column_order            INT,
    domain                  STRING,
    is_active               BOOLEAN     DEFAULT TRUE,
    created_at              TIMESTAMP   DEFAULT current_timestamp(),
    updated_at              TIMESTAMP   DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Column-level mapping from source to Bronze with transformation rules'
TBLPROPERTIES ('quality' = 'metadata')
""")

# ============================================================================
# 4. BUSINESS RULES
# ============================================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.business_rules (
    rule_id                 STRING      NOT NULL,
    rule_name               STRING      NOT NULL,
    rule_category           STRING      NOT NULL,  -- 'transformation', 'derivation', 'validation', 'enrichment'
    domain                  STRING      NOT NULL,
    source_layer            STRING      NOT NULL,  -- 'bronze', 'silver', 'gold'
    target_layer            STRING      NOT NULL,
    source_table            STRING      NOT NULL,
    target_table            STRING      NOT NULL,
    rule_type               STRING      NOT NULL,  -- 'sql_expr', 'python_func', 'lookup', 'scd2', 'aggregate'
    rule_expression         STRING      NOT NULL,  -- SQL expression or function reference
    rule_parameters         STRING,                -- JSON parameters for the rule
    execution_order         INT         DEFAULT 1,
    dependency_rules        STRING,                -- comma-separated rule_ids that must run first
    error_handling          STRING      DEFAULT 'skip_record',  -- 'skip_record', 'fail_batch', 'default_value'
    error_default_value     STRING,
    description             STRING,
    is_active               BOOLEAN     DEFAULT TRUE,
    effective_from          DATE,
    effective_to            DATE,
    version                 INT         DEFAULT 1,
    created_at              TIMESTAMP   DEFAULT current_timestamp(),
    updated_at              TIMESTAMP   DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Configurable business rules for Silver and Gold layer transformations'
TBLPROPERTIES ('quality' = 'metadata')
""")

# ============================================================================
# 5. DATA QUALITY RULES
# ============================================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.data_quality_rules (
    dq_rule_id              STRING      NOT NULL,
    rule_name               STRING      NOT NULL,
    domain                  STRING      NOT NULL,
    target_table            STRING      NOT NULL,
    target_column           STRING,                 -- NULL for table-level rules
    rule_type               STRING      NOT NULL,   -- 'completeness', 'uniqueness', 'referential',
                                                    -- 'range', 'regex', 'custom', 'freshness', 'volume'
    rule_expression         STRING      NOT NULL,   -- SQL boolean expression
    rule_parameters         STRING,                 -- JSON: thresholds, reference tables, etc.
    severity                STRING      DEFAULT 'warning',  -- 'info', 'warning', 'critical', 'blocking'
    threshold_pct           DOUBLE      DEFAULT 100.0,  -- minimum pass percentage
    check_layer             STRING      NOT NULL,   -- 'bronze', 'silver', 'gold'
    check_frequency         STRING      DEFAULT 'per_load',  -- 'per_load', 'daily', 'weekly'
    alert_on_failure        BOOLEAN     DEFAULT TRUE,
    alert_channel           STRING      DEFAULT 'email',  -- 'email', 'teams', 'activator'
    alert_recipients        STRING,                 -- comma-separated emails or channel IDs
    auto_quarantine         BOOLEAN     DEFAULT FALSE,  -- move failing records to quarantine
    is_active               BOOLEAN     DEFAULT TRUE,
    description             STRING,
    created_at              TIMESTAMP   DEFAULT current_timestamp(),
    updated_at              TIMESTAMP   DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Data quality rules executed at each layer transition'
TBLPROPERTIES ('quality' = 'metadata')
""")

# ============================================================================
# 6. DATA QUALITY EXECUTION LOG
# ============================================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.dq_execution_log (
    execution_id            STRING      NOT NULL,
    dq_rule_id              STRING      NOT NULL,
    pipeline_run_id         STRING,
    target_table            STRING      NOT NULL,
    total_records           LONG,
    passed_records          LONG,
    failed_records          LONG,
    pass_percentage         DOUBLE,
    status                  STRING      NOT NULL,   -- 'passed', 'warning', 'failed', 'error'
    failure_sample          STRING,                 -- JSON sample of failing records
    execution_start         TIMESTAMP   NOT NULL,
    execution_end           TIMESTAMP,
    execution_duration_sec  INT,
    error_message           STRING,
    created_at              TIMESTAMP   DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Execution log for all data quality checks'
TBLPROPERTIES ('quality' = 'metadata')
PARTITIONED BY (target_table)
""")

# ============================================================================
# 7. ML MODEL REGISTRY
# ============================================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.ml_model_registry (
    model_id                STRING      NOT NULL,
    model_name              STRING      NOT NULL,
    model_version           INT         NOT NULL,
    model_type              STRING      NOT NULL,  -- 'classification', 'regression', 'clustering', 'anomaly', 'nlp'
    domain                  STRING      NOT NULL,  -- 'fraud', 'risk', 'churn', 'pricing', 'claims_triage'
    algorithm               STRING,                -- 'xgboost', 'random_forest', 'logistic_regression', 'transformer'
    framework               STRING,                -- 'spark_ml', 'sklearn', 'pytorch', 'huggingface'
    mlflow_run_id           STRING,
    mlflow_model_uri        STRING,                -- MLflow model artifact path
    input_features          STRING,                -- JSON array of feature columns
    output_columns          STRING,                -- JSON array of output columns
    training_table          STRING,
    training_date           TIMESTAMP,
    training_metrics        STRING,                -- JSON: accuracy, auc, f1, etc.
    validation_metrics      STRING,
    serving_endpoint        STRING,                -- REST endpoint if deployed
    batch_scoring_table     STRING,                -- table for batch predictions
    scoring_schedule        STRING,                -- cron for batch scoring
    threshold_config        STRING,                -- JSON: decision thresholds
    is_champion             BOOLEAN     DEFAULT FALSE,
    is_active               BOOLEAN     DEFAULT TRUE,
    approved_by             STRING,
    approved_at             TIMESTAMP,
    description             STRING,
    created_at              TIMESTAMP   DEFAULT current_timestamp(),
    updated_at              TIMESTAMP   DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Registry for all ML models used across the insurance platform'
TBLPROPERTIES ('quality' = 'metadata')
""")

# ============================================================================
# 8. AGENT CONFIGURATION
# ============================================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.agent_config (
    agent_id                STRING      NOT NULL,
    agent_name              STRING      NOT NULL,
    agent_type              STRING      NOT NULL,  -- 'data_agent', 'operations_agent', 'customer_agent'
    description             STRING,
    ontology_entities       STRING,                -- JSON array of IQ ontology entities this agent can access
    allowed_domains         STRING,                -- JSON array: ['policy', 'claims', 'billing']
    allowed_actions         STRING,                -- JSON array: ['query', 'summarize', 'alert', 'remediate']
    llm_model               STRING      DEFAULT 'gpt-4',
    system_prompt           STRING,                -- system instruction for the agent
    max_tokens              INT         DEFAULT 4096,
    temperature             DOUBLE      DEFAULT 0.1,
    grounding_sources       STRING,                -- JSON: ontology, graph, semantic models
    tool_definitions        STRING,                -- JSON: available tools/functions
    rls_security_context    STRING,                -- security context for data access
    rate_limit_per_minute   INT         DEFAULT 30,
    is_active               BOOLEAN     DEFAULT TRUE,
    version                 INT         DEFAULT 1,
    created_at              TIMESTAMP   DEFAULT current_timestamp(),
    updated_at              TIMESTAMP   DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Configuration for Fabric IQ agents (Data, Operations, Customer)'
TBLPROPERTIES ('quality' = 'metadata')
""")

# ============================================================================
# 9. AGENT ACTIVITY LOG
# ============================================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.agent_activity_log (
    activity_id             STRING      NOT NULL,
    agent_id                STRING      NOT NULL,
    session_id              STRING,
    user_id                 STRING,
    query_text              STRING,
    response_text           STRING,
    ontology_entities_used  STRING,                -- JSON
    graph_paths_traversed   STRING,                -- JSON
    tools_invoked           STRING,                -- JSON
    tokens_used             INT,
    latency_ms              INT,
    status                  STRING,                -- 'success', 'error', 'partial'
    error_message           STRING,
    feedback_rating         INT,                   -- 1-5 user rating
    feedback_text           STRING,
    created_at              TIMESTAMP   DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Activity log for all agent interactions'
TBLPROPERTIES ('quality' = 'metadata')
PARTITIONED BY (agent_id)
""")

# ============================================================================
# 10. ALERT RULES (Activator)
# ============================================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.alert_rules (
    alert_rule_id           STRING      NOT NULL,
    alert_name              STRING      NOT NULL,
    alert_category          STRING      NOT NULL,  -- 'pipeline', 'data_quality', 'fraud', 'sla', 'business'
    domain                  STRING,
    trigger_type            STRING      NOT NULL,  -- 'threshold', 'anomaly', 'event', 'schedule'
    trigger_condition       STRING      NOT NULL,  -- SQL expression or KQL query
    trigger_parameters      STRING,                -- JSON: thresholds, windows, etc.
    severity                STRING      DEFAULT 'medium',  -- 'low', 'medium', 'high', 'critical'
    notification_channels   STRING      NOT NULL,  -- JSON: ['email', 'teams', 'pagerduty']
    notification_recipients STRING      NOT NULL,  -- JSON array of recipients
    auto_remediation        BOOLEAN     DEFAULT FALSE,
    remediation_notebook    STRING,                -- notebook to execute for auto-fix
    remediation_parameters  STRING,                -- JSON parameters for remediation
    cooldown_minutes        INT         DEFAULT 30,
    escalation_minutes      INT         DEFAULT 120,
    escalation_recipients   STRING,                -- JSON escalation chain
    is_active               BOOLEAN     DEFAULT TRUE,
    last_triggered          TIMESTAMP,
    trigger_count_24h       INT         DEFAULT 0,
    created_at              TIMESTAMP   DEFAULT current_timestamp(),
    updated_at              TIMESTAMP   DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Alert and automation rules for Fabric Activator'
TBLPROPERTIES ('quality' = 'metadata')
""")

# ============================================================================
# 11. PIPELINE EXECUTION LOG
# ============================================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.pipeline_execution_log (
    execution_id            STRING      NOT NULL,
    pipeline_name           STRING      NOT NULL,
    pipeline_type           STRING      NOT NULL,  -- 'ingestion', 'transformation', 'dq', 'ml', 'streaming'
    domain                  STRING,
    ingestion_rule_id       STRING,
    run_id                  STRING,                -- Fabric pipeline run ID
    status                  STRING      NOT NULL,  -- 'running', 'succeeded', 'failed', 'cancelled', 'skipped'
    start_time              TIMESTAMP   NOT NULL,
    end_time                TIMESTAMP,
    duration_seconds        INT,
    records_read            LONG,
    records_written         LONG,
    records_errored         LONG,
    bytes_read              LONG,
    bytes_written           LONG,
    error_message           STRING,
    error_stack_trace       STRING,
    retry_attempt           INT         DEFAULT 0,
    spark_application_id    STRING,
    notebook_path           STRING,
    parameters              STRING,                -- JSON of runtime parameters
    environment             STRING      DEFAULT 'prod',
    triggered_by            STRING,                -- 'schedule', 'manual', 'event', 'retry'
    parent_execution_id     STRING,                -- for sub-pipeline tracking
    created_at              TIMESTAMP   DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Execution log for all pipeline runs across the platform'
TBLPROPERTIES ('quality' = 'metadata')
PARTITIONED BY (domain, status)
""")

# ============================================================================
# 12. ENVIRONMENT CONFIGURATION
# ============================================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.environment_config (
    config_key              STRING      NOT NULL,
    config_value            STRING      NOT NULL,
    config_category         STRING      NOT NULL,  -- 'workspace', 'lakehouse', 'security', 'api', 'notification'
    environment             STRING      NOT NULL,  -- 'dev', 'test', 'prod'
    description             STRING,
    is_secret               BOOLEAN     DEFAULT FALSE,  -- if true, value is a KV reference
    is_active               BOOLEAN     DEFAULT TRUE,
    created_at              TIMESTAMP   DEFAULT current_timestamp(),
    updated_at              TIMESTAMP   DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Environment-specific configuration (workspace IDs, endpoints, etc.)'
TBLPROPERTIES ('quality' = 'metadata')
""")

# ============================================================================
# 13. DOMAIN REGISTRY
# ============================================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.domain_registry (
    domain_id               STRING      NOT NULL,
    domain_name             STRING      NOT NULL,
    domain_description      STRING,
    domain_owner            STRING      NOT NULL,
    bronze_lakehouse        STRING      NOT NULL,
    silver_lakehouse        STRING      NOT NULL,
    gold_lakehouse          STRING      NOT NULL,
    semantic_model          STRING,
    workspace_id            STRING,
    sla_freshness_minutes   INT         DEFAULT 60,
    sla_quality_pct         DOUBLE      DEFAULT 99.0,
    is_active               BOOLEAN     DEFAULT TRUE,
    created_at              TIMESTAMP   DEFAULT current_timestamp(),
    updated_at              TIMESTAMP   DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Registry of all insurance domains with their Fabric resource mappings'
TBLPROPERTIES ('quality' = 'metadata')
""")

# ============================================================================
# 14. RETENTION POLICY
# ============================================================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METADATA_SCHEMA}.retention_policy (
    policy_id               STRING      NOT NULL,
    table_name              STRING      NOT NULL,
    layer                   STRING      NOT NULL,  -- 'bronze', 'silver', 'gold'
    domain                  STRING      NOT NULL,
    retention_days          INT         NOT NULL,
    retention_type          STRING      NOT NULL,  -- 'delete', 'archive', 'snapshot'
    archive_location        STRING,                -- archive path for archived data
    regulatory_requirement  STRING,                -- regulation requiring this retention
    is_active               BOOLEAN     DEFAULT TRUE,
    last_purge_date         DATE,
    created_at              TIMESTAMP   DEFAULT current_timestamp(),
    updated_at              TIMESTAMP   DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Data retention policies per table for regulatory compliance'
TBLPROPERTIES ('quality' = 'metadata')
""")

print("✅ All 14 metadata tables created successfully.")
