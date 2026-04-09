# ──────────────────────────────────────────────────────────────────────────────
# Insurance Fabric Accelerator — Metadata Seed Data
# Initial configuration to bootstrap the platform
# ──────────────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

spark = SparkSession.builder.getOrCreate()
METADATA_SCHEMA = "insurance_metadata"

# ============================================================================
# 1. DOMAIN REGISTRY SEED
# ============================================================================
domain_data = [
    ("DOM001", "policy",       "Policy Administration",      "policy_team@insurer.com",    "bronze_policy",    "silver_policy",    "gold_policy",    "sem_policy",       None, 30,  99.5, True),
    ("DOM002", "claims",       "Claims Management",          "claims_team@insurer.com",    "bronze_claims",    "silver_claims",    "gold_claims",    "sem_claims",       None, 15,  99.0, True),
    ("DOM003", "customer",     "Customer / Party MDM",       "mdm_team@insurer.com",       "bronze_customer",  "silver_customer",  "gold_customer",  "sem_customer360",  None, 60,  99.5, True),
    ("DOM004", "billing",      "Billing & Payments",         "billing_team@insurer.com",   "bronze_billing",   "silver_billing",   "gold_billing",   "sem_billing",      None, 30,  99.0, True),
    ("DOM005", "disbursement", "Disbursements",              "finance_team@insurer.com",   "bronze_disbursement","silver_disbursement","gold_disbursement","sem_finance",  None, 60,  99.0, True),
    ("DOM006", "actuarial",    "Actuarial & Reserving",      "actuarial_team@insurer.com", "bronze_actuarial", "silver_actuarial", "gold_actuarial", "sem_actuarial",    None, 1440,98.0, True),
    ("DOM007", "finance",      "Finance & GL",               "finance_team@insurer.com",   "bronze_finance",   "silver_finance",   "gold_finance",   "sem_finance",      None, 60,  99.5, True),
    ("DOM008", "regulatory",   "Regulatory & Compliance",    "compliance@insurer.com",     "bronze_regulatory","silver_regulatory","gold_regulatory","sem_compliance",   None, 1440,100.0,True),
    ("DOM009", "reinsurance",  "Reinsurance",                "reinsurance@insurer.com",    "bronze_reinsurance","silver_reinsurance","gold_reinsurance","sem_reinsurance",None, 60,  99.0, True),
    ("DOM010", "marketing",    "Marketing & Distribution",   "marketing@insurer.com",      "bronze_marketing", "silver_marketing", "gold_marketing", "sem_marketing",    None, 120, 95.0, True),
    ("DOM011", "integration",  "External Integrations",      "integration@insurer.com",    "bronze_external",  "silver_external",  "gold_external",  None,               None, 30,  99.0, True),
]

domain_schema = StructType([
    StructField("domain_id", StringType()), StructField("domain_name", StringType()),
    StructField("domain_description", StringType()), StructField("domain_owner", StringType()),
    StructField("bronze_lakehouse", StringType()), StructField("silver_lakehouse", StringType()),
    StructField("gold_lakehouse", StringType()), StructField("semantic_model", StringType()),
    StructField("workspace_id", StringType()), StructField("sla_freshness_minutes", IntegerType()),
    StructField("sla_quality_pct", DoubleType()), StructField("is_active", BooleanType()),
])

spark.createDataFrame(domain_data, domain_schema).write.mode("overwrite").saveAsTable(f"{METADATA_SCHEMA}.domain_registry")

# ============================================================================
# 2. SOURCE SYSTEM CONFIG SEED
# ============================================================================
source_data = [
    ("SRC001", "CoreAdmin",      "database",     "jdbc",       "kv-coreadmin-conn",     "coreadmin-db.database.windows.net", 1433, "InsuranceCore",   "managed_identity", None, None, None, None, None, None, None, None, 10, 3, 60, 300, True, "prod", "data_engineering@insurer.com"),
    ("SRC002", "ClaimsTPA",      "api",          "rest",       None,                     None, None, None,                    "service_principal","kv-tpa-secret", "https://claims-tpa.example.com/api/v2", "oauth2", None, None, None, None, None, 5, 3, 30, 120, True, "prod", "claims_team@insurer.com"),
    ("SRC003", "BillingSystem",  "database",     "jdbc",       "kv-billing-conn",        "billing-db.database.windows.net", 1433, "BillingDB",       "managed_identity", None, None, None, None, None, None, None, None, 5, 3, 60, 300, True, "prod", "billing_team@insurer.com"),
    ("SRC004", "PaymentGateway", "event_stream", "eventhub",   None,                     None, None, None,                    "managed_identity", None, None, None, None, None, "ins-payments-ns", "payment-events", "$Default", 1, 3, 10, 60, True, "prod", "payments_team@insurer.com"),
    ("SRC005", "DocumentStore",  "file",         "blob",       "kv-docstorage-conn",     None, None, None,                    "managed_identity", None, None, None, "pdf", "/documents/{domain}/{yyyy}/{MM}/{dd}/", None, None, None, 3, 3, 60, 600, True, "prod", "operations@insurer.com"),
    ("SRC006", "CRM",            "api",          "rest",       None,                     None, None, None,                    "service_principal","kv-crm-secret", "https://crm.example.com/api/v1", "oauth2", None, None, None, None, None, 5, 3, 30, 120, True, "prod", "marketing@insurer.com"),
    ("SRC007", "MedicalProvider","api",          "rest",       None,                     None, None, None,                    "service_principal","kv-medical-secret","https://medical-api.example.com/v1","api_key", None, None, None, None, None, 3, 3, 60, 180, True, "prod", "claims_team@insurer.com"),
    ("SRC008", "GLSystem",       "database",     "jdbc",       "kv-gl-conn",             "finance-db.database.windows.net", 1433,"FinanceGL",        "managed_identity", None, None, None, None, None, None, None, None, 5, 3, 60, 300, True, "prod", "finance_team@insurer.com"),
]

source_schema = StructType([
    StructField("source_system_id", StringType()), StructField("source_system_name", StringType()),
    StructField("source_type", StringType()), StructField("connection_type", StringType()),
    StructField("connection_string_kv", StringType()), StructField("host", StringType()),
    StructField("port", IntegerType()), StructField("database_name", StringType()),
    StructField("authentication_type", StringType()), StructField("credential_kv_secret", StringType()),
    StructField("api_base_url", StringType()), StructField("api_auth_type", StringType()),
    StructField("file_format", StringType()), StructField("file_path_pattern", StringType()),
    StructField("eventhub_namespace", StringType()), StructField("eventhub_name", StringType()),
    StructField("consumer_group", StringType()), StructField("max_concurrent_conn", IntegerType()),
    StructField("retry_count", IntegerType()), StructField("retry_delay_seconds", IntegerType()),
    StructField("timeout_seconds", IntegerType()), StructField("is_active", BooleanType()),
    StructField("environment", StringType()), StructField("owner", StringType()),
])

spark.createDataFrame(source_data, source_schema).write.mode("overwrite").saveAsTable(f"{METADATA_SCHEMA}.source_system_config")

# ============================================================================
# 3. INGESTION RULES SEED
# ============================================================================
ingestion_data = [
    # Policy domain
    ("ING001","SRC001","policy",          "dbo.Policy",               "bronze_policy","policy_raw",           "cdc",       "modified_date",None,"_operation","effective_date","daily",  "0 2 * * *",100000,None,None,None,True,",","UTF-8","evolve","policy_id","policy",   5,30, True),
    ("ING002","SRC001","product_catalog", "dbo.ProductCatalog",       "bronze_policy","product_catalog_raw",  "full",      None,None,None,None,                              "daily",  "0 1 * * *",50000, None,None,None,True,",","UTF-8","evolve","product_id","policy",  5,60, True),
    ("ING003","SRC001","policy_rider",    "dbo.PolicyRider",          "bronze_policy","policy_rider_raw",     "cdc",       "modified_date",None,"_operation","policy_id",     "daily",  "0 2 * * *",100000,None,None,None,True,",","UTF-8","evolve","rider_id","policy",   5,30, True),
    # Claims domain
    ("ING004","SRC002","claim",           "/claims/list",             "bronze_claims","claim_raw",            "incremental","last_updated",None,None,None,                     "hourly", "0 * * * *",50000, None,None,None,True,",","UTF-8","evolve","claim_id","claims",   3,15, True),
    ("ING005","SRC002","claim_document",  "/claims/{id}/documents",   "bronze_claims","claim_document_raw",   "incremental","uploaded_date",None,None,None,                    "hourly", "0 * * * *",10000, None,None,None,True,",","UTF-8","evolve","document_id","claims", 3,30, True),
    # Billing domain
    ("ING006","SRC003","premium_invoice", "dbo.PremiumInvoice",       "bronze_billing","premium_invoice_raw", "cdc",       "modified_date",None,"_operation","billing_period", "daily",  "0 3 * * *",200000,None,None,None,True,",","UTF-8","evolve","invoice_id","billing", 5,30, True),
    # Payment streaming
    ("ING007","SRC004","payment_event",   "payment-events",           "bronze_billing","payment_event_raw",   "streaming", None,None,None,None,                                "realtime",None,10000,None,None,None,True,",","UTF-8","evolve","transaction_id","billing",1,5, True),
    # Customer domain
    ("ING008","SRC001","customer",        "dbo.Customer",             "bronze_customer","customer_raw",       "cdc",       "modified_date",None,"_operation",None,              "daily",  "0 1 * * *",100000,None,None,None,True,",","UTF-8","evolve","customer_id","customer",5,30,True),
    ("ING009","SRC006","crm_contact",     "/contacts/updated",        "bronze_customer","crm_contact_raw",    "incremental","modified_date",None,None,None,                     "daily",  "0 4 * * *",50000, None,None,None,True,",","UTF-8","evolve","contact_id","customer",5,60,True),
    # Finance domain
    ("ING010","SRC008","journal_entry",   "dbo.JournalEntry",         "bronze_finance","journal_entry_raw",   "cdc",       "posting_date",None,"_operation","fiscal_period",   "daily",  "0 5 * * *",500000,None,None,None,True,",","UTF-8","evolve","entry_id","finance",  5,30, True),
    # Document ingestion
    ("ING011","SRC005","claim_pdf",       "/documents/claims/",       "bronze_claims","claim_pdf_raw",        "incremental","upload_date",None,None,None,                       "hourly", "0 * * * *",1000,  None,None,None,True,",","UTF-8","ignore",None,"claims",          3,30, True),
]

ingestion_schema = StructType([
    StructField("ingestion_rule_id", StringType()), StructField("source_system_id", StringType()),
    StructField("entity_name", StringType()), StructField("source_table_or_path", StringType()),
    StructField("target_lakehouse", StringType()), StructField("target_table", StringType()),
    StructField("ingestion_mode", StringType()), StructField("incremental_column", StringType()),
    StructField("incremental_value", StringType()), StructField("cdc_tracking_column", StringType()),
    StructField("partition_columns", StringType()), StructField("load_frequency", StringType()),
    StructField("schedule_cron", StringType()), StructField("batch_size", IntegerType()),
    StructField("source_query", StringType()), StructField("pre_hook_notebook", StringType()),
    StructField("post_hook_notebook", StringType()), StructField("file_header", BooleanType()),
    StructField("file_delimiter", StringType()), StructField("file_encoding", StringType()),
    StructField("schema_enforcement", StringType()), StructField("dedup_columns", StringType()),
    StructField("domain", StringType()), StructField("priority", IntegerType()),
    StructField("sla_minutes", IntegerType()), StructField("is_active", BooleanType()),
])

spark.createDataFrame(ingestion_data, ingestion_schema).write.mode("overwrite").saveAsTable(f"{METADATA_SCHEMA}.ingestion_rules")

# ============================================================================
# 4. ALERT RULES SEED
# ============================================================================
alert_data = [
    ("ALR001", "Pipeline Failure",         "pipeline",     None,       "event",     "status = 'failed'",                                     '{"max_retries": 3}',        "critical", '["email","teams"]', '["dataeng@insurer.com"]',       True,  "notebooks/remediation/retry_pipeline",  None, 5,  60, '["manager@insurer.com"]', True),
    ("ALR002", "DQ Below Threshold",       "data_quality", None,       "threshold", "pass_percentage < threshold_pct",                        '{"window_minutes": 60}',    "high",     '["email","teams"]', '["dq_team@insurer.com"]',       False, None, None, 30,  120, '["data_owner@insurer.com"]', True),
    ("ALR003", "Fraud Score High",         "fraud",        "claims",   "threshold", "fraud_score > 0.85",                                     '{"min_claim_amount": 5000}',"critical", '["email","teams","pagerduty"]','["siu@insurer.com"]', False, None, None, 0, 30, '["siu_manager@insurer.com"]', True),
    ("ALR004", "SLA Breach",               "sla",          None,       "threshold", "DATEDIFF(minute, last_run_timestamp, current_timestamp()) > sla_minutes", '{}', "high", '["email","teams"]', '["ops@insurer.com"]', False, None, None, 15, 60, '["ops_manager@insurer.com"]', True),
    ("ALR005", "Payment Anomaly",          "business",     "billing",  "anomaly",   "payment_amount > avg_amount * 3 OR payment_amount < 0",  '{"lookback_days": 90}',     "high",     '["email"]',         '["billing@insurer.com"]',       False, None, None, 30, 120, None, True),
    ("ALR006", "Missing Data",             "data_quality", None,       "schedule",  "record_count = 0 AND expected_records > 0",              '{}',                        "critical", '["email","teams"]', '["dataeng@insurer.com"]',       True,  "notebooks/remediation/check_source",    None, 15, 60, '["data_owner@insurer.com"]', True),
]

alert_schema = StructType([
    StructField("alert_rule_id", StringType()), StructField("alert_name", StringType()),
    StructField("alert_category", StringType()), StructField("domain", StringType()),
    StructField("trigger_type", StringType()), StructField("trigger_condition", StringType()),
    StructField("trigger_parameters", StringType()), StructField("severity", StringType()),
    StructField("notification_channels", StringType()), StructField("notification_recipients", StringType()),
    StructField("auto_remediation", BooleanType()), StructField("remediation_notebook", StringType()),
    StructField("remediation_parameters", StringType()), StructField("cooldown_minutes", IntegerType()),
    StructField("escalation_minutes", IntegerType()), StructField("escalation_recipients", StringType()),
    StructField("is_active", BooleanType()),
])

spark.createDataFrame(alert_data, alert_schema).write.mode("overwrite").saveAsTable(f"{METADATA_SCHEMA}.alert_rules")

# ============================================================================
# 5. ENVIRONMENT CONFIG SEED
# ============================================================================
env_data = [
    ("fabric_workspace_id",   "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx", "workspace",    "prod", "Production workspace ID",          False, True),
    ("fabric_workspace_id",   "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy", "workspace",    "dev",  "Development workspace ID",         False, True),
    ("key_vault_url",         "https://ins-keyvault-prod.vault.azure.net/", "security","prod", "Production Key Vault URL",         False, True),
    ("key_vault_url",         "https://ins-keyvault-dev.vault.azure.net/",  "security","dev",  "Dev Key Vault URL",                False, True),
    ("notification_email",    "platform-alerts@insurer.com",          "notification", "prod", "Primary alert email",              False, True),
    ("teams_webhook_url",     "kv-teams-webhook-url",                 "notification", "prod", "Teams webhook (KV reference)",     True,  True),
    ("purview_account",       "ins-purview-prod",                     "governance",   "prod", "Purview account name",             False, True),
    ("eventhub_namespace",    "ins-eventhub-prod",                    "streaming",    "prod", "EventHub namespace",               False, True),
    ("ai_services_endpoint",  "https://ins-ai-prod.cognitiveservices.azure.com/", "ai", "prod","Azure AI Services endpoint",     False, True),
    ("openai_deployment",     "gpt-4-turbo",                          "ai",          "prod", "Azure OpenAI deployment name",      False, True),
]

env_schema = StructType([
    StructField("config_key", StringType()), StructField("config_value", StringType()),
    StructField("config_category", StringType()), StructField("environment", StringType()),
    StructField("description", StringType()), StructField("is_secret", BooleanType()),
    StructField("is_active", BooleanType()),
])

spark.createDataFrame(env_data, env_schema).write.mode("overwrite").saveAsTable(f"{METADATA_SCHEMA}.environment_config")

print("✅ All seed data loaded successfully.")
