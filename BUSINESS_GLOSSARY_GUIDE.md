# 📚 Business Glossary & Governance Policy Engine Guide

> **Self-contained enterprise metadata management and automated policy enforcement for Microsoft Fabric**

## Overview

This guide covers **Notebook 67: Business Glossary & Policy Engine**, a comprehensive solution for managing business terminology and enforcing metadata governance standards without external dependencies (no Purview required).

## Features

### 🎯 Core Capabilities

1. **Business Glossary Management**
   - Create, update, approve, and deprecate business terms
   - Hierarchical organization (categories, domains)
   - Version control for term definitions
   - Full-text search capabilities
   - Stewardship and ownership assignments
   - Approval workflows

2. **Term-to-Data Linkage**
   - Auto-link glossary terms to physical columns
   - Confidence scoring (manual = 1.0, auto-suggested < 1.0)
   - Verification workflows
   - Lineage tracking

3. **Automated Policy Engine**
   - DDL comment validation (tables and columns)
   - Business definition requirements
   - Naming convention enforcement
   - Scheduled policy scans
   - Violation tracking and remediation

4. **Compliance Reporting**
   - Per-table compliance scorecards
   - Violation dashboards by severity
   - Coverage metrics
   - Executive reporting

---

## Architecture

### Data Model

```
metadata.glossary_terms
├── term_id (PK)
├── term_name (unique)
├── definition
├── business_definition
├── technical_definition
├── category
├── domain
├── status (draft → pending_review → approved → published → deprecated)
├── version
└── steward, owner, tags, synonyms

metadata.glossary_term_linkages
├── linkage_id (PK)
├── term_id (FK)
├── database_name, table_name, column_name
├── full_path
├── confidence_score (0.0-1.0)
├── linkage_type (manual, auto_suggested, auto_approved)
└── verified (boolean)

metadata.governance_policies
├── policy_id (PK)
├── policy_name
├── policy_type (ddl_comment, business_definition, naming_convention)
├── severity (critical, high, medium, low)
├── enabled (boolean)
└── validation_query

metadata.governance_policy_violations
├── violation_id (PK)
├── policy_id (FK)
├── database_name, table_name, column_name
├── full_path
├── violation_details
├── status (new, acknowledged, remediated, waived)
└── remediation_suggestion

metadata.metadata_compliance_scorecard
├── scorecard_id (PK)
├── database_name, table_name
├── total_columns
├── columns_with_comments
├── columns_with_glossary_terms
├── compliance_score (0-100)
└── violations by severity
```

### Process Flow

```
┌─────────────────────────────────────────────────────────────┐
│  1. TERM CREATION                                           │
│     Create term → Review → Approve → Publish               │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  2. AUTO-LINKING                                            │
│     Scan databases → Match column names → Create linkages  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  3. POLICY ENFORCEMENT                                      │
│     Run policies → Detect violations → Generate reports    │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  4. REMEDIATION                                             │
│     Review violations → Apply fixes → Re-scan              │
└─────────────────────────────────────────────────────────────┘
```

---

## Quick Start (5 Minutes)

### Step 1: Run Notebook Setup

```python
# Open Notebook 67
# Execute Cell 1: Load utilities
# Execute Cell 2: Create all glossary and policy tables
```

### Step 2: Create Your First Term

```python
glossary = BusinessGlossaryManager()

term_id = glossary.create_term(
    term_name="annual_premium",
    definition="Total annual insurance premium amount",
    business_definition="Yearly payment for insurance coverage",
    category="Financial",
    domain="Policy",
    tags=["finance", "policy"]
)

# Approve it
glossary.approve_term(term_id)
```

### Step 3: Run Policy Scan

```python
policy_engine = GovernancePolicyEngine()

# Scan for violations
results = policy_engine.run_all_policies()

# Generate compliance scorecard
scorecard = policy_engine.generate_compliance_scorecard()
display(scorecard)
```

**Done!** You now have:
- ✅ Business glossary with searchable terms
- ✅ Automated policy enforcement
- ✅ Compliance reports

---

## Detailed Usage

### Business Glossary Management

#### Creating Terms with Full Metadata

```python
glossary = BusinessGlossaryManager()

term_id = glossary.create_term(
    term_name="combined_ratio",
    definition="Insurance profitability metric: (Claims + Expenses) / Premiums",
    business_definition="Key indicator of underwriting profitability. Below 100% = profit, Above 100% = loss",
    technical_definition="DECIMAL(5,2) calculated as: (incurred_losses + underwriting_expenses) / earned_premiums * 100",
    category="Metric",
    domain="Finance",
    steward="finance-team@company.com",
    tags=["kpi", "profitability", "underwriting"]
)

# Add examples
spark.sql(f"""
    UPDATE metadata.glossary_terms
    SET examples = 'Example: Combined Ratio = 98.5% means profitable underwriting'
    WHERE term_id = '{term_id}'
""")

# Add synonyms
spark.sql(f"""
    UPDATE metadata.glossary_terms
    SET synonyms = array('underwriting ratio', 'loss and expense ratio')
    WHERE term_id = '{term_id}'
""")

# Add related terms
spark.sql(f"""
    UPDATE metadata.glossary_terms
    SET related_terms = array('loss_ratio', 'expense_ratio', 'earned_premium')
    WHERE term_id = '{term_id}'
""")
```

#### Term Approval Workflow

```python
# 1. Create as draft
term_id = glossary.create_term(
    term_name="new_term",
    definition="...",
    # ... other fields
)
# Status: draft

# 2. Submit for review
spark.sql(f"""
    UPDATE metadata.glossary_terms
    SET status = 'pending_review'
    WHERE term_id = '{term_id}'
""")

# 3. Approve
glossary.approve_term(term_id, approver="data-governance@company.com")
# Status: published

# 4. Later: Deprecate if needed
spark.sql(f"""
    UPDATE metadata.glossary_terms
    SET status = 'deprecated'
    WHERE term_id = '{term_id}'
""")
```

#### Searching the Glossary

```python
# Search by keyword
results = glossary.search_terms("premium")
display(results)

# Search by tag
results = spark.sql("""
    SELECT term_name, definition, domain
    FROM metadata.glossary_terms
    WHERE status = 'published'
      AND array_contains(tags, 'kpi')
    ORDER BY term_name
""")
display(results)

# Search by domain
results = spark.sql("""
    SELECT term_name, definition, category
    FROM metadata.glossary_terms
    WHERE status = 'published'
      AND domain = 'Claims'
    ORDER BY category, term_name
""")
display(results)
```

### Term Linkage

#### Manual Linking

```python
# Link term to specific column
glossary.link_term_to_column(
    term_id="TERM_20260410120000_1234",
    database_name="gold",
    table_name="fact_premiums",
    column_name="annual_premium_amount",
    confidence=1.0,
    linkage_type="manual"
)
```

#### Bulk Linking

```python
# Link a term to multiple columns
term_id = "TERM_20260410120000_1234"

columns_to_link = [
    ("gold", "fact_premiums", "premium_amount"),
    ("silver", "stg_policies", "annual_premium"),
    ("bronze", "src_policies", "total_premium")
]

for db, table, column in columns_to_link:
    glossary.link_term_to_column(
        term_id=term_id,
        database_name=db,
        table_name=table,
        column_name=column,
        confidence=1.0,
        linkage_type="manual"
    )
```

#### Reviewing Auto-Suggested Linkages

```python
# Get all auto-suggested linkages (need verification)
pending_linkages = spark.sql("""
    SELECT 
        l.linkage_id,
        l.term_name,
        l.full_path,
        l.confidence_score,
        t.definition
    FROM metadata.glossary_term_linkages l
    JOIN metadata.glossary_terms t ON l.term_id = t.term_id
    WHERE l.linkage_type = 'auto_suggested'
      AND l.verified = false
    ORDER BY l.confidence_score DESC
""")

display(pending_linkages)

# Verify good linkages
spark.sql("""
    UPDATE metadata.glossary_term_linkages
    SET verified = true,
        verified_by = 'governance-team@company.com',
        verified_timestamp = CURRENT_TIMESTAMP,
        linkage_type = 'auto_approved'
    WHERE linkage_id = 'LINK_...'
""")

# Remove bad linkages
spark.sql("""
    DELETE FROM metadata.glossary_term_linkages
    WHERE linkage_id = 'LINK_...'
""")
```

#### Term Lineage

```python
# Get all physical columns linked to a term
lineage = glossary.get_term_lineage("TERM_20260410120000_1234")
display(lineage)

# Get reverse lineage: which terms are mapped to a column?
column_terms = spark.sql("""
    SELECT 
        l.full_path,
        l.term_name,
        t.definition,
        t.business_definition,
        l.confidence_score
    FROM metadata.glossary_term_linkages l
    JOIN metadata.glossary_terms t ON l.term_id = t.term_id
    WHERE l.database_name = 'gold'
      AND l.table_name = 'fact_premiums'
      AND l.verified = true
    ORDER BY l.column_name
""")
display(column_terms)
```

### Policy Engine

#### Creating Custom Policies

```python
policy_engine = GovernancePolicyEngine()

# Example: Naming convention policy
policy_id = policy_engine.create_policy(
    policy_name="Table Naming Convention",
    policy_type="naming_convention",
    description="All tables must follow naming pattern: {layer}_{domain}_{object}",
    severity="medium",
    enabled=True,
    scope="all"
)

# Example: PII column detection
policy_id = policy_engine.create_policy(
    policy_name="PII Column Documentation",
    policy_type="business_definition",
    description="All PII columns must have glossary terms linked",
    severity="critical",
    enabled=True,
    scope="all"
)
```

#### Running Specific Policies

```python
# Scan DDL comments for one database
violations = policy_engine.scan_ddl_comments("gold")
print(f"Found {violations} DDL comment violations")

# Scan business definitions
violations = policy_engine.scan_business_definitions("silver")
print(f"Found {violations} unmapped columns")

# Run all policies
results = policy_engine.run_all_policies()
```

#### Custom Validation Query

```python
# Create policy with custom SQL validation
spark.sql("""
    INSERT INTO metadata.governance_policies VALUES (
        'POLICY_CUSTOM_001',
        'Fact Table Primary Keys',
        'data_quality',
        'All fact tables must have primary key defined',
        'high',
        true,
        false,
        'table_pattern',
        'fact_%',
        NULL,
        'governance@company.com',
        CURRENT_TIMESTAMP,
        NULL
    )
""")

# Execute custom validation
validation_results = spark.sql("""
    SELECT 
        'gold' as database_name,
        tableName as table_name,
        'Missing Primary Key' as violation_details
    FROM (SHOW TABLES IN gold)
    WHERE tableName LIKE 'fact_%'
      AND tableName NOT IN (
          SELECT DISTINCT table_name 
          FROM information_schema.table_constraints
          WHERE constraint_type = 'PRIMARY KEY'
      )
""")
```

### Compliance Reporting

#### Generate Scorecard

```python
# Full compliance scorecard
scorecard = policy_engine.generate_compliance_scorecard()
display(scorecard)

# Filter: Tables below 50% compliance
low_compliance = scorecard.filter(col("compliance_score") < 50) \
                          .orderBy("compliance_score")
display(low_compliance)

# Filter: Critical violations
critical_tables = scorecard.filter(col("violations_critical") > 0) \
                           .orderBy(desc("violations_critical"))
display(critical_tables)
```

#### Executive Dashboard Queries

```python
# Overall compliance summary
exec_summary = spark.sql("""
    SELECT 
        COUNT(DISTINCT database_name) as databases_scanned,
        COUNT(*) as total_tables,
        SUM(total_columns) as total_columns,
        SUM(columns_with_comments) as columns_documented,
        SUM(columns_with_glossary_terms) as columns_with_glossary,
        ROUND(AVG(compliance_score), 2) as avg_compliance_score,
        SUM(violations_critical) as critical_violations,
        SUM(violations_high) as high_violations
    FROM metadata.metadata_compliance_scorecard
""")
display(exec_summary)

# Compliance trend over time
trend = spark.sql("""
    SELECT 
        DATE(last_scanned_timestamp) as scan_date,
        COUNT(*) as tables_scanned,
        ROUND(AVG(compliance_score), 2) as avg_compliance_score,
        SUM(violations_critical + violations_high) as total_violations
    FROM metadata.metadata_compliance_scorecard
    GROUP BY DATE(last_scanned_timestamp)
    ORDER BY scan_date DESC
    LIMIT 30
""")
display(trend)

# Top violators
top_violators = spark.sql("""
    SELECT 
        database_name,
        table_name,
        compliance_score,
        violations_critical,
        violations_high,
        violations_medium,
        total_columns,
        columns_with_comments,
        columns_with_glossary_terms
    FROM metadata.metadata_compliance_scorecard
    WHERE compliance_score < 70
    ORDER BY 
        violations_critical DESC,
        violations_high DESC,
        compliance_score ASC
    LIMIT 20
""")
display(top_violators)
```

#### Violation Remediation Tracking

```python
# Get open violations with suggested fixes
open_violations = spark.sql("""
    SELECT 
        violation_id,
        severity,
        database_name,
        table_name,
        column_name,
        violation_details,
        remediation_suggestion,
        detected_timestamp
    FROM metadata.governance_policy_violations
    WHERE status = 'new'
    ORDER BY 
        CASE severity
            WHEN 'critical' THEN 1
            WHEN 'high' THEN 2
            WHEN 'medium' THEN 3
            WHEN 'low' THEN 4
        END,
        detected_timestamp DESC
""")
display(open_violations)

# Mark violation as remediated
spark.sql("""
    UPDATE metadata.governance_policy_violations
    SET status = 'remediated',
        remediated_by = 'dev-team@company.com',
        remediated_timestamp = CURRENT_TIMESTAMP
    WHERE violation_id = 'VIO_...'
""")

# Waive violation with reason
spark.sql("""
    UPDATE metadata.governance_policy_violations
    SET status = 'waived',
        waiver_reason = 'System-generated column, no business definition needed',
        waived_by = 'data-architect@company.com'
    WHERE violation_id = 'VIO_...'
""")
```

---

## Advanced Use Cases

### 1. Domain-Specific Glossaries

```python
# Create insurance-specific categories
categories = [
    ("CAT_UNDERWRITING", "Underwriting", None, "Policy"),
    ("CAT_CLAIMS", "Claims Processing", None, "Claims"),
    ("CAT_ACTUARIAL", "Actuarial Metrics", None, "Finance"),
    ("CAT_CUSTOMER", "Customer Attributes", None, "Customer")
]

for cat_id, cat_name, parent, domain in categories:
    spark.sql(f"""
        INSERT INTO metadata.glossary_categories VALUES (
            '{cat_id}',
            '{cat_name}',
            {f"'{parent}'" if parent else 'NULL'},
            'Category for {cat_name} domain',
            '{domain}',
            'system',
            CURRENT_TIMESTAMP
        )
    """)

# Create terms in categories
glossary.create_term(
    term_name="incurred_but_not_reported",
    definition="Claims incurred but not yet reported to insurer",
    category="Actuarial",
    domain="Claims",
    tags=["reserves", "actuarial", "ibnr"]
)
```

### 2. Automated Term Suggestion (ML-Based)

```python
# Use column names + data patterns to suggest terms
from pyspark.ml.feature import Word2Vec
from pyspark.sql.functions import explode, split

# Build vocabulary from column names
all_columns = spark.sql("""
    SELECT DISTINCT 
        database_name,
        table_name,
        col_name as column_name
    FROM (
        SELECT database_name, tableName as table_name
        FROM (SELECT 'gold' as database_name UNION SELECT 'silver' UNION SELECT 'bronze')
        CROSS JOIN (SHOW TABLES IN gold)
    )
    JOIN LATERAL (DESCRIBE {database_name}.{table_name})
    WHERE col_name NOT LIKE '\\_%'
""")

# Tokenize column names
tokens = all_columns.withColumn(
    "tokens",
    split(lower(col("column_name")), "_")
)

# Find similar columns
similar_columns = tokens.filter(
    array_contains(col("tokens"), "premium")
).select("database_name", "table_name", "column_name")

display(similar_columns)

# Suggest glossary term for all matching columns
for row in similar_columns.collect():
    print(f"Suggest linking 'premium_amount' term to {row.database_name}.{row.table_name}.{row.column_name}")
```

### 3. Glossary Import from External Source

```python
# Import terms from CSV
import csv

def import_terms_from_csv(csv_path: str):
    """Import business glossary from external CSV."""
    
    glossary = BusinessGlossaryManager()
    
    df = spark.read.format("csv") \
        .option("header", "true") \
        .load(csv_path)
    
    for row in df.collect():
        try:
            term_id = glossary.create_term(
                term_name=row["term_name"],
                definition=row["definition"],
                business_definition=row["business_definition"],
                category=row["category"],
                domain=row["domain"],
                tags=row["tags"].split("|") if row["tags"] else []
            )
            
            # Auto-approve imported terms
            glossary.approve_term(term_id)
            
            print(f"✅ Imported: {row['term_name']}")
        
        except Exception as e:
            print(f"❌ Failed to import {row['term_name']}: {e}")

# Usage
import_terms_from_csv("Files/glossary_import.csv")
```

### 4. Integration with Data Lineage

```python
# Link glossary terms to data lineage
lineage_with_glossary = spark.sql("""
    SELECT 
        l.source_table,
        l.target_table,
        l.transformation_logic,
        g.term_name,
        g.definition
    FROM metadata.data_lineage l
    LEFT JOIN metadata.glossary_term_linkages gl 
        ON l.target_table = gl.database_name || '.' || gl.table_name
    LEFT JOIN metadata.glossary_terms g
        ON gl.term_id = g.term_id
    WHERE g.status = 'published'
""")

display(lineage_with_glossary)
```

### 5. Policy Exceptions Management

```python
# Create policy exception table
spark.sql("""
    CREATE TABLE IF NOT EXISTS metadata.governance_policy_exceptions (
        exception_id STRING,
        policy_id STRING,
        database_name STRING,
        table_name STRING,
        column_name STRING,
        exception_reason STRING,
        approved_by STRING,
        approved_timestamp TIMESTAMP,
        expiration_date DATE
    ) USING DELTA
""")

# Grant exception
spark.sql("""
    INSERT INTO metadata.governance_policy_exceptions VALUES (
        'EXC_001',
        'POLICY_DDL_COMMENT',
        'bronze',
        'raw_claim_events',
        NULL,
        'External vendor table - no control over DDL',
        'data-governance@company.com',
        CURRENT_TIMESTAMP,
        DATE_ADD(CURRENT_DATE, 90)
    )
""")

# Modify policy scan to respect exceptions
def scan_with_exceptions(database_name: str):
    exceptions = spark.sql(f"""
        SELECT full_path 
        FROM (
            SELECT database_name || '.' || table_name || 
                   CASE WHEN column_name IS NOT NULL THEN '.' || column_name ELSE '' END as full_path
            FROM metadata.governance_policy_exceptions
            WHERE expiration_date >= CURRENT_DATE
        )
    """).collect()
    
    exception_paths = {row["full_path"] for row in exceptions}
    
    # Run scan and filter out exceptions
    # ... implementation
```

---

## Integration with Existing Notebooks

### Notebook 40 (Data Quality Framework)

Add metadata quality checks:

```python
# Add to DQ suite
def check_metadata_compliance():
    """Ensure tables meet metadata standards."""
    
    low_compliance = spark.sql("""
        SELECT 
            database_name,
            table_name,
            compliance_score
        FROM metadata.metadata_compliance_scorecard
        WHERE compliance_score < 60
    """)
    
    if low_compliance.count() > 0:
        send_alert(
            level="warning",
            message=f"{low_compliance.count()} tables below 60% metadata compliance",
            details=low_compliance.toPandas().to_dict('records')
        )
```

### Notebook 45 (Operational Monitoring)

Add governance health checks:

```python
# Add to health dashboard
def governance_health_check():
    """Monitor governance policy compliance."""
    
    metrics = spark.sql("""
        SELECT 
            COUNT(*) as critical_violations,
            AVG(compliance_score) as avg_compliance
        FROM metadata.governance_policy_violations v
        JOIN metadata.metadata_compliance_scorecard s
        WHERE v.severity = 'critical' 
          AND v.status = 'new'
    """).collect()[0]
    
    return {
        "governance_health": {
            "critical_violations": metrics["critical_violations"],
            "avg_compliance_score": round(metrics["avg_compliance"], 2),
            "status": "healthy" if metrics["critical_violations"] == 0 else "degraded"
        }
    }
```

### Notebook 90 (Dashboard)

Add glossary metrics:

```python
# Governance tile
governance_metrics = spark.sql("""
    SELECT 
        COUNT(DISTINCT term_id) as total_terms,
        COUNT(DISTINCT CASE WHEN status = 'published' THEN term_id END) as published_terms,
        COUNT(DISTINCT l.linkage_id) as total_linkages,
        COUNT(DISTINCT s.table_name) as tables_scanned,
        ROUND(AVG(s.compliance_score), 1) as avg_compliance
    FROM metadata.glossary_terms t
    LEFT JOIN metadata.glossary_term_linkages l ON t.term_id = l.term_id
    CROSS JOIN metadata.metadata_compliance_scorecard s
""")

display(governance_metrics)
```

---

## Scheduled Execution

### Daily Governance Scan

Create Fabric Data Pipeline:

```json
{
  "name": "Daily Governance Policy Scan",
  "properties": {
    "activities": [
      {
        "name": "Run Policy Engine",
        "type": "SynapseNotebook",
        "linkedServiceName": "WorkspaceDefaultStorage",
        "typeProperties": {
          "notebook": {
            "referenceName": "67_business_glossary_policy_engine",
            "type": "NotebookReference"
          },
          "parameters": {
            "scan_type": "full",
            "databases": "bronze,silver,gold"
          }
        }
      },
      {
        "name": "Generate Reports",
        "type": "SynapseNotebook",
        "dependsOn": [
          {
            "activity": "Run Policy Engine",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "notebook": {
            "referenceName": "67_business_glossary_policy_engine",
            "type": "NotebookReference"
          },
          "parameters": {
            "section": "compliance_reports"
          }
        }
      },
      {
        "name": "Send Alerts",
        "type": "If",
        "dependsOn": [
          {
            "activity": "Generate Reports",
            "dependencyConditions": ["Succeeded"]
          }
        ],
        "typeProperties": {
          "expression": {
            "@greater(activity('Run Policy Engine').output.critical_violations, 0)"
          },
          "ifTrueActivities": [
            {
              "name": "Send Email",
              "type": "WebActivity",
              "typeProperties": {
                "url": "https://prod-XX.eastus.logic.azure.com:443/...",
                "method": "POST",
                "body": {
                  "subject": "Critical Governance Violations Detected",
                  "violations": "@activity('Run Policy Engine').output.critical_violations"
                }
              }
            }
          ]
        }
      }
    ],
    "triggers": [
      {
        "name": "DailyGovScanTrigger",
        "type": "ScheduleTrigger",
        "typeProperties": {
          "recurrence": {
            "frequency": "Day",
            "interval": 1,
            "startTime": "2026-01-01T02:00:00Z",
            "timeZone": "UTC"
          }
        }
      }
    ]
  }
}
```

### Incremental Scans (Real-Time)

```python
# Scan only recently modified tables
recent_tables = spark.sql("""
    SELECT DISTINCT database_name, table_name
    FROM (
        DESCRIBE HISTORY bronze.*
        UNION ALL
        DESCRIBE HISTORY silver.*
        UNION ALL
        DESCRIBE HISTORY gold.*
    )
    WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL 1 HOUR
""")

for row in recent_tables.collect():
    policy_engine.scan_ddl_comments(row.database_name)
```

---

## Best Practices

### 1. Term Management

✅ **DO:**
- Use consistent naming conventions (snake_case, lowercase)
- Provide both business and technical definitions
- Assign clear ownership and stewardship
- Use tags generously for discoverability
- Link to source system or regulatory references
- Version definitions when making changes

❌ **DON'T:**
- Create duplicate terms with different names
- Use vague or circular definitions
- Leave terms in "draft" status indefinitely
- Orphan terms (no linkages to physical data)

### 2. Policy Configuration

✅ **DO:**
- Start with high-severity policies (DDL comments, PII definitions)
- Set realistic compliance targets (80%+ for gold layer)
- Review auto-suggested linkages before approval
- Document policy exceptions with expiration dates
- Schedule daily scans during off-hours
- Integrate with CI/CD pipelines

❌ **DON'T:**
- Enable all policies at once (phased approach)
- Set overly strict policies that create noise
- Auto-remediate without testing
- Ignore waived violations indefinitely

### 3. Compliance Tracking

✅ **DO:**
- Track compliance trends over time
- Celebrate improvements (gamification)
- Assign violation remediation to teams
- Include metadata quality in Definition of Done
- Report metrics to leadership
- Automate compliance reporting

❌ **DON'T:**
- Treat violations as purely punitive
- Focus on quantity over quality of definitions
- Ignore systemic issues (e.g., external vendor tables)
- Let violations pile up without action

---

## Troubleshooting

### Common Issues

#### Issue: Auto-linking creates too many false positives

**Solution:**
```python
# Adjust confidence threshold
linkages_to_review = spark.sql("""
    SELECT * 
    FROM metadata.glossary_term_linkages
    WHERE linkage_type = 'auto_suggested'
      AND confidence_score < 0.8
""")

# Delete low-confidence linkages
spark.sql("""
    DELETE FROM metadata.glossary_term_linkages
    WHERE linkage_type = 'auto_suggested'
      AND confidence_score < 0.6
""")
```

#### Issue: Policy scans timing out

**Solution:**
```python
# Scan databases in parallel
from concurrent.futures import ThreadPoolExecutor

databases = ["bronze", "silver", "gold"]

with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [
        executor.submit(policy_engine.scan_ddl_comments, db)
        for db in databases
    ]
    results = [f.result() for f in futures]
```

#### Issue: Duplicate violations

**Solution:**
```python
# De-duplicate before inserting
spark.sql("""
    DELETE FROM metadata.governance_policy_violations
    WHERE violation_id NOT IN (
        SELECT MAX(violation_id)
        FROM metadata.governance_policy_violations
        GROUP BY policy_id, full_path
    )
""")
```

#### Issue: Scorecard not updating

**Solution:**
```python
# Force full regeneration
spark.sql("TRUNCATE TABLE metadata.metadata_compliance_scorecard")
scorecard = policy_engine.generate_compliance_scorecard()
```

---

## Performance Optimization

### 1. Partitioning Strategy

```python
# Violations table is partitioned by date + severity
# Optimize queries with partition pruning
recent_critical = spark.sql("""
    SELECT *
    FROM metadata.governance_policy_violations
    WHERE DATE(detected_timestamp) >= CURRENT_DATE - 7
      AND severity = 'critical'
""")
# Uses partition pruning for fast retrieval
```

### 2. Z-Order Optimization

```python
# Optimize frequent query patterns
spark.sql("""
    OPTIMIZE metadata.glossary_term_linkages
    ZORDER BY (database_name, table_name, term_id)
""")

spark.sql("""
    OPTIMIZE metadata.governance_policy_violations
    ZORDER BY (status, severity, database_name)
""")
```

### 3. Vacuum Old Data

```python
# Clean up old violation history (>90 days, remediated)
spark.sql("""
    DELETE FROM metadata.governance_policy_violations
    WHERE status IN ('remediated', 'waived')
      AND detected_timestamp < CURRENT_TIMESTAMP - INTERVAL 90 DAYS
""")

spark.sql("""
    VACUUM metadata.governance_policy_violations RETAIN 7 DAYS
""")
```

---

## Migration from Purview

If migrating from Azure Purview:

### 1. Export Purview Glossary

Use Azure CLI or PowerShell:

```bash
# Export glossary to JSON
az purview glossary export \
    --account-name myaccount \
    --output-file glossary_export.json
```

### 2. Import to Fabric

```python
import json

with open("glossary_export.json", "r") as f:
    purview_terms = json.load(f)

glossary = BusinessGlossaryManager()

for term in purview_terms["terms"]:
    term_id = glossary.create_term(
        term_name=term["name"],
        definition=term["longDescription"],
        business_definition=term["shortDescription"],
        category=term["categories"][0] if term.get("categories") else "General",
        domain=term.get("domain", "General"),
        tags=term.get("labels", [])
    )
    
    glossary.approve_term(term_id)
    print(f"Imported: {term['name']}")
```

### 3. Verify Migration

```python
# Compare counts
fabric_count = spark.sql("SELECT COUNT(*) FROM metadata.glossary_terms").collect()[0][0]
purview_count = len(purview_terms["terms"])

print(f"Purview terms: {purview_count}")
print(f"Fabric terms: {fabric_count}")
print(f"Migration complete: {fabric_count == purview_count}")
```

---

## Summary

**Notebook 67 provides enterprise-grade metadata governance:**

✅ **Self-contained business glossary** (no Purview)  
✅ **Automated policy enforcement** (DDL comments, business definitions)  
✅ **Compliance scorecards** (per-table metrics)  
✅ **Violation tracking** (with remediation suggestions)  
✅ **Search and discovery** (full-text, tags, domains)  
✅ **Term lineage** (physical mappings)  
✅ **Approval workflows** (draft → review → published)  
✅ **Integration ready** (DQ, monitoring, dashboards)  

**Own your metadata, enforce your standards!** 📚🛡️
