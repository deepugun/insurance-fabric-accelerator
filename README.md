# 🏢 Insurance Data & AI Accelerator for Microsoft Fabric

## Marketplace-Ready | Enterprise-Grade | Fully Automated

A **production-grade, metadata-driven Insurance Data & AI Accelerator** built entirely on Microsoft Fabric, covering all insurance business processes (Life, Health, P&C) with unified data, AI agents, and real-time intelligence.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CENTRAL COCKPIT (Power BI Dashboard)                 │
│  Pipeline Status │ DQ Scores │ SLA │ Alerts │ Agent Activity │ Health  │
└─────────────────────────────────────────────────────────────────────────┘
         │                    │                    │
┌────────┴────────┐ ┌────────┴────────┐ ┌────────┴────────┐
│  FABRIC IQ      │ │  AI/ML LAYER     │ │  ACTIVATOR      │
│  ─────────      │ │  ──────────      │ │  ─────────      │
│  Ontology       │ │  Doc Processing  │ │  Alert Rules    │
│  Graph Model    │ │  Fraud Detection │ │  Triggers       │
│  Data Agents    │ │  Risk Scoring    │ │  Workflows      │
│  Ops Agents     │ │  LLM Summarize   │ │  Notifications  │
│  Customer Agent │ │  Claims AI       │ │  Auto-Remediate │
└─────────────────┘ └─────────────────┘ └─────────────────┘
         │                    │                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                     SEMANTIC LAYER (Direct Lake)                        │
│  Policy Model │ Claims Model │ Finance Model │ Customer 360 │ RLS/CLS │
└─────────────────────────────────────────────────────────────────────────┘
         │                    │                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER (Business Models)                    │
│  dim_customer │ dim_policy │ fact_claims │ fact_premiums │ fact_gl     │
│  fact_fraud_scores │ dim_agent │ fact_reserves │ fact_reinsurance      │
└─────────────────────────────────────────────────────────────────────────┘
         │                    │                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                       SILVER LAYER (Standardized)                       │
│  Cleansed │ Deduplicated │ Conformed │ SCD Type 2 │ Quality Checked   │
└─────────────────────────────────────────────────────────────────────────┘
         │                    │                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER (Raw)                              │
│  Policy Systems │ Claims │ Billing │ External │ Documents │ Streaming │
└─────────────────────────────────────────────────────────────────────────┘
         │                    │                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATA INGESTION (Fabric Data Factory)                  │
│  Batch (CSV/Parquet) │ CDC (SQL/Oracle) │ API │ Streaming (EventHub)  │
│                 ALL METADATA-DRIVEN (No Hardcoded Logic)                │
└─────────────────────────────────────────────────────────────────────────┘
         │                    │                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                      SOURCE SYSTEMS                                     │
│  Core Admin │ Claims TPA │ Billing │ CRM │ Medical │ Payment GW │ Gov │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
insurance-fabric-accelerator/
├── README.md
├── architecture/
│   ├── ARCHITECTURE.md              # Detailed architecture document
│   ├── domain_mapping.md            # Fabric capability per domain
│   └── security_model.md            # Security & compliance design
├── metadata/
│   ├── schemas/                     # All metadata table DDLs
│   └── seed_data/                   # Initial configuration data
├── src/
│   ├── framework/                   # Core reusable framework
│   │   ├── ingestion/               # Metadata-driven ingestion
│   │   ├── transformations/         # Medallion transformations
│   │   ├── streaming/               # Real-time pipelines
│   │   ├── data_quality/            # DQ framework
│   │   └── security/               # RLS/CLS enforcement
│   ├── domains/                     # Domain-specific logic
│   │   ├── policy/
│   │   ├── claims/
│   │   ├── billing/
│   │   ├── customer/
│   │   ├── finance/
│   │   ├── actuarial/
│   │   ├── reinsurance/
│   │   └── marketing/
│   ├── ai/                          # AI/ML pipelines
│   │   ├── document_processing/
│   │   ├── fraud_detection/
│   │   ├── risk_scoring/
│   │   └── llm_summarization/
│   └── agents/                      # Fabric IQ Agents
│       ├── data_agent/
│       ├── operations_agent/
│       └── customer_agent/
├── ontology/                        # Fabric IQ ontology & graph
├── demo/                            # Synthetic data generators
├── dashboards/                      # Power BI / Cockpit
├── automation/                      # CI/CD & Fabric APIs
│   ├── fabric_api/
│   ├── github_actions/
│   └── deployment/
├── alerting/                        # Activator configs
├── tests/                           # Test framework
└── docs/                            # Additional documentation
```

---

## 🚀 Quick Start

### Option A: Fabric Mirroring (Real-Time Data) — **RECOMMENDED**

**Run Notebook 15 in Fabric** — 100% Python, no PowerShell required!

1. **Upload Notebook** → Import `notebooks/15_fabric_mirroring_setup.ipynb` to Fabric
2. **Configure** → Update source system details in Section 1
3. **Run All** → Click "Run All" (takes ~5 minutes)
4. **Wait for Snapshot** → Initial data load (10-30 minutes)
5. **Transform** → Run Notebook 30 (medallion transformations)
6. **Dashboard** → View real-time KPIs in Notebook 90

📖 See **[FABRIC_MIRRORING_PYTHON.md](FABRIC_MIRRORING_PYTHON.md)** for detailed guide

**Benefits:**
- ✅ **< 30 second latency** from source to Fabric
- ✅ **No batch ETL** — automatic CDC replication
- ✅ **Pure Python** — runs 100% in Fabric notebooks
- ✅ **Zero maintenance** — schema sync automatic
- ✅ **4 source systems** — Policy, Claims, Finance, CRM

---

### Option B: Demo Data (Synthetic Data)

1. **Upload Notebooks** → Import all notebooks from `notebooks/` folder
2. **Create Lakehouses** → `insurance_bronze`, `insurance_silver`, `insurance_gold`, `insurance_metadata`
3. **Run Notebook 01** → Generate demo data (10K customers, 20K policies, 5K claims)
4. **Run Notebook 30** → Execute medallion transformations
5. **Run Notebook 90** → View dashboard

---

### Option C: REST API Import (Bulk Upload)

Use the Python script for batch import:

```bash
python import_notebooks_to_fabric.py --workspace-id YOUR-GUID
```

See **[IMPORT_VIA_API.md](IMPORT_VIA_API.md)** for details

---

## 📋 Insurance Domains Covered

| Domain | Status | Key Features |
|--------|--------|-------------|
| Policy Administration | ✅ | Issuance, UW, endorsements, renewals, product catalog |
| Customer/Party MDM | ✅ | Identity resolution, address standardization, 360° view |
| Claims Management | ✅ | FNOL, intake, adjudication, settlement, AI-assisted |
| Billing & Payments | ✅ | Premium invoicing, collections, reconciliation |
| Disbursements | ✅ | Claims payouts, agent commissions |
| Actuarial/Reserving | ✅ | Risk models, mortality/lapse, liability projections |
| Finance | ✅ | GL, IFRS17/GAAP, revenue recognition |
| Regulatory/Compliance | ✅ | Audit trails, data retention, solvency reporting |
| Reinsurance | ✅ | Treaty management, risk sharing |
| Marketing/Distribution | ✅ | Campaigns, lead conversion, agent performance |
| External Integrations | ✅ | **SFTP data exchange**, TPAs, payment gateways, medical providers |

---

## 🏗️ Infrastructure Provisioning (NEW!) — Notebook 65

**Enterprise Infrastructure-as-Code for automated deployment** — Complete Terraform, Bicep, and CLI templates

**Generate production-ready IaC for:**
- ✅ **Terraform** — Enterprise-grade with remote state
- ✅ **Bicep** — Azure-native declarative deployment
- ✅ **Fabric CLI** — Python/Bash deployment scripts

**What gets deployed:**
- Fabric capacity (F2/F4/F64)
- Workspace with RBAC (Admin, Contributor, Viewer)
- 4 Lakehouses (Bronze, Silver, Gold, Metadata)
- Warehouse (SQL analytics)
- KQL Database (Real-time)
- Git integration (auto-sync)
- 17 production notebooks

**Multi-environment:**
- Dev → F2 capacity
- Test → F4 capacity
- Prod → F64 capacity

See **[INFRASTRUCTURE_PROVISIONING_GUIDE.md](INFRASTRUCTURE_PROVISIONING_GUIDE.md)** for deployment instructions

---

## 🔄 External Data Exchange (NEW!)

**Enterprise-grade SFTP integration for secure data sharing with external vendors** — Notebook 55

Common use cases:
- 📤 **Send claims to TPAs** — Daily referrals for adjudication
- 📤 **Premium to reinsurers** — Monthly treaty reports
- 📤 **Regulatory filings** — Solvency II, NAIC reports
- 📤 **Commissions to brokers** — Weekly agent statements
- 📤 **Payments to processors** — Daily approved disbursements
- 📥 **Receive vendor data** — Medical networks, credit scores

**Key Features:**
- ✅ Secure SFTP push/pull
- ✅ PGP encryption (HIPAA/PCI-DSS compliant)
- ✅ Multi-format export (CSV, JSON, XML, fixed-width)
- ✅ Automated scheduling (daily/weekly/monthly)
- ✅ Full audit logging and monitoring

See **[Notebook 55](notebooks/55_external_data_exchange_sftp.ipynb)** for details

---

## 🔐 Security & Compliance

### Real-Time Access Monitoring (NEW!) — Notebook 56

**Comprehensive visibility and control across all access layers:**

- 🖥️ **Compute Level**: Notebook executions, Spark jobs, pipeline runs
- 📊 **Data Plane**: Table/file reads/writes, PII access tracking
- ⚙️ **Control Plane**: Workspace changes, RBAC modifications

**Key Features:**
- ✅ Real-time anomaly detection (< 100ms overhead)
- ✅ Auto-flag suspicious patterns (unusual time, excessive data, privilege escalation)
- ✅ PII mass export detection (HIPAA compliance)
- ✅ Compliance reporting (SOC 2, HIPAA, audit trails)
- ✅ Integration with operational monitoring (Notebook 45)
- ✅ Automatic alerts via Fabric Activator

**Detected Anomalies:**
- 🚨 Unusual time access (2 AM from 9-5 user)
- 🚨 Excessive data access (> 100 GB/hour)
- 🚨 Privilege escalation attempts
- 🚨 PII mass export (> 10K rows)

See **[Notebook 56](notebooks/56_access_monitoring_control.ipynb)** for implementation

---

## 📚 Business Glossary & Governance (NEW!) — Notebook 67

**Self-contained enterprise metadata management with automated policy enforcement** — No Purview required!

**Business Glossary Features:**
- ✅ **Term Management** — Create, approve, version, deprecate business terms
- ✅ **Self-Contained** — All metadata in Fabric Delta tables
- ✅ **Category Hierarchies** — Organize by domain/category
- ✅ **Physical Linkage** — Map terms to actual columns
- ✅ **Auto-Linking** — Match terms to columns automatically
- ✅ **Search & Discovery** — Full-text search across all terms
- ✅ **Approval Workflows** — Draft → Review → Approved → Published
- ✅ **Stewardship** — Assign owners and stewards

**Policy Engine Features:**
- 🛡️ **DDL Comment Validation** — Flag tables/columns without comments
- 🛡️ **Business Definition Checks** — Enforce glossary linkage
- 🛡️ **Naming Convention Rules** — Enforce standards
- 🛡️ **Automated Scanning** — Run on every dataset (scheduled)
- 🛡️ **Violation Tracking** — Full audit trail with remediation suggestions
- 🛡️ **Compliance Scorecard** — Per-table metadata quality scores
- 🛡️ **Auto-Remediation** — Suggest and apply fixes

**Use Cases:**
- 📖 **Insurance Terms** — Premium, deductible, loss ratio, FNOL, etc.
- 🔍 **Data Discovery** — Find what columns mean across databases
- 📊 **Compliance** — Ensure all data has business definitions
- 🚨 **Governance Enforcement** — No undocumented data in production
- 📈 **Quality Metrics** — Track metadata compliance over time

**Example:**
```python
# Create business term
glossary = BusinessGlossaryManager()
term_id = glossary.create_term(
    term_name="premium_amount",
    definition="Amount paid for insurance coverage",
    category="Financial",
    domain="Policy"
)

# Link to physical column
glossary.link_term_to_column(
    term_id=term_id,
    database_name="gold",
    table_name="fact_premiums",
    column_name="premium_amt"
)

# Run policy scan
policy_engine = GovernancePolicyEngine()
results = policy_engine.run_all_policies()
```

See **[BUSINESS_GLOSSARY_GUIDE.md](BUSINESS_GLOSSARY_GUIDE.md)** for complete documentation

---

### Security Foundation

- **Entra ID** + Managed Identity authentication
- **Row-Level Security** across all semantic models
- **Column Masking** for PII (SSN, DOB, Medical)
- **Full Audit Trail** with immutable logging
- **IFRS17 / GAAP** compliant financial datasets
- **Data Lineage** end-to-end via Fabric Purview integration

---

## 📜 License

Enterprise License - Microsoft Fabric Marketplace Accelerator
