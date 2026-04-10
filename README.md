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
| External Integrations | ✅ | TPAs, payment gateways, medical providers |

---

## 🔐 Security & Compliance

- **Entra ID** + Managed Identity authentication
- **Row-Level Security** across all semantic models
- **Column Masking** for PII (SSN, DOB, Medical)
- **Full Audit Trail** with immutable logging
- **IFRS17 / GAAP** compliant financial datasets
- **Data Lineage** end-to-end via Fabric Purview integration

---

## 📜 License

Enterprise License - Microsoft Fabric Marketplace Accelerator
