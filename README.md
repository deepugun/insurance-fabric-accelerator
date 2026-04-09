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

1. **Configure** → Update `metadata/seed_data/` with your environment settings
2. **Deploy** → Run `automation/deployment/deploy_all.py` or trigger GitHub Actions
3. **Load Demo Data** → Execute `demo/generate_all.py`
4. **Monitor** → Open the Central Cockpit dashboard

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
