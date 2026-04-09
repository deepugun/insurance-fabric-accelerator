# 🏗️ Insurance Fabric Accelerator — Detailed Architecture

## 1. End-to-End Architecture

### 1.1 Design Principles

| Principle | Implementation |
|-----------|---------------|
| **Metadata-Driven** | All pipelines, rules, and mappings driven by config tables—zero hardcoded logic |
| **Lakehouse-First** | OneLake as the single storage layer; Delta Lake format throughout |
| **Spark-First** | All processing via PySpark (batch + structured streaming) |
| **API-First** | Every deployment action available via Fabric REST API |
| **Domain-Oriented** | Logical separation per insurance domain with shared framework |
| **AI-Native** | AI/ML integrated at every processing stage |
| **Observable** | Central telemetry, health scoring, and alerting |

### 1.2 Layer Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│ PRESENTATION LAYER                                                │
│ ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐     │
│ │ Central    │ │ Domain     │ │ Executive  │ │ Agent      │     │
│ │ Cockpit    │ │ Dashboards │ │ Scorecards │ │ Interfaces │     │
│ └────────────┘ └────────────┘ └────────────┘ └────────────┘     │
│ Power BI (Direct Lake) │ RLS/CLS │ Embedded Analytics            │
├──────────────────────────────────────────────────────────────────┤
│ INTELLIGENCE LAYER                                                │
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐              │
│ │ Fabric IQ    │ │ AI/ML        │ │ Activator    │              │
│ │              │ │              │ │              │              │
│ │ • Ontology   │ │ • Fraud ML   │ │ • Alerts     │              │
│ │ • Graph      │ │ • Risk Score │ │ • Triggers   │              │
│ │ • Data Agent │ │ • Doc AI     │ │ • Workflows  │              │
│ │ • Ops Agent  │ │ • LLM        │ │ • Escalation │              │
│ │ • Cust Agent │ │ • NLP        │ │              │              │
│ └──────────────┘ └──────────────┘ └──────────────┘              │
├──────────────────────────────────────────────────────────────────┤
│ SEMANTIC LAYER                                                    │
│ ┌──────────────────────────────────────────────────────────────┐ │
│ │ Direct Lake Semantic Models                                   │ │
│ │ Policy │ Claims │ Finance │ Customer360 │ Actuarial │ Ops   │ │
│ └──────────────────────────────────────────────────────────────┘ │
├──────────────────────────────────────────────────────────────────┤
│ DATA LAYER (Medallion Architecture in OneLake)                    │
│                                                                    │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐                       │
│  │ BRONZE  │───▶│ SILVER  │───▶│  GOLD   │                       │
│  │ (Raw)   │    │(Clean)  │    │(Business)│                      │
│  │         │    │         │    │         │                        │
│  │ Append  │    │ SCD2    │    │ Star    │                        │
│  │ Full    │    │ Dedup   │    │ Schema  │                        │
│  │ History │    │ Conform │    │ Aggreg  │                        │
│  └─────────┘    └─────────┘    └─────────┘                       │
│                                                                    │
│  ┌─────────────────────────────────────────────────────┐          │
│  │ STREAMING TABLES (Real-Time Intelligence)            │          │
│  │ Claims Events │ Payment Events │ Fraud Scores       │          │
│  └─────────────────────────────────────────────────────┘          │
├──────────────────────────────────────────────────────────────────┤
│ INGESTION LAYER (Fabric Data Factory)                             │
│ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐         │
│ │ Batch  │ │  CDC   │ │  API   │ │Stream  │ │  Doc   │         │
│ │ Files  │ │ Change │ │ REST   │ │EventHub│ │ Upload │         │
│ └────────┘ └────────┘ └────────┘ └────────┘ └────────┘         │
│ ──── ALL DRIVEN BY metadata.source_system_config ────           │
├──────────────────────────────────────────────────────────────────┤
│ GOVERNANCE & SECURITY                                             │
│ Entra ID │ Managed Identity │ RLS │ CLS │ Purview │ Audit │ DR  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. Fabric Component Mapping Per Domain

### 2.1 Policy Administration

| Component | Fabric Feature | Details |
|-----------|---------------|---------|
| Policy data ingestion | Data Factory (batch + CDC) | Daily extracts from core admin; CDC for real-time endorsements |
| Raw storage | Bronze Lakehouse | `bronze.policy_raw`, `bronze.product_catalog_raw` |
| Cleansing | Spark Notebooks | SCD Type 2 for policy history |
| Business models | Gold Lakehouse | `dim_policy`, `dim_product`, `fact_policy_transaction`, `bridge_policy_rider` |
| Semantic model | Direct Lake | Policy analytics model with RLS by region/LOB |
| Underwriting AI | ML Model (MLflow) | Risk scoring model integrated at ingestion |
| Real-time | Event Stream | Policy status change events |
| IQ | Ontology entity | Policy entity with relationships to Customer, Claim, Premium |

### 2.2 Customer / Party (MDM)

| Component | Fabric Feature | Details |
|-----------|---------------|---------|
| Customer ingestion | Data Factory (multi-source) | CRM, policy system, claims, agent portal |
| Identity resolution | Spark (fuzzy matching) | Probabilistic matching on name, DOB, SSN, address |
| Address standardization | Spark UDF + external API | USPS/postal service integration |
| Master record | Gold Lakehouse | `dim_customer_master`, `dim_address`, `dim_party_role` |
| Customer 360 | Direct Lake | Unified customer view across all touchpoints |
| Graph | Fabric IQ Graph | Customer → Policy → Claim → Agent relationships |

### 2.3 Claims Management

| Component | Fabric Feature | Details |
|-----------|---------------|---------|
| FNOL intake | Event Stream + Data Factory | Real-time claim submission via API/events |
| Document processing | AI (Document Intelligence) | Extract data from claim forms, medical records, photos |
| Claim storage | Bronze → Silver → Gold | Full claim lifecycle tracking |
| Adjudication rules | Metadata tables + Spark | Configurable business rules engine |
| Fraud detection | ML Model (Spark ML / MLflow) | Real-time scoring on claim features |
| Settlement | Gold Lakehouse | `fact_claim_payment`, `fact_claim_reserve` |
| Streaming | Real-Time Intelligence | Live claim status, KQL-based analytics |
| Activator | Alert triggers | High-value claim alerts, fraud score thresholds |

### 2.4 Billing & Payments

| Component | Fabric Feature | Details |
|-----------|---------------|---------|
| Premium calculation | Spark batch | Rate application from product catalog |
| Invoice generation | Spark + metadata rules | Billing frequency, grace periods |
| Payment ingestion | Event Stream | Real-time payment gateway events |
| Reconciliation | Spark batch | Match payments to invoices |
| Collections | Spark + Activator | Overdue alerts, dunning workflows |
| Models | Gold Lakehouse | `fact_premium_invoice`, `fact_payment`, `fact_reconciliation` |

### 2.5 Disbursements

| Component | Fabric Feature | Details |
|-----------|---------------|---------|
| Claims payouts | Spark batch | Calculate net payable after deductibles/reinsurance |
| Commission calc | Spark + metadata rules | Configurable commission schedules |
| Payment file | Spark output | Generate bank payment files (ACH, wire) |
| Models | Gold Lakehouse | `fact_disbursement`, `fact_commission` |

### 2.6 Actuarial / Reserving

| Component | Fabric Feature | Details |
|-----------|---------------|---------|
| Experience studies | Spark batch | Mortality, morbidity, lapse analysis |
| Reserve calculations | Spark + ML | IBNR, case reserves, bulk reserves |
| Assumption tables | Metadata config | Mortality tables, lapse curves |
| Projections | Spark + ML | Liability cash flow projections |
| Models | Gold Lakehouse | `fact_reserve`, `dim_assumption`, `fact_projection` |

### 2.7 Finance

| Component | Fabric Feature | Details |
|-----------|---------------|---------|
| GL integration | Data Factory (batch) | Journal entries from sub-ledgers |
| IFRS17 | Spark batch | CSM calculation, risk adjustment, loss component |
| Revenue recognition | Spark + metadata rules | Premium earning patterns |
| Reporting | Direct Lake + Power BI | Financial statements, regulatory reports |
| Models | Gold Lakehouse | `fact_journal_entry`, `fact_ifrs17_csm`, `dim_chart_of_accounts` |

### 2.8 Regulatory & Compliance

| Component | Fabric Feature | Details |
|-----------|---------------|---------|
| Audit trail | Spark + Delta Lake | Immutable audit log with full change history |
| Data retention | Spark + metadata rules | Configurable retention policies per data class |
| Solvency reporting | Spark batch | Capital adequacy calculations |
| Regulatory snapshots | Gold Lakehouse | Point-in-time regulatory data freezes |
| Lineage | Purview integration | End-to-end data lineage tracking |

### 2.9 Reinsurance

| Component | Fabric Feature | Details |
|-----------|---------------|---------|
| Treaty ingestion | Data Factory | Treaty terms, layers, participation |
| Cession calculation | Spark batch | Apply treaty terms to gross claims/premiums |
| Bordereaux | Spark output | Generate reinsurer reporting |
| Models | Gold Lakehouse | `dim_treaty`, `fact_cession`, `fact_recovery` |

### 2.10 Marketing & Distribution

| Component | Fabric Feature | Details |
|-----------|---------------|---------|
| Campaign data | Data Factory | CRM, marketing platform integration |
| Lead scoring | ML Model | Propensity-to-buy models |
| Agent performance | Spark batch | Production, retention, quality metrics |
| Models | Gold Lakehouse | `fact_campaign`, `fact_lead`, `fact_agent_performance` |

### 2.11 External Integrations

| Component | Fabric Feature | Details |
|-----------|---------------|---------|
| TPA feeds | Data Factory (API + batch) | Claims TPA data exchange |
| Payment gateways | Event Stream | Real-time payment notifications |
| Medical providers | Data Factory (API) | Pre-authorization, medical records |
| Government/DMV | Data Factory (batch) | Regulatory filings, identity verification |

---

## 3. Cross-Cutting Concerns

### 3.1 Data Quality

- Executed at every layer transition (Bronze→Silver→Gold)
- Rules stored in `metadata.data_quality_rules`
- Results tracked in `metadata.dq_execution_log`
- Scores aggregated to Central Cockpit

### 3.2 Security

- Entra ID for all authentication
- Managed Identity for service-to-service
- RLS defined per semantic model via security roles
- Column masking for PII fields (SSN, DOB, medical data)
- Encryption at rest (OneLake default) + in transit (TLS 1.2+)

### 3.3 Observability

- Pipeline execution logs → `metadata.pipeline_execution_log`
- Data quality scores → `metadata.dq_execution_log`
- Agent activity → `metadata.agent_activity_log`
- All aggregated in Central Cockpit dashboard

### 3.4 HA/DR

- OneLake geo-redundant storage (GZRS)
- All pipelines are idempotent and replayable
- Delta Lake checkpointing for streaming
- Metadata-driven recovery (re-run from last checkpoint)
- Cross-region workspace replication strategy
