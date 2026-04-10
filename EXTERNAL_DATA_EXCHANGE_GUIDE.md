# 📤 External Data Exchange — SFTP Quick Start

**Secure, enterprise-grade data sharing with external vendors via SFTP**

## Overview

Notebook 55 enables secure, auditable SFTP data exchange with external parties — a critical enterprise pattern for insurance companies sharing data with:

- **Third-Party Administrators (TPAs)** → Claims data
- **Reinsurance Companies** → Risk exposure, premium ceding
- **Regulatory Bodies** → Compliance filings, solvency reports
- **Payment Processors** → Disbursements, collections
- **Medical Providers** → Claims adjudication
- **External Auditors** → Financial data
- **Distribution Partners** → Commission statements

---

## 🚀 Quick Setup (3 Steps)

### Step 1: Store Credentials in Key Vault

For each vendor, store SFTP connection details:

```bash
# Example: TPA Vendor

# SFTP Connection
az keyvault secret set --vault-name insurance-kv \
  --name acme-tpa-sftp-host --value "sftp.acmetpa.com"

az keyvault secret set --vault-name insurance-kv \
  --name acme-tpa-sftp-port --value "22"

az keyvault secret set --vault-name insurance-kv \
  --name acme-tpa-sftp-username --value "insurance_client"

# Option A: Password authentication
az keyvault secret set --vault-name insurance-kv \
  --name acme-tpa-sftp-password --value "YourSecurePassword123!"

# Option B: SSH private key (more secure)
az keyvault secret set --vault-name insurance-kv \
  --name acme-tpa-sftp-private-key --value @~/.ssh/acme_tpa_rsa

# PGP public key ID (for encryption)
az keyvault secret set --vault-name insurance-kv \
  --name acme-tpa-pgp-public-key-id --value "security@acmetpa.com"
```

### Step 2: Add Required Packages to Fabric Environment

In your Fabric workspace:
1. Go to **Settings** → **Environment**
2. Add these packages:
   - `paramiko` (SFTP client)
   - `python-gnupg` (PGP encryption)

### Step 3: Upload Notebook 55 to Fabric

1. Open your Fabric workspace
2. Click **New** → **Import notebook**
3. Select `notebooks/55_external_data_exchange_sftp.ipynb`
4. Run the notebook!

---

## 📋 Common Use Cases

### Use Case 1: Send Daily Claims to TPA

```python
from ExternalDataExchange import ExternalDataExchange

exchange = ExternalDataExchange()

result = exchange.send_to_vendor(
    vendor_name="acme_tpa",
    source_table="gold.fact_claims",
    filter_condition="status = 'REFERRED' AND referral_date >= CURRENT_DATE - INTERVAL 1 DAY",
    file_format="csv",
    encrypt=True
)

print(f"✅ Sent {result['row_count']:,} claims to TPA")
```

### Use Case 2: Send Monthly Premium to Reinsurer

```python
result = exchange.send_to_vendor(
    vendor_name="global_reinsurance",
    source_table="gold.fact_premiums",
    filter_condition="treaty_id IS NOT NULL AND premium_date >= CURRENT_DATE - INTERVAL 1 MONTH",
    file_format="csv",
    encrypt=True
)
```

### Use Case 3: Submit Solvency Report to Regulator

```python
result = exchange.send_to_vendor(
    vendor_name="state_insurance_dept",
    source_table="gold.regulatory_solvency_report",
    filter_condition="report_date = LAST_DAY(CURRENT_DATE - INTERVAL 1 MONTH)",
    file_format="xml",
    encrypt=True
)
```

### Use Case 4: Receive Data from Vendor

```python
result = exchange.receive_from_vendor(
    vendor_name="credit_bureau",
    remote_file="/outgoing/credit_scores_20260410.csv.pgp",
    target_table="bronze.external_credit_scores",
    file_format="csv",
    encrypted=True
)

print(f"✅ Loaded {result['row_count']:,} credit scores")
```

---

## ⏰ Automated Scheduling

### Define Exchange Patterns (Metadata-Driven)

All exchanges are configured in Delta table `metadata.external_exchange_patterns`:

```python
# Pattern examples already configured in Notebook 55:
# - P001: Claims to TPA (daily)
# - P002: Premium to Reinsurer (monthly)
# - P003: Solvency Report to Regulator (monthly)
# - P004: Commissions to Distribution Partner (weekly)
# - P005: Claims Payment to Processor (daily)
```

### Execute Scheduled Exchanges

```python
# Run all daily exchanges
from notebook_55 import run_scheduled_exchanges

run_scheduled_exchanges("daily")

# Run all weekly exchanges
run_scheduled_exchanges("weekly")

# Run all monthly exchanges
run_scheduled_exchanges("monthly")
```

### Integration with Fabric Data Pipeline

Create a Fabric Data Pipeline to run scheduled exchanges:

```json
{
  "name": "Daily Vendor Exchanges",
  "activities": [
    {
      "type": "SynapseNotebook",
      "notebook": "55_external_data_exchange_sftp",
      "parameters": {
        "schedule_type": "daily"
      }
    }
  ],
  "trigger": {
    "type": "Schedule",
    "recurrence": {
      "frequency": "Day",
      "interval": 1,
      "startTime": "2026-01-01T06:00:00Z"
    }
  }
}
```

---

## 🔒 Security Features

### PGP Encryption

All sensitive files are encrypted before transmission:

```python
# Automatic PGP encryption when encrypt=True
result = exchange.send_to_vendor(
    vendor_name="tpa_vendor",
    source_table="gold.fact_claims",
    encrypt=True  # ← Encrypts with vendor's public key
)

# File is encrypted as: claims_20260410.csv.pgp
```

**Compliance:**
- ✅ HIPAA-compliant (PHI protected)
- ✅ PCI-DSS compliant (payment data)
- ✅ SOC 2 Type II (audit trail)

### Key Vault Integration

**Zero hardcoded credentials** — all secrets stored in Azure Key Vault:
- SFTP passwords/SSH keys
- PGP passphrases
- Vendor API keys

### Audit Logging

Every transfer logged to `metadata.external_data_exchange_log`:

```python
# View recent exchanges
spark.sql("""
    SELECT 
        exchange_id,
        vendor_name,
        direction,
        row_count,
        transfer_start,
        success
    FROM metadata.external_data_exchange_log
    ORDER BY transfer_start DESC
    LIMIT 10
""").show()
```

---

## 📊 Monitoring Dashboard

Notebook 55 (Section 9) provides built-in monitoring:

### Exchange Volume Trends
```sql
SELECT 
    vendor_name,
    COUNT(*) as transfer_count,
    SUM(row_count) as total_rows,
    AVG(duration_seconds) as avg_duration
FROM metadata.external_data_exchange_log
WHERE transfer_start >= CURRENT_DATE - INTERVAL 30 DAY
GROUP BY vendor_name
```

### Recent Failures
```sql
SELECT * 
FROM metadata.external_data_exchange_log
WHERE success = false
  AND transfer_start >= CURRENT_DATE - INTERVAL 7 DAY
```

### SLA Compliance
```sql
SELECT 
    vendor_name,
    AVG(duration_seconds) as avg_duration,
    CASE 
        WHEN AVG(duration_seconds) < 60 THEN '✅ Excellent'
        WHEN AVG(duration_seconds) < 300 THEN '⚠️ Acceptable'
        ELSE '❌ Slow'
    END as sla_status
FROM metadata.external_data_exchange_log
GROUP BY vendor_name
```

---

## 🔧 Supported File Formats

### CSV (Most Common)
```python
export_engine.export_to_csv(
    df=claims_df,
    output_path="claims.csv",
    delimiter=",",
    header=True,
    quote_all=False
)
```

### JSON Lines
```python
export_engine.export_to_json(
    df=claims_df,
    output_path="claims.jsonl"
)
```

### XML (Regulatory Filings)
```python
export_engine.export_to_xml(
    df=solvency_df,
    output_path="solvency_report.xml",
    root_tag="SolvencyReport"
)
```

### Fixed-Width (Legacy Systems)
```python
field_widths = {
    "policy_number": 15,
    "customer_name": 30,
    "premium_amount": 12
}

export_engine.export_to_fixed_width(
    df=policies_df,
    output_path="policies.txt",
    field_widths=field_widths
)
```

---

## 🎯 Integration with Other Notebooks

### Notebook 30 (Medallion Transformations)
Source data from Gold layer for vendor exchange:
```python
# Get latest data from Gold layer
claims_df = spark.table("gold.fact_claims") \
    .filter("referral_date >= CURRENT_DATE - INTERVAL 1 DAY")

# Send to TPA
exchange.send_to_vendor(...)
```

### Notebook 45 (Operational Monitoring)
Add SFTP health checks:
```python
def check_vendor_exchanges():
    """Monitor SFTP exchange health."""
    failures = spark.table("metadata.external_data_exchange_log") \
        .filter("success = false AND transfer_start >= CURRENT_DATE - INTERVAL 1 DAY")
    
    if failures.count() > 0:
        send_alert("🚨 SFTP transfer failures detected!")
```

### Notebook 70 (CI/CD)
Include in deployment automation:
```python
# Deploy exchange patterns
patterns_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("metadata.external_exchange_patterns")
```

### Notebook 90 (Dashboard)
Add vendor exchange metrics:
- Daily exchange volume
- Vendor SLA compliance
- Transfer success rate
- Data volume by vendor

---

## 📖 Vendor Onboarding Template

When adding a new vendor:

**1. Gather Information:**
- [ ] SFTP hostname
- [ ] SFTP port (default: 22)
- [ ] Authentication method (password or SSH key)
- [ ] Credentials
- [ ] PGP public key (if encryption required)
- [ ] File format requirements (CSV, JSON, XML, fixed-width)
- [ ] Transfer schedule (daily, weekly, monthly)
- [ ] Remote directory paths

**2. Configure Key Vault:**
```bash
VENDOR="new_vendor"

az keyvault secret set --vault-name insurance-kv \
  --name ${VENDOR}-sftp-host --value "sftp.vendor.com"

az keyvault secret set --vault-name insurance-kv \
  --name ${VENDOR}-sftp-username --value "username"

az keyvault secret set --vault-name insurance-kv \
  --name ${VENDOR}-sftp-password --value "password"
```

**3. Test Connection:**
```python
# In Notebook 55
from SecureSFTPClient import SecureSFTPClient

client = SecureSFTPClient("new_vendor")
client.connect()
client.list_files("/")
client.disconnect()
```

**4. Define Exchange Pattern:**
```python
new_pattern = {
    "pattern_id": "P006",
    "name": "Description of Exchange",
    "vendor": "new_vendor",
    "source_table": "gold.fact_xyz",
    "filter": "date >= CURRENT_DATE - INTERVAL 1 DAY",
    "format": "csv",
    "encrypt": True,
    "schedule": "daily"
}

# Add to patterns table
spark.createDataFrame([new_pattern]) \
    .write.format("delta").mode("append") \
    .saveAsTable("metadata.external_exchange_patterns")
```

**5. Test Transfer:**
```python
result = exchange.send_to_vendor(
    vendor_name="new_vendor",
    source_table="gold.fact_xyz",
    filter_condition="date = CURRENT_DATE",
    file_format="csv",
    encrypt=True
)

# Verify on vendor SFTP server
```

---

## 🚨 Troubleshooting

### Connection Timeout
**Error:** `Connection timeout after 30 seconds`

**Fix:**
- Check firewall rules (outbound port 22)
- Verify SFTP hostname resolves
- Confirm network connectivity from Fabric

### Authentication Failed
**Error:** `Authentication failed`

**Fix:**
- Verify credentials in Key Vault
- Test username/password manually
- Check SSH key format (OpenSSH vs RSA)

### PGP Encryption Failed
**Error:** `Encryption failed: No public key`

**Fix:**
- Import vendor's public key:
  ```bash
  gpg --import vendor_public.key
  gpg --list-keys
  ```
- Store correct key ID in Key Vault

### File Not Found on Remote Server
**Error:** `No such file or directory: /incoming/`

**Fix:**
- Verify remote directory exists
- Check SFTP user permissions
- Use absolute paths (e.g., `/home/user/incoming/`)

---

## 📈 Performance Tuning

### Large Files (> 1 GB)

**Problem:** Slow transfers, timeouts

**Solution:**
```python
# 1. Partition data into smaller files
df.repartition(10).write.format("csv").save("path")

# 2. Use compression
df.write.format("csv").option("compression", "gzip").save("path")

# 3. Increase timeout
client = SecureSFTPClient("vendor")
client.client.get_transport().set_keepalive(60)  # 60 second keepalive
```

### High Volume Exchanges

**Problem:** Slow export from Delta tables

**Solution:**
```python
# Use coalesce to optimize partitions
df.coalesce(1).write.format("csv").save("path")

# Cache frequently accessed data
df.cache()
```

---

## 📚 Additional Resources

- [Notebook 55](notebooks/55_external_data_exchange_sftp.ipynb) — Full implementation
- [ARCHITECTURE.md](ARCHITECTURE.md) — Architectural context
- [Notebook 30](notebooks/30_medallion_transformations.ipynb) — Source data from Gold layer
- [Notebook 45](notebooks/45_operational_monitoring.ipynb) — Add monitoring
- [Notebook 70](notebooks/70_cicd_deployment_automation.ipynb) — Schedule deployments

---

## 🎯 Summary

**Notebook 55 provides:**

✅ **Secure SFTP** — Production-grade file transfers  
✅ **PGP Encryption** — HIPAA/PCI-DSS compliant  
✅ **Multi-format Export** — CSV, JSON, XML, fixed-width  
✅ **Audit Logging** — Complete traceability  
✅ **Scheduled Execution** — Daily/weekly/monthly automation  
✅ **Monitoring Dashboard** — SLA tracking, failure alerts  
✅ **Metadata-driven** — All configurations in Delta tables  
✅ **Key Vault Integration** — Zero hardcoded credentials  

**Enterprise patterns supported:**
- Claims to TPAs
- Premium to reinsurers
- Regulatory filings
- Commission statements
- Payment processing
- Vendor data ingestion

**Ready for production use!** 🚀
