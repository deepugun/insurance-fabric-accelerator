# 🐍 Fabric Mirroring Setup — Pure Python 

**Run everything from Fabric notebooks — No PowerShell needed!**

## Quick Start (3 Steps)

### 1. Upload Notebook to Fabric
```
1. Open your Fabric workspace
2. Click "New" → "Import notebook"
3. Select: notebooks/15_fabric_mirroring_setup.ipynb
4. Upload completed!
```

### 2. Configure Prerequisites

**Enable CDC on Source Databases:**
```sql
-- On each source database (Policy, Claims, Finance, CRM)
EXEC sys.sp_cdc_enable_db;

-- Enable CDC on each table
EXEC sys.sp_cdc_enable_table 
    @source_schema = 'dbo', 
    @source_name = 'policies',
    @role_name = NULL;
```

**Create Database User:**
```sql
CREATE LOGIN mirroring_user WITH PASSWORD = 'YourSecurePassword123!';
CREATE USER mirroring_user FOR LOGIN mirroring_user;

-- Grant SELECT on all tables
EXEC sp_addrolemember 'db_datareader', 'mirroring_user';

-- Grant VIEW CHANGE TRACKING
GRANT VIEW CHANGE TRACKING TO mirroring_user;
```

**Store Credentials in Key Vault:**
```bash
# Create Key Vault (if needed)
az keyvault create \
  --name insurance-kv \
  --resource-group insurance-rg \
  --location eastus

# Store credentials
az keyvault secret set \
  --vault-name insurance-kv \
  --name mirroring-user-name \
  --value "mirroring_user"

az keyvault secret set \
  --vault-name insurance-kv \
  --name mirroring-user-password \
  --value "YourSecurePassword123!"

# Grant Fabric workspace Managed Identity access
az keyvault set-policy \
  --name insurance-kv \
  --object-id <WORKSPACE_MANAGED_IDENTITY_ID> \
  --secret-permissions get list
```

### 3. Run the Notebook

Open **Notebook 15** in Fabric and click **"Run All"**

That's it! ✅

---

## What Gets Created

### Mirrored Databases (4)
1. **PolicySystem_Mirror** — 6 tables from Policy Administration
2. **ClaimsSystem_Mirror** — 6 tables from Claims Management  
3. **FinanceSystem_Mirror** — 5 tables from Finance System
4. **CRM_Mirror** — 5 tables from CRM

### Lakehouse Shortcuts
- Created in `insurance_bronze` lakehouse
- Direct access to mirrored data
- Example: `mirrored_policies_policy_system`

### Monitoring Tables
- `metadata.mirroring_setup_log` — Setup results
- `metadata.mirroring_shortcuts_log` — Shortcut tracking
- `metadata.mirroring_health_log` — Health monitoring

---

## Configuration

Edit **Section 1** in the notebook to customize:

```python
MIRRORING_CONFIG = {
    "policy_system": {
        "mirror_name": "PolicySystem_Mirror",
        "source_server": "your-server.database.windows.net",  # 👈 CHANGE
        "source_database": "PolicyAdministration",            # 👈 CHANGE
        "tables": [
            "dbo.policies",        # 👈 ADD YOUR TABLES
            "dbo.coverages",
            # ...
        ]
    },
    # Add more systems...
}
```

**Supported Source Types:**
- `AzureSQLDatabase` — Azure SQL Database
- `SQLServer` — On-premises SQL Server
- `CosmosDB` — Cosmos DB Core SQL API
- `Snowflake` — Snowflake Data Warehouse

---

## How to Use Mirrored Data

### Method 1: Direct SQL Query
```python
# Query the mirrored database directly
df = spark.sql("""
    SELECT *
    FROM PolicySystem_Mirror.dbo.policies
    WHERE effective_date >= CURRENT_DATE
""")
```

### Method 2: Lakehouse Shortcut
```python
# Use the shortcut in Bronze lakehouse
df = spark.table("insurance_bronze.mirrored_policies_policy_system")
```

### Method 3: Integration with Medallion Architecture
```python
# In Notebook 30 (medallion_transformations)
# Bronze Layer - use mirrored data
df_bronze = spark.table("PolicySystem_Mirror.dbo.policies") \
    .withColumn("_source_system", lit("PolicySystem")) \
    .withColumn("_ingestion_time", current_timestamp())

# Save to Bronze (Delta format)
df_bronze.write.format("delta") \
    .mode("append") \
    .saveAsTable("bronze.policies")

# Continue with Silver/Gold transformations as usual
```

---

## Monitoring

### Check Mirroring Health
```python
# Built-in health check (Section 7 in notebook)
health = spark.table("metadata.mirroring_health_log") \
    .orderBy(col("check_timestamp").desc()) \
    .limit(10)

display(health)
```

### Check Data Freshness
```python
# Compare source vs mirror
source_count = spark.sql("SELECT COUNT(*) FROM PolicySystem_Mirror.dbo.policies").collect()[0][0]
bronze_count = spark.sql("SELECT COUNT(*) FROM bronze.policies").collect()[0][0]

print(f"Mirrored: {source_count:,}")
print(f"Bronze: {bronze_count:,}")
print(f"Lag: {source_count - bronze_count} rows")
```

### Add to Operational Monitoring (Notebook 45)
```python
def check_mirroring_lag():
    """Alert if mirroring falls behind."""
    health = spark.table("metadata.mirroring_health_log") \
        .filter(col("check_timestamp") > current_timestamp() - interval 5 minutes)
    
    issues = health.filter(col("has_error") == True)
    
    if issues.count() > 0:
        send_alert("🚨 Mirroring issues detected!")
        return False
    
    return True
```

---

## Troubleshooting

### "Mirrored database creation failed"
**Cause:** Insufficient capacity or permissions

**Fix:**
1. Verify workspace capacity: F64+ required for mirroring
2. Check permissions: workspace contributor or admin
3. Verify source database CDC is enabled

### "Credentials retrieval failed"
**Cause:** Key Vault not accessible

**Fix:**
1. Grant workspace Managed Identity access to Key Vault:
   ```bash
   az keyvault set-policy \
     --name insurance-kv \
     --object-id <WORKSPACE_MI_ID> \
     --secret-permissions get list
   ```
2. Verify secrets exist in Key Vault

### "Snapshot in progress"
**Cause:** Initial data load not complete

**Fix:**
- Wait 10-30 minutes for initial snapshot
- Check status in Fabric UI: Workspace → Mirrored Database → Status
- Large databases (> 100 GB) may take hours

### "No data in mirrored tables"
**Cause:** CDC not enabled on source

**Fix:**
```sql
-- On source database
EXEC sys.sp_cdc_enable_db;

-- On each table
EXEC sys.sp_cdc_enable_table 
    @source_schema = 'dbo', 
    @source_name = 'your_table',
    @role_name = NULL;
```

---

## Real-Time Data Flow

```
Source Database              Fabric Mirroring            Medallion Arch
┌─────────────────┐         ┌──────────────────┐       ┌──────────────┐
│  SQL Server     │  CDC    │  Mirrored DB     │       │   Bronze     │
│                 │ ──────> │  (read-only)     │ ────> │  (Delta)     │
│  INSERT/UPDATE  │ <30 sec │                  │       │              │
│  DELETE         │         │  Auto-sync       │       │  Transform   │
└─────────────────┘         └──────────────────┘       └──────────────┘
                                    │                          │
                                    │                          v
                            Lakehouse Shortcuts         ┌──────────────┐
                            (instant access)            │   Silver     │
                                                        │  (SCD Type2) │
                                                        └──────────────┘
                                                               │
                                                               v
                                                        ┌──────────────┐
                                                        │    Gold      │
                                                        │ (Star Schema)│
                                                        └──────────────┘
```

**Latency:**
- Source → Mirror: < 30 seconds (CDC)
- Mirror → Bronze: < 1 second (query)
- Bronze → Gold: 5-15 minutes (batch transform)

**Comparison:**

| Method | Setup Time | Latency | Maintenance |
|--------|-----------|---------|-------------|
| **Traditional ETL** | 40+ hours | 6-24 hours | High (ongoing coding) |
| **Fabric Mirroring** | 5 minutes | < 30 seconds | Zero (auto schema detection) |

---

## Benefits of Python Notebook Approach

✅ **No PowerShell required** — runs 100% in Fabric  
✅ **Interactive execution** — see results step-by-step  
✅ **Easy customization** — modify config in notebook  
✅ **Built-in monitoring** — health checks included  
✅ **Version controlled** — commit notebook to Git  
✅ **Reusable** — run multiple times safely  
✅ **Team friendly** — share with colleagues in workspace  
✅ **Logging built-in** — all results saved to Delta tables  

---

## Next Steps

1. **Run Notebook 15** → Setup mirroring (5 min)
2. **Wait for snapshot** → Initial data load (10-30 min)
3. **Run Notebook 30** → Transform mirrored data to Silver/Gold
4. **Add monitoring** → Integrate with Notebook 45
5. **Test real-time** → Update source data, verify < 30 sec sync

**Ready to go! 🚀**

---

## Comparison: PowerShell vs Python Notebook

| Feature | PowerShell Script | Python Notebook |
|---------|------------------|----------------|
| **Execution** | Local machine, Azure CLI | Fabric workspace, browser |
| **Prerequisites** | PowerShell, Az CLI, Auth | None (runs in Fabric) |
| **Interactivity** | Command-line only | Cell-by-cell execution |
| **Results** | JSON file | Delta tables + display |
| **Monitoring** | External script | Built-in health checks |
| **Team Sharing** | Email script file | Share workspace notebook |
| **Reusability** | Re-run script | Cell + parameter changes |
| **Version Control** | Manual Git commit | Fabric Git integration |

**Recommendation:** Use **Python Notebook** (Notebook 15) for easier execution and better team collaboration!

---

**Questions?** 
- Check `FABRIC_MIRRORING_GUIDE.md` for detailed documentation
- See PowerShell alternative: `setup_fabric_mirroring.ps1` (if needed)
