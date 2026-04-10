# 🔄 Fabric Mirroring - Easy Setup Guide

## What is Fabric Mirroring?

**Fabric Mirroring** replicates data from external databases into OneLake using **Change Data Capture (CDC)** for near real-time sync. Perfect for insurance systems!

### Supported Sources:
- ✅ Azure SQL Database
- ✅ SQL Server (2019+)
- ✅ Azure Cosmos DB
- ✅ Snowflake
- ✅ Azure Database for PostgreSQL

### Benefits for Insurance:
- **Near real-time replication** - Changes appear in seconds
- **No ETL coding** - Point-and-click setup
- **Automatic schema detection** - Discovers tables/columns
- **Change tracking** - Full CDC support
- **OneLake integration** - Data available immediately to Spark/Power BI

---

## 🎯 Insurance Use Cases

### 1. Policy Administration System → Fabric
- Mirror policy, product, coverage tables
- Real-time policy status changes
- Instant access to underwriting data

### 2. Claims Management System → Fabric
- Live FNOL (First Notice of Loss) data
- Claim status updates in real-time
- Adjudication workflow tracking

### 3. Core Banking/Finance → Fabric
- Real-time payment transactions
- Journal entry replication
- GL account balances

### 4. CRM System → Fabric
- Customer data synchronization
- Sales/marketing campaign tracking
- Agent performance metrics

---

## 🚀 Quick Setup (5 Minutes)

### Option 1: Fabric UI (Point-and-Click)

**Step 1: Create Mirrored Database**

1. Open Fabric workspace
2. Click **+ New** → **Mirrored database**
3. Choose source:
   - **Azure SQL Database**
   - **SQL Server**
   - **Snowflake**
   - **Cosmos DB**

**Step 2: Configure Connection**

For **Azure SQL Database**:
```
Server: your-server.database.windows.net
Database: insurance_core
Authentication: SQL Auth or Microsoft Entra ID
Username: mirroring_user
Password: ***
```

**Step 3: Select Tables**

- ✅ dbo.policies
- ✅ dbo.claims
- ✅ dbo.customers
- ✅ dbo.payments
- ✅ dbo.invoices

**Step 4: Start Mirroring**

- Click **Start**
- Initial snapshot: 5-15 minutes
- Real-time sync: Active immediately after

**Step 5: Query Data**

Data appears in:
- **Lakehouse shortcut** to mirrored database
- **SQL endpoint** for T-SQL queries
- **Spark** for transformations

---

## 🔧 Automated Setup (PowerShell + REST API)

### PowerShell Script for Batch Setup

```powershell
# ============================================================================
# Create Multiple Mirrored Databases - Automated Setup
# ============================================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$WorkspaceId,
    
    [Parameter(Mandatory=$true)]
    [string]$SourceServer,
    
    [Parameter(Mandatory=$true)]
    [string]$SourceDatabase,
    
    [Parameter(Mandatory=$true)]
    [string]$Username,
    
    [Parameter(Mandatory=$true)]
    [string]$Password
)

# Import Fabric utilities
. .\src\framework\fabric_native_utils.py

function Create-MirroredDatabase {
    param(
        [string]$MirrorName,
        [string]$Server,
        [string]$Database,
        [string]$User,
        [string]$Pass,
        [string[]]$Tables
    )
    
    Write-Host "`n🔄 Creating Mirrored Database: $MirrorName" -ForegroundColor Cyan
    
    # Get access token
    $token = (az account get-access-token --resource "https://api.fabric.microsoft.com" | ConvertFrom-Json).accessToken
    
    # Create mirrored database
    $createBody = @{
        displayName = $MirrorName
        description = "Mirrored from $Server/$Database"
        type = "MirroredDatabase"
        definition = @{
            format = "SQL"
            parts = @(
                @{
                    path = "connection.json"
                    payload = @{
                        source = @{
                            type = "AzureSQLDatabase"
                            server = $Server
                            database = $Database
                            authentication = @{
                                type = "Basic"
                                username = $User
                                password = $Pass
                            }
                        }
                        tables = $Tables
                    } | ConvertTo-Json -Depth 10
                    payloadType = "InlineBase64"
                }
            )
        }
    } | ConvertTo-Json -Depth 10
    
    $headers = @{
        "Authorization" = "Bearer $token"
        "Content-Type" = "application/json"
    }
    
    $url = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/mirroredDatabases"
    
    try {
        $response = Invoke-RestMethod -Method Post -Uri $url -Headers $headers -Body $createBody
        Write-Host "✅ Mirrored Database Created: $($response.id)" -ForegroundColor Green
        return $response.id
    }
    catch {
        Write-Host "❌ Failed to create mirrored database: $_" -ForegroundColor Red
        return $null
    }
}

function Start-Mirroring {
    param(
        [string]$MirrorId
    )
    
    Write-Host "▶️  Starting mirroring for: $MirrorId" -ForegroundColor Yellow
    
    $token = (az account get-access-token --resource "https://api.fabric.microsoft.com" | ConvertFrom-Json).accessToken
    
    $headers = @{
        "Authorization" = "Bearer $token"
        "Content-Type" = "application/json"
    }
    
    $url = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/mirroredDatabases/$MirrorId/startMirroring"
    
    try {
        Invoke-RestMethod -Method Post -Uri $url -Headers $headers
        Write-Host "✅ Mirroring started successfully" -ForegroundColor Green
    }
    catch {
        Write-Host "❌ Failed to start mirroring: $_" -ForegroundColor Red
    }
}

# ============================================================================
# Main Execution - Create All Insurance Mirrored Databases
# ============================================================================

Write-Host "`n╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║  Insurance Fabric Mirroring - Automated Setup                  ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════════╝`n" -ForegroundColor Cyan

# Mirror 1: Policy Administration System
$policyTables = @(
    "dbo.policies",
    "dbo.policy_holders",
    "dbo.coverages",
    "dbo.endorsements",
    "dbo.products",
    "dbo.riders"
)

$policyMirrorId = Create-MirroredDatabase `
    -MirrorName "PolicySystem_Mirror" `
    -Server $SourceServer `
    -Database "PolicyAdministration" `
    -User $Username `
    -Pass $Password `
    -Tables $policyTables

if ($policyMirrorId) {
    Start-Mirroring -MirrorId $policyMirrorId
}

# Mirror 2: Claims Management System
$claimsTables = @(
    "dbo.claims",
    "dbo.claim_details",
    "dbo.claim_documents",
    "dbo.claim_payments",
    "dbo.adjusters"
)

$claimsMirrorId = Create-MirroredDatabase `
    -MirrorName "ClaimsSystem_Mirror" `
    -Server $SourceServer `
    -Database "ClaimsManagement" `
    -User $Username `
    -Pass $Password `
    -Tables $claimsTables

if ($claimsMirrorId) {
    Start-Mirroring -MirrorId $claimsMirrorId
}

# Mirror 3: Finance System
$financeTables = @(
    "dbo.journal_entries",
    "dbo.gl_accounts",
    "dbo.invoices",
    "dbo.payments",
    "dbo.reconciliations"
)

$financeMirrorId = Create-MirroredDatabase `
    -MirrorName "FinanceSystem_Mirror" `
    -Server $SourceServer `
    -Database "Finance" `
    -User $Username `
    -Pass $Password `
    -Tables $financeTables

if ($financeMirrorId) {
    Start-Mirroring -MirrorId $financeMirrorId
}

Write-Host "`n════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "✅ Mirroring Setup Complete!" -ForegroundColor Green
Write-Host "════════════════════════════════════════════════════════════════`n" -ForegroundColor Cyan

Write-Host "📊 Next Steps:" -ForegroundColor Yellow
Write-Host "  1. Initial snapshot will complete in 10-30 minutes" -ForegroundColor White
Write-Host "  2. Real-time CDC will start after snapshot" -ForegroundColor White
Write-Host "  3. Access data via Lakehouse shortcuts" -ForegroundColor White
Write-Host "  4. Query using Spark or SQL endpoint" -ForegroundColor White
Write-Host "`n"
```

---

## 📓 Python Notebook for Mirroring Setup

### Create: `15_fabric_mirroring_setup.ipynb`

```python
# ╔══════════════════════════════════════════════════════════════════════╗
# ║  Fabric Mirroring Setup — Near Real-Time Data Replication           ║
# ╚══════════════════════════════════════════════════════════════════════╝

# === CONFIGURATION ===

mirroring_config = {
    "policy_system": {
        "source_type": "AzureSQLDatabase",
        "server": "insurance-prod-sql.database.windows.net",
        "database": "PolicyAdministration",
        "tables": [
            "dbo.policies",
            "dbo.policy_holders",
            "dbo.coverages",
            "dbo.products",
            "dbo.endorsements"
        ],
        "mirror_name": "PolicySystem_Mirror"
    },
    "claims_system": {
        "source_type": "AzureSQLDatabase",
        "server": "insurance-prod-sql.database.windows.net",
        "database": "ClaimsManagement",
        "tables": [
            "dbo.claims",
            "dbo.claim_details",
            "dbo.claim_payments",
            "dbo.fnol"
        ],
        "mirror_name": "ClaimsSystem_Mirror"
    },
    "finance_system": {
        "source_type": "AzureSQLDatabase",
        "server": "insurance-prod-sql.database.windows.net",
        "database": "Finance",
        "tables": [
            "dbo.journal_entries",
            "dbo.invoices",
            "dbo.payments",
            "dbo.gl_accounts"
        ],
        "mirror_name": "FinanceSystem_Mirror"
    },
    "crm_system": {
        "source_type": "SQLServer",
        "server": "crm-server.contoso.com",
        "database": "CRM",
        "tables": [
            "dbo.customers",
            "dbo.contacts",
            "dbo.opportunities",
            "dbo.campaigns"
        ],
        "mirror_name": "CRM_Mirror"
    }
}

# === STEP 1: Setup Mirroring Using Fabric API ===

import requests
import json
from pyspark.sql.functions import *

class FabricMirroringManager:
    """Manage Fabric Mirroring setup and monitoring."""
    
    def __init__(self, workspace_id: str, access_token: str = None):
        self.workspace_id = workspace_id
        self.access_token = access_token or self._get_token()
        self.api_base = "https://api.fabric.microsoft.com/v1"
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
    
    def _get_token(self):
        """Get access token from Azure CLI."""
        import subprocess
        result = subprocess.run(
            ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com"],
            capture_output=True,
            text=True
        )
        return json.loads(result.stdout)["accessToken"]
    
    def create_mirrored_database(self, config: dict, credentials: dict):
        """
        Create a mirrored database from source system.
        
        Args:
            config: Mirroring configuration (server, database, tables)
            credentials: Authentication (username, password)
        """
        print(f"\n🔄 Creating Mirrored Database: {config['mirror_name']}")
        
        payload = {
            "displayName": config["mirror_name"],
            "description": f"Mirrored from {config['server']}/{config['database']}",
            "type": "MirroredDatabase",
            "definition": {
                "format": "SQL",
                "parts": [
                    {
                        "path": "connection.json",
                        "payload": json.dumps({
                            "source": {
                                "type": config["source_type"],
                                "server": config["server"],
                                "database": config["database"],
                                "authentication": {
                                    "type": "Basic",
                                    "username": credentials["username"],
                                    "password": credentials["password"]
                                }
                            },
                            "tables": config["tables"]
                        }),
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
        
        url = f"{self.api_base}/workspaces/{self.workspace_id}/mirroredDatabases"
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            mirror_id = response.json()["id"]
            print(f"✅ Mirrored Database Created: {mirror_id}")
            return mirror_id
        except Exception as e:
            print(f"❌ Failed: {e}")
            return None
    
    def start_mirroring(self, mirror_id: str):
        """Start the mirroring process (initial snapshot + CDC)."""
        print(f"\n▶️  Starting mirroring: {mirror_id}")
        
        url = f"{self.api_base}/workspaces/{self.workspace_id}/mirroredDatabases/{mirror_id}/startMirroring"
        
        try:
            response = requests.post(url, headers=self.headers)
            response.raise_for_status()
            print("✅ Mirroring started successfully")
        except Exception as e:
            print(f"❌ Failed to start: {e}")
    
    def get_mirroring_status(self, mirror_id: str):
        """Check mirroring status."""
        url = f"{self.api_base}/workspaces/{self.workspace_id}/mirroredDatabases/{mirror_id}/status"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            status = response.json()
            
            print(f"\n📊 Mirroring Status:")
            print(f"   State: {status.get('state', 'Unknown')}")
            print(f"   Last Sync: {status.get('lastSyncTime', 'N/A')}")
            print(f"   Rows Replicated: {status.get('rowsReplicated', 0):,}")
            
            return status
        except Exception as e:
            print(f"❌ Failed to get status: {e}")
            return None

# === STEP 2: Create All Mirrored Databases ===

# Initialize manager
workspace_id = "your-workspace-id"  # Replace with actual workspace ID
manager = FabricMirroringManager(workspace_id)

# Credentials (in production, use Key Vault)
credentials = {
    "username": "mirroring_user",
    "password": "***"  # Get from Key Vault
}

# Create mirrored databases
mirror_ids = {}

for system_name, config in mirroring_config.items():
    mirror_id = manager.create_mirrored_database(config, credentials)
    if mirror_id:
        mirror_ids[system_name] = mirror_id
        manager.start_mirroring(mirror_id)

print(f"\n✅ Created {len(mirror_ids)} mirrored databases")

# === STEP 3: Create Lakehouse Shortcuts to Mirrored Data ===

# After mirroring is set up, create shortcuts in your lakehouses

def create_shortcut_to_mirror(lakehouse_name: str, mirror_name: str, table_name: str):
    """Create OneLake shortcut from lakehouse to mirrored table."""
    
    # Get lakehouse and mirror IDs
    lakehouse_id = get_lakehouse_id(lakehouse_name)
    mirror_id = mirror_ids.get(mirror_name)
    
    if not lakehouse_id or not mirror_id:
        print(f"❌ Lakehouse or mirror not found")
        return
    
    shortcut_name = f"mirrored_{table_name}"
    
    # Create shortcut using Fabric API
    shortcut_payload = {
        "name": shortcut_name,
        "path": f"Tables/{table_name}",
        "target": {
            "type": "OneLake",
            "itemId": mirror_id,
            "path": f"/{table_name}"
        }
    }
    
    url = f"{manager.api_base}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/shortcuts"
    
    try:
        response = requests.post(url, headers=manager.headers, json=shortcut_payload)
        response.raise_for_status()
        print(f"✅ Shortcut created: {shortcut_name}")
    except Exception as e:
        print(f"❌ Failed to create shortcut: {e}")

# Create shortcuts for Bronze lakehouse
for system_name, config in mirroring_config.items():
    for table in config["tables"]:
        table_name = table.split(".")[-1]  # Extract table name
        create_shortcut_to_mirror("insurance_bronze", system_name, table_name)

print("✅ Shortcuts created in Bronze lakehouse")

# === STEP 4: Query Mirrored Data ===

# Option 1: Query via SQL endpoint
policies_df = spark.sql("""
    SELECT *
    FROM PolicySystem_Mirror.dbo.policies
    WHERE effective_date >= CURRENT_DATE - INTERVAL 30 DAYS
""")

print(f"Policies (last 30 days): {policies_df.count()}")

# Option 2: Query via lakehouse shortcut
claims_df = spark.table("insurance_bronze.mirrored_claims")
print(f"Claims: {claims_df.count()}")

# === STEP 5: Monitor Mirroring Health ===

for system_name, mirror_id in mirror_ids.items():
    print(f"\n{'='*60}")
    print(f"System: {system_name}")
    print(f"{'='*60}")
    manager.get_mirroring_status(mirror_id)

print("\n✅ Fabric Mirroring Setup Complete!")
```

---

## 📋 Mirroring Monitoring Dashboard

### Add to Notebook 45 (Operational Monitoring)

```python
# ╔══════════════════════════════════════════════════════════════════════╗
# ║  Mirroring Health Monitoring                                        ║
# ╚══════════════════════════════════════════════════════════════════════╝

def monitor_mirroring_health():
    """Monitor health of all mirrored databases."""
    
    # Query mirroring metadata (available in system tables)
    mirroring_health = spark.sql("""
        SELECT
            mirror_name,
            source_system,
            last_sync_time,
            rows_replicated,
            replication_lag_seconds,
            status,
            error_message
        FROM metadata.mirroring_status
        WHERE status != 'Healthy'
        ORDER BY replication_lag_seconds DESC
    """)
    
    print("\n⚠️  Mirroring Issues Detected:")
    mirroring_health.show(truncate=False)
    
    # Create alerts for high lag
    high_lag = mirroring_health.filter(col("replication_lag_seconds") > 300)  # > 5 minutes
    
    if high_lag.count() > 0:
        print(f"\n🚨 {high_lag.count()} mirrors with high replication lag!")
        
        for row in high_lag.collect():
            send_alert(
                title=f"Mirroring Lag: {row.mirror_name}",
                message=f"Replication lag: {row.replication_lag_seconds}s",
                severity="Warning"
            )
```

---

## 🎯 Integration with Medallion Architecture

### Bronze Layer - Use Mirrored Data

```python
# Instead of copying files to Bronze, use mirrored data directly

# OLD WAY (File ingestion):
# df_policies = spark.read.csv("bronze/policies/*.csv")

# NEW WAY (Mirrored data):
df_policies = spark.table("PolicySystem_Mirror.dbo.policies")

# Mirrored data is already in Bronze - just apply minimal transformations
df_bronze = df_policies \
    .withColumn("_source_system", lit("PolicySystem_Mirror")) \
    .withColumn("_ingestion_time", current_timestamp()) \
    .write.format("delta").mode("append").saveAsTable("bronze.policies")
```

### Real-Time Updates

Mirrored data automatically updates via CDC!
```python
# Query always returns latest data
df_latest_claims = spark.table("ClaimsSystem_Mirror.dbo.claims")

# No need to run batch ingestion - data is already current!
```

---

## 📊 Comparison: Traditional ETL vs. Mirroring

| Aspect | Traditional ETL/Copy | Fabric Mirroring |
|--------|---------------------|------------------|
| **Setup Time** | Hours/days (code pipelines) | Minutes (point-and-click) |
| **Latency** | Batch (hours/daily) | Near real-time (seconds) |
| **Maintenance** | Code changes for schema | Automatic schema detection |
| **CDC Support** | Custom implementation | Built-in CDC |
| **Failure Handling** | Manual retry logic | Automatic retry |
| **Resource Usage** | Spark jobs, compute | Minimal overhead |
| **Cost** | Compute + storage | OneLake storage only |

**Winner**: Mirroring for 90% of insurance data sources!

---

## 🔐 Security Best Practices

### 1. Use Managed Identity (Recommended)

```json
{
    "authentication": {
        "type": "ManagedIdentity"
    }
}
```

### 2. Store Credentials in Key Vault

```python
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

credential = DefaultAzureCredential()
client = SecretClient(vault_url="https://your-vault.vault.azure.net", credential=credential)

db_password = client.get_secret("mirroring-user-password").value
```

### 3. Enable Private Link

- Fabric workspace with Private Link
- Azure SQL with private endpoint
- No public internet access

### 4. Apply RLS/CLS

Mirrored data inherits source permissions, but you can add additional security in Fabric.

---

## 🚀 Quick Start Checklist

- [ ] **Enable CDC on source database** (SQL Server/Azure SQL)
  ```sql
  -- On source database
  EXEC sys.sp_cdc_enable_db;
  EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'policies';
  ```

- [ ] **Create mirroring user with permissions**
  ```sql
  CREATE LOGIN mirroring_user WITH PASSWORD = 'StrongPassword!';
  CREATE USER mirroring_user FOR LOGIN mirroring_user;
  GRANT SELECT ON SCHEMA::dbo TO mirroring_user;
  EXEC sp_addrolemember 'db_datareader', 'mirroring_user';
  ```

- [ ] **Create mirrored database in Fabric** (use UI or PowerShell script above)

- [ ] **Create lakehouse shortcuts** to mirrored tables

- [ ] **Update Notebook 30** to use mirrored data instead of file ingestion

- [ ] **Add monitoring** to Notebook 45 for mirroring health

---

## 📁 Files to Create

1. **setup_fabric_mirroring.ps1** - PowerShell automation script
2. **15_fabric_mirroring_setup.ipynb** - Notebook for mirroring setup
3. **Update Notebook 30** - Use mirrored data in Bronze layer
4. **Update Notebook 45** - Add mirroring health monitoring

---

## 💡 Pro Tips

1. **Start with critical systems** - Policy and Claims first
2. **Test in Dev workspace** first before production
3. **Monitor replication lag** - Set alerts for > 5 minutes
4. **Use shortcuts, not copies** - Avoid duplicating data
5. **Schema changes auto-detected** - No code updates needed!
6. **Incremental refresh** - Mirroring handles this automatically

---

## 🎯 Expected Results

### After Setup:

1. **Policy System**: 20K policies synced in 10 minutes
2. **Claims System**: 5K claims + real-time FNOL
3. **Finance**: Journal entries updated every second
4. **Latency**: < 30 seconds for most changes
5. **Storage**: OneLake (no separate database costs)

### In Production:

- **Zero maintenance** - CDC runs automatically
- **Schema evolution** - New columns detected automatically
- **Always current** - Latest data without batch jobs
- **Cost effective** - No compute for data movement

---

**Status**: Fabric Mirroring setup guide ready to implement! 🔄

**Next**: Run PowerShell script or create Notebook 15 to set up mirroring.
