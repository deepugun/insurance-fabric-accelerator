# 🚀 Import Notebooks to Fabric via REST API

Programmatically import all 15 Insurance Accelerator notebooks to your Fabric workspace using the Fabric REST API.

## ⚡ Quick Start

### Prerequisites

1. **Python 3.8+** installed
2. **Azure CLI** installed and logged in
3. **Fabric workspace ID** (GUID)

### One-Command Import

```powershell
# Run PowerShell script (easiest)
.\import_notebooks.ps1 -WorkspaceId "YOUR-WORKSPACE-GUID"

# Or run Python directly
python import_notebooks_to_fabric.py --workspace-id "YOUR-WORKSPACE-GUID"
```

---

## 📋 Prerequisites Setup

### 1. Install Azure CLI

If not already installed:

```powershell
# Install via winget
winget install Microsoft.AzureCLI

# Or download: https://aka.ms/install-azure-cli
```

### 2. Login to Azure

```powershell
az login
```

This opens your browser for authentication. Select the account with access to your Fabric workspace.

### 3. Get Fabric Workspace ID

**Option A: From Fabric Portal**
1. Open https://app.fabric.microsoft.com
2. Navigate to your workspace
3. Look at the URL: `...workspaces/{WORKSPACE-ID}/...`
4. Copy the GUID

**Option B: Via Azure CLI**
```powershell
# List workspaces (requires Fabric API access)
az rest --url "https://api.fabric.microsoft.com/v1/workspaces" --resource "https://api.fabric.microsoft.com"
```

### 4. Install Python Dependencies

```powershell
pip install -r requirements-fabric-import.txt
```

---

## 🎯 Usage

### PowerShell Script (Recommended)

```powershell
# Import all notebooks
.\import_notebooks.ps1 -WorkspaceId "12345678-1234-1234-1234-123456789abc"

# Import with overwrite (replace existing notebooks)
.\import_notebooks.ps1 -WorkspaceId "YOUR-GUID" -Overwrite

# Specify custom notebooks directory
.\import_notebooks.ps1 -WorkspaceId "YOUR-GUID" -NotebooksDir "path/to/notebooks"
```

### Python Script (Direct)

```powershell
# Basic import
python import_notebooks_to_fabric.py --workspace-id "YOUR-WORKSPACE-GUID"

# Import with overwrite
python import_notebooks_to_fabric.py --workspace-id "YOUR-GUID" --overwrite

# Custom notebooks directory
python import_notebooks_to_fabric.py --workspace-id "YOUR-GUID" --notebooks-dir "path/to/notebooks"

# Use custom access token
python import_notebooks_to_fabric.py --workspace-id "YOUR-GUID" --token "YOUR-BEARER-TOKEN"
```

---

## 📦 What Gets Imported

All 15 notebooks from the `/notebooks` directory:

1. ✅ `00_fabric_native_utils` - Foundation utilities
2. ✅ `01_demo_data_generator` - Sample data generator
3. ✅ `10_admin_governance_console` - Admin & RBAC
4. ✅ `20_ai_showcase_all_features` - AI features demo
5. ✅ `25_document_extraction_foundry_summarization` - Document processing
6. ✅ `30_medallion_transformations` - Bronze→Silver→Gold
7. ✅ `35_streaming_realtime_intelligence` - Real-time analytics
8. ✅ `40_data_quality_framework` - Data quality
9. ✅ `45_operational_monitoring` - Health monitoring
10. ✅ `50_security_compliance` - PII/PCI masking
11. ✅ `60_test_runner` - Testing framework
12. ✅ `70_cicd_deployment_automation` - CI/CD
13. ✅ `80_fabric_iq_ontology` - Insurance ontology
14. ✅ `90_central_cockpit_dashboard` - Dashboards
15. ✅ `99_marketplace_deployment` - Marketplace packaging

---

## 🔧 How It Works

### Authentication

The script uses **Azure CLI authentication** by default:

```python
# Get token from Azure CLI
az account get-access-token --resource "https://api.fabric.microsoft.com"
```

Alternatively, provide a bearer token directly:

```powershell
python import_notebooks_to_fabric.py --workspace-id "YOUR-GUID" --token "eyJ0eXAi..."
```

### Import Process

For each notebook file:

1. **Read** the `.ipynb` file content
2. **Encode** as base64
3. **POST** to Fabric REST API:
   ```
   POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/notebooks
   ```
4. **Verify** successful import
5. **Report** results

### API Endpoint

```
POST https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/notebooks

Headers:
  Authorization: Bearer {token}
  Content-Type: application/json

Body:
{
  "displayName": "01_demo_data_generator",
  "definition": {
    "format": "ipynb",
    "parts": [
      {
        "path": "notebook-content.py",
        "payload": "<base64-encoded-notebook>",
        "payloadType": "InlineBase64"
      }
    ]
  }
}
```

---

## 🎯 After Import

### 1. Verify Notebooks in Fabric

1. Open https://app.fabric.microsoft.com
2. Navigate to your workspace
3. Verify all 15 notebooks appear

### 2. Create Lakehouses

Create 4 lakehouses in your workspace:

```powershell
# From Fabric UI:
New → Lakehouse → "insurance_bronze"
New → Lakehouse → "insurance_silver"
New → Lakehouse → "insurance_gold"
New → Lakehouse → "insurance_metadata"
```

### 3. Attach Lakehouses to Notebooks

For each notebook:
1. Open in Fabric
2. Click "Add lakehouse"
3. Select appropriate lakehouse(s)
4. Set default lakehouse

### 4. Run Initial Setup

```
1. Run: 01_demo_data_generator.ipynb
   → Generates sample data in Bronze lakehouse

2. Run: 30_medallion_transformations.ipynb
   → Processes Bronze → Silver → Gold

3. Open: 90_central_cockpit_dashboard.ipynb
   → View dashboard with 25+ KPIs
```

---

## ⚠️ Troubleshooting

### Error: "Not logged into Azure"

**Solution:**
```powershell
az login
```

### Error: "Workspace not found"

**Causes:**
- Incorrect workspace ID
- No access to workspace
- Workspace in different tenant

**Solution:**
```powershell
# Verify workspace ID
az rest --url "https://api.fabric.microsoft.com/v1/workspaces" --resource "https://api.fabric.microsoft.com"

# Check current tenant
az account show
```

### Error: "Access denied to workspace"

**Solution:**
Grant your account **Admin** or **Contributor** role on the Fabric workspace:
1. Open workspace in Fabric
2. Settings → Manage access
3. Add your account with appropriate role

### Error: "Notebook already exists"

**Behavior:**
- By default, script skips existing notebooks
- Use `--overwrite` flag to replace

**Solution:**
```powershell
# Overwrite existing notebooks
.\import_notebooks.ps1 -WorkspaceId "YOUR-GUID" -Overwrite
```

### Error: "Module 'requests' not found"

**Solution:**
```powershell
pip install requests
```

---

## 🔐 Service Principal Authentication

For CI/CD pipelines, use a service principal:

### 1. Create Service Principal

```powershell
az ad sp create-for-rbac --name "fabric-importer" --role Contributor
```

Output:
```json
{
  "appId": "12345678-1234-1234-1234-123456789abc",
  "password": "your-secret",
  "tenant": "your-tenant-id"
}
```

### 2. Get Access Token

```powershell
$appId = "YOUR-APP-ID"
$secret = "YOUR-SECRET"
$tenant = "YOUR-TENANT-ID"

$body = @{
    grant_type    = "client_credentials"
    client_id     = $appId
    client_secret = $secret
    resource      = "https://api.fabric.microsoft.com"
}

$response = Invoke-RestMethod -Method Post -Uri "https://login.microsoftonline.com/$tenant/oauth2/token" -Body $body
$token = $response.access_token
```

### 3. Import with Token

```powershell
python import_notebooks_to_fabric.py --workspace-id "YOUR-GUID" --token $token
```

---

## 🚀 CI/CD Integration

### GitHub Actions Example

```yaml
name: Import Notebooks to Fabric

on:
  push:
    branches: [master]
    paths: ['notebooks/**']

jobs:
  import:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: pip install requests
      
      - name: Import notebooks
        env:
          WORKSPACE_ID: ${{ secrets.FABRIC_WORKSPACE_ID }}
          ACCESS_TOKEN: ${{ secrets.FABRIC_ACCESS_TOKEN }}
        run: |
          python import_notebooks_to_fabric.py \
            --workspace-id "$WORKSPACE_ID" \
            --token "$ACCESS_TOKEN" \
            --overwrite
```

### Azure DevOps Pipeline Example

```yaml
trigger:
  paths:
    include:
      - notebooks/*

pool:
  vmImage: 'ubuntu-latest'

steps:
- task: UsePythonVersion@0
  inputs:
    versionSpec: '3.11'

- script: pip install requests
  displayName: 'Install dependencies'

- script: |
    python import_notebooks_to_fabric.py \
      --workspace-id "$(FABRIC_WORKSPACE_ID)" \
      --token "$(FABRIC_ACCESS_TOKEN)" \
      --overwrite
  displayName: 'Import notebooks to Fabric'
```

---

## 📊 Comparison with Other Methods

| Method | Speed | Complexity | Automation | Version Control |
|--------|-------|------------|------------|-----------------|
| **REST API Import** | Fast | Medium | ✅ Yes | Manual |
| **Git Integration** | Medium | Low | ✅ Auto | ✅ Yes |
| **Manual Upload** | Slow | Low | ❌ No | ❌ No |

**When to use REST API:**
- ✅ CI/CD pipelines
- ✅ Automated deployments
- ✅ Bulk operations
- ✅ Custom workflows
- ✅ Programmatic control

**When to use Git Integration:**
- ✅ Continuous sync
- ✅ Team collaboration
- ✅ Version history
- ✅ Branch workflows

---

## 📚 Additional Resources

- **Fabric REST API Docs**: https://learn.microsoft.com/fabric/rest-api/
- **Notebook API Reference**: https://learn.microsoft.com/fabric/rest-api/core/notebooks
- **Azure CLI Installation**: https://aka.ms/install-azure-cli
- **Python Requests Library**: https://requests.readthedocs.io/

---

## 💡 Tips

1. **Use Git Integration for ongoing work** - REST API is best for initial bulk import
2. **Grant workspace permissions** - Ensure your account has Contributor or Admin role
3. **Use --overwrite cautiously** - This replaces existing notebooks completely
4. **Automate with CI/CD** - Integrate into your deployment pipelines
5. **Store credentials securely** - Use Azure Key Vault or GitHub Secrets

---

**Status**: Ready to import 15 notebooks to your Fabric workspace!

**Next**: Run `.\import_notebooks.ps1 -WorkspaceId "YOUR-WORKSPACE-GUID"`
