# ✅ REST API Import - Ready to Use!

## 🎉 What Was Created

I've built a complete REST API import utility with these files:

### 1. **import_notebooks_to_fabric.py** (11 KB)
Complete Python script that:
- ✅ Authenticates via Azure CLI automatically
- ✅ Imports all 15 notebooks from `/notebooks` folder
- ✅ Uses Fabric REST API (POST to `/v1/workspaces/{id}/notebooks`)
- ✅ Handles existing notebooks (skip or overwrite)
- ✅ Provides detailed progress and error reporting
- ✅ Returns success/failure summary

### 2. **import_notebooks.ps1** (4.4 KB)
PowerShell wrapper that:
- ✅ Checks Python installation
- ✅ Checks Azure CLI installation
- ✅ Verifies Azure login
- ✅ Installs dependencies (`requests` library)
- ✅ Runs the Python script with parameters
- ✅ Displays formatted progress

### 3. **requirements-fabric-import.txt**
Python dependencies:
```
requests>=2.31.0
```

### 4. **IMPORT_VIA_API.md** (10 KB)
Complete documentation with:
- Quick start guide
- Prerequisites setup
- Usage examples (PowerShell & Python)
- Authentication methods
- Troubleshooting guide
- CI/CD integration examples (GitHub Actions, Azure DevOps)
- Service principal setup

---

## 🚀 How to Use It

### Step 1: Get Your Fabric Workspace ID

**Option A - From URL:**
1. Go to https://app.fabric.microsoft.com
2. Open your workspace
3. Look at URL: `https://app.fabric.microsoft.com/groups/12345678-1234-1234-1234-123456789abc/...`
4. Copy the GUID: `12345678-1234-1234-1234-123456789abc`

**Option B - Create New Workspace:**
1. Click "+ Workspaces"
2. Click "New workspace"
3. Name it: "Insurance Accelerator"
4. Copy the workspace ID from the URL

### Step 2: Install Prerequisites (One-Time)

**If you don't have Azure CLI:**
```powershell
winget install Microsoft.AzureCLI
```

**Login to Azure:**
```powershell
az login
```

**Install Python dependencies:**
```powershell
pip install -r requirements-fabric-import.txt
```

### Step 3: Run the Import

**Simple command:**
```powershell
.\import_notebooks.ps1 -WorkspaceId "YOUR-WORKSPACE-GUID-HERE"
```

**Full example:**
```powershell
# Replace with your actual workspace ID
.\import_notebooks.ps1 -WorkspaceId "12345678-1234-1234-1234-123456789abc"
```

**With overwrite (replace existing):**
```powershell
.\import_notebooks.ps1 -WorkspaceId "YOUR-GUID" -Overwrite
```

---

## 📊 What Happens When You Run It

```
========================================
Fabric Notebook Import - PowerShell Wrapper
========================================

Checking Python installation...
✅ Python 3.11.0

Checking Azure CLI installation...
✅ Azure CLI version 2.57.0

Checking Azure authentication...
✅ Logged in as: your.email@microsoft.com
   Subscription: Your Subscription Name

Installing dependencies from requirements file...
✅ Dependencies installed

========================================
Starting Notebook Import...
========================================

🔍 Verifying workspace: 12345678-1234-1234-1234-123456789abc
✅ Workspace found: Insurance Accelerator

📋 Listing existing notebooks in workspace...
✅ Found 0 existing notebooks

📤 Importing: 00_fabric_native_utils
   ➕ Creating new notebook...
   ✅ Success! Notebook ID: abcd1234-...

📤 Importing: 01_demo_data_generator
   ➕ Creating new notebook...
   ✅ Success! Notebook ID: efgh5678-...

... (continues for all 15 notebooks)

========================================
📊 IMPORT SUMMARY
========================================
✅ Successful: 15
❌ Failed: 0
📦 Total: 15

💡 Next Steps:
   1. Open Fabric workspace in browser
   2. Create lakehouses: insurance_bronze, insurance_silver, insurance_gold, insurance_metadata
   3. Attach lakehouses to notebooks
   4. Run 01_demo_data_generator.ipynb to create sample data
========================================

✅ IMPORT COMPLETED SUCCESSFULLY!

🔗 Next Steps:
   1. Open workspace: https://app.fabric.microsoft.com
   2. Create lakehouses (if not exists):
      - insurance_bronze
      - insurance_silver
      - insurance_gold
      - insurance_metadata
   3. Attach lakehouses to notebooks
   4. Run 01_demo_data_generator.ipynb
```

---

## 🎯 Your Actual Command

Replace `YOUR-WORKSPACE-GUID` with your actual workspace ID:

```powershell
# Navigate to the insurance folder
cd "c:\Users\kgundavaram\OneDrive - Microsoft\Desktop\insurance"

# Run the import (replace with your workspace ID)
.\import_notebooks.ps1 -WorkspaceId "YOUR-WORKSPACE-GUID-HERE"
```

---

## 📋 Notebooks That Will Be Imported

All 15 notebooks from the `/notebooks` folder:

1. ✅ `00_fabric_native_utils` - Foundation utilities
2. ✅ `01_demo_data_generator` - Sample data generator (10K customers, 20K policies, 5K claims)
3. ✅ `10_admin_governance_console` - Admin console with RBAC
4. ✅ `20_ai_showcase_all_features` - AI features demo (10 capabilities)
5. ✅ `25_document_extraction_foundry_summarization` - Document AI processing
6. ✅ `30_medallion_transformations` - Bronze → Silver → Gold pipelines
7. ✅ `35_streaming_realtime_intelligence` - Real-time analytics with EventHub
8. ✅ `40_data_quality_framework` - Data quality (6 dimensions, metadata-driven)
9. ✅ `45_operational_monitoring` - Health monitoring and incident management
10. ✅ `50_security_compliance` - PII/PCI masking and compliance reports
11. ✅ `60_test_runner` - Automated testing framework
12. ✅ `70_cicd_deployment_automation` - CI/CD with Git integration
13. ✅ `80_fabric_iq_ontology` - Insurance ontology for Copilot
14. ✅ `90_central_cockpit_dashboard` - Dashboard with 25+ KPIs
15. ✅ `99_marketplace_deployment` - Marketplace packaging

---

## 🔧 Advanced Usage

### Python Direct (More Control)

```powershell
# Basic import
python import_notebooks_to_fabric.py --workspace-id "YOUR-GUID"

# With overwrite
python import_notebooks_to_fabric.py --workspace-id "YOUR-GUID" --overwrite

# Custom directory
python import_notebooks_to_fabric.py --workspace-id "YOUR-GUID" --notebooks-dir "path/to/notebooks"

# With custom token (for service principals)
python import_notebooks_to_fabric.py --workspace-id "YOUR-GUID" --token "eyJ0eXAi..."
```

### CI/CD Pipeline

Add to GitHub Actions or Azure DevOps:

```yaml
- name: Import notebooks to Fabric
  run: |
    python import_notebooks_to_fabric.py \
      --workspace-id "${{ secrets.FABRIC_WORKSPACE_ID }}" \
      --token "${{ secrets.FABRIC_ACCESS_TOKEN }}" \
      --overwrite
```

---

## ⚠️ Troubleshooting

### "Not logged into Azure"
```powershell
az login
```

### "Workspace not found"
- Check workspace ID is correct
- Verify you have access to the workspace
- Ensure workspace is in the same tenant

### "Python not found"
```powershell
# Install Python from Microsoft Store or python.org
winget install Python.Python.3.11
```

### "Azure CLI not found"
```powershell
winget install Microsoft.AzureCLI
```

### "Access denied to workspace"
- You need **Admin** or **Contributor** role on the workspace
- Go to Fabric workspace → Settings → Manage access
- Add your account with proper permissions

---

## 🎯 After Import - Next Steps

### 1. Verify in Fabric (2 minutes)
1. Open https://app.fabric.microsoft.com
2. Navigate to your workspace
3. Confirm all 15 notebooks are present

### 2. Create Lakehouses (3 minutes)
In Fabric workspace:
- Click "+ New" → "Lakehouse"
- Create: `insurance_bronze`
- Repeat for: `insurance_silver`, `insurance_gold`, `insurance_metadata`

### 3. Attach Lakehouses (5 minutes)
For each notebook:
- Open in Fabric
- Click "Add lakehouse" 
- Select appropriate lakehouse(s)
- Set default lakehouse

### 4. Run Data Generation (5 minutes)
- Open `01_demo_data_generator.ipynb`
- Click "Run all"
- Generates 10K customers, 20K policies, 5K claims

### 5. Run Transformations (10 minutes)
- Open `30_medallion_transformations.ipynb`
- Click "Run all"
- Processes Bronze → Silver → Gold

### 6. View Dashboard (2 minutes)
- Open `90_central_cockpit_dashboard.ipynb`
- View 25+ insurance KPIs

---

## 📊 Comparison: REST API vs Git Integration

| Feature | REST API Import | Git Integration |
|---------|----------------|-----------------|
| **Speed** | Fast (2-3 min) | Medium (5 min initial) |
| **Setup** | Workspace ID only | Full Git config |
| **Sync** | Manual re-run needed | Automatic on push |
| **Best For** | Initial bulk import | Ongoing development |
| **CI/CD** | ✅ Perfect | ✅ Perfect |
| **Version Control** | Manual commits | Automatic |

**Recommendation:**
1. **Use REST API** for initial import (you're doing this now!)
2. **Then setup Git Integration** for ongoing sync (Option 1 from earlier)

This gives you the best of both worlds!

---

## 💾 Files Committed to GitHub

All import utilities are now on GitHub:

```
Repository: https://github.com/deepugun/insurance-fabric-accelerator
Commit: beb16e1 - "Add REST API notebook import utility with PowerShell wrapper"
```

Files:
- ✅ import_notebooks_to_fabric.py
- ✅ import_notebooks.ps1
- ✅ requirements-fabric-import.txt
- ✅ IMPORT_VIA_API.md

---

## 🎯 Your Action Items

### Right Now:
1. **Get workspace ID** from Fabric portal
2. **Run:** `.\import_notebooks.ps1 -WorkspaceId "YOUR-GUID"`
3. **Wait** 2-3 minutes for import
4. **Verify** notebooks in Fabric

### After Import:
1. Create 4 lakehouses
2. Attach lakehouses to notebooks
3. Run data generator
4. Run transformations
5. View dashboard

**Total time:** ~30 minutes from start to finish! 🚀

---

**Ready to import?** Just need your Fabric workspace ID!
