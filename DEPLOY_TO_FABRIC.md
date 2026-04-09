# 🚀 Deploy Insurance Accelerator to Microsoft Fabric

## Current Status

✅ **Local Files Created**: All 15 notebooks exist in `/notebooks` folder  
❌ **Fabric Deployment**: Not yet uploaded to Fabric workspace  

---

## Deployment Options

### Option 1: Manual Import via Fabric UI (Recommended for First Time)

1. **Open Microsoft Fabric** → Navigate to your workspace
2. **Import Notebooks**:
   - Click **New** → **Import notebook**
   - Select all 15 `.ipynb` files from `/notebooks` folder
   - Upload in batches of 5-10 files at a time

3. **Create Lakehouses** (if not exists):
   ```
   - insurance_bronze
   - insurance_silver  
   - insurance_gold
   - insurance_metadata
   ```

4. **Attach Lakehouses to Notebooks**:
   - Open each notebook
   - Click **Lakehouse** → **Add** → Select appropriate lakehouse
   - Notebook 00-35: Attach all 4 lakehouses
   - Notebook 40-99: Attach metadata lakehouse primarily

5. **Set Default Lakehouse**:
   - For most notebooks, set `insurance_gold` as default

---

### Option 2: Automated Upload via REST API

Use **notebook 99** (`99_marketplace_deployment.ipynb`) which includes:

```python
# After importing notebook 99 to Fabric, run:
installation_result = run_installation_wizard({
    "workspace_name": "Insurance-Accelerator-Demo",
    "capacity_id": "YOUR_FABRIC_CAPACITY_ID",
    "generate_sample_data": True,
    "deploy_power_bi": True
})
```

This will:
- ✅ Create new workspace
- ✅ Create 4 lakehouses
- ✅ Set up workspace structure
- ⚠️ Still requires manual notebook import (Fabric limitation)

---

### Option 3: Git Integration (Recommended for Production)

1. **Initialize Git in this folder**:
   ```powershell
   cd "c:\Users\kgundavaram\OneDrive - Microsoft\Desktop\insurance"
   git init
   git add .
   git commit -m "Initial commit - Insurance Fabric Accelerator"
   ```

2. **Push to Azure DevOps or GitHub**:
   ```powershell
   git remote add origin https://github.com/YOUR_ORG/insurance-accelerator.git
   git push -u origin main
   ```

3. **Connect Fabric Workspace to Git**:
   - In Fabric workspace → **Workspace settings** → **Git integration**
   - Connect to your repository
   - Select branch: `main`
   - Folder: `/notebooks`
   - Click **Sync**

4. **Benefits**:
   - ✅ Version control for all notebooks
   - ✅ Automatic sync on git push
   - ✅ CI/CD integration (notebook 70)
   - ✅ Deployment pipelines (Dev/Test/Prod)

---

## Post-Deployment Steps

### 1. Initial Setup (5 minutes)

```python
# Run these notebooks in order:
1. Import all notebooks to Fabric workspace
2. Create 4 lakehouses (bronze, silver, gold, metadata)
3. Attach lakehouses to notebooks
```

### 2. Generate Sample Data (10 minutes)

```python
# In Fabric, open and run:
01_demo_data_generator.ipynb
# Creates: 10K customers, 20K policies, 5K claims, 15K invoices
```

### 3. Execute Medallion Pipeline (15 minutes)

```python
# In Fabric, open and run:
30_medallion_transformations.ipynb
# Processes: Bronze → Silver → Gold layers
```

### 4. View Dashboard (5 minutes)

```python
# In Fabric, open and run:
90_central_cockpit_dashboard.ipynb
# Then create Power BI report from generated semantic model
```

---

## Deployment Checklist

### Prerequisites
- [ ] Microsoft Fabric workspace (F4+ capacity recommended)
- [ ] Workspace admin permissions
- [ ] Power BI Premium or PPU license

### Fabric Resources to Create
- [ ] 4 Lakehouses: bronze, silver, gold, metadata
- [ ] 15 Notebooks imported
- [ ] 1 Direct Lake semantic model (via notebook 90)
- [ ] 1 Power BI dashboard (Central Cockpit)

### Optional Azure Resources
- [ ] Azure AI Document Intelligence (for notebook 25)
- [ ] Azure AI Foundry (for notebook 25)
- [ ] Azure Key Vault (for secrets management)

---

## Current File Structure

```
insurance/
├── notebooks/                    ← 15 .ipynb files ready for import
│   ├── 00_fabric_native_utils.ipynb
│   ├── 01_demo_data_generator.ipynb
│   ├── 10_admin_governance_console.ipynb
│   ├── 20_ai_showcase_all_features.ipynb
│   ├── 25_document_extraction_foundry_summarization.ipynb
│   ├── 30_medallion_transformations.ipynb
│   ├── 35_streaming_realtime_intelligence.ipynb
│   ├── 40_data_quality_framework.ipynb
│   ├── 45_operational_monitoring.ipynb
│   ├── 50_security_compliance.ipynb
│   ├── 60_test_runner.ipynb
│   ├── 70_cicd_deployment_automation.ipynb
│   ├── 80_fabric_iq_ontology.ipynb
│   ├── 90_central_cockpit_dashboard.ipynb
│   └── 99_marketplace_deployment.ipynb
│
├── architecture/
│   └── ARCHITECTURE.md           ← Reference architecture
│
├── README.md                     ← Project overview
└── DEPLOY_TO_FABRIC.md          ← This file

```

---

## Troubleshooting

### Issue: "Cannot import .ipynb file"
**Solution**: Ensure file is valid JSON. Open in VS Code and check for syntax errors.

### Issue: "Lakehouse not found"
**Solution**: Create lakehouses first, then attach to notebooks.

### Issue: "Kernel error when running notebook"
**Solution**: Select kernel "Microsoft Fabric Runtime" → "Synapse PySpark"

### Issue: "Import fails with timeout"
**Solution**: Import in smaller batches (5 notebooks at a time).

---

## Next Steps After Deployment

1. **Run Demo Data** → Generate synthetic insurance data
2. **Execute Pipelines** → Bronze → Silver → Gold transformations
3. **View Dashboard** → Central Cockpit with 25+ KPIs
4. **Enable Security** → Run notebook 50 for PII/PCI masking
5. **Set Up CI/CD** → Configure Git integration (notebook 70)
6. **Run Tests** → Validate deployment (notebook 60)

---

## Need Help?

- **Documentation**: See [ARCHITECTURE.md](architecture/ARCHITECTURE.md)
- **Quick Start**: See [README.md](README.md)
- **Support**: Open an issue or contact your Fabric admin

---

**Ready to deploy? Start with Option 1 (Manual Import) for quickest results!** 🚀
