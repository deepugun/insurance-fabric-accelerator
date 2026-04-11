# 🔄 Git & Fabric Sync Setup Guide

## ✅ Git Repository Created Successfully!

Your local Git repository has been initialized with:
- **31 files** committed
- **15 notebooks** ready for Fabric sync
- **Architecture docs** and deployment guides
- **Source code** (Python utilities)

**Commit**: `e7cb95c` - "Initial commit: Insurance Fabric Accelerator with 15 production notebooks"  
**Branch**: `master`

---

## 🚀 Next Steps: Connect to Remote & Sync with Fabric

### Option A: GitHub (Recommended)

#### Step 1: Create GitHub Repository

1. Go to [GitHub](https://github.com/new)
2. Create new repository:
   - **Name**: `insurance-fabric-accelerator`
   - **Visibility**: Private (or Public if approved)
   - **✅ DO NOT** initialize with README (you already have one)
3. Copy the repository URL (e.g., `https://github.com/YOUR_USERNAME/insurance-fabric-accelerator.git`)

#### Step 2: Push to GitHub

Run these commands in PowerShell:

```powershell
cd "c:\Users\kgundavaram\OneDrive - Microsoft\Desktop\insurance"

# Add GitHub as remote
git remote add origin https://github.com/YOUR_USERNAME/insurance-fabric-accelerator.git

# Push to GitHub
git push -u origin master
```

#### Step 3: Connect Fabric Workspace to GitHub

1. Open **Microsoft Fabric** → Navigate to your workspace
2. Click **Workspace Settings** (gear icon) → **Git integration**
3. Click **Connect**
4. Select **GitHub**
5. Authenticate with GitHub
6. Select your repository: `insurance-fabric-accelerator`
7. Select branch: `master`
8. Select folder: `/notebooks` (or root `/` to sync everything)
9. Click **Connect and sync**

**Result**: All notebooks automatically imported to Fabric! 🎉

---

### Option B: Azure DevOps

#### Step 1: Create Azure DevOps Repository

1. Go to [Azure DevOps](https://dev.azure.com)
2. Navigate to your organization
3. Create new project (or use existing)
4. Go to **Repos** → **Initialize** repository
5. Copy the Git URL (e.g., `https://dev.azure.com/YOUR_ORG/YOUR_PROJECT/_git/insurance-accelerator`)

#### Step 2: Push to Azure DevOps

```powershell
cd "c:\Users\kgundavaram\OneDrive - Microsoft\Desktop\insurance"

# Add Azure DevOps as remote
git remote add origin https://dev.azure.com/YOUR_ORG/YOUR_PROJECT/_git/insurance-accelerator

# Push to Azure DevOps
git push -u origin master
```

#### Step 3: Connect Fabric Workspace to Azure DevOps

1. Open **Microsoft Fabric** → Navigate to your workspace
2. Click **Workspace Settings** → **Git integration**
3. Click **Connect**
4. Select **Azure DevOps**
5. Select your organization, project, and repository
6. Select branch: `master`
7. Select folder: `/notebooks`
8. Click **Connect and sync**

**Result**: All notebooks synced to Fabric!

---

## 📋 Fabric Git Integration Settings

### Recommended Configuration

| Setting | Value | Why |
|---------|-------|-----|
| **Branch** | `master` or `main` | Production-ready code |
| **Folder** | `/notebooks` | Only sync notebooks (cleaner) |
| **Auto-sync** | ✅ Enabled | Automatic updates on git push |
| **Commit on save** | ⚠️ User choice | Auto-commit notebook changes |

### Sync Behavior

When you **push to Git**:
- ✅ Fabric workspace auto-updates with new notebooks
- ✅ Existing notebooks are updated
- ✅ Deleted notebooks are removed from workspace

When you **edit in Fabric**:
- ✅ Changes can be committed back to Git
- ✅ Creates new commit in your repository
- ✅ Full version history maintained

---

## 🔄 Daily Workflow After Setup

### Working Locally (VS Code)

```powershell
# 1. Make changes to notebooks locally
# 2. Commit changes
git add .
git commit -m "Updated data quality rules"

# 3. Push to remote
git push origin master

# 4. Fabric workspace auto-syncs (within 1-2 minutes)
```

### Working in Fabric

1. Edit notebook in Fabric workspace
2. Click **Source control** icon in notebook
3. Click **Commit** → Enter commit message
4. Changes pushed to Git automatically
5. Pull changes locally: `git pull origin master`

---

## 🔐 Authentication Options

### GitHub Personal Access Token (PAT)

If prompted for credentials:

1. Go to GitHub → **Settings** → **Developer settings** → **Personal access tokens**
2. Generate new token (classic)
3. Scopes: `repo` (full control)
4. Copy token
5. Use as password when Git prompts

### Azure DevOps PAT

1. Go to Azure DevOps → **User settings** → **Personal access tokens**
2. New token → Scopes: `Code (Read & Write)`
3. Copy token
4. Use as password when Git prompts

### SSH Keys (Advanced)

```powershell
# Generate SSH key
ssh-keygen -t ed25519 -C "your_email@example.com"

# Add to GitHub/Azure DevOps
cat ~/.ssh/id_ed25519.pub  # Copy this to GitHub/Azure DevOps settings

# Update remote to use SSH
git remote set-url origin git@github.com:YOUR_USERNAME/insurance-accelerator.git
```

---

## 📁 What Gets Synced?

### ✅ Synced to Fabric

- All `.ipynb` files (notebooks)
- Notebook metadata and outputs
- Lakehouse attachments (referenced, not files)

### ❌ NOT Synced to Fabric

- Python `.py` files (stay in Git only)
- Documentation `.md` files
- Data files (`.csv`, `.parquet`)
- Configuration files

**Note**: This is intentional - Fabric only syncs notebooks, keeping code separate from documentation.

---

## 🎯 Sync Verification

After connecting Fabric to Git, verify sync:

1. **In Fabric Workspace**:
   - You should see all 15 notebooks appear
   - Each notebook shows Git branch icon
   - Source control panel shows "Synced"

2. **Test Round-Trip**:
   ```powershell
   # Make a small change locally
   # Push to Git
   git push origin master
   
   # Check Fabric workspace (wait 1-2 min)
   # Notebook should show updated content
   ```

3. **Check Sync Status**:
   - Workspace Settings → Git integration → View sync status
   - Should show: "Last synced: [timestamp]"

---

## 🛠️ Troubleshooting

### Issue: "Authentication failed"
**Solution**: Generate PAT (Personal Access Token) and use as password

### Issue: "Sync failed - conflict detected"
**Solution**: 
```powershell
git pull origin master --rebase
git push origin master --force
```

### Issue: "Notebooks not appearing in Fabric"
**Solution**: 
1. Check folder path in Git integration settings
2. Ensure notebooks are in correct folder (`/notebooks`)
3. Click "Sync now" in Fabric workspace settings

### Issue: "Cannot commit from Fabric"
**Solution**: Check workspace permissions - you need Contributor or Admin role

---

## 🌳 Branch Strategy (Optional)

For production use, consider branching:

```powershell
# Create development branch
git checkout -b dev
git push -u origin dev

# Create feature branch
git checkout -b feature/new-notebook
# ... make changes ...
git push -u origin feature/new-notebook

# Merge to dev
git checkout dev
git merge feature/new-notebook
git push origin dev

# Merge to master (production)
git checkout master
git merge dev
git push origin master
```

**Fabric Setup**: Connect separate workspaces to different branches:
- `master` → Production workspace
- `dev` → Development workspace
- `feature/*` → Individual developer workspaces

---

## 📊 Current Repository Structure

```
insurance-fabric-accelerator/
├── .git/                           ← Git repository
├── .gitignore                      ← Git ignore rules
├── notebooks/                      ← 15 notebooks (synced to Fabric)
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
├── architecture/                   ← Architecture docs (Git only)
├── src/                           ← Python source (Git only)
├── metadata/                      ← Metadata schemas (Git only)
├── README.md                      ← Project overview
├── DEPLOY_TO_FABRIC.md           ← Deployment guide
└── GIT_FABRIC_SYNC.md            ← This file
```

---

## ✅ Quick Start Checklist

- [x] ✅ Local Git repository initialized
- [x] ✅ Initial commit created (31 files)
- [ ] Create remote repository (GitHub/Azure DevOps)
- [ ] Push local commits to remote
- [ ] Connect Fabric workspace to Git
- [ ] Verify notebooks synced to Fabric
- [ ] Create lakehouses in Fabric
- [ ] Attach lakehouses to notebooks
- [ ] Run `01_demo_data_generator.ipynb`

---

## 🎉 Ready to Sync!

Your Git repository is ready. Pick Option A (GitHub) or Option B (Azure DevOps) above and follow the steps.

**Estimated time**: 10-15 minutes

**Questions?** See [DEPLOY_TO_FABRIC.md](DEPLOY_TO_FABRIC.md) for full deployment guide.

---

**Last Updated**: April 9, 2026  
**Commit**: `e7cb95c`  
**Status**: ✅ Ready for remote sync
