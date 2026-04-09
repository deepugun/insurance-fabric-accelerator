# Quick GitHub Setup - Insurance Fabric Accelerator

## ⚡ Quick Start (After Authentication)

Once you've completed `gh auth login --web` with code **F118-7793**, run this automated script:

```powershell
.\push_to_github.ps1
```

This script will:
- ✅ Create GitHub repository `insurance-fabric-accelerator`
- ✅ Push all 31 files (15 notebooks + docs + source)
- ✅ Display repository URL
- ✅ Show next steps for Fabric Git integration

---

## 📋 Or Follow These Manual Steps

### 1. Authenticate (In Progress)
```powershell
# You should have already run this and received code: F118-7793
gh auth login --web

# After browser authentication, verify:
gh auth status
```

### 2. Create Repository & Push
```powershell
# Create repository
gh repo create insurance-fabric-accelerator --public --source=. --description "Complete insurance operations platform built on Microsoft Fabric with AI, real-time analytics, and compliance features"

# Push code
git push -u origin master
```

### 3. Verify Push
```powershell
# Check repository
gh repo view --web
```

---

## 🔗 Connect Fabric Workspace to Git

After pushing to GitHub:

1. **Open Fabric workspace** in browser
2. **Go to Settings** → Git integration
3. **Click Connect**
4. **Select GitHub**
5. **Configure:**
   - Repository: `insurance-fabric-accelerator`
   - Branch: `master`
   - Folder: `/notebooks`
6. **Click "Connect and sync"**

Result: All 15 notebooks auto-import to Fabric! 🎉

---

## 📦 What Gets Pushed

- **15 Notebooks**: Complete insurance platform
- **Documentation**: README, ARCHITECTURE, deployment guides
- **Source Code**: Python utilities, AI engines, transformations
- **Metadata**: Schema scripts, seed data
- **Total**: 31 files, 17,839 lines of code

---

## ⚠️ Troubleshooting

**Authentication Failed?**
```powershell
# Try with browser authentication
gh auth login --web --hostname github.com
```

**Repository Already Exists?**
```powershell
# Add remote manually (replace YOUR_USERNAME)
git remote add origin https://github.com/YOUR_USERNAME/insurance-fabric-accelerator.git
git push -u origin master
```

**Push Rejected?**
```powershell
# Force push (only for new repo)
git push -u origin master --force
```

---

## 🚀 After GitHub Push

1. ✅ **View repository**: `gh repo view --web`
2. 🔄 **Connect Fabric workspace** to Git (see above)
3. 🏗️ **Create lakehouses**: bronze, silver, gold, metadata
4. 📊 **Run notebook 01**: Generate sample data
5. ⚙️ **Run notebook 30**: Execute medallion pipeline
6. 📈 **View dashboard**: Open notebook 90 for metrics

---

**Status**: Authentication code `F118-7793` is waiting for your approval in the browser.

**Next**: Complete browser authentication → Run `.\push_to_github.ps1`
