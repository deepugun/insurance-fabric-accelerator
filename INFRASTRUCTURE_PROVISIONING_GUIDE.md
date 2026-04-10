# 🏗️ Infrastructure Provisioning — Quick Start

**Enterprise-grade Infrastructure-as-Code for Microsoft Fabric**

## Overview

Notebook 65 generates complete IaC templates for deploying Microsoft Fabric infrastructure across Dev, Test, and Production environments using:

- **Terraform** (recommended for enterprise)
- **Bicep** (Azure-native)
- **Fabric CLI** (quick deployment)

All templates are auto-generated and production-ready!

---

## 🚀 Quick Start (3 Steps)

### Step 1: Upload Notebook to Fabric

1. Open your Fabric workspace
2. Click **New** → **Import notebook**
3. Select `notebooks/65_infrastructure_provisioning_iac.ipynb`

### Step 2: Run the Notebook

Execute **Section 5** to generate all IaC files:
- Terraform templates
- Bicep modules
- CLI deployment scripts

Files are generated in `/tmp/infrastructure/`

### Step 3: Deploy Your Environment

Choose your deployment method and execute!

---

## 📦 What Gets Generated

### Terraform Files
```
terraform/
├── dev_main.tf       # Development environment
├── test_main.tf      # Testing environment  
└── prod_main.tf      # Production environment
```

### Bicep Files
```
bicep/
├── dev_main.bicep           # Dev main template
├── test_main.bicep          # Test main template
├── prod_main.bicep          # Prod main template
├── capacity.bicep           # Fabric capacity module
├── workspace_*.bicep        # Workspace modules
├── dev_parameters.json      # Dev parameters
├── test_parameters.json     # Test parameters
└── prod_parameters.json     # Prod parameters
```

### CLI Scripts
```
cli/
├── deploy_dev.sh      # Bash deployment script (Dev)
├── deploy_dev.py      # Python deployment script (Dev)
├── deploy_test.sh     # Bash deployment script (Test)
├── deploy_test.py     # Python deployment script (Test)
├── deploy_prod.sh     # Bash deployment script (Prod)
└── deploy_prod.py     # Python deployment script (Prod)
```

---

## 🛠️ Deployment Methods

### Method 1: Terraform (Recommended)

**Best for:**
- Enterprise deployments
- Team collaboration
- State management
- Drift detection

**Prerequisites:**
```bash
# Install Terraform
brew install terraform  # macOS
choco install terraform # Windows

# Install Azure CLI
brew install azure-cli  # macOS
choco install azure-cli # Windows

# Authenticate
az login
```

**Deploy Development:**
```bash
cd /tmp/infrastructure/terraform

# Initialize
terraform init

# Review plan
terraform plan -out=tfplan

# Apply
terraform apply tfplan
```

**What it creates:**
- ✅ Fabric capacity (F2/F4/F64 based on environment)
- ✅ Workspace with RBAC assignments
- ✅ 4 Lakehouses (bronze, silver, gold, metadata)
- ✅ 1 Warehouse (analytics)
- ✅ 17 production notebooks
- ✅ Python environment with packages
- ✅ KQL database (real-time analytics)
- ✅ Git integration (automatic sync)

**Deploy Production:**
```bash
# Use separate state file for prod
terraform init -backend-config="key=fabric-prod.terraform.tfstate"

terraform plan -out=prod.tfplan
terraform apply prod.tfplan
```

**Destroy (when needed):**
```bash
terraform destroy
```

---

### Method 2: Bicep (Azure-Native)

**Best for:**
- Azure-centric organizations
- ARM template users
- Native Azure integration

**Prerequisites:**
```bash
# Azure CLI with Bicep
az bicep install
az bicep version

# Authenticate
az login
```

**Deploy Development:**
```bash
cd /tmp/infrastructure/bicep

# Validate template
az deployment sub validate \
  --location eastus \
  --template-file dev_main.bicep \
  --parameters dev_parameters.json

# Deploy
az deployment sub create \
  --location eastus \
  --template-file dev_main.bicep \
  --parameters dev_parameters.json
```

**Deploy Production:**
```bash
az deployment sub create \
  --location eastus2 \
  --template-file prod_main.bicep \
  --parameters prod_parameters.json
```

---

### Method 3: Fabric CLI (Quick Deploy)

**Best for:**
- Quick prototyping
- One-off deployments
- Simple environments

**Prerequisites:**
```bash
# Azure CLI
az login

# For bash scripts
apt-get install jq  # Linux
brew install jq     # macOS

# For Python scripts
pip install requests
```

**Deploy with Bash:**
```bash
cd /tmp/infrastructure/cli

# Make executable
chmod +x deploy_dev.sh

# Run
./deploy_dev.sh
```

**Deploy with Python:**
```bash
cd /tmp/infrastructure/cli

python deploy_dev.py
```

---

## ⚙️ Configuration

### Environment Settings

The configuration is defined in **Section 1** of the notebook. Customize as needed:

```python
infrastructure_config_schema = {
    "environments": {
        "dev": {
            "capacity_sku": "F2",        # Change SKU
            "region": "eastus",          # Change region
            "workspaces": [
                {
                    "name": "insurance-dev",  # Workspace name
                    "rbac": [
                        {"principal": "dev-team@company.com", "role": "Admin"}
                    ],
                    "artifacts": {
                        "lakehouses": [...],  # Configure artifacts
                        "warehouses": [...],
                        "notebooks": [...]
                    }
                }
            ]
        }
    }
}
```

### Capacity SKUs by Environment

| Environment | SKU | vCores | RAM | Recommended For |
|-------------|-----|--------|-----|-----------------|
| Dev | F2 | 2 | 4 GB | Development, testing |
| Test | F4 | 4 | 8 GB | QA, integration testing |
| Prod | F64 | 64 | 128 GB | Production workloads |

### RBAC Roles

| Role | Permissions |
|------|-------------|
| Admin | Full control (create/delete artifacts, manage RBAC) |
| Member | Create and manage artifacts |
| Contributor | Edit artifacts, cannot create new |
| Viewer | Read-only access |

---

## 🔄 CI/CD Integration

### GitHub Actions

Create `.github/workflows/deploy-dev.yml`:

```yaml
name: Deploy to Development

on:
  push:
    branches: [develop]

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout
        uses: actions/checkout@v3
      
      - name: Setup Terraform
        uses: hashicorp/setup-terraform@v2
        with:
          terraform_version: 1.6.0
      
      - name: Azure Login
        uses: azure/login@v1
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}
      
      - name: Terraform Init
        run: terraform init
        working-directory: ./infrastructure/terraform
      
      - name: Terraform Plan
        run: terraform plan -out=tfplan
        working-directory: ./infrastructure/terraform
      
      - name: Terraform Apply
        run: terraform apply -auto-approve tfplan
        working-directory: ./infrastructure/terraform
```

### Azure DevOps Pipeline

Create `azure-pipelines.yml`:

```yaml
trigger:
  branches:
    include:
      - develop
      - main

pool:
  vmImage: ubuntu-latest

stages:
  - stage: Deploy
    jobs:
      - job: TerraformDeploy
        steps:
          - task: TerraformInstaller@0
            inputs:
              terraformVersion: '1.6.0'
          
          - task: AzureCLI@2
            inputs:
              azureSubscription: 'Azure-Subscription'
              scriptType: 'bash'
              scriptLocation: 'inlineScript'
              inlineScript: |
                cd infrastructure/terraform
                terraform init
                terraform plan -out=tfplan
                terraform apply -auto-approve tfplan
```

---

## 📊 State Management (Terraform)

### Local State (Default)

State files stored locally in `terraform.tfstate`. **Not recommended for teams.**

### Remote State (Recommended)

Store state in Azure Storage for team collaboration:

**1. Create storage account:**
```bash
# Create resource group
az group create --name insurance-tfstate-rg --location eastus

# Create storage account
az storage account create \
  --name insurancetfstate \
  --resource-group insurance-tfstate-rg \
  --location eastus \
  --sku Standard_LRS

# Create container
az storage container create \
  --name tfstate \
  --account-name insurancetfstate
```

**2. Configure backend in Terraform:**

Add to `main.tf`:
```hcl
terraform {
  backend "azurerm" {
    resource_group_name  = "insurance-tfstate-rg"
    storage_account_name = "insurancetfstate"
    container_name       = "tfstate"
    key                  = "fabric-dev.terraform.tfstate"
  }
}
```

**3. Initialize with backend:**
```bash
terraform init -reconfigure
```

---

## 🔍 Drift Detection

Detect configuration drift between IaC and actual Fabric state:

**Manual drift check:**
```bash
terraform plan -detailed-exitcode

# Exit codes:
# 0 = No changes
# 1 = Error
# 2 = Drift detected (changes required)
```

**Automated drift detection (Cron job):**
```bash
#!/bin/bash
# drift-check.sh

terraform plan -detailed-exitcode

if [ $? -eq 2 ]; then
  echo "Drift detected! Sending alert..."
  # Send email/Teams/Slack notification
fi
```

**Schedule daily:**
```bash
crontab -e

# Run every day at 6 AM
0 6 * * * /path/to/drift-check.sh
```

---

## 🚨 Troubleshooting

### Terraform Init Fails

**Error:** `Backend initialization required`

**Fix:**
```bash
terraform init -reconfigure
```

### Bicep Deployment Fails

**Error:** `InvalidTemplateDeployment`

**Fix:**
1. Validate template:
   ```bash
   az deployment sub validate \
     --location eastus \
     --template-file dev_main.bicep \
     --parameters dev_parameters.json
   ```

2. Check error details in output

### CLI Script: Unauthorized

**Error:** `401 Unauthorized`

**Fix:**
1. Re-authenticate:
   ```bash
   az login --use-device-code
   ```

2. Verify token scope:
   ```bash
   az account get-access-token \
     --resource https://api.fabric.microsoft.com
   ```

### Capacity Not Found

**Error:** `Capacity ID not found`

**Fix:**
1. List available capacities:
   ```bash
   az rest --method GET \
     --url "https://api.fabric.microsoft.com/v1/capacities"
   ```

2. Update `capacity_id` in configuration

---

## 📈 Deployment Tracking

All deployments are logged to `metadata.infrastructure_deployments`:

```sql
-- View deployment history
SELECT 
    deployment_id,
    environment,
    method,
    status,
    timestamp,
    deployed_by
FROM metadata.infrastructure_deployments
ORDER BY timestamp DESC

-- Deployments by environment
SELECT 
    environment,
    COUNT(*) as deployment_count,
    MAX(timestamp) as last_deployment
FROM metadata.infrastructure_deployments
GROUP BY environment
```

---

## 🎯 Use Cases

### Use Case 1: New Environment Setup

**Scenario:** Setup complete Dev environment from scratch

**Steps:**
1. Run Notebook 65
2. Customize Dev configuration (Section 1)
3. Re-run Section 5 to generate templates
4. Deploy with Terraform:
   ```bash
   cd /tmp/infrastructure/terraform
   terraform init
   terraform apply
   ```

**Result:** Complete Dev environment in 10-15 minutes

### Use Case 2: Production Deployment

**Scenario:** Deploy to Production with approval gates

**Steps:**
1. Generate Prod templates (Notebook 65)
2. Review Terraform plan:
   ```bash
   terraform plan > prod-plan.txt
   ```
3. Send `prod-plan.txt` for approval
4. After approval, apply:
   ```bash
   terraform apply
   ```

### Use Case 3: Disaster Recovery

**Scenario:** Re-create entire Production environment after failure

**Steps:**
1. Ensure Terraform state is in remote storage
2. Deploy from state:
   ```bash
   terraform init
   terraform apply -auto-approve
   ```

**Result:** Production environment restored in 20-30 minutes

---

## 📚 Additional Resources

- [Notebook 65](notebooks/65_infrastructure_provisioning_iac.ipynb) — Full implementation
- [Terraform Fabric Provider](https://registry.terraform.io/providers/Azure/azapi/latest/docs)
- [Azure Bicep Documentation](https://learn.microsoft.com/en-us/azure/azure-resource-manager/bicep/)
- [Fabric REST API Docs](https://learn.microsoft.com/en-us/rest/api/fabric/)
- [Notebook 70](notebooks/70_cicd_deployment_automation.ipynb) — CI/CD integration
- [ARCHITECTURE.md](ARCHITECTURE.md) — Infrastructure architecture

---

## ✅ Summary

**Notebook 65 provides:**

✅ **Terraform** — Enterprise IaC with state management  
✅ **Bicep** — Azure-native declarative deployment  
✅ **Fabric CLI** — Quick scripted deployment  
✅ **Multi-environment** — Dev, Test, Prod configurations  
✅ **Complete artifacts** — Lakehouses, warehouses, notebooks, KQL, mirroring  
✅ **CI/CD ready** — GitHub Actions, Azure DevOps templates  
✅ **Deployment tracking** — Delta table audit logs  
✅ **Drift detection** — Automated configuration drift checks  
✅ **Modular** — Reusable infrastructure components  

**Enterprise-ready infrastructure provisioning!** 🏗️
