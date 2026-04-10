terraform {
  required_version = ">= 1.6"
  
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    azapi = {
      source  = "Azure/azapi"
      version = "~> 1.0"
    }
  }
  
  # Remote state storage (recommended for production)
  backend "azurerm" {
    resource_group_name  = "insurance-tfstate-rg"
    storage_account_name = "insurancetfstate"
    container_name       = "tfstate"
    key                  = "fabric-prod.terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

provider "azapi" {
  # Uses Azure CLI or Managed Identity authentication
}

# Variables
variable "environment" {
  description = "Environment name (dev, test, prod)"
  type        = string
  default     = "prod"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus2"
}

variable "capacity_sku" {
  description = "Fabric capacity SKU"
  type        = string
  default     = "F64"
}

variable "tags" {
  description = "Tags for all resources"
  type        = map(string)
  default     = {
    Project     = "Insurance Accelerator"
    Environment = "production"
    ManagedBy   = "Terraform"
    CostCenter  = "DataPlatform"
  }
}

# Resource Group
resource "azurerm_resource_group" "fabric_rg" {
  name     = "insurance-fabric-prod-rg"
  location = var.location
  tags     = var.tags
}

# Fabric Capacity
resource "azapi_resource" "fabric_capacity" {
  type      = "Microsoft.Fabric/capacities@2023-11-01"
  name      = "insurance-fabric-prod"
  location  = var.location
  parent_id = azurerm_resource_group.fabric_rg.id

  body = jsonencode({
    sku = {
      name = var.capacity_sku
      tier = "Fabric"
    }
    properties = {
      administration = {
        members = [
          "prod-admin@company.com",
          "platform-team@company.com"
        ]
      }
    }
  })

  tags = var.tags
}

# Production Workspace
resource "azapi_resource" "workspace_insurance_prod" {
  type      = "Microsoft.Fabric/workspaces@2023-11-01"
  name      = "insurance-prod"
  parent_id = azapi_resource.fabric_capacity.id

  body = jsonencode({
    properties = {
      displayName = "insurance-prod"
      description = "Production environment for Insurance Accelerator"
      capacityId  = azapi_resource.fabric_capacity.id
    }
  })

  tags = var.tags
}

# RBAC - Admin Role
resource "azapi_resource" "workspace_rbac_admin" {
  type      = "Microsoft.Fabric/workspaces/roleAssignments@2023-11-01"
  name      = "admin-role"
  parent_id = azapi_resource.workspace_insurance_prod.id

  body = jsonencode({
    properties = {
      principal = "prod-admins@company.com"
      role      = "Admin"
    }
  })
}

# RBAC - Contributor Role
resource "azapi_resource" "workspace_rbac_contributor" {
  type      = "Microsoft.Fabric/workspaces/roleAssignments@2023-11-01"
  name      = "contributor-role"
  parent_id = azapi_resource.workspace_insurance_prod.id

  body = jsonencode({
    properties = {
      principal = "data-engineers@company.com"
      role      = "Contributor"
    }
  })
}

# RBAC - Viewer Role
resource "azapi_resource" "workspace_rbac_viewer" {
  type      = "Microsoft.Fabric/workspaces/roleAssignments@2023-11-01"
  name      = "viewer-role"
  parent_id = azapi_resource.workspace_insurance_prod.id

  body = jsonencode({
    properties = {
      principal = "business-users@company.com"
      role      = "Viewer"
    }
  })
}

# Lakehouse - Bronze
resource "azapi_resource" "lakehouse_bronze" {
  type      = "Microsoft.Fabric/workspaces/lakehouses@2023-11-01"
  name      = "insurance_bronze"
  parent_id = azapi_resource.workspace_insurance_prod.id

  body = jsonencode({
    properties = {
      displayName = "insurance_bronze"
      description = "Raw data - Production"
    }
  })
}

# Lakehouse - Silver
resource "azapi_resource" "lakehouse_silver" {
  type      = "Microsoft.Fabric/workspaces/lakehouses@2023-11-01"
  name      = "insurance_silver"
  parent_id = azapi_resource.workspace_insurance_prod.id

  body = jsonencode({
    properties = {
      displayName = "insurance_silver"
      description = "Cleansed data - Production"
    }
  })
}

# Lakehouse - Gold
resource "azapi_resource" "lakehouse_gold" {
  type      = "Microsoft.Fabric/workspaces/lakehouses@2023-11-01"
  name      = "insurance_gold"
  parent_id = azapi_resource.workspace_insurance_prod.id

  body = jsonencode({
    properties = {
      displayName = "insurance_gold"
      description = "Business-ready data - Production"
    }
  })
}

# Lakehouse - Metadata
resource "azapi_resource" "lakehouse_metadata" {
  type      = "Microsoft.Fabric/workspaces/lakehouses@2023-11-01"
  name      = "insurance_metadata"
  parent_id = azapi_resource.workspace_insurance_prod.id

  body = jsonencode({
    properties = {
      displayName = "insurance_metadata"
      description = "Operational metadata - Production"
    }
  })
}

# Warehouse - Analytics
resource "azapi_resource" "warehouse_analytics" {
  type      = "Microsoft.Fabric/workspaces/warehouses@2023-11-01"
  name      = "insurance_analytics"
  parent_id = azapi_resource.workspace_insurance_prod.id

  body = jsonencode({
    properties = {
      displayName = "insurance_analytics"
      description = "SQL analytics - Production"
    }
  })
}

# KQL Database - Real-time
resource "azapi_resource" "kql_realtime" {
  type      = "Microsoft.Fabric/workspaces/kqlDatabases@2023-11-01"
  name      = "insurance_realtime"
  parent_id = azapi_resource.workspace_insurance_prod.id

  body = jsonencode({
    properties = {
      displayName = "insurance_realtime"
      description = "Real-time analytics - Production"
    }
  })
}

# Outputs
output "capacity_id" {
  value = azapi_resource.fabric_capacity.id
}

output "workspace_id" {
  value = azapi_resource.workspace_insurance_prod.id
}

output "lakehouse_bronze_id" {
  value = azapi_resource.lakehouse_bronze.id
}

output "lakehouse_silver_id" {
  value = azapi_resource.lakehouse_silver.id
}

output "lakehouse_gold_id" {
  value = azapi_resource.lakehouse_gold.id
}

output "warehouse_id" {
  value = azapi_resource.warehouse_analytics.id
}
