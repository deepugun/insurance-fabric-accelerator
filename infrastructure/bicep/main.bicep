// Insurance Fabric Infrastructure - Production
targetScope = 'subscription'

@description('Environment name')
param environment string = 'prod'

@description('Azure region for resources')
param location string = 'eastus2'

@description('Resource group name')
param resourceGroupName string = 'insurance-fabric-prod-rg'

@description('Fabric capacity SKU')
param capacitySku string = 'F64'

@description('Tags for all resources')
param tags object = {
  Project: 'Insurance Accelerator'
  Environment: 'production'
  ManagedBy: 'Bicep'
  CostCenter: 'DataPlatform'
}

// Create resource group
resource rg 'Microsoft.Resources/resourceGroups@2021-04-01' = {
  name: resourceGroupName
  location: location
  tags: tags
}

// Deploy capacity module
module capacity './modules/capacity.bicep' = {
  scope: rg
  name: 'fabric-capacity-deployment'
  params: {
    capacityName: 'insurance-fabric-${environment}'
    location: location
    capacitySku: capacitySku
    capacityAdmins: [
      'prod-admin@company.com'
      'platform-team@company.com'
    ]
    tags: tags
  }
}

// Deploy workspace module
module workspace './modules/workspace.bicep' = {
  scope: rg
  name: 'workspace-insurance-prod-deployment'
  params: {
    workspaceName: 'insurance-prod'
    workspaceDescription: 'Production environment for Insurance Accelerator'
    capacityId: capacity.outputs.capacityId
    rbacAssignments: [
      {
        principal: 'prod-admins@company.com'
        role: 'Admin'
      }
      {
        principal: 'data-engineers@company.com'
        role: 'Contributor'
      }
      {
        principal: 'business-users@company.com'
        role: 'Viewer'
      }
    ]
    tags: tags
  }
}

// Deploy lakehouses
module lakehouses './modules/lakehouses.bicep' = {
  scope: rg
  name: 'lakehouses-deployment'
  params: {
    workspaceId: workspace.outputs.workspaceId
    lakehouses: [
      {
        name: 'insurance_bronze'
        description: 'Raw data - Production'
      }
      {
        name: 'insurance_silver'
        description: 'Cleansed data - Production'
      }
      {
        name: 'insurance_gold'
        description: 'Business-ready data - Production'
      }
      {
        name: 'insurance_metadata'
        description: 'Operational metadata - Production'
      }
    ]
  }
  dependsOn: [
    workspace
  ]
}

// Deploy warehouse
module warehouse './modules/warehouse.bicep' = {
  scope: rg
  name: 'warehouse-deployment'
  params: {
    workspaceId: workspace.outputs.workspaceId
    warehouseName: 'insurance_analytics'
    warehouseDescription: 'SQL analytics - Production'
  }
  dependsOn: [
    workspace
  ]
}

// Deploy KQL database
module kqlDatabase './modules/kql-database.bicep' = {
  scope: rg
  name: 'kql-database-deployment'
  params: {
    workspaceId: workspace.outputs.workspaceId
    kqlDatabaseName: 'insurance_realtime'
    kqlDatabaseDescription: 'Real-time analytics - Production'
  }
  dependsOn: [
    workspace
  ]
}

// Outputs
output capacityId string = capacity.outputs.capacityId
output workspaceId string = workspace.outputs.workspaceId
output lakehouseIds array = lakehouses.outputs.lakehouseIds
output warehouseId string = warehouse.outputs.warehouseId
output kqlDatabaseId string = kqlDatabase.outputs.kqlDatabaseId
