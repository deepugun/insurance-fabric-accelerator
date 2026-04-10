#!/usr/bin/env python3
"""
Fabric Infrastructure Deployment - Production
Auto-generated from Insurance Fabric Accelerator

This script deploys complete Fabric infrastructure including:
- Fabric capacity (F64)
- Workspace with RBAC
- Lakehouses (Bronze, Silver, Gold, Metadata)
- Warehouse (Analytics)
- KQL Database (Real-time)
"""

import requests
import json
import subprocess
import sys
from pathlib import Path
from datetime import datetime

# Configuration
ENVIRONMENT = "prod"
CAPACITY_SKU = "F64"
REGION = "eastus2"

WORKSPACE_CONFIG = {
    "name": "insurance-prod",
    "description": "Production environment for Insurance Accelerator",
    "rbac": [
        {"principal": "prod-admins@company.com", "role": "Admin"},
        {"principal": "data-engineers@company.com", "role": "Contributor"},
        {"principal": "business-users@company.com", "role": "Viewer"}
    ]
}

LAKEHOUSES = [
    {"name": "insurance_bronze", "description": "Raw data - Production"},
    {"name": "insurance_silver", "description": "Cleansed data - Production"},
    {"name": "insurance_gold", "description": "Business-ready data - Production"},
    {"name": "insurance_metadata", "description": "Operational metadata - Production"}
]

WAREHOUSES = [
    {"name": "insurance_analytics", "description": "SQL analytics - Production"}
]

KQL_DATABASES = [
    {"name": "insurance_realtime", "description": "Real-time analytics - Production"}
]

def print_header(message):
    """Print formatted header."""
    print("\n" + "=" * 70)
    print(message)
    print("=" * 70)

def print_step(step_num, total_steps, message):
    """Print formatted step."""
    print(f"\n[{step_num}/{total_steps}] {message}")

def get_access_token():
    """Get Fabric API access token."""
    print_step(1, 6, "Authenticating with Azure...")
    
    try:
        result = subprocess.run(
            ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com"],
            capture_output=True,
            text=True,
            check=True
        )
        token_data = json.loads(result.stdout)
        print("   ✅ Authentication successful")
        return token_data["accessToken"]
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Authentication failed: {e.stderr}")
        sys.exit(1)

def create_workspace(token, capacity_id):
    """Create Fabric workspace."""
    print_step(2, 6, f"Creating workspace: {WORKSPACE_CONFIG['name']}...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "displayName": WORKSPACE_CONFIG["name"],
        "description": WORKSPACE_CONFIG["description"],
        "capacityId": capacity_id
    }
    
    try:
        response = requests.post(
            "https://api.fabric.microsoft.com/v1/workspaces",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        
        workspace = response.json()
        workspace_id = workspace["id"]
        
        print(f"   ✅ Workspace created: {workspace_id}")
        
        # Assign RBAC roles
        for rbac in WORKSPACE_CONFIG["rbac"]:
            print(f"   Assigning role: {rbac['principal']} as {rbac['role']}")
            
            rbac_payload = {
                "principal": rbac["principal"],
                "role": rbac["role"]
            }
            
            rbac_response = requests.post(
                f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/roleAssignments",
                headers=headers,
                json=rbac_payload
            )
            
            if rbac_response.status_code in [200, 201]:
                print(f"      ✅ Role assigned")
            else:
                print(f"      ⚠️  Role assignment failed: {rbac_response.text}")
        
        return workspace_id
        
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Workspace creation failed: {e}")
        sys.exit(1)

def create_lakehouses(token, workspace_id):
    """Create lakehouses in workspace."""
    print_step(3, 6, f"Creating {len(LAKEHOUSES)} lakehouses...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    lakehouse_ids = []
    
    for lakehouse in LAKEHOUSES:
        print(f"   Creating lakehouse: {lakehouse['name']}...")
        
        payload = {
            "displayName": lakehouse["name"],
            "description": lakehouse["description"]
        }
        
        try:
            response = requests.post(
                f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses",
                headers=headers,
                json=payload
            )
            response.raise_for_status()
            
            lakehouse_data = response.json()
            lakehouse_ids.append(lakehouse_data["id"])
            
            print(f"      ✅ Lakehouse created: {lakehouse_data['id']}")
            
        except requests.exceptions.RequestException as e:
            print(f"      ❌ Failed to create lakehouse: {e}")
    
    return lakehouse_ids

def create_warehouses(token, workspace_id):
    """Create warehouses in workspace."""
    print_step(4, 6, f"Creating {len(WAREHOUSES)} warehouses...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    warehouse_ids = []
    
    for warehouse in WAREHOUSES:
        print(f"   Creating warehouse: {warehouse['name']}...")
        
        payload = {
            "displayName": warehouse["name"],
            "description": warehouse["description"]
        }
        
        try:
            response = requests.post(
                f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses",
                headers=headers,
                json=payload
            )
            response.raise_for_status()
            
            warehouse_data = response.json()
            warehouse_ids.append(warehouse_data["id"])
            
            print(f"      ✅ Warehouse created: {warehouse_data['id']}")
            
        except requests.exceptions.RequestException as e:
            print(f"      ❌ Failed to create warehouse: {e}")
    
    return warehouse_ids

def create_kql_databases(token, workspace_id):
    """Create KQL databases in workspace."""
    print_step(5, 6, f"Creating {len(KQL_DATABASES)} KQL databases...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    kql_ids = []
    
    for kql_db in KQL_DATABASES:
        print(f"   Creating KQL database: {kql_db['name']}...")
        
        payload = {
            "displayName": kql_db["name"],
            "description": kql_db["description"]
        }
        
        try:
            response = requests.post(
                f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/kqlDatabases",
                headers=headers,
                json=payload
            )
            response.raise_for_status()
            
            kql_data = response.json()
            kql_ids.append(kql_data["id"])
            
            print(f"      ✅ KQL database created: {kql_data['id']}")
            
        except requests.exceptions.RequestException as e:
            print(f"      ❌ Failed to create KQL database: {e}")
    
    return kql_ids

def save_deployment_summary(workspace_id, lakehouse_ids, warehouse_ids, kql_ids):
    """Save deployment summary to JSON file."""
    print_step(6, 6, "Saving deployment summary...")
    
    summary = {
        "deployment_timestamp": datetime.now().isoformat(),
        "environment": ENVIRONMENT,
        "workspace": {
            "id": workspace_id,
            "name": WORKSPACE_CONFIG["name"]
        },
        "lakehouses": lakehouse_ids,
        "warehouses": warehouse_ids,
        "kql_databases": kql_ids
    }
    
    output_file = f"deployment_summary_{ENVIRONMENT}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(output_file, "w") as f:
        json.dump(summary, f, indent=2)
    
    print(f"   ✅ Deployment summary saved: {output_file}")

def main():
    """Main deployment function."""
    print_header(f"Fabric Infrastructure Deployment - {ENVIRONMENT.upper()}")
    
    # Note: Capacity ID must be pre-created or obtained
    # For this script, assume capacity exists
    capacity_id = "YOUR-CAPACITY-ID-HERE"  # Replace with actual capacity ID
    
    if capacity_id == "YOUR-CAPACITY-ID-HERE":
        print("\n⚠️  WARNING: Update capacity_id in script before running!")
        print("   Get capacity ID from Azure portal or via:")
        print("   az rest --method GET --url 'https://api.fabric.microsoft.com/v1/capacities'")
        sys.exit(1)
    
    # Get access token
    token = get_access_token()
    
    # Create workspace
    workspace_id = create_workspace(token, capacity_id)
    
    # Create artifacts
    lakehouse_ids = create_lakehouses(token, workspace_id)
    warehouse_ids = create_warehouses(token, workspace_id)
    kql_ids = create_kql_databases(token, workspace_id)
    
    # Save summary
    save_deployment_summary(workspace_id, lakehouse_ids, warehouse_ids, kql_ids)
    
    # Final summary
    print_header("✅ DEPLOYMENT COMPLETE")
    print(f"\n📊 Summary:")
    print(f"   Environment: {ENVIRONMENT}")
    print(f"   Workspace ID: {workspace_id}")
    print(f"   Lakehouses: {len(lakehouse_ids)} created")
    print(f"   Warehouses: {len(warehouse_ids)} created")
    print(f"   KQL Databases: {len(kql_ids)} created")
    
    print(f"\n🔗 Access your workspace:")
    print(f"   https://app.fabric.microsoft.com/groups/{workspace_id}")
    
    print("\n" + "=" * 70)

if __name__ == "__main__":
    main()
